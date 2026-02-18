from __future__ import annotations

from datetime import date
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import time

import typer

from src import db
from src.collectors import community, first_party, jobs, news, technographics
from src.discovery import hunt as hunt_pipeline
from src.discovery import pipeline as discovery_pipeline
from src.discovery import watchlist_builder
from src.discovery.config import classify_signal, load_signal_classes
from src.export import csv_exporter
from src.reporting import calibration, icp_playbook, quality
from src.review.import_reviews import import_reviews_for_date, prepare_review_input_for_date
from src.scoring.engine import run_scoring
from src.scoring.rules import load_keyword_lexicon, load_signal_rules, load_source_registry, load_thresholds
from src.settings import Settings, load_settings
from src.sync.google_sheets import sync_outputs
from src.utils import ensure_project_directories, load_csv_rows, normalize_domain, parse_date, write_csv_rows

app = typer.Typer(add_completion=False, no_args_is_help=True)


def _bootstrap(settings: Settings | None = None):
    local_settings = settings or load_settings()
    ensure_project_directories(
        [
            local_settings.project_root,
            local_settings.config_dir,
            local_settings.data_dir,
            local_settings.raw_dir,
            local_settings.out_dir,
        ]
    )
    conn = db.get_connection(local_settings.db_path)
    db.init_db(conn)
    seeded_base = db.seed_accounts(conn, local_settings.seed_accounts_path)
    seeded_watchlist = db.seed_accounts(conn, local_settings.watchlist_accounts_path)
    seeded = seeded_base + seeded_watchlist
    return local_settings, conn, seeded


def _collect_all(conn, settings: Settings) -> dict[str, dict[str, int]]:
    lexicon = load_keyword_lexicon(settings.keyword_lexicon_path)
    source_reliability = load_source_registry(settings.source_registry_path)

    results = {
        "jobs": jobs.collect(conn, settings, lexicon, source_reliability),
        "news": news.collect(conn, settings, lexicon, source_reliability),
        "technographics": technographics.collect(conn, settings, lexicon, source_reliability),
        "community": community.collect(conn, settings, lexicon, source_reliability),
        "first_party": first_party.collect(conn, settings, lexicon, source_reliability),
    }
    return results


def _baseline_score_7d(conn, account_id: str, product: str, run_date: str) -> float | None:
    cur = conn.execute(
        """
        SELECT s.score
        FROM account_scores s
        JOIN score_runs r ON r.run_id = s.run_id
        WHERE s.account_id = ?
          AND s.product = ?
          AND date(r.run_date) <= date(?, '-7 day')
        ORDER BY date(r.run_date) DESC, r.started_at DESC
        LIMIT 1
        """,
        (account_id, product, run_date),
    )
    row = cur.fetchone()
    return None if row is None else float(row["score"])


def _run_scoring(conn, settings: Settings, run_date: date) -> str:
    run_date_str = run_date.isoformat()
    run_id = db.create_score_run(conn, run_date_str)

    rules = load_signal_rules(settings.signal_registry_path)
    thresholds = load_thresholds(settings.thresholds_path)
    source_registry = load_source_registry(settings.source_registry_path)
    signal_classes = load_signal_classes(settings.signal_classes_path)

    try:
        observations = db.fetch_observations_for_scoring(conn, run_date_str)
        result = run_scoring(
            run_id=run_id,
            run_date=run_date,
            observations=[dict(row) for row in observations],
            rules=rules,
            thresholds=thresholds,
            source_reliability_defaults=source_registry,
            delta_lookup=None,
        )

        signals_by_account_product: dict[tuple[str, str], set[str]] = {}
        for component in result.component_scores:
            key = (component.account_id, component.product)
            signals_by_account_product.setdefault(key, set()).add(component.signal_code)

        for score in result.account_scores:
            baseline = _baseline_score_7d(conn, score.account_id, score.product, run_date_str)
            score.delta_7d = round(score.score - baseline, 2) if baseline is not None else 0.0
            has_primary = any(
                classify_signal(signal_code, signal_classes) == "primary"
                for signal_code in signals_by_account_product.get((score.account_id, score.product), set())
            )
            if score.tier in {"medium", "high"} and not has_primary:
                score.tier = "low"

        db.replace_run_scores(conn, run_id, result.component_scores, result.account_scores)
        db.finish_score_run(conn, run_id, status="completed", error_summary=None)
        return run_id
    except Exception as exc:
        db.finish_score_run(conn, run_id, status="failed", error_summary=str(exc)[:1000])
        raise


def _run_exports(conn, settings: Settings, run_date: date, run_id: str) -> dict[str, int | str]:
    paths = csv_exporter.output_paths(settings.out_dir, run_date)

    queue_count = csv_exporter.export_review_queue(conn, run_id, paths["review_queue"])
    score_count = csv_exporter.export_daily_scores(conn, run_id, paths["daily_scores"])

    quality.compute_and_persist_source_metrics(conn, run_date)
    quality_rows = csv_exporter.export_source_quality(conn, run_date.isoformat(), paths["source_quality"])

    readiness_rows = quality.compute_promotion_readiness(conn, run_date)
    readiness_count = csv_exporter.export_promotion_readiness(readiness_rows, paths["promotion_readiness"])

    return {
        "review_queue": queue_count,
        "daily_scores": score_count,
        "source_quality": quality_rows,
        "promotion_readiness": readiness_count,
        "review_queue_path": str(paths["review_queue"]),
        "daily_scores_path": str(paths["daily_scores"]),
        "source_quality_path": str(paths["source_quality"]),
        "promotion_readiness_path": str(paths["promotion_readiness"]),
    }


def _write_icp_coverage_report(conn, settings: Settings, run_id: str, run_date: date) -> dict[str, int | float | str]:
    reference_path = settings.config_dir / "icp_reference_accounts.csv"
    rows, summary = quality.compute_icp_coverage(conn, run_id=run_id, reference_csv_path=reference_path)
    path = settings.out_dir / f"icp_coverage_{run_date.strftime('%Y%m%d')}.csv"
    write_csv_rows(
        path,
        rows,
        fieldnames=[
            "company_name",
            "domain",
            "relationship_stage",
            "zopdev_score",
            "zopdev_tier",
            "zopday_score",
            "zopday_tier",
            "zopnight_score",
            "zopnight_tier",
            "max_score",
            "max_tier",
        ],
    )
    return {
        "path": str(path),
        "total_accounts": int(summary["total_accounts"]),
        "high_or_medium_accounts": int(summary["high_or_medium_accounts"]),
        "coverage_rate": float(summary["coverage_rate"]),
    }


def _run_ingest_cycle(run_date: date) -> dict[str, int | str]:
    settings, conn, seeded = _bootstrap()
    del seeded
    try:
        collect_results = _collect_all(conn, settings)
        collect_inserted = sum(result["inserted"] for result in collect_results.values())
        collect_seen = sum(result["seen"] for result in collect_results.values())
        return {
            "run_date": run_date.isoformat(),
            "observations_seen": collect_seen,
            "observations_inserted": collect_inserted,
        }
    finally:
        conn.close()


def _run_score_cycle(run_date: date) -> dict[str, int | float | str]:
    settings, conn, seeded = _bootstrap()
    del seeded
    try:
        run_id = _run_scoring(conn, settings, run_date)
        export_result = _run_exports(conn, settings, run_date, run_id)
        icp_report = _write_icp_coverage_report(conn, settings, run_id, run_date)
        return {
            "run_id": run_id,
            "daily_scores_rows": int(export_result["daily_scores"]),
            "review_queue_rows": int(export_result["review_queue"]),
            "icp_coverage_rate": float(icp_report["coverage_rate"]),
        }
    finally:
        conn.close()


def _run_discovery_cycle(run_date: date) -> dict[str, int | str]:
    settings, conn, seeded = _bootstrap()
    del seeded
    try:
        ingest_result = discovery_pipeline.ingest_external_events(conn, settings, run_date)
        score_run_id = _run_scoring(conn, settings, run_date)
        scoring_result = discovery_pipeline.score_discovery_candidates(
            conn=conn,
            settings=settings,
            run_date=run_date,
            score_run_id=score_run_id,
            source_events_processed=int(ingest_result["events_processed"]),
            observations_inserted=int(ingest_result["observations_inserted"]),
        )
        report_result = discovery_pipeline.write_discovery_reports(
            conn=conn,
            settings=settings,
            run_date=run_date,
            discovery_run_id=str(scoring_result["discovery_run_id"]),
        )
        return {
            "discovery_run_id": str(scoring_result["discovery_run_id"]),
            "events_processed": int(ingest_result["events_processed"]),
            "candidates": int(scoring_result["total_candidates"]),
            "crm_candidates_rows": int(report_result["crm_candidates_rows"]),
        }
    finally:
        conn.close()


def _run_hunt_cycle(run_date: date, profile_name: str = "light") -> dict[str, int | float | str]:
    settings, conn, seeded = _bootstrap()
    del seeded
    try:
        profile = hunt_pipeline.resolve_profile(profile_name)
        frontier_result = hunt_pipeline.build_frontier(conn, settings, run_date, profile=profile)
        fetch_result = hunt_pipeline.fetch_documents(conn, settings, run_date, profile=profile)
        extract_result = hunt_pipeline.extract_documents(conn, settings, run_date, profile=profile)

        score_run_id = _run_scoring(conn, settings, run_date)
        scoring_result = discovery_pipeline.score_discovery_candidates(
            conn=conn,
            settings=settings,
            run_date=run_date,
            score_run_id=score_run_id,
            source_events_processed=int(frontier_result["events_seen"]),
            observations_inserted=int(extract_result["observations_inserted"]),
            enforce_quality_gates=True,
            min_evidence_quality=0.8,
            min_relevance_score=0.65,
        )
        report_result = discovery_pipeline.write_discovery_reports(
            conn=conn,
            settings=settings,
            run_date=run_date,
            discovery_run_id=str(scoring_result["discovery_run_id"]),
        )
        hunt_reports = hunt_pipeline.write_hunt_reports(conn, settings, run_date)

        return {
            "run_date": run_date.isoformat(),
            "profile": profile.name,
            "events_seen": int(frontier_result["events_seen"]),
            "frontier_queued": int(frontier_result["frontier_queued"]),
            "documents_fetched": int(fetch_result["documents_fetched"]),
            "documents_parsed": int(extract_result["documents_parsed"]),
            "mentions_inserted": int(extract_result["mentions_inserted"]),
            "observations_inserted": int(extract_result["observations_inserted"]),
            "score_run_id": score_run_id,
            "discovery_run_id": str(scoring_result["discovery_run_id"]),
            "total_candidates": int(scoring_result["total_candidates"]),
            "crm_candidates_rows": int(report_result["crm_candidates_rows"]),
            "story_evidence_rows": int(hunt_reports["story_evidence_rows"]),
            "signal_lineage_rows": int(hunt_reports["signal_lineage_rows"]),
        }
    finally:
        conn.close()


@app.command("ingest")
def ingest(all_sources: bool = typer.Option(True, "--all/--no-all", help="Run all collectors")) -> None:
    if not all_sources:
        raise typer.BadParameter("Partial ingest is not supported yet. Use --all.")
    settings, conn, seeded = _bootstrap()
    try:
        results = _collect_all(conn, settings)
        inserted_total = sum(result["inserted"] for result in results.values())
        seen_total = sum(result["seen"] for result in results.values())
        typer.echo(f"seeded_accounts={seeded} observations_seen={seen_total} observations_inserted={inserted_total}")
        for name, result in results.items():
            typer.echo(f"collector={name} seen={result['seen']} inserted={result['inserted']}")
    finally:
        conn.close()


@app.command("score")
def score(date_str: str = typer.Option(None, "--date", help="Scoring date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_id = _run_scoring(conn, settings, run_date)
        summary = db.dump_run_summary(conn, run_id)
        typer.echo(f"run_id={run_id} account_count={summary['account_count']} score_rows={summary['score_rows']}")
    finally:
        conn.close()


@app.command("export")
def export(date_str: str = typer.Option(None, "--date", help="Export date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_id = db.get_latest_run_id_for_date(conn, run_date.isoformat())
        if not run_id:
            raise typer.BadParameter(f"No score run found for date {run_date.isoformat()}")
        result = _run_exports(conn, settings, run_date, run_id)
        typer.echo(
            " ".join(
                [
                    f"review_queue_rows={result['review_queue']}",
                    f"daily_scores_rows={result['daily_scores']}",
                    f"source_quality_rows={result['source_quality']}",
                    f"promotion_readiness_rows={result['promotion_readiness']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("sync-sheet")
def sync_sheet(date_str: str = typer.Option(None, "--date", help="Sync date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del conn, seeded

    result = sync_outputs(settings, run_date)
    typer.echo(
        " ".join(
            [
                f"review_queue_rows={result['review_queue_rows']}",
                f"daily_scores_rows={result['daily_scores_rows']}",
                f"source_quality_rows={result['source_quality_rows']}",
            ]
        )
    )


@app.command("import-reviews")
def import_reviews(date_str: str = typer.Option(None, "--date", help="Import date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        imported = import_reviews_for_date(conn, settings, run_date)
        quality.compute_and_persist_source_metrics(conn, run_date)
        readiness = quality.compute_promotion_readiness(conn, run_date)

        paths = csv_exporter.output_paths(settings.out_dir, run_date)
        csv_exporter.export_source_quality(conn, run_date.isoformat(), paths["source_quality"])
        csv_exporter.export_promotion_readiness(readiness, paths["promotion_readiness"])

        typer.echo(f"imported_reviews={imported}")
    finally:
        conn.close()


@app.command("prepare-review-input")
def prepare_review_input(date_str: str = typer.Option(None, "--date", help="Review date YYYY-MM-DD")) -> None:
    settings = load_settings()
    ensure_project_directories(
        [
            settings.project_root,
            settings.data_dir,
            settings.raw_dir,
            settings.out_dir,
        ]
    )
    run_date = parse_date(date_str, settings.run_timezone)
    prepared = prepare_review_input_for_date(settings, run_date)
    typer.echo(f"prepared_review_rows={prepared}")


@app.command("build-cpg-watchlist")
def build_cpg_watchlist(
    limit: int = typer.Option(1000, "--limit", min=100, help="Maximum watchlist rows to keep"),
    merge_handles: bool = typer.Option(
        True,
        "--merge-handles/--no-merge-handles",
        help="Merge generated domains into account_source_handles.csv",
    ),
) -> None:
    settings = load_settings()
    ensure_project_directories([settings.project_root, settings.config_dir, settings.data_dir])

    result = watchlist_builder.build_cpg_watchlist(
        settings=settings,
        limit=limit,
        merge_handles=merge_handles,
    )

    top_regions = ",".join(
        f"{region}:{count}"
        for region, count in sorted(
            result["selected_per_region"].items(),  # type: ignore[arg-type]
            key=lambda item: item[1],
            reverse=True,
        )
    )
    failed_country_count = int(result.get("failed_country_count", 0) or 0)
    typer.echo(
        " ".join(
            [
                f"requested_limit={result['requested_limit']}",
                f"raw_rows={result['raw_rows']}",
                f"deduped_rows={result['deduped_rows']}",
                f"selected_rows={result['selected_rows']}",
                f"handles_inserted={result['handles_inserted']}",
                f"failed_country_count={failed_country_count}",
                f"watchlist_path={result['watchlist_path']}",
                f"region_split={top_regions}",
            ]
        )
    )
    if failed_country_count > 0:
        failures = result.get("failed_countries", {})  # type: ignore[assignment]
        if isinstance(failures, dict):
            for country, message in sorted(failures.items()):
                typer.echo(f"country_error={country} detail={str(message)[:240]}")


@app.command("migrate-watchlist-from-db")
def migrate_watchlist_from_db(
    limit: int = typer.Option(1000, "--limit", min=100, help="Maximum rows to persist into watchlist_accounts.csv"),
) -> None:
    settings, conn, seeded = _bootstrap()
    del seeded
    try:
        existing_rows = load_csv_rows(settings.watchlist_accounts_path)
        existing_by_domain: dict[str, dict[str, str]] = {}
        for row in existing_rows:
            domain = normalize_domain(row.get("domain", ""))
            if not domain:
                continue
            existing_by_domain[domain] = row

        handle_rows = load_csv_rows(settings.account_source_handles_path)
        website_by_domain: dict[str, str] = {}
        for row in handle_rows:
            domain = normalize_domain(row.get("domain", ""))
            if not domain:
                continue
            website = str(row.get("website_url", "")).strip()
            if website:
                website_by_domain[domain] = website

        account_rows = conn.execute(
            """
            SELECT company_name, domain
            FROM accounts
            WHERE source_type = 'seed'
            ORDER BY datetime(created_at) ASC, company_name ASC
            """
        ).fetchall()

        refreshed_on = date.today().isoformat()
        migrated_rows: list[dict[str, str | int | float]] = []
        preserved_metadata_rows = 0
        for account in account_rows:
            domain = normalize_domain(str(account["domain"] or ""))
            if not domain or domain == "zop.dev" or domain.endswith(".example"):
                continue
            company_name = str(account["company_name"] or domain).strip() or domain

            existing = existing_by_domain.get(domain, {})
            if existing:
                preserved_metadata_rows += 1
            website_url = str(existing.get("website_url", "")).strip() or website_by_domain.get(domain, "") or f"https://{domain}"

            migrated_rows.append(
                {
                    "company_name": company_name,
                    "domain": domain,
                    "source_type": "seed",
                    "country": str(existing.get("country", "")).strip(),
                    "region_group": str(existing.get("region_group", "")).strip(),
                    "industry_label": str(existing.get("industry_label", "")).strip(),
                    "website_url": website_url,
                    "wikidata_id": str(existing.get("wikidata_id", "")).strip(),
                    "sitelinks": int(float(existing.get("sitelinks", "0") or 0)),
                    "revenue_usd": float(existing.get("revenue_usd", "0") or 0),
                    "employees": int(float(existing.get("employees", "0") or 0)),
                    "ranking_score": float(existing.get("ranking_score", "0") or 0),
                    "data_source": str(existing.get("data_source", "migration")).strip() or "migration",
                    "last_refreshed_on": refreshed_on,
                }
            )
            if len(migrated_rows) >= max(1, int(limit)):
                break

        write_csv_rows(
            settings.watchlist_accounts_path,
            migrated_rows,
            fieldnames=[
                "company_name",
                "domain",
                "source_type",
                "country",
                "region_group",
                "industry_label",
                "website_url",
                "wikidata_id",
                "sitelinks",
                "revenue_usd",
                "employees",
                "ranking_score",
                "data_source",
                "last_refreshed_on",
            ],
        )
        typer.echo(
            " ".join(
                [
                    f"watchlist_path={settings.watchlist_accounts_path}",
                    f"rows_written={len(migrated_rows)}",
                    f"preserved_metadata_rows={preserved_metadata_rows}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("crawl-diagnostics")
def crawl_diagnostics(
    date_str: str = typer.Option(None, "--date", help="Diagnostics date YYYY-MM-DD"),
    failure_limit: int = typer.Option(10, "--failure-limit", help="Number of recent failures to show"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_date_str = run_date.isoformat()
        summary_rows = db.fetch_crawl_attempt_summary(conn, run_date_str)
        failure_rows = db.fetch_latest_crawl_failures(conn, run_date_str, limit=max(1, failure_limit))

        if not summary_rows:
            typer.echo(f"run_date={run_date_str} crawl_attempts=0")
            return

        total_attempts = sum(int(row["attempt_count"]) for row in summary_rows)
        totals_by_status: dict[str, int] = {"success": 0, "http_error": 0, "exception": 0, "skipped": 0}
        source_status_counts: dict[str, dict[str, int]] = {}

        for row in summary_rows:
            source = str(row["source"])
            status = str(row["status"])
            count = int(row["attempt_count"])
            totals_by_status[status] = totals_by_status.get(status, 0) + count
            source_status_counts.setdefault(source, {}).setdefault(status, 0)
            source_status_counts[source][status] += count

        typer.echo(
            " ".join(
                [
                    f"run_date={run_date_str}",
                    f"crawl_attempts={total_attempts}",
                    f"success={totals_by_status.get('success', 0)}",
                    f"http_error={totals_by_status.get('http_error', 0)}",
                    f"exception={totals_by_status.get('exception', 0)}",
                    f"skipped={totals_by_status.get('skipped', 0)}",
                ]
            )
        )

        for source in sorted(source_status_counts):
            source_total = sum(source_status_counts[source].values())
            typer.echo(
                " ".join(
                    [
                        f"source={source}",
                        f"attempts={source_total}",
                        f"success={source_status_counts[source].get('success', 0)}",
                        f"http_error={source_status_counts[source].get('http_error', 0)}",
                        f"exception={source_status_counts[source].get('exception', 0)}",
                        f"skipped={source_status_counts[source].get('skipped', 0)}",
                    ]
                )
            )

        for row in failure_rows:
            error_summary = str(row["error_summary"] or "").replace("\n", " ").replace("\r", " ").strip()
            typer.echo(
                " ".join(
                    [
                        f"failure_source={row['source']}",
                        f"status={row['status']}",
                        f"account_id={row['account_id']}",
                        f"attempted_at={row['attempted_at']}",
                        f"endpoint={row['endpoint']}",
                        f"error={error_summary}",
                    ]
                )
            )
    finally:
        conn.close()


@app.command("calibrate-thresholds")
def calibrate_thresholds(
    date_str: str = typer.Option(None, "--date", help="Calibration date YYYY-MM-DD"),
    medium_target_coverage: float = typer.Option(0.6, "--medium-target-coverage", min=0.0, max=1.0),
    high_target_coverage: float = typer.Option(0.2, "--high-target-coverage", min=0.0, max=1.0),
    write: bool = typer.Option(False, "--write", help="Persist suggested thresholds into config/thresholds.csv"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_id = db.get_latest_run_id_for_date(conn, run_date.isoformat())
        if not run_id:
            raise typer.BadParameter(f"No score run found for date {run_date.isoformat()}")

        current_thresholds = load_thresholds(settings.thresholds_path)
        suggestion = calibration.suggest_thresholds_for_run(
            conn=conn,
            run_id=run_id,
            reference_csv_path=settings.config_dir / "icp_reference_accounts.csv",
            medium_target_coverage=medium_target_coverage,
            high_target_coverage=high_target_coverage,
            current_thresholds=current_thresholds,
        )

        if write:
            calibration.write_thresholds(
                settings.thresholds_path,
                high=suggestion.high,
                medium=suggestion.medium,
                low=suggestion.low,
            )

        typer.echo(
            " ".join(
                [
                    f"run_id={run_id}",
                    f"suggested_high={suggestion.high}",
                    f"suggested_medium={suggestion.medium}",
                    f"suggested_low={suggestion.low}",
                    f"icp_accounts={suggestion.icp_accounts}",
                    f"icp_high_coverage={suggestion.icp_high_coverage}",
                    f"icp_medium_coverage={suggestion.icp_medium_coverage}",
                    f"non_icp_accounts={suggestion.non_icp_accounts}",
                    f"non_icp_high_hit_rate={suggestion.non_icp_high_hit_rate}",
                    f"non_icp_medium_hit_rate={suggestion.non_icp_medium_hit_rate}",
                    f"written={int(write)}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("tune-profile")
def tune_profile(
    date_str: str = typer.Option(None, "--date", help="Tuning date YYYY-MM-DD"),
    min_icp_medium_coverage: float = typer.Option(0.6, "--min-icp-medium-coverage", min=0.0, max=1.0),
    max_non_icp_medium_hit_rate: float = typer.Option(0.5, "--max-non-icp-medium-hit-rate", min=0.0, max=1.0),
    max_non_icp_high_hit_rate: float = typer.Option(0.25, "--max-non-icp-high-hit-rate", min=0.0, max=1.0),
    min_scenario_pass_rate: float = typer.Option(0.9, "--min-scenario-pass-rate", min=0.0, max=1.0),
    scenarios_path: str = typer.Option(
        "config/profile_scenarios.csv",
        "--scenarios-path",
        help="Scenario CSV path relative to project root (or absolute path)",
    ),
    write: bool = typer.Option(False, "--write", help="Persist tuned thresholds into config/thresholds.csv"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_id = db.get_latest_run_id_for_date(conn, run_date.isoformat())
        if not run_id:
            raise typer.BadParameter(f"No score run found for date {run_date.isoformat()}")

        raw_scenario_path = Path(scenarios_path)
        scenario_path = raw_scenario_path if raw_scenario_path.is_absolute() else (settings.project_root / raw_scenario_path)
        scenarios = calibration.load_scenarios(scenario_path)
        current_thresholds = load_thresholds(settings.thresholds_path)

        suggestion = calibration.suggest_profile_for_run(
            conn=conn,
            run_id=run_id,
            reference_csv_path=settings.config_dir / "icp_reference_accounts.csv",
            scenarios=scenarios,
            min_icp_medium_coverage=min_icp_medium_coverage,
            max_non_icp_medium_hit_rate=max_non_icp_medium_hit_rate,
            max_non_icp_high_hit_rate=max_non_icp_high_hit_rate,
            min_scenario_pass_rate=min_scenario_pass_rate,
            current_thresholds=current_thresholds,
        )

        if write:
            calibration.write_thresholds(
                settings.thresholds_path,
                high=suggestion.high,
                medium=suggestion.medium,
                low=suggestion.low,
            )

        typer.echo(
            " ".join(
                [
                    f"run_id={run_id}",
                    f"suggested_high={suggestion.high}",
                    f"suggested_medium={suggestion.medium}",
                    f"suggested_low={suggestion.low}",
                    f"icp_accounts={suggestion.icp_accounts}",
                    f"icp_medium_coverage={suggestion.icp_medium_coverage}",
                    f"icp_high_coverage={suggestion.icp_high_coverage}",
                    f"non_icp_accounts={suggestion.non_icp_accounts}",
                    f"non_icp_medium_hit_rate={suggestion.non_icp_medium_hit_rate}",
                    f"non_icp_high_hit_rate={suggestion.non_icp_high_hit_rate}",
                    f"scenario_count={suggestion.scenario_count}",
                    f"scenario_pass_rate={suggestion.scenario_pass_rate}",
                    f"constraints_satisfied={int(suggestion.constraints_satisfied)}",
                    f"written={int(write)}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("run-daily")
def run_daily(date_str: str = typer.Option(None, "--date", help="Run date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    try:
        collect_results = _collect_all(conn, settings)
        collect_inserted = sum(result["inserted"] for result in collect_results.values())

        run_id = _run_scoring(conn, settings, run_date)
        export_result = _run_exports(conn, settings, run_date, run_id)
        prepared_reviews = prepare_review_input_for_date(settings, run_date)

        sync_error = ""
        try:
            sync_result = sync_outputs(settings, run_date)
        except Exception as exc:
            sync_result = {"review_queue_rows": 0, "daily_scores_rows": 0, "source_quality_rows": 0}
            sync_error = str(exc)

        imported = import_reviews_for_date(conn, settings, run_date)
        quality.compute_and_persist_source_metrics(conn, run_date)
        readiness_rows = quality.compute_promotion_readiness(conn, run_date)
        paths = csv_exporter.output_paths(settings.out_dir, run_date)
        csv_exporter.export_source_quality(conn, run_date.isoformat(), paths["source_quality"])
        csv_exporter.export_promotion_readiness(readiness_rows, paths["promotion_readiness"])
        icp_report = _write_icp_coverage_report(conn, settings, run_id, run_date)

        typer.echo(
            " ".join(
                [
                    f"seeded_accounts={seeded}",
                    f"ingested={collect_inserted}",
                    f"run_id={run_id}",
                    f"review_queue_rows={export_result['review_queue']}",
                    f"daily_scores_rows={export_result['daily_scores']}",
                    f"source_quality_rows={export_result['source_quality']}",
                    f"prepared_review_rows={prepared_reviews}",
                    f"imported_reviews={imported}",
                    f"synced_review_queue_rows={sync_result['review_queue_rows']}",
                    f"icp_accounts={icp_report['total_accounts']}",
                    f"icp_high_or_medium={icp_report['high_or_medium_accounts']}",
                    f"icp_coverage={icp_report['coverage_rate']}",
                    f"sync_error={sync_error}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("icp-report")
def icp_report(date_str: str = typer.Option(None, "--date", help="Run date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_id = db.get_latest_run_id_for_date(conn, run_date.isoformat())
        if not run_id:
            raise typer.BadParameter(f"No score run found for date {run_date.isoformat()}")
        report = _write_icp_coverage_report(conn, settings, run_id, run_date)
        typer.echo(
            " ".join(
                [
                    f"path={report['path']}",
                    f"total_accounts={report['total_accounts']}",
                    f"high_or_medium_accounts={report['high_or_medium_accounts']}",
                    f"coverage_rate={report['coverage_rate']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("icp-signal-gaps")
def icp_signal_gaps(
    date_str: str = typer.Option(None, "--date", help="Run date YYYY-MM-DD"),
    playbook_path: str = typer.Option(
        "config/icp_signal_playbook.csv",
        "--playbook-path",
        help="Playbook CSV path relative to project root (or absolute path)",
    ),
    reference_path: str = typer.Option(
        "config/icp_reference_accounts.csv",
        "--reference-path",
        help="ICP reference CSV path relative to project root (or absolute path)",
    ),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_id = db.get_latest_run_id_for_date(conn, run_date.isoformat())
        if not run_id:
            raise typer.BadParameter(f"No score run found for date {run_date.isoformat()}")

        raw_playbook_path = Path(playbook_path)
        resolved_playbook_path = (
            raw_playbook_path if raw_playbook_path.is_absolute() else (settings.project_root / raw_playbook_path)
        )
        raw_reference_path = Path(reference_path)
        resolved_reference_path = (
            raw_reference_path if raw_reference_path.is_absolute() else (settings.project_root / raw_reference_path)
        )

        rows, summary = icp_playbook.compute_icp_signal_gaps(
            conn=conn,
            run_id=run_id,
            reference_csv_path=resolved_reference_path,
            playbook_path=resolved_playbook_path,
        )

        output_path = settings.out_dir / f"icp_signal_gaps_{run_date.strftime('%Y%m%d')}.csv"
        icp_playbook.write_icp_signal_gap_report(output_path, rows)

        typer.echo(
            " ".join(
                [
                    f"path={output_path}",
                    f"total_accounts={summary['total_accounts']}",
                    f"expected_signals={summary['expected_signals']}",
                    f"observed_signals={summary['observed_signals']}",
                    f"coverage_rate={summary['coverage_rate']}",
                    f"high_priority_gaps={summary['high_priority_gaps']}",
                    f"accounts_with_full_coverage={summary['accounts_with_full_coverage']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("discover-ingest")
def discover_ingest(date_str: str = typer.Option(None, "--date", help="Discovery ingest date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        result = discovery_pipeline.ingest_external_events(conn, settings, run_date)
        typer.echo(
            " ".join(
                [
                    f"run_date={result['run_date']}",
                    f"events_seen={result['events_seen']}",
                    f"events_processed={result['events_processed']}",
                    f"events_failed={result['events_failed']}",
                    f"signal_matches={result['signal_matches']}",
                    f"observations_inserted={result['observations_inserted']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("discover-frontier")
def discover_frontier(
    date_str: str = typer.Option(None, "--date", help="Frontier build date YYYY-MM-DD"),
    profile: str = typer.Option("light", "--profile", help="Hunt profile: light|balanced|heavy"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        profile_cfg = hunt_pipeline.resolve_profile(profile)
        result = hunt_pipeline.build_frontier(conn, settings, run_date, profile=profile_cfg)
        typer.echo(
            " ".join(
                [
                    f"run_date={result['run_date']}",
                    f"profile={profile_cfg.name}",
                    f"events_seen={result['events_seen']}",
                    f"frontier_queued={result['frontier_queued']}",
                    f"frontier_duplicates={result['frontier_duplicates']}",
                    f"events_failed={result['events_failed']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("discover-fetch")
def discover_fetch(
    date_str: str = typer.Option(None, "--date", help="Fetch date YYYY-MM-DD"),
    profile: str = typer.Option("light", "--profile", help="Hunt profile: light|balanced|heavy"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        profile_cfg = hunt_pipeline.resolve_profile(profile)
        result = hunt_pipeline.fetch_documents(conn, settings, run_date, profile=profile_cfg)
        typer.echo(
            " ".join(
                [
                    f"run_date={result['run_date']}",
                    f"profile={profile_cfg.name}",
                    f"frontier_rows_seen={result['frontier_rows_seen']}",
                    f"documents_fetched={result['documents_fetched']}",
                    f"documents_failed={result['documents_failed']}",
                    f"js_fetches_used={result['js_fetches_used']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("discover-extract")
def discover_extract(
    date_str: str = typer.Option(None, "--date", help="Extraction date YYYY-MM-DD"),
    profile: str = typer.Option("light", "--profile", help="Hunt profile: light|balanced|heavy"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        profile_cfg = hunt_pipeline.resolve_profile(profile)
        result = hunt_pipeline.extract_documents(conn, settings, run_date, profile=profile_cfg)
        typer.echo(
            " ".join(
                [
                    f"run_date={result['run_date']}",
                    f"profile={profile_cfg.name}",
                    f"documents_seen={result['documents_seen']}",
                    f"documents_parsed={result['documents_parsed']}",
                    f"listing_pages={result['listing_pages']}",
                    f"links_enqueued={result['links_enqueued']}",
                    f"mentions_inserted={result['mentions_inserted']}",
                    f"observations_inserted={result['observations_inserted']}",
                    f"people_activity_inserted={result['people_activity_inserted']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("discover-score")
def discover_score(
    date_str: str = typer.Option(None, "--date", help="Discovery scoring date YYYY-MM-DD"),
    quality_gates: bool = typer.Option(False, "--quality-gates/--no-quality-gates", help="Enforce evidence/relevance gates"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        score_run_id = _run_scoring(conn, settings, run_date)
        result = discovery_pipeline.score_discovery_candidates(
            conn=conn,
            settings=settings,
            run_date=run_date,
            score_run_id=score_run_id,
            source_events_processed=0,
            observations_inserted=0,
            enforce_quality_gates=quality_gates,
            min_evidence_quality=0.8,
            min_relevance_score=0.65,
        )
        typer.echo(
            " ".join(
                [
                    f"score_run_id={score_run_id}",
                    f"quality_gates={int(quality_gates)}",
                    f"discovery_run_id={result['discovery_run_id']}",
                    f"total_candidates={result['total_candidates']}",
                    f"high_candidates={result['high_candidates']}",
                    f"medium_candidates={result['medium_candidates']}",
                    f"explore_candidates={result['explore_candidates']}",
                    f"crm_eligible_candidates={result['crm_eligible_candidates']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("discover-report")
def discover_report(date_str: str = typer.Option(None, "--date", help="Discovery report date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        discovery_run_id = db.get_latest_discovery_run_id_for_date(conn, run_date.isoformat())
        if not discovery_run_id:
            raise typer.BadParameter(f"No discovery run found for date {run_date.isoformat()}")
        result = discovery_pipeline.write_discovery_reports(conn, settings, run_date, discovery_run_id)
        typer.echo(
            " ".join(
                [
                    f"discovery_run_id={discovery_run_id}",
                    f"discovery_queue_rows={result['discovery_queue_rows']}",
                    f"crm_candidates_rows={result['crm_candidates_rows']}",
                    f"metrics_rows={result['metrics_rows']}",
                    f"discovery_queue_path={result['discovery_queue_path']}",
                    f"crm_candidates_path={result['crm_candidates_path']}",
                    f"discovery_metrics_path={result['discovery_metrics_path']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("run-discovery")
def run_discovery(
    date_str: str = typer.Option(None, "--date", help="Discovery run date YYYY-MM-DD"),
    profile: str = typer.Option("light", "--profile", help="Hunt profile: light|balanced|heavy"),
) -> None:
    # Backward-compatible command name; runtime is now story-deep hunt only.
    run_date = parse_date(date_str, load_settings().run_timezone)
    result = _run_hunt_cycle(run_date, profile_name=profile)
    typer.echo(
        " ".join(
            [
                f"run_date={result['run_date']}",
                f"profile={result['profile']}",
                f"events_seen={result['events_seen']}",
                f"frontier_queued={result['frontier_queued']}",
                f"documents_fetched={result['documents_fetched']}",
                f"documents_parsed={result['documents_parsed']}",
                f"mentions_inserted={result['mentions_inserted']}",
                f"observations_inserted={result['observations_inserted']}",
                f"score_run_id={result['score_run_id']}",
                f"discovery_run_id={result['discovery_run_id']}",
                f"total_candidates={result['total_candidates']}",
                f"crm_candidates_rows={result['crm_candidates_rows']}",
                f"story_evidence_rows={result['story_evidence_rows']}",
                f"signal_lineage_rows={result['signal_lineage_rows']}",
            ]
        )
    )


@app.command("run-hunt")
def run_hunt(
    date_str: str = typer.Option(None, "--date", help="Hunt run date YYYY-MM-DD"),
    profile: str = typer.Option("light", "--profile", help="Hunt profile: light|balanced|heavy"),
) -> None:
    run_date = parse_date(date_str, load_settings().run_timezone)
    result = _run_hunt_cycle(run_date, profile_name=profile)
    typer.echo(
        " ".join(
            [
                f"run_date={result['run_date']}",
                f"profile={result['profile']}",
                f"events_seen={result['events_seen']}",
                f"frontier_queued={result['frontier_queued']}",
                f"documents_fetched={result['documents_fetched']}",
                f"documents_parsed={result['documents_parsed']}",
                f"mentions_inserted={result['mentions_inserted']}",
                f"observations_inserted={result['observations_inserted']}",
                f"score_run_id={result['score_run_id']}",
                f"discovery_run_id={result['discovery_run_id']}",
                f"total_candidates={result['total_candidates']}",
                f"crm_candidates_rows={result['crm_candidates_rows']}",
                f"story_evidence_rows={result['story_evidence_rows']}",
                f"signal_lineage_rows={result['signal_lineage_rows']}",
            ]
        )
    )


@app.command("run-autonomous-loop")
def run_autonomous_loop(
    ingest_interval_minutes: int = typer.Option(15, "--ingest-interval-minutes", min=1),
    score_interval_minutes: int = typer.Option(60, "--score-interval-minutes", min=5),
    discovery_interval_minutes: int = typer.Option(180, "--discovery-interval-minutes", min=10),
    hunt_profile: str = typer.Option("light", "--hunt-profile", help="Hunt profile: light|balanced|heavy"),
    sleep_seconds: int = typer.Option(5, "--sleep-seconds", min=1),
    once: bool = typer.Option(False, "--once", help="Run one cycle for each due job and exit"),
) -> None:
    next_ingest_at = 0.0
    next_score_at = 0.0
    next_discovery_at = 0.0

    ingest_every = float(ingest_interval_minutes * 60)
    score_every = float(score_interval_minutes * 60)
    discovery_every = float(discovery_interval_minutes * 60)

    while True:
        now_mono = time.monotonic()
        run_date = parse_date(None, load_settings().run_timezone)
        now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        did_work = False

        due_ingest = now_mono >= next_ingest_at
        due_discovery = now_mono >= next_discovery_at
        due_score = now_mono >= next_score_at

        # Ordering matters: discovery runs before score so exports reflect same-cycle webhook events.
        if due_ingest:
            did_work = True
            try:
                ingest_result = _run_ingest_cycle(run_date)
                typer.echo(
                    " ".join(
                        [
                            f"ts={now_iso}",
                            "job=ingest",
                            f"run_date={ingest_result['run_date']}",
                            f"observations_seen={ingest_result['observations_seen']}",
                            f"observations_inserted={ingest_result['observations_inserted']}",
                        ]
                    )
                )
            except Exception as exc:
                typer.echo(f"ts={now_iso} job=ingest status=failed error={str(exc)[:240]}")
            next_ingest_at = now_mono + ingest_every

        if due_discovery:
            did_work = True
            try:
                discovery_result = _run_hunt_cycle(run_date, profile_name=hunt_profile)
                typer.echo(
                    " ".join(
                        [
                            f"ts={now_iso}",
                            "job=discovery",
                            f"discovery_run_id={discovery_result['discovery_run_id']}",
                            f"profile={discovery_result.get('profile', hunt_profile)}",
                            f"events_processed={discovery_result.get('events_seen', 0)}",
                            f"candidates={discovery_result.get('total_candidates', 0)}",
                            f"crm_candidates_rows={discovery_result['crm_candidates_rows']}",
                        ]
                    )
                )
            except Exception as exc:
                typer.echo(f"ts={now_iso} job=discovery status=failed error={str(exc)[:240]}")
            next_discovery_at = now_mono + discovery_every

        if due_score:
            did_work = True
            try:
                score_result = _run_score_cycle(run_date)
                typer.echo(
                    " ".join(
                        [
                            f"ts={now_iso}",
                            "job=score",
                            f"run_id={score_result['run_id']}",
                            f"daily_scores_rows={score_result['daily_scores_rows']}",
                            f"review_queue_rows={score_result['review_queue_rows']}",
                            f"icp_coverage_rate={score_result['icp_coverage_rate']}",
                        ]
                    )
                )
            except Exception as exc:
                typer.echo(f"ts={now_iso} job=score status=failed error={str(exc)[:240]}")
            next_score_at = now_mono + score_every

        if once and did_work:
            return

        time.sleep(float(sleep_seconds))


@app.command("serve-discovery-webhook")
def serve_discovery_webhook(
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(8787, "--port"),
    log_level: str = typer.Option("info", "--log-level"),
) -> None:
    try:
        import uvicorn  # type: ignore
    except Exception as exc:
        raise typer.BadParameter("uvicorn is required. Install project dependencies first.") from exc

    from src.discovery.webhook import app as discovery_app

    if discovery_app is None:
        raise typer.BadParameter("fastapi is required. Install project dependencies first.")

    uvicorn.run(discovery_app, host=host, port=port, log_level=log_level)


if __name__ == "__main__":
    app()
