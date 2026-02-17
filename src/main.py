from __future__ import annotations

from datetime import date
import sqlite3

import typer

from src import db
from src.collectors import community, first_party, jobs, news, technographics
from src.export import csv_exporter
from src.reporting import quality
from src.review.import_reviews import import_reviews_for_date
from src.scoring.engine import run_scoring
from src.scoring.rules import load_keyword_lexicon, load_signal_rules, load_source_registry, load_thresholds
from src.settings import Settings, load_settings
from src.sync.google_sheets import sync_outputs
from src.utils import ensure_project_directories, parse_date, write_csv_rows

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
    seeded = db.seed_accounts(conn, local_settings.seed_accounts_path)
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

        for score in result.account_scores:
            baseline = _baseline_score_7d(conn, score.account_id, score.product, run_date_str)
            score.delta_7d = round(score.score - baseline, 2) if baseline is not None else 0.0

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


@app.command("run-daily")
def run_daily(date_str: str = typer.Option(None, "--date", help="Run date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    try:
        collect_results = _collect_all(conn, settings)
        collect_inserted = sum(result["inserted"] for result in collect_results.values())

        run_id = _run_scoring(conn, settings, run_date)
        export_result = _run_exports(conn, settings, run_date, run_id)

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


if __name__ == "__main__":
    app()
