"""Daily pipeline orchestrator and composite cycle functions."""

from __future__ import annotations

import logging
import os
import uuid
from datetime import date

import typer

from src import db
from src.discovery import hunt as hunt_pipeline
from src.discovery import pipeline as discovery_pipeline
from src.export import csv_exporter
from src.notifier import send_alert
from src.pipeline.export import persist_ops_metrics, run_exports, write_icp_coverage_report
from src.pipeline.helpers import (
    _RUN_DAILY_LOCK_NAME,
    StageExecutionError,
    bootstrap,
    enqueue_retry_task,
    review_queue_excluded_domains,
    run_with_watchdog,
)
from src.pipeline.ingest import collect_all
from src.pipeline.score import run_scoring_stage
from src.reporting import quality
from src.research.orchestrator import run_research_stage
from src.review.import_reviews import import_reviews_for_date, prepare_review_input_for_date
from src.settings import Settings
from src.sync.google_sheets import sync_outputs
from src.utils import parse_date

logger = logging.getLogger(__name__)


def run_score_cycle(run_date: date) -> dict[str, int | float | str]:
    settings, conn, seeded = bootstrap()
    del seeded
    try:
        run_id = run_scoring_stage(conn, settings, run_date)
        export_result = run_exports(conn, settings, run_date, run_id)
        icp_report = write_icp_coverage_report(conn, settings, run_id, run_date)
        return {
            "run_id": run_id,
            "daily_scores_rows": int(export_result["daily_scores"]),
            "review_queue_rows": int(export_result["review_queue"]),
            "icp_coverage_rate": float(icp_report["coverage_rate"]),
        }
    finally:
        conn.close()


def run_discovery_cycle(run_date: date) -> dict[str, int | str]:
    settings, conn, seeded = bootstrap()
    del seeded
    try:
        ingest_result = discovery_pipeline.ingest_external_events(conn, settings, run_date)
        score_run_id = run_scoring_stage(conn, settings, run_date)
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
            "manual_review_rows": int(report_result["manual_review_rows"]),
        }
    finally:
        conn.close()


def run_hunt_cycle(run_date: date, profile_name: str = "light") -> dict[str, int | float | str]:
    settings, conn, seeded = bootstrap()
    del seeded
    try:
        profile = hunt_pipeline.resolve_profile(profile_name)
        frontier_result = hunt_pipeline.build_frontier(conn, settings, run_date, profile=profile)
        fetch_result = hunt_pipeline.fetch_documents(conn, settings, run_date, profile=profile)
        extract_result = hunt_pipeline.extract_documents(conn, settings, run_date, profile=profile)

        score_run_id = run_scoring_stage(conn, settings, run_date)
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
            "manual_review_rows": int(report_result["manual_review_rows"]),
            "story_evidence_rows": int(hunt_reports["story_evidence_rows"]),
            "signal_lineage_rows": int(hunt_reports["signal_lineage_rows"]),
        }
    finally:
        conn.close()


def run_daily_impl(
    date_str: str | None,
    live_max_accounts: int | None,
    live_workers_per_source: int | None,
    stage_timeout_seconds: int | None,
) -> None:
    settings, conn, seeded = bootstrap()
    overrides: dict[str, object] = {}
    if live_max_accounts is not None:
        overrides["live_max_accounts"] = max(1, int(live_max_accounts))
    if live_workers_per_source is not None:
        overrides["live_workers_per_source"] = max(1, int(live_workers_per_source))
    if stage_timeout_seconds is not None:
        overrides["stage_timeout_seconds"] = max(30, int(stage_timeout_seconds))
    if overrides:
        settings = settings.model_copy(update=overrides)
    run_date = parse_date(date_str, settings.run_timezone)
    lock_owner = f"pid{os.getpid()}-{uuid.uuid4().hex[:8]}"
    lock_acquired = False
    try:
        lock_acquired = db.try_advisory_lock(
            conn,
            lock_name=_RUN_DAILY_LOCK_NAME,
            owner_id=lock_owner,
            details=f"run_date={run_date.isoformat()}",
        )
        if not lock_acquired:
            typer.echo(f"status=skipped reason=lock_busy lock_name={_RUN_DAILY_LOCK_NAME}")
            return

        typer.echo(
            f"stage=ingest status=started live_max_accounts={settings.live_max_accounts} "
            f"live_workers_per_source={settings.live_workers_per_source} "
            f"timeout_seconds={settings.stage_timeout_seconds}"
        )
        collect_results, collect_elapsed = run_with_watchdog(
            "ingest",
            settings.stage_timeout_seconds,
            lambda: collect_all(conn, settings),
        )
        collect_inserted = sum(result["inserted"] for result in collect_results.values())
        typer.echo(
            f"stage=ingest status=completed duration_seconds={round(collect_elapsed, 2)} inserted={collect_inserted}"
        )

        typer.echo("stage=score status=started")
        run_id, score_elapsed = run_with_watchdog(
            "score", settings.stage_timeout_seconds, lambda: run_scoring_stage(conn, settings, run_date)
        )
        typer.echo(f"stage=score status=completed duration_seconds={round(score_elapsed, 2)} run_id={run_id}")

        # Research stage — non-blocking. If it fails, export still happens.
        research_result = {"attempted": 0, "completed": 0, "failed": 0, "skipped": 0}
        try:
            research_result, _ = run_with_watchdog(
                "research",
                settings.stage_timeout_seconds,
                lambda: run_research_stage(conn, settings, run_date.isoformat(), run_id),
            )
        except Exception as exc:
            logger.warning("research stage failed, continuing to export: %s", exc, exc_info=True)

        # Sales-ready CSV export.
        excluded = review_queue_excluded_domains(settings)
        sales_ready_path = settings.out_dir / f"sales_ready_{csv_exporter.date_suffix(run_date)}.csv"
        sales_ready_rows = 0
        try:
            sales_ready_rows = csv_exporter.export_sales_ready(conn, run_id, sales_ready_path, excluded)
        except Exception as exc:
            logger.warning("sales-ready export failed: %s", exc, exc_info=True)

        typer.echo("stage=export status=started")
        export_result, _ = run_with_watchdog(
            "export",
            settings.stage_timeout_seconds,
            lambda: run_exports(conn, settings, run_date, run_id),
        )
        typer.echo(
            f"stage=export status=completed review_queue_rows={export_result['review_queue']} "
            f"daily_scores_rows={export_result['daily_scores']}"
        )
        typer.echo("stage=prepare-review-input status=started")
        prepared_reviews, _ = run_with_watchdog(
            "prepare-review-input",
            settings.stage_timeout_seconds,
            lambda: prepare_review_input_for_date(settings, run_date),
        )
        typer.echo(f"stage=prepare-review-input status=completed prepared_review_rows={prepared_reviews}")

        sync_error = ""
        sync_result = {"review_queue_rows": 0, "daily_scores_rows": 0, "source_quality_rows": 0}
        try:
            typer.echo("stage=sync-sheet status=started")
            sync_result, _ = run_with_watchdog(
                "sync-sheet",
                settings.stage_timeout_seconds,
                lambda: sync_outputs(settings, run_date),
            )
            typer.echo(
                f"stage=sync-sheet status=completed review_queue_rows={sync_result['review_queue_rows']} "
                f"daily_scores_rows={sync_result['daily_scores_rows']}"
            )
        except Exception as exc:
            sync_error = str(exc)
            typer.echo(f"stage=sync-sheet status=failed error={sync_error[:220]}")

        typer.echo("stage=import-reviews status=started")
        imported, _ = run_with_watchdog(
            "import-reviews",
            settings.stage_timeout_seconds,
            lambda: import_reviews_for_date(conn, settings, run_date),
        )
        typer.echo(f"stage=import-reviews status=completed imported_reviews={imported}")

        def _refresh_quality_outputs() -> dict[str, int]:
            quality.compute_and_persist_source_metrics(conn, run_date)
            readiness_rows = quality.compute_promotion_readiness(conn, run_date)
            paths = csv_exporter.output_paths(settings.out_dir, run_date)
            quality_rows = csv_exporter.export_source_quality(conn, run_date.isoformat(), paths["source_quality"])
            readiness_count = csv_exporter.export_promotion_readiness(readiness_rows, paths["promotion_readiness"])
            return {"source_quality_rows": quality_rows, "promotion_readiness_rows": readiness_count}

        typer.echo("stage=quality-refresh status=started")
        quality_result, quality_elapsed = run_with_watchdog(
            "quality-refresh",
            settings.stage_timeout_seconds,
            _refresh_quality_outputs,
        )
        typer.echo(
            f"stage=quality-refresh status=completed duration_seconds={round(quality_elapsed, 2)} "
            f"source_quality_rows={quality_result['source_quality_rows']}"
        )
        typer.echo("stage=icp-coverage-report status=started")
        icp_report, icp_elapsed = run_with_watchdog(
            "icp-coverage-report",
            settings.stage_timeout_seconds,
            lambda: write_icp_coverage_report(conn, settings, run_id, run_date),
        )
        typer.echo(
            f"stage=icp-coverage-report status=completed duration_seconds={round(icp_elapsed, 2)} "
            f"icp_coverage={icp_report['coverage_rate']}"
        )
        typer.echo("stage=ops-metrics status=started")
        ops_result, ops_elapsed = run_with_watchdog(
            "ops-metrics",
            settings.stage_timeout_seconds,
            lambda: persist_ops_metrics(conn, settings, run_date),
        )
        typer.echo(
            f"stage=ops-metrics status=completed duration_seconds={round(ops_elapsed, 2)} "
            f"ops_metrics_rows={ops_result['ops_metrics_rows']}"
        )

        typer.echo(
            " ".join(
                [
                    f"seeded_accounts={seeded}",
                    f"ingested={collect_inserted}",
                    f"run_id={run_id}",
                    f"review_queue_rows={export_result['review_queue']}",
                    f"daily_scores_rows={export_result['daily_scores']}",
                    f"source_quality_rows={quality_result['source_quality_rows']}",
                    f"promotion_readiness_rows={quality_result['promotion_readiness_rows']}",
                    f"prepared_review_rows={prepared_reviews}",
                    f"imported_reviews={imported}",
                    f"synced_review_queue_rows={sync_result['review_queue_rows']}",
                    f"icp_accounts={icp_report['total_accounts']}",
                    f"icp_high_or_medium={icp_report['high_or_medium_accounts']}",
                    f"icp_coverage={icp_report['coverage_rate']}",
                    f"ops_metrics_rows={ops_result['ops_metrics_rows']}",
                    f"retry_depth={ops_result['retry_depth']}",
                    f"retry_queue_size={ops_result['retry_queue_size']}",
                    f"quarantine_size={ops_result['quarantine_size']}",
                    f"lock_busy_24h={ops_result['lock_busy_24h']}",
                    f"lock_release_missed_24h={ops_result['lock_release_missed_24h']}",
                    f"research_attempted={research_result['attempted']}",
                    f"research_completed={research_result['completed']}",
                    f"sales_ready_rows={sales_ready_rows}",
                    f"sync_error={sync_error}",
                ]
            )
        )
    except StageExecutionError as exc:
        _handle_daily_stage_failure(conn, settings, run_date, exc)
    except Exception as exc:
        _handle_daily_exception(conn, settings, run_date, exc)
    finally:
        if lock_acquired:
            db.release_advisory_lock(conn, lock_name=_RUN_DAILY_LOCK_NAME, owner_id=lock_owner)
        conn.close()


def _handle_daily_stage_failure(conn, settings: Settings, run_date: date, exc: StageExecutionError) -> None:
    enqueue_retries = os.getenv("SIGNALS_DISABLE_AUTO_RETRY_ENQUEUE", "").strip().lower() not in {"1", "true", "yes"}
    retry_task_id = ""
    if enqueue_retries:
        retry_task_id = enqueue_retry_task(
            conn,
            settings,
            task_type="run_daily",
            payload={"run_date": run_date.isoformat()},
            reason=str(exc),
        )
    db.record_stage_failure(
        conn,
        run_type="run_daily",
        run_date=run_date.isoformat(),
        stage=exc.stage,
        error_summary=str(exc),
        duration_seconds=exc.duration_seconds,
        timed_out=exc.timed_out,
        retry_task_id=retry_task_id,
        commit=True,
    )
    send_alert(
        settings,
        title="run-daily failed",
        body=(
            f"run_date={run_date.isoformat()} stage={exc.stage} "
            f"timed_out={int(exc.timed_out)} duration_seconds={round(exc.duration_seconds, 2)} "
            f"retry_task_id={retry_task_id}"
        ),
        severity="error",
    )
    raise typer.Exit(code=1)


def _handle_daily_exception(conn, settings: Settings, run_date: date, exc: Exception) -> None:
    enqueue_retries = os.getenv("SIGNALS_DISABLE_AUTO_RETRY_ENQUEUE", "").strip().lower() not in {"1", "true", "yes"}
    retry_task_id = ""
    if enqueue_retries:
        retry_task_id = enqueue_retry_task(
            conn,
            settings,
            task_type="run_daily",
            payload={"run_date": run_date.isoformat()},
            reason=str(exc),
        )
    db.record_stage_failure(
        conn,
        run_type="run_daily",
        run_date=run_date.isoformat(),
        stage="run_daily",
        error_summary=str(exc),
        duration_seconds=0.0,
        timed_out=False,
        retry_task_id=retry_task_id,
        commit=True,
    )
    send_alert(
        settings,
        title="run-daily failed",
        body=f"run_date={run_date.isoformat()} error={str(exc)[:400]} retry_task_id={retry_task_id}",
        severity="error",
    )
    raise typer.Exit(code=1)
