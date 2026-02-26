"""Autonomous scheduling loop and retry execution."""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

import typer

from src import db
from src.notifier import send_alert
from src.pipeline.daily import run_daily_impl, run_hunt_cycle, run_score_cycle
from src.pipeline.helpers import (
    _AUTONOMOUS_LOCK_NAME,
    _RETRY_BACKOFF_SECONDS,
    StageExecutionError,
    enqueue_retry_task,
    retry_due_iso,
    run_with_watchdog,
)
from src.pipeline.ingest import run_ingest_cycle
from src.settings import Settings, load_settings
from src.utils import parse_date

logger = logging.getLogger(__name__)


def run_autonomous_loop_impl(
    ingest_interval_minutes: int,
    score_interval_minutes: int,
    discovery_interval_minutes: int,
    hunt_profile: str,
    sleep_seconds: int,
    once: bool,
) -> None:
    settings = load_settings()
    lock_conn = db.get_connection(settings.pg_dsn)
    db.init_db(lock_conn)
    lock_owner = f"pid{os.getpid()}-{uuid.uuid4().hex[:8]}"
    lock_acquired = db.try_advisory_lock(
        lock_conn,
        lock_name=_AUTONOMOUS_LOCK_NAME,
        owner_id=lock_owner,
        details=f"hunt_profile={hunt_profile}",
    )
    if not lock_acquired:
        typer.echo(f"status=skipped reason=lock_busy lock_name={_AUTONOMOUS_LOCK_NAME}")
        lock_conn.close()
        return

    next_ingest_at = 0.0
    next_score_at = 0.0
    next_discovery_at = 0.0

    ingest_every = float(ingest_interval_minutes * 60)
    score_every = float(score_interval_minutes * 60)
    discovery_every = float(discovery_interval_minutes * 60)

    try:
        while True:
            now_mono = time.monotonic()
            run_date = parse_date(None, settings.run_timezone)
            now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            did_work = False

            due_ingest = now_mono >= next_ingest_at
            due_discovery = now_mono >= next_discovery_at
            due_score = now_mono >= next_score_at

            if due_ingest:
                did_work = True
                try:
                    ingest_result, _ = run_with_watchdog(
                        "ingest_cycle",
                        settings.stage_timeout_seconds,
                        lambda: run_ingest_cycle(run_date),
                    )
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
                except StageExecutionError as exc:
                    retry_task_id = enqueue_retry_task(
                        lock_conn,
                        settings,
                        task_type="ingest_cycle",
                        payload={"run_date": run_date.isoformat()},
                        reason=str(exc),
                    )
                    db.record_stage_failure(
                        lock_conn,
                        run_type="autonomous_loop",
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
                        title="Autonomous ingest job failed",
                        body=(
                            f"run_date={run_date.isoformat()} stage={exc.stage} timed_out={int(exc.timed_out)} "
                            f"duration_seconds={round(exc.duration_seconds, 2)} retry_task_id={retry_task_id}"
                        ),
                        severity="error",
                    )
                    typer.echo(
                        f"ts={now_iso} job=ingest status=failed error={str(exc)[:200]} retry_task_id={retry_task_id}"
                    )
                except Exception as exc:
                    typer.echo(f"ts={now_iso} job=ingest status=failed error={str(exc)[:240]}")
                next_ingest_at = now_mono + ingest_every

            if due_discovery:
                did_work = True
                try:
                    discovery_result, _ = run_with_watchdog(
                        "discovery_cycle",
                        settings.stage_timeout_seconds,
                        lambda: run_hunt_cycle(run_date, profile_name=hunt_profile),
                    )
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
                                f"manual_review_rows={discovery_result.get('manual_review_rows', 0)}",
                            ]
                        )
                    )
                except StageExecutionError as exc:
                    retry_task_id = enqueue_retry_task(
                        lock_conn,
                        settings,
                        task_type="discovery_cycle",
                        payload={"run_date": run_date.isoformat(), "hunt_profile": hunt_profile},
                        reason=str(exc),
                    )
                    db.record_stage_failure(
                        lock_conn,
                        run_type="autonomous_loop",
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
                        title="Autonomous discovery job failed",
                        body=(
                            f"run_date={run_date.isoformat()} stage={exc.stage} timed_out={int(exc.timed_out)} "
                            f"duration_seconds={round(exc.duration_seconds, 2)} retry_task_id={retry_task_id}"
                        ),
                        severity="error",
                    )
                    typer.echo(
                        f"ts={now_iso} job=discovery status=failed error={str(exc)[:200]} retry_task_id={retry_task_id}"
                    )
                except Exception as exc:
                    typer.echo(f"ts={now_iso} job=discovery status=failed error={str(exc)[:240]}")
                next_discovery_at = now_mono + discovery_every

            if due_score:
                did_work = True
                try:
                    score_result, _ = run_with_watchdog(
                        "score_cycle",
                        settings.stage_timeout_seconds,
                        lambda: run_score_cycle(run_date),
                    )
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
                except StageExecutionError as exc:
                    retry_task_id = enqueue_retry_task(
                        lock_conn,
                        settings,
                        task_type="score_cycle",
                        payload={"run_date": run_date.isoformat()},
                        reason=str(exc),
                    )
                    db.record_stage_failure(
                        lock_conn,
                        run_type="autonomous_loop",
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
                        title="Autonomous score job failed",
                        body=(
                            f"run_date={run_date.isoformat()} stage={exc.stage} timed_out={int(exc.timed_out)} "
                            f"duration_seconds={round(exc.duration_seconds, 2)} retry_task_id={retry_task_id}"
                        ),
                        severity="error",
                    )
                    typer.echo(
                        f"ts={now_iso} job=score status=failed error={str(exc)[:200]} retry_task_id={retry_task_id}"
                    )
                except Exception as exc:
                    typer.echo(f"ts={now_iso} job=score status=failed error={str(exc)[:240]}")
                next_score_at = now_mono + score_every

            if once and did_work:
                return

            time.sleep(float(sleep_seconds))
    finally:
        if lock_acquired:
            db.release_advisory_lock(lock_conn, lock_name=_AUTONOMOUS_LOCK_NAME, owner_id=lock_owner)
        lock_conn.close()


def execute_retry_task(task: dict[str, object], settings: Settings) -> None:
    task_type = str(task.get("task_type", "") or "").strip().lower()
    payload_raw = str(task.get("payload_json", "{}") or "{}")
    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError as exc:
        raise ValueError("invalid_retry_payload_json") from exc
    if not isinstance(payload, dict):
        raise ValueError("invalid_retry_payload")

    raw_run_date = payload.get("run_date")
    run_date_value = str(raw_run_date).strip() if raw_run_date is not None else ""
    run_date = parse_date(run_date_value or None, settings.run_timezone)
    if task_type == "run_daily":
        previous_flag = os.getenv("SIGNALS_DISABLE_AUTO_RETRY_ENQUEUE")
        os.environ["SIGNALS_DISABLE_AUTO_RETRY_ENQUEUE"] = "1"
        try:
            run_daily_impl(
                date_str=run_date.isoformat(),
                live_max_accounts=None,
                live_workers_per_source=None,
                stage_timeout_seconds=None,
            )
        finally:
            if previous_flag is None:
                os.environ.pop("SIGNALS_DISABLE_AUTO_RETRY_ENQUEUE", None)
            else:
                os.environ["SIGNALS_DISABLE_AUTO_RETRY_ENQUEUE"] = previous_flag
        return

    if task_type == "ingest_cycle":
        run_ingest_cycle(run_date)
        return
    if task_type == "score_cycle":
        run_score_cycle(run_date)
        return
    if task_type == "discovery_cycle":
        profile = str(payload.get("hunt_profile", "light") or "light")
        run_hunt_cycle(run_date, profile_name=profile)
        return
    raise ValueError(f"unsupported_retry_task_type={task_type}")
