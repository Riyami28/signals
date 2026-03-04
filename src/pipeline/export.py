"""CSV export stage — review queue, daily scores, ops metrics."""

from __future__ import annotations

import json
import logging
from datetime import date

from src import db
from src.export import csv_exporter
from src.notifier import send_alert
from src.pipeline.helpers import review_queue_excluded_domains
from src.reporting import quality
from src.settings import Settings
from src.utils import load_csv_rows, write_csv_rows

logger = logging.getLogger(__name__)


def run_exports(conn, settings: Settings, run_date: date, run_id: str) -> dict[str, int | str]:
    paths = csv_exporter.output_paths(settings.out_dir, run_date)

    queue_count = csv_exporter.export_review_queue(
        conn,
        run_id,
        paths["review_queue"],
        excluded_domains=review_queue_excluded_domains(settings),
    )
    score_count = csv_exporter.export_daily_scores(conn, run_id, paths["daily_scores"])

    quality.compute_and_persist_source_metrics(conn, run_date)
    quality_rows = csv_exporter.export_source_quality(conn, run_date.isoformat(), paths["source_quality"])

    readiness_rows = quality.compute_promotion_readiness(conn, run_date)
    readiness_count = csv_exporter.export_promotion_readiness(readiness_rows, paths["promotion_readiness"])

    # Export Twitter signals with detailed summary
    twitter_signals_path = settings.out_dir / f"twitter_signals_{run_date.strftime('%Y%m%d')}.csv"
    twitter_count = csv_exporter.export_twitter_signals(conn, twitter_signals_path)

    return {
        "review_queue": queue_count,
        "daily_scores": score_count,
        "source_quality": quality_rows,
        "promotion_readiness": readiness_count,
        "twitter_signals": twitter_count,
        "review_queue_path": str(paths["review_queue"]),
        "daily_scores_path": str(paths["daily_scores"]),
        "source_quality_path": str(paths["source_quality"]),
        "promotion_readiness_path": str(paths["promotion_readiness"]),
        "twitter_signals_path": str(twitter_signals_path),
    }


def write_icp_coverage_report(conn, settings: Settings, run_id: str, run_date: date) -> dict[str, int | float | str]:
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


def persist_ops_metrics(conn, settings: Settings, run_date: date) -> dict[str, int | float | str]:
    run_date_str = run_date.isoformat()
    queue_path = settings.out_dir / f"discovery_queue_{run_date.strftime('%Y%m%d')}.csv"
    crm_path = settings.out_dir / f"crm_candidates_{run_date.strftime('%Y%m%d')}.csv"
    queue_rows = load_csv_rows(queue_path)
    crm_rows = load_csv_rows(crm_path)

    ingest_lag = db.fetch_latest_event_ingest_lag_seconds(conn, run_date_str)
    retry_depth = db.fetch_retry_depth(conn)
    retry_queue_size = db.fetch_retry_queue_size(conn)
    quarantine_size = db.fetch_quarantine_size(conn)
    handoff_success_rate = round((len(crm_rows) / len(queue_rows)) if queue_rows else 0.0, 4)

    precision_rows = db.fetch_precision_by_band(conn, run_date_str, lookback_days=settings.ops_metrics_lookback_days)
    lock_events = db.fetch_lock_event_counts(conn, lookback_hours=24)
    lock_busy_24h = int(lock_events.get("busy", 0))
    lock_release_missed_24h = int(lock_events.get("release_missed", 0))
    precision_by_band: dict[str, tuple[float, int]] = {}
    for row in precision_rows:
        band = str(row.get("band", "") or "").strip().lower()
        if not band:
            continue
        precision_by_band[band] = (
            round(float(row.get("approved_rate", 0.0) or 0.0), 4),
            int(row.get("sample_size", 0) or 0),
        )

    high_precision, high_sample = precision_by_band.get("high", (0.0, 0))
    medium_precision, medium_sample = precision_by_band.get("medium", (0.0, 0))

    metric_rows = [
        {"metric": "ingest_lag_seconds", "value": round(float(ingest_lag or 0.0), 2), "meta_json": "{}"},
        {"metric": "handoff_success_rate", "value": handoff_success_rate, "meta_json": "{}"},
        {"metric": "retry_depth", "value": float(retry_depth), "meta_json": "{}"},
        {"metric": "retry_queue_size", "value": float(retry_queue_size), "meta_json": "{}"},
        {"metric": "quarantine_size", "value": float(quarantine_size), "meta_json": "{}"},
        {"metric": "lock_busy_24h", "value": float(lock_busy_24h), "meta_json": "{}"},
        {"metric": "lock_release_missed_24h", "value": float(lock_release_missed_24h), "meta_json": "{}"},
        {
            "metric": "precision_high_band",
            "value": high_precision,
            "meta_json": json.dumps({"sample_size": high_sample}, ensure_ascii=True),
        },
        {
            "metric": "precision_medium_band",
            "value": medium_precision,
            "meta_json": json.dumps({"sample_size": medium_sample}, ensure_ascii=True),
        },
    ]

    db.replace_ops_metrics(conn, run_date_str, metric_rows)
    ops_count = csv_exporter.export_ops_metrics(
        conn, run_date_str, settings.out_dir / f"ops_metrics_{run_date.strftime('%Y%m%d')}.csv"
    )

    if high_sample > 0 and high_precision < settings.alert_min_high_precision:
        send_alert(
            settings,
            title="High-band precision degraded",
            body=(
                f"run_date={run_date_str} high_precision={high_precision} "
                f"threshold={settings.alert_min_high_precision} sample_size={high_sample}"
            ),
            severity="warn",
        )
    if medium_sample > 0 and medium_precision < settings.alert_min_medium_precision:
        send_alert(
            settings,
            title="Medium-band precision degraded",
            body=(
                f"run_date={run_date_str} medium_precision={medium_precision} "
                f"threshold={settings.alert_min_medium_precision} sample_size={medium_sample}"
            ),
            severity="warn",
        )

    return {
        "ops_metrics_rows": int(ops_count),
        "retry_depth": int(retry_depth),
        "retry_queue_size": int(retry_queue_size),
        "quarantine_size": int(quarantine_size),
        "lock_busy_24h": int(lock_busy_24h),
        "lock_release_missed_24h": int(lock_release_missed_24h),
        "handoff_success_rate": float(handoff_success_rate),
    }
