from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from typing import Any

from src import db
from src.utils import write_csv_rows


def date_suffix(run_date: date) -> str:
    return run_date.strftime("%Y%m%d")


def output_paths(out_dir: Path, run_date: date) -> dict[str, Path]:
    suffix = date_suffix(run_date)
    return {
        "review_queue": out_dir / f"review_queue_{suffix}.csv",
        "daily_scores": out_dir / f"daily_scores_{suffix}.csv",
        "source_quality": out_dir / f"source_quality_{suffix}.csv",
        "promotion_readiness": out_dir / f"promotion_readiness_{suffix}.csv",
    }


def _parse_reasons(top_reasons_json: str) -> list[dict[str, Any]]:
    if not top_reasons_json:
        return []
    try:
        parsed = json.loads(top_reasons_json)
        if isinstance(parsed, list):
            return [row for row in parsed if isinstance(row, dict)]
    except json.JSONDecodeError:
        return []
    return []


def export_daily_scores(conn, run_id: str, output_path: Path) -> int:
    rows = db.fetch_scores_for_run(conn, run_id)
    export_rows: list[dict[str, Any]] = []
    for row in rows:
        export_rows.append(
            {
                "run_date": row["run_date"],
                "account_id": row["account_id"],
                "company_name": row["company_name"],
                "domain": row["domain"],
                "product": row["product"],
                "score": row["score"],
                "tier": row["tier"],
                "delta_7d": row["delta_7d"],
                "top_reasons_json": row["top_reasons_json"],
            }
        )

    write_csv_rows(
        output_path,
        export_rows,
        fieldnames=[
            "run_date",
            "account_id",
            "company_name",
            "domain",
            "product",
            "score",
            "tier",
            "delta_7d",
            "top_reasons_json",
        ],
    )
    return len(export_rows)


def export_review_queue(conn, run_id: str, output_path: Path) -> int:
    rows = db.fetch_scores_for_run(conn, run_id)
    queue_rows: list[dict[str, Any]] = []

    for row in rows:
        tier = str(row["tier"]).lower()
        if tier not in {"high", "medium"}:
            continue

        reasons = _parse_reasons(str(row["top_reasons_json"] or ""))
        reason_1 = reasons[0]["signal_code"] if len(reasons) > 0 else ""
        reason_2 = reasons[1]["signal_code"] if len(reasons) > 1 else ""
        reason_3 = reasons[2]["signal_code"] if len(reasons) > 2 else ""

        links = []
        for reason in reasons:
            url = str(reason.get("evidence_url", "")).strip()
            if url:
                links.append(url)

        queue_rows.append(
            {
                "run_date": row["run_date"],
                "account_id": row["account_id"],
                "company_name": row["company_name"],
                "domain": row["domain"],
                "product": row["product"],
                "score": row["score"],
                "tier": row["tier"],
                "top_reason_1": reason_1,
                "top_reason_2": reason_2,
                "top_reason_3": reason_3,
                "evidence_links": " | ".join(links),
            }
        )

    write_csv_rows(
        output_path,
        queue_rows,
        fieldnames=[
            "run_date",
            "account_id",
            "company_name",
            "domain",
            "product",
            "score",
            "tier",
            "top_reason_1",
            "top_reason_2",
            "top_reason_3",
            "evidence_links",
        ],
    )
    return len(queue_rows)


def export_source_quality(conn, run_date: str, output_path: Path) -> int:
    rows = db.fetch_source_metrics(conn, run_date)
    export_rows: list[dict[str, Any]] = []
    for row in rows:
        export_rows.append(
            {
                "run_date": row["run_date"],
                "source": row["source"],
                "approved_rate": row["approved_rate"],
                "sample_size": row["sample_size"],
            }
        )

    write_csv_rows(
        output_path,
        export_rows,
        fieldnames=["run_date", "source", "approved_rate", "sample_size"],
    )
    return len(export_rows)


def export_promotion_readiness(rows: list[dict[str, Any]], output_path: Path) -> int:
    write_csv_rows(
        output_path,
        rows,
        fieldnames=[
            "run_date",
            "window",
            "approved_rate",
            "sample_size",
            "meets_rate",
            "meets_sample",
            "ready_for_promotion",
        ],
    )
    return len(rows)
