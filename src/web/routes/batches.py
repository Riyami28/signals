"""Batch results API."""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from src import db
from src.settings import load_settings

logger = logging.getLogger(__name__)

router = APIRouter(tags=["batches"])


def _get_conn():
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
    return conn


def _serialize(obj):
    """Convert non-serializable types for JSON output."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


@router.get("/batches/{batch_id}")
def get_batch(batch_id: str):
    """Return batch metadata and status."""
    conn = _get_conn()
    try:
        batch = db.get_upload_batch(conn, batch_id)
        if not batch:
            raise HTTPException(status_code=404, detail=f"Batch {batch_id} not found")
        row = dict(batch)
        return {k: _serialize(v) for k, v in row.items()}
    finally:
        conn.close()


@router.get("/batches/{batch_id}/results")
def get_batch_results(batch_id: str, format: str = "json"):
    """Return scored accounts for a batch.

    Query params:
        format: "json" (default) or "csv"
    """
    conn = _get_conn()
    try:
        batch = db.get_upload_batch(conn, batch_id)
        if not batch:
            raise HTTPException(status_code=404, detail=f"Batch {batch_id} not found")

        results = db.get_batch_results(conn, batch_id)

        if format == "csv":
            if not results:
                raise HTTPException(status_code=404, detail="No results available yet")
            output = io.StringIO()
            fieldnames = [
                "company_name",
                "domain",
                "industry",
                "employee_count",
                "account_id",
                "score",
                "tier",
                "product",
                "top_reasons_json",
                "delta_7d",
            ]
            writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in results:
                writer.writerow({k: row.get(k, "") for k in fieldnames})

            csv_content = output.getvalue()
            return StreamingResponse(
                iter([csv_content]),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename=batch_{batch_id}_results.csv"},
            )

        serialized = []
        for row in results:
            serialized.append({k: _serialize(v) for k, v in row.items()})
        return {
            "batch_id": batch_id,
            "status": batch["status"],
            "count": len(serialized),
            "results": serialized,
        }
    finally:
        conn.close()
