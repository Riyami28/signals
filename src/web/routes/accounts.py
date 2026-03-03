"""Accounts API routes."""

from __future__ import annotations

import csv
import io
import json
import re

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from src import db
from src.export.dossier import render_dossier
from src.settings import load_settings

router = APIRouter(tags=["accounts"])

_ALLOWED_SORT_FIELDS = {"score", "company_name", "domain", "tier"}
_ALLOWED_SORT_DIRS = {"asc", "desc"}
_ALLOWED_TIERS = {"", "high", "medium", "low", "explore"}
_MAX_SEARCH_LENGTH = 200


def _get_conn():
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
    return conn


# Cache signal registry metadata for timeline enrichment
_signal_meta_cache: dict[str, dict] | None = None


def _get_signal_meta() -> dict[str, dict]:
    """Load signal registry as a lookup: signal_code → {dimension, category, base_weight}."""
    global _signal_meta_cache  # noqa: PLW0603
    if _signal_meta_cache is not None:
        return _signal_meta_cache

    settings = load_settings()
    meta: dict[str, dict] = {}
    path = settings.signal_registry_path
    if path.exists():
        with path.open("r", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                code = (row.get("signal_code") or "").strip()
                if not code:
                    continue
                try:
                    weight = int(row.get("base_weight", 0))
                except (ValueError, TypeError):
                    weight = 0
                meta[code] = {
                    "dimension": (row.get("dimension") or "").strip(),
                    "category": (row.get("category") or "").strip(),
                    "base_weight": weight,
                }
    _signal_meta_cache = meta
    return meta


def _sanitize_search(q: str) -> str:
    """Strip control characters and truncate to max length."""
    cleaned = re.sub(r"[\x00-\x1f\x7f]", "", q).strip()
    return cleaned[:_MAX_SEARCH_LENGTH]


@router.get("/accounts")
def list_accounts(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    sort: str = Query("score"),
    dir: str = Query("desc"),
    tier: str = Query(""),
    label: str = Query(""),
    q: str = Query(""),
):
    if sort not in _ALLOWED_SORT_FIELDS:
        raise HTTPException(status_code=400, detail=f"invalid sort field, allowed: {sorted(_ALLOWED_SORT_FIELDS)}")
    if dir.lower() not in _ALLOWED_SORT_DIRS:
        raise HTTPException(status_code=400, detail="invalid sort direction, allowed: asc, desc")
    if tier and tier.lower() not in _ALLOWED_TIERS:
        raise HTTPException(status_code=400, detail=f"invalid tier filter, allowed: {sorted(_ALLOWED_TIERS - {''})}")

    safe_search = _sanitize_search(q)
    safe_label = label.strip()[:100]

    conn = _get_conn()
    try:
        rows, total = db.get_accounts_paginated(
            conn,
            page=page,
            per_page=per_page,
            sort_by=sort,
            sort_dir=dir,
            tier_filter=tier,
            label_filter=safe_label,
            search=safe_search,
        )
        # Serialize datetimes
        for r in rows:
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
        return {
            "items": rows,
            "total": total,
            "page": page,
            "pages": max(1, (total + per_page - 1) // per_page),
        }
    finally:
        conn.close()


@router.get("/accounts/{account_id}")
def get_account(account_id: str):
    conn = _get_conn()
    try:
        detail = db.get_account_detail(conn, account_id)
        if not detail:
            return {"error": "not found"}, 404
        # Serialize datetimes
        _serialize_dates(detail)
        return detail
    finally:
        conn.close()


@router.get("/accounts/{account_id}/dossier")
def get_account_dossier(
    account_id: str,
    refresh: bool = Query(False),
):
    conn = _get_conn()
    try:
        if not db.account_exists(conn, account_id):
            raise HTTPException(status_code=404, detail="account not found")

        if not refresh:
            latest = db.get_latest_dossier(conn, account_id)
            if latest:
                _serialize_dates(latest)
                return latest

        detail = db.get_account_detail(conn, account_id)
        if not detail:
            raise HTTPException(status_code=404, detail="account not found")

        research = detail.get("research") if isinstance(detail.get("research"), dict) else {}
        enrichment: dict = {}
        enrichment_raw = str(research.get("enrichment_json", "") or "").strip()
        if enrichment_raw:
            try:
                parsed = json.loads(enrichment_raw)
                if isinstance(parsed, dict):
                    enrichment = parsed
            except json.JSONDecodeError:
                enrichment = {}

        scores = detail.get("scores") if isinstance(detail.get("scores"), list) else []
        score_row = scores[0] if scores and isinstance(scores[0], dict) else {}

        dossier = render_dossier(
            account=detail,
            research=research,
            enrichment=enrichment,
            contacts=detail.get("contacts") if isinstance(detail.get("contacts"), list) else [],
            scores=score_row,
            dimension_scores=detail.get("dimension_scores") if isinstance(detail.get("dimension_scores"), dict) else {},
            signals=detail.get("signals") if isinstance(detail.get("signals"), list) else [],
        )
        db.save_dossier(conn, dossier)
        _serialize_dates(dossier)
        return dossier
    finally:
        conn.close()


@router.get("/accounts/{account_id}/timeline")
def get_account_timeline(
    account_id: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    signal_code: str = Query(""),
    source: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
):
    """Return enriched scored timeline for an account.

    Each observation is enriched with scoring context:
    - ``dimension``: scoring dimension (trigger_intent, tech_fit, etc.)
    - ``category``: signal category (trigger_events, hiring, etc.)
    - ``base_weight``: signal importance weight from registry
    - ``component_score``: actual contribution to account score
    """
    conn = _get_conn()
    try:
        if not db.account_exists(conn, account_id):
            return {"error": "not found"}, 404

        # Load signal registry for enrichment
        signal_meta = _get_signal_meta()

        items, total = db.get_signal_timeline(
            conn,
            account_id,
            limit=limit,
            offset=offset,
            signal_code=signal_code,
            source=source,
            date_from=date_from,
            date_to=date_to,
        )

        # Enrich each item with dimension/category/weight from registry
        for item in items:
            code = item.get("signal_code", "")
            meta = signal_meta.get(code, {})
            item["dimension"] = meta.get("dimension", "")
            item["category"] = meta.get("category", "")
            item["base_weight"] = meta.get("base_weight", 0)

        _serialize_dates(items)
        return {
            "items": items,
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    finally:
        conn.close()


def _serialize_dates(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if hasattr(v, "isoformat"):
                obj[k] = v.isoformat()
            elif isinstance(v, (dict, list)):
                _serialize_dates(v)
    elif isinstance(obj, list):
        for item in obj:
            _serialize_dates(item)


# ---------------------------------------------------------------------------
# CSV Export
# ---------------------------------------------------------------------------


@router.get("/export/csv")
def export_accounts_csv(
    tier: str = Query(""),
    label: str = Query(""),
    q: str = Query(""),
):
    """Export all scored accounts as a downloadable CSV file.

    Includes: company_name, domain, score, tier, velocity_7d, velocity_14d,
    top signals, dimension breakdown, and research status.
    """
    conn = _get_conn()
    try:
        rows, total = db.get_accounts_paginated(
            conn,
            page=1,
            per_page=10000,  # Export all
            sort_by="score",
            sort_dir="desc",
            tier_filter=tier,
            label_filter=label.strip()[:100],
            search=_sanitize_search(q) if q else "",
        )

        # Build CSV in memory
        output = io.StringIO()
        fieldnames = [
            "company_name",
            "domain",
            "score",
            "tier",
            "velocity_7d",
            "velocity_14d",
            "velocity_30d",
            "signal_count",
            "top_signals",
            "research_status",
            "labels",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            # Get signal details for this account
            signals_summary = ""
            try:
                detail = db.get_account_detail(conn, str(row.get("account_id", "")))
                if detail and detail.get("signals"):
                    signal_list = detail["signals"]
                    # Filter out internal:// signals
                    real_signals = [
                        s for s in signal_list if not str(s.get("evidence_url", "")).startswith("internal://")
                    ]
                    signal_codes = [s.get("signal_code", "") for s in real_signals[:5]]
                    signals_summary = "; ".join(signal_codes)
            except Exception:
                pass

            writer.writerow(
                {
                    "company_name": row.get("company_name", ""),
                    "domain": row.get("domain", ""),
                    "score": row.get("score", 0),
                    "tier": row.get("tier", ""),
                    "velocity_7d": row.get("velocity_7d", 0),
                    "velocity_14d": row.get("velocity_14d", 0),
                    "velocity_30d": row.get("velocity_30d", 0),
                    "signal_count": row.get("signal_count", 0),
                    "top_signals": signals_summary,
                    "research_status": row.get("research_status", ""),
                    "labels": row.get("labels", ""),
                }
            )

        csv_content = output.getvalue()
        output.close()

        return StreamingResponse(
            io.BytesIO(csv_content.encode("utf-8")),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=signals_export.csv"},
        )
    finally:
        conn.close()
