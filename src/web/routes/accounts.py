"""Accounts API routes."""

from __future__ import annotations

import json
import re

from fastapi import APIRouter, HTTPException, Query

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
    """Return paginated signal timeline for an account with optional filters."""
    conn = _get_conn()
    try:
        if not db.account_exists(conn, account_id):
            return {"error": "not found"}, 404
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
