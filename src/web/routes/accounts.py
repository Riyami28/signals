"""Accounts API routes."""

from __future__ import annotations

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
    db.init_db(conn)
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
