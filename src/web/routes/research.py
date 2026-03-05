"""Research API routes."""

from __future__ import annotations

import json
from fastapi import APIRouter, HTTPException, Query

from src import db
from src.export.dossier import render_dossier
from src.settings import load_settings

router = APIRouter(tags=["research"])


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


@router.get("/research/{account_id}")
def get_research(account_id: str):
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
    try:
        research = db.get_company_research(conn, account_id)
        contacts = db.get_contacts_for_account(conn, account_id)
        # Serialize
        _serialize_dates(research)
        _serialize_dates(contacts)
        return {
            "research": research,
            "contacts": contacts,
        }
    finally:
        conn.close()


@router.get("/accounts/{account_id}/dossier")
def get_account_dossier(
    account_id: str,
    refresh: bool = Query(False),
):
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
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
