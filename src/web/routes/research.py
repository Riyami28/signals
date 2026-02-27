"""Research API routes."""

from __future__ import annotations

from fastapi import APIRouter

from src import db
from src.settings import load_settings

router = APIRouter(tags=["research"])


@router.get("/research/{account_id}")
def get_research(account_id: str):
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
    try:
        research = db.get_company_research(conn, account_id)
        contacts = db.get_contacts_for_account(conn, account_id)
        # Serialize
        if research:
            for k, v in research.items():
                if hasattr(v, "isoformat"):
                    research[k] = v.isoformat()
        for c in contacts:
            for k, v in c.items():
                if hasattr(v, "isoformat"):
                    c[k] = v.isoformat()
        return {
            "research": research,
            "contacts": contacts,
        }
    finally:
        conn.close()
