"""Labels API routes."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from src import db
from src.settings import load_settings

router = APIRouter(tags=["labels"])


class LabelCreate(BaseModel):
    account_id: str
    label: str
    notes: str = ""


def _get_conn():
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
    db.init_db(conn)
    return conn


@router.post("/labels")
def create_label(body: LabelCreate):
    conn = _get_conn()
    try:
        label_id = db.insert_account_label(
            conn,
            account_id=body.account_id,
            label=body.label,
            notes=body.notes,
        )
        return {"label_id": label_id}
    finally:
        conn.close()


@router.delete("/labels/{label_id}")
def remove_label(label_id: str):
    conn = _get_conn()
    try:
        db.delete_account_label(conn, label_id)
        return {"deleted": True}
    finally:
        conn.close()


@router.get("/labels/{account_id}")
def get_labels(account_id: str):
    conn = _get_conn()
    try:
        labels = db.get_labels_for_account(conn, account_id)
        for label in labels:
            for k, v in label.items():
                if hasattr(v, "isoformat"):
                    label[k] = v.isoformat()
        return {"labels": labels}
    finally:
        conn.close()
