"""Labels API routes."""

from __future__ import annotations

import re

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

from src import db
from src.settings import load_settings

router = APIRouter(tags=["labels"])

_MAX_LABEL_LENGTH = 100
_MAX_NOTES_LENGTH = 500


class LabelCreate(BaseModel):
    account_id: str
    label: str
    notes: str = ""

    @field_validator("label")
    @classmethod
    def validate_label(cls, v: str) -> str:
        cleaned = re.sub(r"[\x00-\x1f\x7f]", "", v).strip()
        if not cleaned:
            raise ValueError("label must not be empty")
        if len(cleaned) > _MAX_LABEL_LENGTH:
            raise ValueError(f"label must be at most {_MAX_LABEL_LENGTH} characters")
        return cleaned

    @field_validator("notes")
    @classmethod
    def validate_notes(cls, v: str) -> str:
        cleaned = re.sub(r"[\x00-\x1f\x7f]", "", v).strip()
        return cleaned[:_MAX_NOTES_LENGTH]

    @field_validator("account_id")
    @classmethod
    def validate_account_id(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("account_id must not be empty")
        return stripped


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
    if not label_id.strip():
        raise HTTPException(status_code=400, detail="label_id must not be empty")
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
