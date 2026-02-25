from __future__ import annotations

import csv
from datetime import date
import logging

from src import db
from src.models import ReviewLabel
from src.settings import Settings
from src.utils import load_csv_rows, parse_datetime, stable_hash

logger = logging.getLogger(__name__)

VALID_DECISIONS = {"approved", "rejected", "needs_more_info"}
REVIEW_INPUT_FIELDS = ["run_date", "account_id", "decision", "reviewer", "notes", "created_at"]


def _normalize_created_at(value: str, run_date_str: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return f"{run_date_str}T00:00:00+00:00"
    try:
        parsed = parse_datetime(raw).replace(microsecond=0)
        return parsed.isoformat()
    except Exception:
        logger.debug("failed to parse created_at=%s", raw, exc_info=True)
        return f"{run_date_str}T00:00:00+00:00"


def _read_rows_from_local_csv(settings: Settings) -> list[dict[str, str]]:
    path = settings.raw_dir / "review_input.csv"
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [{k: (v or "").strip() for k, v in row.items()} for row in reader]


def _write_rows_to_local_csv(settings: Settings, rows: list[dict[str, str]]) -> None:
    path = settings.raw_dir / "review_input.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REVIEW_INPUT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: (row.get(field, "") or "").strip() for field in REVIEW_INPUT_FIELDS})


def prepare_review_input_for_date(settings: Settings, run_date: date) -> int:
    run_date_str = run_date.isoformat()
    queue_path = settings.out_dir / f"review_queue_{run_date.strftime('%Y%m%d')}.csv"
    queue_rows = load_csv_rows(queue_path)

    local_rows = _read_rows_from_local_csv(settings)
    normalized_rows: list[dict[str, str]] = []
    existing_keys: set[tuple[str, str]] = set()

    for row in local_rows:
        normalized = {field: (row.get(field, "") or "").strip() for field in REVIEW_INPUT_FIELDS}
        if not normalized["run_date"] or not normalized["account_id"]:
            continue
        key = (normalized["run_date"], normalized["account_id"])
        if key in existing_keys:
            continue
        existing_keys.add(key)
        normalized_rows.append(normalized)

    inserted = 0
    for queue_row in queue_rows:
        account_id = (queue_row.get("account_id", "") or "").strip()
        if not account_id:
            continue
        key = (run_date_str, account_id)
        if key in existing_keys:
            continue
        normalized_rows.append(
            {
                "run_date": run_date_str,
                "account_id": account_id,
                "decision": "",
                "reviewer": "",
                "notes": "",
                "created_at": "",
            }
        )
        existing_keys.add(key)
        inserted += 1

    normalized_rows.sort(key=lambda row: (row["run_date"], row["account_id"]))
    _write_rows_to_local_csv(settings, normalized_rows)
    return inserted


def _read_rows_from_google_sheet(settings: Settings) -> list[dict[str, str]]:
    if not settings.google_sheet_id or not settings.google_service_account_file:
        return []
    if not settings.google_service_account_file.exists():
        return []

    from google.oauth2.service_account import Credentials
    import gspread

    creds = Credentials.from_service_account_file(
        str(settings.google_service_account_file),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(settings.google_sheet_id)

    try:
        ws = sheet.worksheet("review_input")
    except Exception:
        logger.warning("failed to read review_input worksheet from Google Sheets", exc_info=True)
        return []

    rows = ws.get_all_records()
    normalized: list[dict[str, str]] = []
    for row in rows:
        normalized.append({str(k).strip(): str(v).strip() for k, v in row.items()})
    return normalized


def import_reviews_for_date(conn, settings: Settings, run_date: date) -> int:
    run_id = db.get_latest_run_id_for_date(conn, run_date.isoformat())
    if not run_id:
        return 0

    rows = _read_rows_from_google_sheet(settings)
    if not rows:
        rows = _read_rows_from_local_csv(settings)

    inserted = 0
    run_date_str = run_date.isoformat()

    for row in rows:
        row_date = row.get("run_date", "")
        if row_date != run_date_str:
            continue

        account_id = row.get("account_id", "")
        if not account_id or not db.account_exists(conn, account_id):
            continue

        decision = (row.get("decision", "") or "").strip().lower()
        if decision not in VALID_DECISIONS:
            continue

        reviewer = row.get("reviewer", "") or "unknown"
        notes = row.get("notes", "")
        created_at = _normalize_created_at(row.get("created_at", ""), run_date_str)

        review_id = stable_hash(
            {
                "run_id": run_id,
                "account_id": account_id,
                "decision": decision,
                "reviewer": reviewer,
                "created_at": created_at,
            },
            prefix="rev",
        )

        label = ReviewLabel(
            review_id=review_id,
            run_id=run_id,
            account_id=account_id,
            decision=decision,
            reviewer=reviewer,
            notes=notes,
            created_at=created_at,
        )
        if db.insert_review_label(conn, label):
            inserted += 1

    return inserted
