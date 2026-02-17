from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from src.export.csv_exporter import output_paths
from src.settings import Settings


def _require_google_config(settings: Settings) -> None:
    if not settings.google_sheet_id:
        raise RuntimeError("GOOGLE_SHEETS_SPREADSHEET_ID is required for sync-sheet")
    if not settings.google_service_account_file:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_FILE is required for sync-sheet")
    if not settings.google_service_account_file.exists():
        raise RuntimeError(f"Google service account file does not exist: {settings.google_service_account_file}")


def _read_csv(path: Path) -> tuple[list[str], list[list[str]]]:
    if not path.exists():
        return [], []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        rows = list(reader)
    if not rows:
        return [], []
    header = [str(v) for v in rows[0]]
    values = [[str(v) for v in row] for row in rows[1:]]
    return header, values


def _get_or_create_worksheet(sheet: Any, title: str) -> Any:
    try:
        return sheet.worksheet(title)
    except Exception:
        return sheet.add_worksheet(title=title, rows=2000, cols=40)


def _overwrite_tab(sheet: Any, tab_name: str, csv_path: Path) -> int:
    header, rows = _read_csv(csv_path)
    ws = _get_or_create_worksheet(sheet, tab_name)
    ws.clear()
    if not header:
        return 0
    ws.update([header] + rows)
    return len(rows)


def _append_tab(sheet: Any, tab_name: str, csv_path: Path) -> int:
    header, rows = _read_csv(csv_path)
    if not header:
        return 0

    ws = _get_or_create_worksheet(sheet, tab_name)
    existing_header = ws.row_values(1)
    if not existing_header:
        ws.update([header])
        existing_header = header

    if existing_header != header:
        raise RuntimeError(
            f"Worksheet '{tab_name}' header mismatch. Expected {existing_header}, file has {header}."
        )

    if rows:
        ws.append_rows(rows, value_input_option="RAW")
    return len(rows)


def _append_tab_dedup_by_run_date(sheet: Any, tab_name: str, csv_path: Path, run_date: str) -> int:
    header, rows = _read_csv(csv_path)
    if not header:
        return 0

    ws = _get_or_create_worksheet(sheet, tab_name)
    existing_values = ws.get_all_values()
    if not existing_values:
        ws.update([header] + rows)
        return len(rows)

    existing_header = [str(value) for value in existing_values[0]]
    if existing_header != header:
        raise RuntimeError(
            f"Worksheet '{tab_name}' header mismatch. Expected {existing_header}, file has {header}."
        )

    run_date_idx = header.index("run_date") if "run_date" in header else -1
    merged_rows: list[list[str]] = []
    for row in existing_values[1:]:
        normalized_row = [str(value) for value in row]
        if run_date_idx >= 0 and len(normalized_row) > run_date_idx and normalized_row[run_date_idx] == run_date:
            continue
        merged_rows.append(normalized_row)

    merged_rows.extend(rows)
    ws.clear()
    ws.update([header] + merged_rows)
    return len(rows)


def sync_outputs(settings: Settings, run_date) -> dict[str, int]:
    _require_google_config(settings)

    from google.oauth2.service_account import Credentials
    import gspread

    creds = Credentials.from_service_account_file(
        str(settings.google_service_account_file),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(settings.google_sheet_id)

    paths = output_paths(settings.out_dir, run_date)
    run_date_str = run_date.isoformat()

    review_rows = _overwrite_tab(sheet, "review_queue", paths["review_queue"])
    score_rows = _append_tab_dedup_by_run_date(sheet, "daily_scores", paths["daily_scores"], run_date_str)
    quality_rows = _append_tab_dedup_by_run_date(sheet, "source_quality", paths["source_quality"], run_date_str)

    _get_or_create_worksheet(sheet, "review_input")

    return {
        "review_queue_rows": review_rows,
        "daily_scores_rows": score_rows,
        "source_quality_rows": quality_rows,
    }
