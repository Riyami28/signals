from __future__ import annotations

from pathlib import Path

from src.sync.google_sheets import _append_tab_dedup_by_run_date


class _FakeWorksheet:
    def __init__(self, values: list[list[str]] | None = None):
        self._values = values or []

    def get_all_values(self):
        return [list(row) for row in self._values]

    def clear(self):
        self._values = []

    def update(self, values):
        self._values = [[str(v) for v in row] for row in values]


class _FakeSheet:
    def __init__(self, worksheets: dict[str, _FakeWorksheet]):
        self._worksheets = worksheets

    def worksheet(self, title: str):
        if title not in self._worksheets:
            raise RuntimeError("missing worksheet")
        return self._worksheets[title]

    def add_worksheet(self, title: str, rows: int, cols: int):
        del rows, cols
        ws = _FakeWorksheet()
        self._worksheets[title] = ws
        return ws


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_append_tab_dedup_by_run_date_replaces_matching_date_rows(tmp_path: Path):
    csv_path = tmp_path / "daily_scores.csv"
    _write(
        csv_path,
        "run_date,account_id,score\n2026-02-16,acc_new,90\n",
    )
    sheet = _FakeSheet(
        {
            "daily_scores": _FakeWorksheet(
                [
                    ["run_date", "account_id", "score"],
                    ["2026-02-15", "acc_old", "50"],
                    ["2026-02-16", "acc_prev", "70"],
                ]
            )
        }
    )

    inserted = _append_tab_dedup_by_run_date(sheet, "daily_scores", csv_path, "2026-02-16")

    assert inserted == 1
    assert sheet.worksheet("daily_scores").get_all_values() == [
        ["run_date", "account_id", "score"],
        ["2026-02-15", "acc_old", "50"],
        ["2026-02-16", "acc_new", "90"],
    ]
