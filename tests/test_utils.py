from datetime import date
from pathlib import Path

from src.utils import load_csv_rows, normalize_domain, parse_date, stable_hash


def test_normalize_domain_strips_protocol_and_path():
    assert normalize_domain("https://www.Example.com/path") == "example.com"


def test_stable_hash_is_deterministic():
    payload = {"b": 2, "a": 1}
    first = stable_hash(payload, prefix="x")
    second = stable_hash(payload, prefix="x")
    assert first == second


def test_parse_date_accepts_timezone_name():
    assert parse_date("2026-02-16", "America/Los_Angeles") == date(2026, 2, 16)


def test_load_csv_rows_tolerates_extra_columns(tmp_path: Path):
    path = tmp_path / "rows.csv"
    path.write_text("a,b\n1,2,3\n", encoding="utf-8")
    rows = load_csv_rows(path)
    assert rows == [{"a": "1", "b": "2"}]
