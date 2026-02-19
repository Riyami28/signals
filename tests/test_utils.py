from datetime import date
from pathlib import Path

from src.utils import classify_text, load_csv_rows, normalize_domain, parse_date, stable_hash


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


def test_classify_text_matches_keyword_on_word_boundaries():
    rows = [{"signal_code": "cost_reduction_mandate", "keyword": "cost transformation office", "confidence": "0.9"}]
    matches = classify_text("Board approved the cost transformation office plan.", rows)
    assert matches
    assert matches[0][0] == "cost_reduction_mandate"


def test_classify_text_does_not_match_partial_inside_larger_word():
    rows = [{"signal_code": "cost_reduction_mandate", "keyword": "erp", "confidence": "0.9"}]
    matches = classify_text("This describes a sharperpops migration.", rows)
    assert matches == []
