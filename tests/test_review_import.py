from __future__ import annotations

from datetime import date
from pathlib import Path

from src import db
from src.review.import_reviews import import_reviews_for_date
from src.settings import load_settings


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_import_reviews_is_idempotent_when_created_at_missing(tmp_path: Path):
    root = tmp_path / "signals"
    _write(
        root / "config" / "seed_accounts.csv",
        "company_name,domain,source_type\nAcme,acme.example,seed\n",
    )

    settings = load_settings(project_root=root)
    conn = db.get_connection(settings.db_path)
    db.init_db(conn)
    db.seed_accounts(conn, settings.seed_accounts_path)

    account = db.get_account_by_domain(conn, "acme.example")
    assert account is not None
    account_id = str(account["account_id"])

    run_date = "2026-02-16"
    run_id = db.create_score_run(conn, run_date)
    db.finish_score_run(conn, run_id, status="completed")

    _write(
        root / "data" / "raw" / "review_input.csv",
        "run_date,account_id,decision,reviewer,notes,created_at\n"
        f"{run_date},{account_id},approved,tester,ok,\n",
    )

    first = import_reviews_for_date(conn, settings, date(2026, 2, 16))
    second = import_reviews_for_date(conn, settings, date(2026, 2, 16))

    count = conn.execute("SELECT COUNT(*) AS n FROM review_labels").fetchone()["n"]
    assert first == 1
    assert second == 0
    assert int(count) == 1
