from __future__ import annotations

import os
import re
from urllib.parse import urlparse, urlunparse

import pytest

from src import db

try:
    import psycopg
except Exception:  # pragma: no cover - environment-specific.
    psycopg = None  # type: ignore[assignment]


def _default_test_dsn() -> str:
    return os.getenv(
        "SIGNALS_TEST_PG_DSN",
        "postgresql://signals:signals_dev_password@127.0.0.1:55432/signals_test",
    )


def _ensure_test_database_exists(pg_dsn: str) -> None:
    if psycopg is None:
        raise RuntimeError("psycopg is required for Postgres tests.")

    parsed = urlparse(pg_dsn)
    db_name = parsed.path.lstrip("/")
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", db_name):
        raise RuntimeError(f"Unsafe test database name: {db_name!r}")

    admin_db = os.getenv("SIGNALS_TEST_PG_ADMIN_DB", "postgres").strip() or "postgres"
    admin_parsed = parsed._replace(path=f"/{admin_db}")
    admin_dsn = urlunparse(admin_parsed)

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        row = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s LIMIT 1", (db_name,)).fetchone()
        if row is None:
            conn.execute(f"CREATE DATABASE {db_name}")


def _clear_all_tables(conn) -> None:
    rows = conn.execute("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()").fetchall()
    safe_names: list[str] = []
    for row in rows:
        name = str(row.get("tablename", "")).strip()
        if not name:
            continue
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
            continue
        safe_names.append(name)
    if safe_names:
        conn.execute(f"TRUNCATE TABLE {', '.join(sorted(safe_names))} RESTART IDENTITY CASCADE")
    conn.commit()


@pytest.fixture(autouse=True)
def postgres_test_isolation(monkeypatch: pytest.MonkeyPatch):
    pg_dsn = _default_test_dsn()
    monkeypatch.setenv("SIGNALS_PG_DSN", pg_dsn)
    monkeypatch.delenv("SIGNALS_DB_PATH", raising=False)

    try:
        _ensure_test_database_exists(pg_dsn)
        conn = db.get_connection(pg_dsn)
        db.init_db(conn)
        _clear_all_tables(conn)
        conn.close()
    except Exception as exc:  # pragma: no cover - environment-specific.
        pytest.skip(f"Postgres test environment unavailable: {exc}")

    yield

    conn = db.get_connection(pg_dsn)
    _clear_all_tables(conn)
    conn.close()
