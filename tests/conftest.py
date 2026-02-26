from __future__ import annotations

import json
import os
import re
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import pytest

from src import db
from src.models import SignalObservation
from src.utils import utc_now_iso

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


# ---------------------------------------------------------------------------
# Factory helpers — reusable across all test files
# ---------------------------------------------------------------------------


def make_account(conn, domain: str = "acme.com", company_name: str = "Acme Inc", source_type: str = "seed") -> str:
    """Insert a test account and return account_id."""
    return db.upsert_account(conn, company_name=company_name, domain=domain, source_type=source_type)


def make_observation(
    conn,
    account_id: str,
    signal_code: str = "devops_role_open",
    source: str = "jobs_greenhouse",
    confidence: float = 0.8,
    source_reliability: float = 0.9,
    product: str = "shared",
    evidence_url: str = "https://example.com/evidence",
    evidence_text: str = "Test evidence",
) -> bool:
    """Insert a test signal observation. Returns True if inserted."""
    from src.utils import stable_hash

    observed_at = utc_now_iso()
    payload = {"test": True, "signal_code": signal_code, "account_id": account_id}
    raw_hash = stable_hash(payload, prefix="raw")
    obs_id = stable_hash(
        {
            "account_id": account_id,
            "signal_code": signal_code,
            "source": source,
            "observed_at": observed_at,
            "raw": raw_hash,
        },
        prefix="obs",
    )
    obs = SignalObservation(
        obs_id=obs_id,
        account_id=account_id,
        signal_code=signal_code,
        product=product,
        source=source,
        observed_at=observed_at,
        evidence_url=evidence_url,
        evidence_text=evidence_text,
        confidence=confidence,
        source_reliability=source_reliability,
        raw_payload_hash=raw_hash,
    )
    return db.insert_signal_observation(conn, obs)


def make_score_run(conn, run_date: str = "2026-02-25") -> str:
    """Create a score run and return run_id."""
    return db.create_score_run(conn, run_date)


def make_scores(conn, run_id: str, account_id: str, product: str = "zopdev", score: float = 25.0, tier: str = "high"):
    """Insert component + account scores for testing."""
    from src.models import AccountScore, ComponentScore

    comp = ComponentScore(
        run_id=run_id, account_id=account_id, product=product, signal_code="devops_role_open", component_score=score
    )
    reasons = [
        {
            "signal_code": "devops_role_open",
            "component_score": score,
            "source": "jobs_greenhouse",
            "evidence_url": "https://example.com",
        }
    ]
    acct_score = AccountScore(
        run_id=run_id,
        account_id=account_id,
        product=product,
        score=score,
        tier=tier,
        top_reasons_json=json.dumps(reasons),
        delta_7d=2.0,
    )
    db.replace_run_scores(conn, run_id, [comp], [acct_score])
