from __future__ import annotations

import logging
import os
from pathlib import Path

from .schema import SCHEMA_SQL

logger = logging.getLogger(__name__)

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - psycopg may be absent in lightweight envs.
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


def _is_integrity_error(exc: Exception) -> bool:
    return bool(psycopg is not None and isinstance(exc, psycopg.IntegrityError))


def get_connection(pg_dsn: str | Path | None = None):
    if psycopg is None:
        raise RuntimeError("psycopg is required. Install dependencies and ensure postgres driver is available.")
    dsn = str(pg_dsn or "").strip()
    # Allow legacy callsites that still pass a local file path; postgres is mandatory now.
    if "://" not in dsn:
        dsn = os.getenv("SIGNALS_PG_DSN", "").strip()
    if not dsn:
        host = os.getenv("SIGNALS_PG_HOST", "127.0.0.1").strip()
        port = os.getenv("SIGNALS_PG_PORT", "55432").strip()
        user = os.getenv("SIGNALS_PG_USER", "signals").strip()
        password = os.getenv("SIGNALS_PG_PASSWORD", "signals_dev_password").strip()
        database = os.getenv("SIGNALS_PG_DB", "signals").strip()
        dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    if not dsn:
        raise ValueError("Postgres DSN is required. Set SIGNALS_PG_DSN or SIGNALS_PG_* environment variables.")
    conn = psycopg.connect(dsn, row_factory=dict_row, autocommit=False)
    conn.execute("SET search_path = signals, public")
    return conn


def init_db(conn) -> None:
    conn.execute(SCHEMA_SQL)
    _run_column_migrations(conn)
    conn.commit()


# ---------------------------------------------------------------------------
# Versioned migration system
# ---------------------------------------------------------------------------


def _migration_dir() -> Path:
    """Return the migrations/ directory relative to this file's package root."""
    return Path(__file__).parent.parent / "migrations"


def _ensure_schema_version_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
          version     INTEGER PRIMARY KEY,
          description TEXT    NOT NULL,
          applied_at  TEXT    NOT NULL
        )
        """
    )


def _applied_versions(conn) -> set[int]:
    _ensure_schema_version_table(conn)
    rows = conn.execute("SELECT version FROM schema_version ORDER BY version").fetchall()
    return {int(r["version"]) for r in rows}


def run_migrations(conn) -> list[int]:
    """Apply any unapplied numbered SQL files from migrations/.

    Returns the list of newly applied version numbers.
    """
    migrations_path = _migration_dir()
    if not migrations_path.is_dir():
        logger.warning("migrations/ directory not found at %s — skipping", migrations_path)
        return []

    applied = _applied_versions(conn)
    sql_files = sorted(migrations_path.glob("*.sql"))
    newly_applied: list[int] = []

    for sql_file in sql_files:
        # Expect filenames like: 001_initial_schema.sql
        stem = sql_file.stem
        try:
            version = int(stem.split("_")[0])
        except (ValueError, IndexError):
            logger.warning("Skipping migration file with unexpected name: %s", sql_file.name)
            continue

        if version in applied:
            continue

        logger.info("Applying migration %d from %s", version, sql_file.name)
        sql = sql_file.read_text(encoding="utf-8")
        conn.execute(sql)
        conn.execute(
            "INSERT INTO schema_version (version, description, applied_at) VALUES (%s, %s, NOW()::TEXT)"
            " ON CONFLICT (version) DO NOTHING",
            (version, stem),
        )
        conn.commit()
        newly_applied.append(version)
        logger.info("Migration %d applied", version)

    return newly_applied


def _column_exists(conn, table: str, column: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (table, column),
    ).fetchone()
    return row is not None


def _ensure_column(conn, table: str, column: str, ddl_fragment: str) -> None:
    if _column_exists(conn, table, column):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_fragment}")


def _run_column_migrations(conn) -> None:
    """Backfill legacy columns for databases created before SCHEMA_SQL was updated."""
    _ensure_column(conn, "signal_observations", "document_id", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "signal_observations", "mention_id", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "signal_observations", "evidence_sentence", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "signal_observations", "evidence_sentence_en", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "signal_observations", "matched_phrase", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "signal_observations", "language", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "signal_observations", "speaker_name", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "signal_observations", "speaker_role", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "signal_observations", "evidence_quality", "REAL NOT NULL DEFAULT 0.0")
    _ensure_column(conn, "signal_observations", "relevance_score", "REAL NOT NULL DEFAULT 0.0")
    _ensure_column(conn, "account_scores", "dimension_scores_json", "TEXT NOT NULL DEFAULT '{}'")

    _ensure_column(conn, "external_discovery_events", "entry_url", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "external_discovery_events", "url_type", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "external_discovery_events", "language_hint", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "external_discovery_events", "author_hint", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "external_discovery_events", "published_at_hint", "TEXT NOT NULL DEFAULT ''")
