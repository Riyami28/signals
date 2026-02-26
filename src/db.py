from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import date
from pathlib import Path
from typing import Any

from src.models import (
    Account,
    AccountScore,
    ComponentScore,
    ReviewLabel,
    SignalObservation,
)
from src.utils import load_csv_rows, normalize_domain, stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - psycopg may be absent in lightweight envs.
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


def _is_integrity_error(exc: Exception) -> bool:
    return bool(psycopg is not None and isinstance(exc, psycopg.IntegrityError))


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS accounts (
  account_id TEXT PRIMARY KEY,
  company_name TEXT NOT NULL,
  domain TEXT NOT NULL UNIQUE,
  source_type TEXT NOT NULL CHECK (source_type IN ('seed', 'discovered')),
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signal_observations (
  obs_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  signal_code TEXT NOT NULL,
  product TEXT NOT NULL CHECK (product IN ('zopdev', 'zopday', 'zopnight', 'shared')),
  source TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  evidence_url TEXT,
  evidence_text TEXT,
  document_id TEXT NOT NULL DEFAULT '',
  mention_id TEXT NOT NULL DEFAULT '',
  evidence_sentence TEXT NOT NULL DEFAULT '',
  evidence_sentence_en TEXT NOT NULL DEFAULT '',
  matched_phrase TEXT NOT NULL DEFAULT '',
  language TEXT NOT NULL DEFAULT '',
  speaker_name TEXT NOT NULL DEFAULT '',
  speaker_role TEXT NOT NULL DEFAULT '',
  evidence_quality REAL NOT NULL DEFAULT 0.0,
  relevance_score REAL NOT NULL DEFAULT 0.0,
  confidence REAL NOT NULL,
  source_reliability REAL NOT NULL,
  raw_payload_hash TEXT NOT NULL,
  FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_observation_dedupe
ON signal_observations(account_id, signal_code, source, observed_at, raw_payload_hash);

CREATE INDEX IF NOT EXISTS idx_signal_observations_account_observed
ON signal_observations(account_id, observed_at);

CREATE TABLE IF NOT EXISTS score_runs (
  run_id TEXT PRIMARY KEY,
  run_date TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
  started_at TEXT NOT NULL,
  finished_at TEXT,
  error_summary TEXT
);

CREATE INDEX IF NOT EXISTS idx_score_runs_date ON score_runs(run_date);

CREATE TABLE IF NOT EXISTS score_components (
  run_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  product TEXT NOT NULL CHECK (product IN ('zopdev', 'zopday', 'zopnight')),
  signal_code TEXT NOT NULL,
  component_score REAL NOT NULL,
  PRIMARY KEY (run_id, account_id, product, signal_code),
  FOREIGN KEY(run_id) REFERENCES score_runs(run_id),
  FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE TABLE IF NOT EXISTS account_scores (
  run_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  product TEXT NOT NULL CHECK (product IN ('zopdev', 'zopday', 'zopnight')),
  score REAL NOT NULL,
  tier TEXT NOT NULL CHECK (tier IN ('high', 'medium', 'low')),
  top_reasons_json TEXT NOT NULL,
  delta_7d REAL NOT NULL,
  PRIMARY KEY (run_id, account_id, product),
  FOREIGN KEY(run_id) REFERENCES score_runs(run_id),
  FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE TABLE IF NOT EXISTS review_labels (
  review_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  decision TEXT NOT NULL CHECK (decision IN ('approved', 'rejected', 'needs_more_info')),
  reviewer TEXT NOT NULL,
  notes TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES score_runs(run_id),
  FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE INDEX IF NOT EXISTS idx_review_labels_run ON review_labels(run_id);

CREATE TABLE IF NOT EXISTS source_metrics (
  run_date TEXT NOT NULL,
  source TEXT NOT NULL,
  approved_rate REAL NOT NULL,
  sample_size INTEGER NOT NULL,
  PRIMARY KEY (run_date, source)
);

CREATE TABLE IF NOT EXISTS crawl_checkpoints (
  source TEXT NOT NULL,
  account_id TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  last_crawled_at TEXT NOT NULL,
  PRIMARY KEY (source, account_id, endpoint)
);

CREATE TABLE IF NOT EXISTS crawl_attempts (
  attempt_id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  account_id TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  attempted_at TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('success', 'http_error', 'exception', 'skipped')),
  error_summary TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_crawl_attempts_attempted_at ON crawl_attempts(attempted_at);
CREATE INDEX IF NOT EXISTS idx_crawl_attempts_source_attempted_at ON crawl_attempts(source, attempted_at);

CREATE TABLE IF NOT EXISTS external_discovery_events (
  event_id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  source_event_id TEXT NOT NULL,
  dedupe_key TEXT NOT NULL UNIQUE,
  observed_at TEXT NOT NULL,
  title TEXT NOT NULL,
  text TEXT NOT NULL,
  url TEXT NOT NULL,
  entry_url TEXT NOT NULL DEFAULT '',
  url_type TEXT NOT NULL DEFAULT '',
  language_hint TEXT NOT NULL DEFAULT '',
  author_hint TEXT NOT NULL DEFAULT '',
  published_at_hint TEXT NOT NULL DEFAULT '',
  company_name_hint TEXT NOT NULL,
  domain_hint TEXT NOT NULL,
  raw_payload_json TEXT NOT NULL,
  ingested_at TEXT NOT NULL,
  processing_status TEXT NOT NULL CHECK (processing_status IN ('pending', 'processed', 'failed')),
  processed_run_id TEXT NOT NULL,
  processed_at TEXT NOT NULL,
  error_summary TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_external_discovery_events_status_observed
ON external_discovery_events(processing_status, observed_at);

CREATE TABLE IF NOT EXISTS discovery_runs (
  discovery_run_id TEXT PRIMARY KEY,
  run_date TEXT NOT NULL,
  score_run_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
  source_events_processed INTEGER NOT NULL,
  observations_inserted INTEGER NOT NULL,
  total_candidates INTEGER NOT NULL,
  crm_eligible_candidates INTEGER NOT NULL,
  error_summary TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_discovery_runs_date ON discovery_runs(run_date);

CREATE TABLE IF NOT EXISTS discovery_candidates (
  discovery_run_id TEXT NOT NULL,
  score_run_id TEXT NOT NULL,
  run_date TEXT NOT NULL,
  account_id TEXT NOT NULL,
  company_name TEXT NOT NULL,
  domain TEXT NOT NULL,
  best_product TEXT NOT NULL,
  score REAL NOT NULL,
  tier TEXT NOT NULL,
  confidence_band TEXT NOT NULL CHECK (confidence_band IN ('high', 'medium', 'explore')),
  cpg_like_group_count INTEGER NOT NULL,
  primary_signal_count INTEGER NOT NULL,
  source_count INTEGER NOT NULL,
  has_poc_progression_first_party INTEGER NOT NULL,
  relationship_stage TEXT NOT NULL,
  vertical_tag TEXT NOT NULL,
  is_self INTEGER NOT NULL,
  exclude_from_crm INTEGER NOT NULL,
  eligible_for_crm INTEGER NOT NULL,
  novelty_score REAL NOT NULL,
  rank_score REAL NOT NULL,
  reasons_json TEXT NOT NULL,
  PRIMARY KEY (discovery_run_id, account_id)
);

CREATE TABLE IF NOT EXISTS discovery_evidence (
  discovery_run_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  signal_code TEXT NOT NULL,
  source TEXT NOT NULL,
  evidence_url TEXT NOT NULL,
  evidence_text TEXT NOT NULL,
  component_score REAL NOT NULL,
  PRIMARY KEY (discovery_run_id, account_id, signal_code, source, evidence_url)
);

CREATE TABLE IF NOT EXISTS crawl_frontier (
  frontier_id TEXT PRIMARY KEY,
  run_date TEXT NOT NULL,
  source TEXT NOT NULL,
  source_event_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  domain TEXT NOT NULL,
  url TEXT NOT NULL,
  canonical_url TEXT NOT NULL,
  url_type TEXT NOT NULL CHECK (url_type IN ('article', 'listing', 'profile', 'other')),
  depth INTEGER NOT NULL,
  priority REAL NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'fetched', 'parsed', 'failed', 'skipped')),
  retry_count INTEGER NOT NULL,
  max_retries INTEGER NOT NULL,
  first_seen_at TEXT NOT NULL,
  last_attempt_at TEXT NOT NULL,
  last_error TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  UNIQUE(run_date, canonical_url)
);

CREATE INDEX IF NOT EXISTS idx_crawl_frontier_status_priority
ON crawl_frontier(status, priority DESC, first_seen_at ASC);

CREATE INDEX IF NOT EXISTS idx_crawl_frontier_run_date
ON crawl_frontier(run_date);

CREATE TABLE IF NOT EXISTS documents (
  document_id TEXT PRIMARY KEY,
  frontier_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  domain TEXT NOT NULL,
  source TEXT NOT NULL,
  source_event_id TEXT NOT NULL,
  url TEXT NOT NULL,
  canonical_url TEXT NOT NULL UNIQUE,
  content_sha256 TEXT NOT NULL UNIQUE,
  title TEXT NOT NULL,
  author TEXT NOT NULL,
  published_at TEXT NOT NULL,
  section TEXT NOT NULL,
  language TEXT NOT NULL,
  body_text TEXT NOT NULL,
  body_text_en TEXT NOT NULL,
  raw_html TEXT NOT NULL,
  parser_version TEXT NOT NULL,
  evidence_quality REAL NOT NULL,
  relevance_score REAL NOT NULL,
  fetched_with TEXT NOT NULL,
  outbound_links_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_documents_account_created
ON documents(account_id, created_at DESC);

CREATE TABLE IF NOT EXISTS document_mentions (
  mention_id TEXT PRIMARY KEY,
  document_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  signal_code TEXT NOT NULL,
  matched_phrase TEXT NOT NULL,
  evidence_sentence TEXT NOT NULL,
  evidence_sentence_en TEXT NOT NULL,
  language TEXT NOT NULL,
  speaker_name TEXT NOT NULL,
  speaker_role TEXT NOT NULL,
  confidence REAL NOT NULL,
  evidence_quality REAL NOT NULL,
  relevance_score REAL NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(document_id, signal_code, matched_phrase)
);

CREATE INDEX IF NOT EXISTS idx_document_mentions_account_signal
ON document_mentions(account_id, signal_code);

CREATE TABLE IF NOT EXISTS observation_lineage (
  obs_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  mention_id TEXT NOT NULL,
  source_event_id TEXT NOT NULL,
  run_date TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_observation_lineage_run_date
ON observation_lineage(run_date);

CREATE TABLE IF NOT EXISTS people_watchlist (
  watch_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  person_name TEXT NOT NULL,
  role_title TEXT NOT NULL,
  role_weight REAL NOT NULL,
  source_url TEXT NOT NULL,
  is_active INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(account_id, person_name, role_title)
);

CREATE TABLE IF NOT EXISTS people_activity (
  activity_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  person_name TEXT NOT NULL,
  role_title TEXT NOT NULL,
  document_id TEXT NOT NULL,
  activity_type TEXT NOT NULL,
  summary TEXT NOT NULL,
  published_at TEXT NOT NULL,
  url TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(account_id, person_name, document_id, activity_type)
);

CREATE TABLE IF NOT EXISTS run_lock_events (
  event_id BIGSERIAL PRIMARY KEY,
  lock_name TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  action TEXT NOT NULL CHECK (action IN ('acquired', 'released', 'busy', 'release_missed')),
  details TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_lock_events_lock_name_created
ON run_lock_events(lock_name, created_at);

CREATE TABLE IF NOT EXISTS stage_failures (
  failure_id BIGSERIAL PRIMARY KEY,
  run_type TEXT NOT NULL,
  run_date TEXT NOT NULL,
  stage TEXT NOT NULL,
  duration_seconds REAL NOT NULL DEFAULT 0,
  timed_out INTEGER NOT NULL DEFAULT 0,
  error_summary TEXT NOT NULL DEFAULT '',
  retry_task_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stage_failures_run_date_created
ON stage_failures(run_date, created_at);

CREATE TABLE IF NOT EXISTS retry_queue (
  task_id TEXT PRIMARY KEY,
  task_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'quarantined', 'failed')),
  due_at TEXT NOT NULL,
  last_error TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_retry_queue_status_due
ON retry_queue(status, due_at);

CREATE TABLE IF NOT EXISTS quarantine_failures (
  quarantine_id BIGSERIAL PRIMARY KEY,
  task_id TEXT NOT NULL,
  task_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  error_summary TEXT NOT NULL DEFAULT '',
  quarantined_at TEXT NOT NULL,
  resolved INTEGER NOT NULL DEFAULT 0,
  resolved_at TEXT NOT NULL DEFAULT '',
  resolution_note TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_quarantine_failures_resolved
ON quarantine_failures(resolved, quarantined_at);

CREATE TABLE IF NOT EXISTS ops_metrics (
  metric_id BIGSERIAL PRIMARY KEY,
  run_date TEXT NOT NULL,
  recorded_at TEXT NOT NULL,
  metric TEXT NOT NULL,
  value REAL NOT NULL,
  meta_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_ops_metrics_run_date_metric
ON ops_metrics(run_date, metric, recorded_at);

CREATE TABLE IF NOT EXISTS company_research (
    account_id          TEXT PRIMARY KEY REFERENCES accounts(account_id),
    research_brief      TEXT,
    research_profile    TEXT,
    enrichment_json     TEXT NOT NULL DEFAULT '{}',
    research_status     TEXT NOT NULL DEFAULT 'pending'
        CHECK (research_status IN ('pending', 'in_progress', 'completed', 'failed', 'skipped')),
    researched_at       TEXT,
    model_used          TEXT,
    prompt_hash         TEXT,
    created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS contact_research (
    contact_id          TEXT PRIMARY KEY,
    account_id          TEXT NOT NULL REFERENCES accounts(account_id),
    first_name          TEXT NOT NULL,
    last_name           TEXT NOT NULL,
    title               TEXT,
    email               TEXT,
    linkedin_url        TEXT,
    management_level    TEXT
        CHECK (management_level IN ('C-Level', 'VP', 'Director', 'Manager', 'IC')),
    year_joined         INTEGER,
    created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_contact_research_account
    ON contact_research(account_id);

CREATE TABLE IF NOT EXISTS research_runs (
    research_run_id     TEXT PRIMARY KEY,
    run_date            TEXT NOT NULL,
    score_run_id        TEXT NOT NULL,
    accounts_attempted  INTEGER NOT NULL DEFAULT 0,
    accounts_completed  INTEGER NOT NULL DEFAULT 0,
    accounts_failed     INTEGER NOT NULL DEFAULT 0,
    accounts_skipped    INTEGER NOT NULL DEFAULT 0,
    started_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at         TEXT,
    status              TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'completed', 'failed'))
);

CREATE TABLE IF NOT EXISTS account_labels (
    label_id            TEXT PRIMARY KEY,
    account_id          TEXT NOT NULL,
    label               TEXT NOT NULL,
    reviewer            TEXT NOT NULL DEFAULT 'web_ui',
    notes               TEXT NOT NULL DEFAULT '',
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_account_labels_account ON account_labels(account_id);
CREATE INDEX IF NOT EXISTS idx_account_labels_label ON account_labels(label);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    pipeline_run_id     TEXT PRIMARY KEY,
    started_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at         TIMESTAMP,
    status              TEXT NOT NULL DEFAULT 'running',
    account_ids_json    TEXT NOT NULL DEFAULT '[]',
    stages_json         TEXT NOT NULL DEFAULT '[]',
    result_json         TEXT NOT NULL DEFAULT '{}'
);
"""


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
    """Return the migrations/ directory at the repo root.

    Walks up from this file until it finds pyproject.toml (the repo root marker),
    so the path stays correct whether db.py is a flat module or inside src/db/.
    Falls back to SIGNALS_PROJECT_ROOT env var, then to two levels up from __file__.
    """
    # Prefer the explicit project root env var (set in .env / bootstrap).
    env_root = os.getenv("SIGNALS_PROJECT_ROOT", "").strip()
    if env_root:
        candidate = Path(env_root) / "migrations"
        if candidate.is_dir():
            return candidate

    # Walk upward from this file looking for pyproject.toml.
    current = Path(__file__).resolve().parent
    for _ in range(6):  # cap at 6 levels to avoid infinite loop on bad setups
        if (current / "pyproject.toml").exists():
            return current / "migrations"
        parent = current.parent
        if parent == current:
            break
        current = parent

    # Last-resort fallback: two levels up from this file (works for src/db.py).
    return Path(__file__).resolve().parent.parent / "migrations"


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

    _ensure_column(conn, "external_discovery_events", "entry_url", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "external_discovery_events", "url_type", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "external_discovery_events", "language_hint", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "external_discovery_events", "author_hint", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "external_discovery_events", "published_at_hint", "TEXT NOT NULL DEFAULT ''")


def _build_account_id(domain: str) -> str:
    return stable_hash({"domain": normalize_domain(domain)}, prefix="acc", length=12)


def get_account_by_domain(conn: Any, domain: str) -> dict[str, Any] | None:
    normalized = normalize_domain(domain)
    if not normalized:
        return None
    cur = conn.execute("SELECT * FROM accounts WHERE domain = %s", (normalized,))
    return cur.fetchone()


def upsert_account(
    conn: Any,
    company_name: str,
    domain: str,
    source_type: str = "discovered",
    commit: bool = True,
) -> str:
    normalized_domain = normalize_domain(domain)
    if not normalized_domain:
        raise ValueError("domain is required")
    existing = get_account_by_domain(conn, normalized_domain)
    if existing:
        return str(existing["account_id"])

    account_id = _build_account_id(normalized_domain)
    account = Account(
        account_id=account_id,
        company_name=(company_name or normalized_domain).strip(),
        domain=normalized_domain,
        source_type="seed" if source_type == "seed" else "discovered",
    )
    conn.execute(
        """
        INSERT INTO accounts (account_id, company_name, domain, source_type, created_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            account.account_id,
            account.company_name,
            account.domain,
            account.source_type,
            account.created_at,
        ),
    )
    if commit:
        conn.commit()
    return account.account_id


def seed_accounts(conn: Any, seed_accounts_csv: Path) -> int:
    rows = load_csv_rows(seed_accounts_csv)
    inserted = 0
    for row in rows:
        domain = row.get("domain", "")
        if not domain:
            continue
        existing = get_account_by_domain(conn, domain)
        if existing:
            continue
        upsert_account(
            conn,
            company_name=row.get("company_name", domain),
            domain=domain,
            source_type=row.get("source_type", "seed") or "seed",
        )
        inserted += 1
    return inserted


def insert_signal_observation(conn: Any, observation: SignalObservation, commit: bool = True) -> bool:
    cur = conn.execute(
        """
        INSERT INTO signal_observations (
            obs_id,
            account_id,
            signal_code,
            product,
            source,
            observed_at,
            evidence_url,
            evidence_text,
            document_id,
            mention_id,
            evidence_sentence,
            evidence_sentence_en,
            matched_phrase,
            language,
            speaker_name,
            speaker_role,
            evidence_quality,
            relevance_score,
            confidence,
            source_reliability,
            raw_payload_hash
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING obs_id
        """,
        (
            observation.obs_id,
            observation.account_id,
            observation.signal_code,
            observation.product,
            observation.source,
            observation.observed_at,
            observation.evidence_url,
            observation.evidence_text,
            observation.document_id,
            observation.mention_id,
            observation.evidence_sentence,
            observation.evidence_sentence_en,
            observation.matched_phrase,
            observation.language,
            observation.speaker_name,
            observation.speaker_role,
            float(observation.evidence_quality),
            float(observation.relevance_score),
            float(observation.confidence),
            float(observation.source_reliability),
            observation.raw_payload_hash,
        ),
    )
    inserted = cur.fetchone() is not None
    if commit:
        conn.commit()
    return inserted


def create_score_run(conn: Any, run_date: str) -> str:
    run_id = f"run_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO score_runs (run_id, run_date, status, started_at)
        VALUES (%s, %s, 'running', %s)
        """,
        (run_id, run_date, utc_now_iso()),
    )
    conn.commit()
    return run_id


def finish_score_run(
    conn: Any,
    run_id: str,
    status: str,
    error_summary: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE score_runs
        SET status = %s, finished_at = %s, error_summary = %s
        WHERE run_id = %s
        """,
        (status, utc_now_iso(), error_summary or "", run_id),
    )
    conn.commit()


def fetch_observations_for_scoring(
    conn: Any,
    run_date: str,
    lookback_days: int = 120,
) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT *
        FROM signal_observations
        WHERE observed_at::date <= %s::date
          AND observed_at::date >= (%s::date + %s::interval)
        """,
        (run_date, run_date, f"-{lookback_days} days"),
    )
    return list(cur.fetchall())


def replace_run_scores(
    conn: Any,
    run_id: str,
    component_scores: list[ComponentScore],
    account_scores: list[AccountScore],
) -> None:
    conn.execute("DELETE FROM score_components WHERE run_id = %s", (run_id,))
    conn.execute("DELETE FROM account_scores WHERE run_id = %s", (run_id,))

    for component in component_scores:
        conn.execute(
            """
            INSERT INTO score_components (run_id, account_id, product, signal_code, component_score)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                component.run_id,
                component.account_id,
                component.product,
                component.signal_code,
                component.component_score,
            ),
        )

    for score in account_scores:
        conn.execute(
            """
            INSERT INTO account_scores (
                run_id,
                account_id,
                product,
                score,
                tier,
                top_reasons_json,
                delta_7d
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                score.run_id,
                score.account_id,
                score.product,
                score.score,
                score.tier,
                score.top_reasons_json,
                score.delta_7d,
            ),
        )
    conn.commit()


def get_score_delta_7d(conn: Any, account_id: str, product: str, run_date: str) -> float:
    cur = conn.execute(
        """
        SELECT s.score
        FROM account_scores s
        JOIN score_runs r ON s.run_id = r.run_id
        WHERE s.account_id = %s
          AND s.product = %s
          AND r.run_date::date <= (%s::date - INTERVAL '7 days')
        ORDER BY r.run_date::date DESC
        LIMIT 1
        """,
        (account_id, product, run_date),
    )
    row = cur.fetchone()
    if not row:
        return 0.0

    cur2 = conn.execute(
        """
        SELECT s.score
        FROM account_scores s
        JOIN score_runs r ON s.run_id = r.run_id
        WHERE s.account_id = %s
          AND s.product = %s
          AND r.run_date::date = %s::date
        ORDER BY r.started_at DESC
        LIMIT 1
        """,
        (account_id, product, run_date),
    )
    current_row = cur2.fetchone()
    if not current_row:
        return 0.0
    return round(float(current_row["score"]) - float(row["score"]), 2)


def get_latest_run_id_for_date(conn: Any, run_date: str) -> str | None:
    cur = conn.execute(
        """
        SELECT run_id
        FROM score_runs
        WHERE run_date::date = %s::date
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (run_date,),
    )
    row = cur.fetchone()
    return None if not row else str(row["run_id"])


def list_runs(conn: Any) -> list[dict[str, Any]]:
    cur = conn.execute("SELECT * FROM score_runs ORDER BY started_at DESC")
    return list(cur.fetchall())


def fetch_scores_for_run(conn: Any, run_id: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT
            r.run_date,
            s.account_id,
            a.company_name,
            a.domain,
            s.product,
            s.score,
            s.tier,
            s.delta_7d,
            s.top_reasons_json
        FROM account_scores s
        JOIN accounts a ON a.account_id = s.account_id
        JOIN score_runs r ON r.run_id = s.run_id
        WHERE s.run_id = %s
        ORDER BY s.score DESC
        """,
        (run_id,),
    )
    return list(cur.fetchall())


def insert_review_label(conn: Any, label: ReviewLabel) -> bool:
    cur = conn.execute(
        """
        INSERT INTO review_labels (review_id, run_id, account_id, decision, reviewer, notes, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING review_id
        """,
        (
            label.review_id,
            label.run_id,
            label.account_id,
            label.decision,
            label.reviewer,
            label.notes,
            label.created_at,
        ),
    )
    conn.commit()
    return cur.fetchone() is not None


def fetch_review_rows_for_date(conn: Any, run_date: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT rl.*, r.run_date
        FROM review_labels rl
        JOIN score_runs r ON r.run_id = rl.run_id
        WHERE r.run_date::date = %s::date
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def fetch_sources_for_account_window(
    conn: Any,
    account_id: str,
    run_date: str,
    lookback_days: int = 30,
) -> list[str]:
    cur = conn.execute(
        """
        SELECT DISTINCT source
        FROM signal_observations
        WHERE account_id = %s
          AND observed_at::date <= %s::date
          AND observed_at::date >= (%s::date + %s::interval)
        ORDER BY source
        """,
        (account_id, run_date, run_date, f"-{lookback_days} days"),
    )
    return [str(row["source"]) for row in cur.fetchall()]


def fetch_scored_sources_for_run_account(
    conn: Any,
    run_id: str,
    account_id: str,
) -> list[str]:
    cur = conn.execute(
        """
        SELECT top_reasons_json
        FROM account_scores
        WHERE run_id = %s
          AND account_id = %s
        """,
        (run_id, account_id),
    )
    rows = cur.fetchall()

    sources: set[str] = set()
    for row in rows:
        raw = str(row["top_reasons_json"] or "").strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, list):
            continue
        for reason in parsed:
            if not isinstance(reason, dict):
                continue
            source = str(reason.get("source", "")).strip()
            if source:
                sources.add(source)

    return sorted(sources)


def upsert_source_metrics(
    conn: Any,
    run_date: str,
    rows: list[dict[str, float | int | str]],
) -> None:
    for row in rows:
        conn.execute(
            """
            INSERT INTO source_metrics (run_date, source, approved_rate, sample_size)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT(run_date, source)
            DO UPDATE SET approved_rate = excluded.approved_rate,
                          sample_size = excluded.sample_size
            """,
            (
                run_date,
                str(row.get("source", "unknown")),
                float(row.get("approved_rate", 0.0)),
                int(row.get("sample_size", 0)),
            ),
        )
    conn.commit()


def fetch_source_metrics(conn: Any, run_date: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT run_date, source, approved_rate, sample_size
        FROM source_metrics
        WHERE run_date::date = %s::date
        ORDER BY source
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def fetch_recent_reviews(conn: Any, run_date: str, days: int) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT rl.*, r.run_date
        FROM review_labels rl
        JOIN score_runs r ON r.run_id = rl.run_id
        WHERE r.run_date::date <= %s::date
          AND r.run_date::date >= (%s::date + %s::interval)
        ORDER BY r.run_date::date DESC
        """,
        (run_date, run_date, f"-{days} days"),
    )
    return list(cur.fetchall())


def account_exists(conn: Any, account_id: str) -> bool:
    cur = conn.execute("SELECT 1 FROM accounts WHERE account_id = %s LIMIT 1", (account_id,))
    return cur.fetchone() is not None


def dump_run_summary(conn: Any, run_id: str) -> dict[str, object]:
    cur = conn.execute(
        """
        SELECT
            COUNT(*) AS score_rows,
            COUNT(DISTINCT account_id) AS account_count
        FROM account_scores
        WHERE run_id = %s
        """,
        (run_id,),
    )
    row = cur.fetchone()
    return {
        "run_id": run_id,
        "score_rows": int(row["score_rows"] if row else 0),
        "account_count": int(row["account_count"] if row else 0),
    }


def was_crawled_today(
    conn: Any,
    source: str,
    account_id: str,
    endpoint: str,
) -> bool:
    cur = conn.execute(
        """
        SELECT 1
        FROM crawl_checkpoints
        WHERE source = %s
          AND account_id = %s
          AND endpoint = %s
          AND last_crawled_at::timestamp >= (CURRENT_TIMESTAMP - INTERVAL '20 hours')
        LIMIT 1
        """,
        (source, account_id, endpoint),
    )
    return cur.fetchone() is not None


def mark_crawled(
    conn: Any,
    source: str,
    account_id: str,
    endpoint: str,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO crawl_checkpoints (source, account_id, endpoint, last_crawled_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT(source, account_id, endpoint)
        DO UPDATE SET last_crawled_at = excluded.last_crawled_at
        """,
        (source, account_id, endpoint, utc_now_iso()),
    )
    if commit:
        conn.commit()


def record_crawl_attempt(
    conn: Any,
    source: str,
    account_id: str,
    endpoint: str,
    status: str,
    error_summary: str = "",
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO crawl_attempts (source, account_id, endpoint, attempted_at, status, error_summary)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (source, account_id, endpoint, utc_now_iso(), status, (error_summary or "")[:500]),
    )
    if commit:
        conn.commit()


def fetch_crawl_attempt_summary(conn: Any, run_date: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT source, status, COUNT(*) AS attempt_count
        FROM crawl_attempts
        WHERE attempted_at::date = %s::date
        GROUP BY source, status
        ORDER BY source, status
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def fetch_latest_crawl_failures(conn: Any, run_date: str, limit: int = 10) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT source, account_id, endpoint, status, error_summary, attempted_at
        FROM crawl_attempts
        WHERE attempted_at::date = %s::date
          AND status IN ('http_error', 'exception')
        ORDER BY attempted_at DESC
        LIMIT %s
        """,
        (run_date, max(1, int(limit))),
    )
    return list(cur.fetchall())


def select_accounts_for_live_crawl(
    conn: Any,
    source: str,
    limit: int,
    include_domains: list[str] | tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    bounded_limit = max(1, int(limit))
    domain_filters: list[str] = []
    for raw in list(include_domains or []):
        normalized = normalize_domain(str(raw))
        if not normalized or normalized.endswith(".example"):
            continue
        domain_filters.append(normalized)
    domain_filters = sorted(set(domain_filters))

    where_clauses = [
        "COALESCE(a.domain, '') <> ''",
        "LOWER(a.domain) NOT LIKE %s",
    ]
    params: list[Any] = [str(source).strip(), "%.example"]
    if domain_filters:
        placeholders = ", ".join("%s" for _ in domain_filters)
        where_clauses.append(f"LOWER(a.domain) IN ({placeholders})")
        params.extend(domain_filters)
    where_sql = " AND ".join(where_clauses)

    cur = conn.execute(
        f"""
        SELECT
            a.account_id,
            a.domain,
            a.company_name,
            a.created_at,
            latest.last_attempted_at
        FROM accounts a
        LEFT JOIN (
            SELECT account_id, MAX(attempted_at) AS last_attempted_at
            FROM crawl_attempts
            WHERE source = %s
            GROUP BY account_id
        ) latest
          ON latest.account_id = a.account_id
        WHERE {where_sql}
        ORDER BY
            CASE
                WHEN latest.last_attempted_at IS NULL OR latest.last_attempted_at = '' THEN 0
                ELSE 1
            END,
            latest.last_attempted_at ASC,
            a.created_at ASC
        LIMIT %s
        """,
        tuple(params + [bounded_limit]),
    )
    return list(cur.fetchall())


def insert_external_discovery_event(
    conn: Any,
    source: str,
    source_event_id: str,
    observed_at: str,
    title: str,
    text: str,
    url: str = "",
    entry_url: str = "",
    url_type: str = "",
    language_hint: str = "",
    author_hint: str = "",
    published_at_hint: str = "",
    company_name_hint: str = "",
    domain_hint: str = "",
    raw_payload_json: str = "{}",
) -> bool:
    normalized_source = (source or "huginn_webhook").strip().lower()
    normalized_event_id = (source_event_id or "").strip()
    dedupe_key = (
        f"{normalized_source}:{normalized_event_id}"
        if normalized_event_id
        else stable_hash(
            {
                "source": normalized_source,
                "url": (url or "").strip(),
                "entry_url": (entry_url or "").strip(),
                "observed_at": (observed_at or "").strip(),
                "title": (title or "").strip(),
                "text": (text or "").strip(),
            },
            prefix="disc",
            length=24,
        )
    )
    cur = conn.execute(
        """
        INSERT INTO external_discovery_events (
            source,
            source_event_id,
            dedupe_key,
            observed_at,
            title,
            text,
            url,
            entry_url,
            url_type,
            language_hint,
            author_hint,
            published_at_hint,
            company_name_hint,
            domain_hint,
            raw_payload_json,
            ingested_at,
            processing_status,
            processed_run_id,
            processed_at,
            error_summary
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', '', '', '')
        ON CONFLICT DO NOTHING
        RETURNING event_id
        """,
        (
            normalized_source,
            normalized_event_id,
            dedupe_key,
            observed_at,
            title,
            text,
            (url or "").strip(),
            (entry_url or "").strip(),
            (url_type or "").strip().lower(),
            (language_hint or "").strip().lower(),
            (author_hint or "").strip(),
            (published_at_hint or "").strip(),
            (company_name_hint or "").strip(),
            normalize_domain(domain_hint or ""),
            (raw_payload_json or "{}")[:8000],
            utc_now_iso(),
        ),
    )
    conn.commit()
    return cur.fetchone() is not None


def fetch_pending_external_discovery_events(
    conn: Any,
    run_date: str,
    limit: int = 500,
) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT *
        FROM external_discovery_events
        WHERE processing_status = 'pending'
          AND observed_at::date <= %s::date
        ORDER BY observed_at::timestamp ASC, event_id ASC
        LIMIT %s
        """,
        (run_date, max(1, int(limit))),
    )
    return list(cur.fetchall())


def mark_external_discovery_event_processed(
    conn: Any,
    event_id: int,
    processed_run_id: str,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        UPDATE external_discovery_events
        SET processing_status = 'processed',
            processed_run_id = %s,
            processed_at = %s,
            error_summary = ''
        WHERE event_id = %s
        """,
        ((processed_run_id or "").strip(), utc_now_iso(), int(event_id)),
    )
    if commit:
        conn.commit()


def mark_external_discovery_event_failed(
    conn: Any,
    event_id: int,
    processed_run_id: str,
    error_summary: str,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        UPDATE external_discovery_events
        SET processing_status = 'failed',
            processed_run_id = %s,
            processed_at = %s,
            error_summary = %s
        WHERE event_id = %s
        """,
        (
            (processed_run_id or "").strip(),
            utc_now_iso(),
            (error_summary or "")[:500],
            int(event_id),
        ),
    )
    if commit:
        conn.commit()


def insert_crawl_frontier(
    conn: Any,
    run_date: str,
    source: str,
    source_event_id: str,
    account_id: str,
    domain: str,
    url: str,
    canonical_url: str,
    url_type: str = "article",
    depth: int = 0,
    priority: float = 0.5,
    max_retries: int = 2,
    payload_json: str = "{}",
    commit: bool = True,
) -> bool:
    frontier_id = stable_hash(
        {
            "run_date": run_date,
            "canonical_url": canonical_url,
            "source": source,
        },
        prefix="frn",
        length=16,
    )
    cur = conn.execute(
        """
        INSERT INTO crawl_frontier (
            frontier_id,
            run_date,
            source,
            source_event_id,
            account_id,
            domain,
            url,
            canonical_url,
            url_type,
            depth,
            priority,
            status,
            retry_count,
            max_retries,
            first_seen_at,
            last_attempt_at,
            last_error,
            payload_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', 0, %s, %s, '', '', %s)
        ON CONFLICT DO NOTHING
        RETURNING frontier_id
        """,
        (
            frontier_id,
            run_date,
            (source or "").strip().lower(),
            (source_event_id or "").strip(),
            account_id,
            normalize_domain(domain or ""),
            (url or "").strip(),
            (canonical_url or "").strip(),
            (url_type or "article").strip().lower(),
            max(0, int(depth)),
            max(0.0, float(priority)),
            max(0, int(max_retries)),
            utc_now_iso(),
            (payload_json or "{}")[:12000],
        ),
    )
    inserted = cur.fetchone() is not None
    if commit:
        conn.commit()
    return inserted


def fetch_crawl_frontier_by_status(
    conn: Any,
    run_date: str,
    status: str,
    limit: int = 500,
) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT *
        FROM crawl_frontier
        WHERE run_date = %s
          AND status = %s
        ORDER BY priority DESC, first_seen_at ASC
        LIMIT %s
        """,
        (run_date, status, max(1, int(limit))),
    )
    return list(cur.fetchall())


def mark_crawl_frontier_status(
    conn: Any,
    frontier_id: str,
    status: str,
    error_summary: str = "",
    bump_retry: bool = False,
    commit: bool = True,
) -> None:
    if bump_retry:
        conn.execute(
            """
            UPDATE crawl_frontier
            SET status = %s,
                retry_count = retry_count + 1,
                last_attempt_at = %s,
                last_error = %s
            WHERE frontier_id = %s
            """,
            (status, utc_now_iso(), (error_summary or "")[:500], frontier_id),
        )
    else:
        conn.execute(
            """
            UPDATE crawl_frontier
            SET status = %s,
                last_attempt_at = %s,
                last_error = %s
            WHERE frontier_id = %s
            """,
            (status, utc_now_iso(), (error_summary or "")[:500], frontier_id),
        )
    if commit:
        conn.commit()


def get_document_by_frontier_id(conn: Any, frontier_id: str) -> dict[str, Any] | None:
    cur = conn.execute(
        """
        SELECT *
        FROM documents
        WHERE frontier_id = %s
        LIMIT 1
        """,
        (frontier_id,),
    )
    return cur.fetchone()


def upsert_document(
    conn: Any,
    frontier_id: str,
    account_id: str,
    domain: str,
    source: str,
    source_event_id: str,
    url: str,
    canonical_url: str,
    content_sha256: str,
    title: str,
    author: str,
    published_at: str,
    section: str,
    language: str,
    body_text: str,
    body_text_en: str,
    raw_html: str,
    parser_version: str,
    evidence_quality: float,
    relevance_score: float,
    fetched_with: str,
    outbound_links_json: str = "[]",
    commit: bool = True,
) -> str:
    document_id = stable_hash({"canonical_url": canonical_url}, prefix="doc", length=16)
    now = utc_now_iso()
    savepoint_name = f"sp_doc_{uuid.uuid4().hex[:8]}"
    conn.execute(f"SAVEPOINT {savepoint_name}")
    try:
        conn.execute(
            """
            INSERT INTO documents (
                document_id,
                frontier_id,
                account_id,
                domain,
                source,
                source_event_id,
                url,
                canonical_url,
                content_sha256,
                title,
                author,
                published_at,
                section,
                language,
                body_text,
                body_text_en,
                raw_html,
                parser_version,
                evidence_quality,
                relevance_score,
                fetched_with,
                outbound_links_json,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(canonical_url) DO UPDATE
            SET frontier_id = excluded.frontier_id,
                account_id = excluded.account_id,
                domain = excluded.domain,
                source = excluded.source,
                source_event_id = excluded.source_event_id,
                url = excluded.url,
                content_sha256 = excluded.content_sha256,
                title = excluded.title,
                author = excluded.author,
                published_at = excluded.published_at,
                section = excluded.section,
                language = excluded.language,
                body_text = excluded.body_text,
                body_text_en = excluded.body_text_en,
                raw_html = excluded.raw_html,
                parser_version = excluded.parser_version,
                evidence_quality = excluded.evidence_quality,
                relevance_score = excluded.relevance_score,
                fetched_with = excluded.fetched_with,
                outbound_links_json = excluded.outbound_links_json,
                updated_at = excluded.updated_at
            """,
            (
                document_id,
                frontier_id,
                account_id,
                normalize_domain(domain or ""),
                (source or "").strip().lower(),
                (source_event_id or "").strip(),
                (url or "").strip(),
                (canonical_url or "").strip(),
                (content_sha256 or "").strip(),
                (title or "")[:500],
                (author or "")[:250],
                (published_at or "").strip(),
                (section or "")[:120],
                (language or "").strip().lower(),
                (body_text or "")[:200000],
                (body_text_en or "")[:200000],
                (raw_html or "")[:200000],
                (parser_version or "")[:80],
                max(0.0, min(1.0, float(evidence_quality))),
                max(0.0, min(1.0, float(relevance_score))),
                (fetched_with or "")[:40],
                (outbound_links_json or "[]")[:12000],
                now,
                now,
            ),
        )
        if commit:
            conn.commit()
        else:
            conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
    except Exception as exc:
        if not _is_integrity_error(exc):
            raise
        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        # Content hash collisions can happen when multiple URLs resolve to the same article.
        row = conn.execute(
            """
            SELECT document_id
            FROM documents
            WHERE canonical_url = %s
               OR content_sha256 = %s
            LIMIT 1
            """,
            ((canonical_url or "").strip(), (content_sha256 or "").strip()),
        ).fetchone()
        if row is not None:
            return str(row["document_id"])
        raise exc
    return document_id


def fetch_documents_for_run_by_frontier_status(
    conn: Any,
    run_date: str,
    frontier_status: str,
    limit: int = 500,
) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT d.*, f.url_type, f.depth, f.priority, f.payload_json, f.frontier_id, f.source_event_id, f.source
        FROM documents d
        JOIN crawl_frontier f ON f.frontier_id = d.frontier_id
        WHERE f.run_date = %s
          AND f.status = %s
        ORDER BY f.priority DESC, d.updated_at ASC
        LIMIT %s
        """,
        (run_date, frontier_status, max(1, int(limit))),
    )
    return list(cur.fetchall())


def insert_document_mention(
    conn: Any,
    document_id: str,
    account_id: str,
    signal_code: str,
    matched_phrase: str,
    evidence_sentence: str,
    evidence_sentence_en: str,
    language: str,
    speaker_name: str,
    speaker_role: str,
    confidence: float,
    evidence_quality: float,
    relevance_score: float,
    commit: bool = True,
) -> tuple[str, bool]:
    normalized_phrase = (matched_phrase or "").strip().lower()
    mention_id = stable_hash(
        {
            "document_id": document_id,
            "signal_code": signal_code,
            "matched_phrase": normalized_phrase,
        },
        prefix="mnt",
        length=16,
    )
    cur = conn.execute(
        """
        INSERT INTO document_mentions (
            mention_id,
            document_id,
            account_id,
            signal_code,
            matched_phrase,
            evidence_sentence,
            evidence_sentence_en,
            language,
            speaker_name,
            speaker_role,
            confidence,
            evidence_quality,
            relevance_score,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING mention_id
        """,
        (
            mention_id,
            document_id,
            account_id,
            signal_code,
            normalized_phrase,
            (evidence_sentence or "")[:1500],
            (evidence_sentence_en or "")[:1500],
            (language or "").strip().lower(),
            (speaker_name or "")[:200],
            (speaker_role or "")[:120],
            max(0.0, min(1.0, float(confidence))),
            max(0.0, min(1.0, float(evidence_quality))),
            max(0.0, min(1.0, float(relevance_score))),
            utc_now_iso(),
        ),
    )
    if commit:
        conn.commit()
    return mention_id, (cur.fetchone() is not None)


def insert_observation_lineage(
    conn: Any,
    obs_id: str,
    account_id: str,
    document_id: str,
    mention_id: str,
    source_event_id: str,
    run_date: str,
    commit: bool = True,
) -> bool:
    cur = conn.execute(
        """
        INSERT INTO observation_lineage (
            obs_id,
            account_id,
            document_id,
            mention_id,
            source_event_id,
            run_date,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING obs_id
        """,
        (
            obs_id,
            account_id,
            document_id,
            mention_id,
            (source_event_id or "")[:250],
            run_date,
            utc_now_iso(),
        ),
    )
    if commit:
        conn.commit()
    return cur.fetchone() is not None


def upsert_people_watchlist_entry(
    conn: Any,
    account_id: str,
    person_name: str,
    role_title: str,
    role_weight: float,
    source_url: str,
    is_active: bool = True,
    commit: bool = True,
) -> str:
    watch_id = stable_hash(
        {"account_id": account_id, "person_name": person_name, "role_title": role_title},
        prefix="pwl",
        length=16,
    )
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO people_watchlist (
            watch_id,
            account_id,
            person_name,
            role_title,
            role_weight,
            source_url,
            is_active,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(account_id, person_name, role_title) DO UPDATE
        SET role_weight = excluded.role_weight,
            source_url = excluded.source_url,
            is_active = excluded.is_active,
            updated_at = excluded.updated_at
        """,
        (
            watch_id,
            account_id,
            (person_name or "")[:200],
            (role_title or "")[:120],
            max(0.0, min(2.0, float(role_weight))),
            (source_url or "")[:500],
            1 if is_active else 0,
            now,
            now,
        ),
    )
    if commit:
        conn.commit()
    return watch_id


def insert_people_activity(
    conn: Any,
    account_id: str,
    person_name: str,
    role_title: str,
    document_id: str,
    activity_type: str,
    summary: str,
    published_at: str,
    url: str,
    commit: bool = True,
) -> bool:
    activity_id = stable_hash(
        {
            "account_id": account_id,
            "person_name": person_name,
            "document_id": document_id,
            "activity_type": activity_type,
        },
        prefix="pac",
        length=16,
    )
    cur = conn.execute(
        """
        INSERT INTO people_activity (
            activity_id,
            account_id,
            person_name,
            role_title,
            document_id,
            activity_type,
            summary,
            published_at,
            url,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING activity_id
        """,
        (
            activity_id,
            account_id,
            (person_name or "")[:200],
            (role_title or "")[:120],
            document_id,
            (activity_type or "")[:120],
            (summary or "")[:1500],
            (published_at or "")[:80],
            (url or "")[:500],
            utc_now_iso(),
        ),
    )
    if commit:
        conn.commit()
    return cur.fetchone() is not None


def fetch_story_evidence_rows(conn: Any, run_date: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT
            d.document_id,
            d.account_id,
            a.company_name,
            a.domain,
            d.canonical_url,
            d.title,
            d.author,
            d.published_at,
            d.language,
            d.evidence_quality,
            d.relevance_score,
            d.fetched_with,
            d.updated_at
        FROM documents d
        JOIN accounts a ON a.account_id = d.account_id
        JOIN crawl_frontier f ON f.frontier_id = d.frontier_id
        WHERE f.run_date = %s
        ORDER BY d.evidence_quality DESC, d.relevance_score DESC, d.updated_at DESC
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def fetch_signal_lineage_rows(conn: Any, run_date: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT
            ol.run_date,
            ol.obs_id,
            a.company_name,
            a.domain,
            so.signal_code,
            so.source,
            so.confidence,
            so.evidence_quality,
            so.relevance_score,
            so.evidence_url,
            so.evidence_sentence,
            so.evidence_sentence_en,
            so.matched_phrase,
            so.language,
            so.speaker_name,
            so.speaker_role,
            ol.document_id,
            ol.mention_id,
            ol.source_event_id
        FROM observation_lineage ol
        JOIN signal_observations so ON so.obs_id = ol.obs_id
        JOIN accounts a ON a.account_id = ol.account_id
        WHERE ol.run_date = %s
        ORDER BY so.evidence_quality DESC, so.relevance_score DESC, so.confidence DESC
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def create_discovery_run(conn: Any, run_date: str, score_run_id: str) -> str:
    discovery_run_id = f"disc_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO discovery_runs (
            discovery_run_id,
            run_date,
            score_run_id,
            created_at,
            status,
            source_events_processed,
            observations_inserted,
            total_candidates,
            crm_eligible_candidates,
            error_summary
        )
        VALUES (%s, %s, %s, %s, 'running', 0, 0, 0, 0, '')
        """,
        (discovery_run_id, run_date, score_run_id, utc_now_iso()),
    )
    conn.commit()
    return discovery_run_id


def finish_discovery_run(
    conn: Any,
    discovery_run_id: str,
    status: str,
    source_events_processed: int,
    observations_inserted: int,
    total_candidates: int,
    crm_eligible_candidates: int,
    error_summary: str = "",
) -> None:
    conn.execute(
        """
        UPDATE discovery_runs
        SET status = %s,
            source_events_processed = %s,
            observations_inserted = %s,
            total_candidates = %s,
            crm_eligible_candidates = %s,
            error_summary = %s
        WHERE discovery_run_id = %s
        """,
        (
            status,
            max(0, int(source_events_processed)),
            max(0, int(observations_inserted)),
            max(0, int(total_candidates)),
            max(0, int(crm_eligible_candidates)),
            (error_summary or "")[:1000],
            discovery_run_id,
        ),
    )
    conn.commit()


def replace_discovery_candidates(
    conn: Any,
    discovery_run_id: str,
    candidates: list[dict[str, object]],
    evidence_rows: list[dict[str, object]],
) -> None:
    conn.execute("DELETE FROM discovery_candidates WHERE discovery_run_id = %s", (discovery_run_id,))
    conn.execute("DELETE FROM discovery_evidence WHERE discovery_run_id = %s", (discovery_run_id,))

    for row in candidates:
        conn.execute(
            """
            INSERT INTO discovery_candidates (
                discovery_run_id,
                score_run_id,
                run_date,
                account_id,
                company_name,
                domain,
                best_product,
                score,
                tier,
                confidence_band,
                cpg_like_group_count,
                primary_signal_count,
                source_count,
                has_poc_progression_first_party,
                relationship_stage,
                vertical_tag,
                is_self,
                exclude_from_crm,
                eligible_for_crm,
                novelty_score,
                rank_score,
                reasons_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                discovery_run_id,
                str(row.get("score_run_id", "")),
                str(row.get("run_date", "")),
                str(row.get("account_id", "")),
                str(row.get("company_name", "")),
                str(row.get("domain", "")),
                str(row.get("best_product", "")),
                float(row.get("score", 0.0)),
                str(row.get("tier", "low")),
                str(row.get("confidence_band", "explore")),
                int(row.get("cpg_like_group_count", 0)),
                int(row.get("primary_signal_count", 0)),
                int(row.get("source_count", 0)),
                int(row.get("has_poc_progression_first_party", 0)),
                str(row.get("relationship_stage", "unknown")),
                str(row.get("vertical_tag", "unknown")),
                int(row.get("is_self", 0)),
                int(row.get("exclude_from_crm", 0)),
                int(row.get("eligible_for_crm", 0)),
                float(row.get("novelty_score", 0.0)),
                float(row.get("rank_score", 0.0)),
                str(row.get("reasons_json", "[]")),
            ),
        )

    for row in evidence_rows:
        conn.execute(
            """
            INSERT INTO discovery_evidence (
                discovery_run_id,
                account_id,
                signal_code,
                source,
                evidence_url,
                evidence_text,
                component_score
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(discovery_run_id, account_id, signal_code, source, evidence_url) DO UPDATE
            SET evidence_text = excluded.evidence_text,
                component_score = excluded.component_score
            """,
            (
                discovery_run_id,
                str(row.get("account_id", "")),
                str(row.get("signal_code", "")),
                str(row.get("source", "")),
                str(row.get("evidence_url", "")),
                str(row.get("evidence_text", ""))[:500],
                float(row.get("component_score", 0.0)),
            ),
        )

    conn.commit()


def get_latest_discovery_run_id_for_date(conn: Any, run_date: str) -> str | None:
    cur = conn.execute(
        """
        SELECT discovery_run_id
        FROM discovery_runs
        WHERE run_date::date = %s::date
          AND status = 'completed'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (run_date,),
    )
    row = cur.fetchone()
    return None if row is None else str(row["discovery_run_id"])


def fetch_discovery_candidates_for_run(conn: Any, discovery_run_id: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT *
        FROM discovery_candidates
        WHERE discovery_run_id = %s
        ORDER BY rank_score DESC, score DESC, company_name ASC
        """,
        (discovery_run_id,),
    )
    return list(cur.fetchall())


def fetch_discovery_run(conn: Any, discovery_run_id: str) -> dict[str, Any] | None:
    cur = conn.execute(
        """
        SELECT *
        FROM discovery_runs
        WHERE discovery_run_id = %s
        LIMIT 1
        """,
        (discovery_run_id,),
    )
    return cur.fetchone()


def try_advisory_lock(conn: Any, lock_name: str, owner_id: str, details: str = "") -> bool:
    row = conn.execute("SELECT pg_try_advisory_lock(hashtext(%s)) AS locked", (lock_name,)).fetchone()
    locked = bool(row and bool(row["locked"]))
    conn.execute(
        """
        INSERT INTO run_lock_events (lock_name, owner_id, action, details, created_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            (lock_name or "")[:120],
            (owner_id or "")[:120],
            "acquired" if locked else "busy",
            (details or "")[:300],
            utc_now_iso(),
        ),
    )
    conn.commit()
    return locked


def release_advisory_lock(conn: Any, lock_name: str, owner_id: str, details: str = "") -> bool:
    row = conn.execute("SELECT pg_advisory_unlock(hashtext(%s)) AS unlocked", (lock_name,)).fetchone()
    unlocked = bool(row and bool(row["unlocked"]))
    conn.execute(
        """
        INSERT INTO run_lock_events (lock_name, owner_id, action, details, created_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            (lock_name or "")[:120],
            (owner_id or "")[:120],
            "released" if unlocked else "release_missed",
            (details or "")[:300],
            utc_now_iso(),
        ),
    )
    conn.commit()
    return unlocked


def record_stage_failure(
    conn: Any,
    run_type: str,
    run_date: str,
    stage: str,
    error_summary: str,
    duration_seconds: float,
    timed_out: bool,
    retry_task_id: str = "",
    commit: bool = True,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO stage_failures (
            run_type,
            run_date,
            stage,
            duration_seconds,
            timed_out,
            error_summary,
            retry_task_id,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING failure_id
        """,
        (
            (run_type or "")[:80],
            (run_date or "")[:30],
            (stage or "")[:120],
            max(0.0, float(duration_seconds)),
            1 if timed_out else 0,
            (error_summary or "")[:1000],
            (retry_task_id or "")[:80],
            utc_now_iso(),
        ),
    )
    row = cur.fetchone()
    failure_id = int(row["failure_id"]) if row is not None else 0
    if commit:
        conn.commit()
    return failure_id


def enqueue_retry_task(
    conn: Any,
    task_type: str,
    payload_json: str,
    due_at: str,
    max_attempts: int = 3,
    commit: bool = True,
) -> str:
    task_id = f"retry_{uuid.uuid4().hex[:12]}"
    now_iso = utc_now_iso()
    conn.execute(
        """
        INSERT INTO retry_queue (
            task_id,
            task_type,
            payload_json,
            attempt_count,
            max_attempts,
            status,
            due_at,
            last_error,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, 0, %s, 'pending', %s, '', %s, %s)
        """,
        (
            task_id,
            (task_type or "")[:80],
            (payload_json or "{}")[:12000],
            max(1, int(max_attempts)),
            (due_at or now_iso)[:80],
            now_iso,
            now_iso,
        ),
    )
    if commit:
        conn.commit()
    return task_id


def fetch_due_retry_tasks(conn: Any, limit: int = 20, now_iso: str | None = None) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT *
        FROM retry_queue
        WHERE status = 'pending'
          AND CAST(due_at AS TIMESTAMP) <= CAST(%s AS TIMESTAMP)
        ORDER BY due_at ASC, created_at ASC
        LIMIT %s
        """,
        ((now_iso or utc_now_iso()), max(1, int(limit))),
    )
    return list(cur.fetchall())


def mark_retry_task_running(conn: Any, task_id: str, commit: bool = True) -> None:
    conn.execute(
        """
        UPDATE retry_queue
        SET status = 'running', updated_at = %s
        WHERE task_id = %s
        """,
        (utc_now_iso(), task_id),
    )
    if commit:
        conn.commit()


def mark_retry_task_completed(conn: Any, task_id: str, commit: bool = True) -> None:
    conn.execute(
        """
        UPDATE retry_queue
        SET status = 'completed', updated_at = %s, last_error = ''
        WHERE task_id = %s
        """,
        (utc_now_iso(), task_id),
    )
    if commit:
        conn.commit()


def reschedule_retry_task(
    conn: Any,
    task_id: str,
    attempt_count: int,
    due_at: str,
    error_summary: str,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        UPDATE retry_queue
        SET status = 'pending',
            attempt_count = %s,
            due_at = %s,
            last_error = %s,
            updated_at = %s
        WHERE task_id = %s
        """,
        (
            max(0, int(attempt_count)),
            (due_at or utc_now_iso())[:80],
            (error_summary or "")[:1000],
            utc_now_iso(),
            task_id,
        ),
    )
    if commit:
        conn.commit()


def quarantine_retry_task(
    conn: Any,
    task_id: str,
    task_type: str,
    payload_json: str,
    attempt_count: int,
    error_summary: str,
    commit: bool = True,
) -> None:
    now_iso = utc_now_iso()
    conn.execute(
        """
        UPDATE retry_queue
        SET status = 'quarantined',
            attempt_count = %s,
            last_error = %s,
            updated_at = %s
        WHERE task_id = %s
        """,
        (max(0, int(attempt_count)), (error_summary or "")[:1000], now_iso, task_id),
    )
    conn.execute(
        """
        INSERT INTO quarantine_failures (
            task_id,
            task_type,
            payload_json,
            attempt_count,
            error_summary,
            quarantined_at,
            resolved,
            resolved_at,
            resolution_note
        )
        VALUES (%s, %s, %s, %s, %s, %s, 0, '', '')
        """,
        (
            task_id,
            (task_type or "")[:80],
            (payload_json or "{}")[:12000],
            max(0, int(attempt_count)),
            (error_summary or "")[:1000],
            now_iso,
        ),
    )
    if commit:
        conn.commit()


def fetch_retry_queue_size(conn: Any) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM retry_queue
        WHERE status IN ('pending', 'running')
        """
    ).fetchone()
    return int(row["c"] if row is not None else 0)


def fetch_retry_depth(conn: Any) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(MAX(attempt_count), 0) AS depth
        FROM retry_queue
        WHERE status IN ('pending', 'running')
        """
    ).fetchone()
    return int(row["depth"] if row is not None else 0)


def fetch_quarantine_size(conn: Any) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM quarantine_failures
        WHERE resolved = 0
        """
    ).fetchone()
    return int(row["c"] if row is not None else 0)


def fetch_pending_retry_tasks(conn: Any, limit: int = 100) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT *
        FROM retry_queue
        WHERE status IN ('pending', 'running')
        ORDER BY due_at ASC
        LIMIT %s
        """,
        (max(1, int(limit)),),
    )
    return list(cur.fetchall())


def requeue_external_discovery_events(conn: Any, run_date: str, include_processed: bool = False) -> int:
    if include_processed:
        where_clause = "processing_status IN ('processed', 'failed')"
    else:
        where_clause = "processing_status = 'failed'"

    cur = conn.execute(
        f"""
        UPDATE external_discovery_events
        SET processing_status = 'pending',
            processed_run_id = '',
            processed_at = '',
            error_summary = ''
        WHERE observed_at::date = %s::date
          AND {where_clause}
        RETURNING event_id
        """,
        (run_date,),
    )
    rows = cur.fetchall()
    conn.commit()
    return len(rows)


def replace_ops_metrics(conn: Any, run_date: str, rows: list[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM ops_metrics WHERE run_date::date = %s::date", (run_date,))
    now_iso = utc_now_iso()
    for row in rows:
        conn.execute(
            """
            INSERT INTO ops_metrics (run_date, recorded_at, metric, value, meta_json)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                run_date,
                now_iso,
                str(row.get("metric", "unknown"))[:120],
                float(row.get("value", 0.0)),
                str(row.get("meta_json", "{}"))[:4000],
            ),
        )
    conn.commit()


def fetch_ops_metrics(conn: Any, run_date: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT run_date, recorded_at, metric, value, meta_json
        FROM ops_metrics
        WHERE run_date::date = %s::date
        ORDER BY metric ASC, recorded_at ASC
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def fetch_latest_event_ingest_lag_seconds(conn: Any, run_date: str) -> float | None:
    row = conn.execute(
        """
        SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(CAST(ingested_at AS TIMESTAMP)))) AS lag_seconds
        FROM external_discovery_events
        WHERE observed_at::date <= %s::date
        """,
        (run_date,),
    ).fetchone()
    if row is None or row["lag_seconds"] is None:
        return None
    return max(0.0, float(row["lag_seconds"]))


def fetch_precision_by_band(conn: Any, run_date: str, lookback_days: int = 14) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        WITH decisions AS (
            SELECT rl.run_id, rl.account_id, rl.decision
            FROM review_labels rl
            JOIN score_runs sr ON sr.run_id = rl.run_id
            WHERE sr.run_date::date <= %s::date
              AND sr.run_date::date >= (%s::date + %s::interval)
              AND rl.decision IN ('approved', 'rejected')
        ),
        best_band AS (
            SELECT
                s.run_id,
                s.account_id,
                CASE MAX(CASE s.tier WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END)
                    WHEN 3 THEN 'high'
                    WHEN 2 THEN 'medium'
                    ELSE 'low'
                END AS band
            FROM account_scores s
            JOIN decisions d ON d.run_id = s.run_id AND d.account_id = s.account_id
            GROUP BY s.run_id, s.account_id
        )
        SELECT
            b.band,
            COUNT(*) AS sample_size,
            AVG(CASE d.decision WHEN 'approved' THEN 1.0 ELSE 0.0 END) AS approved_rate
        FROM decisions d
        JOIN best_band b ON b.run_id = d.run_id AND b.account_id = d.account_id
        GROUP BY b.band
        ORDER BY b.band
        """,
        (run_date, run_date, f"-{max(1, int(lookback_days))} days"),
    )
    return list(cur.fetchall())


def fetch_lock_event_counts(conn: Any, lookback_hours: int = 24) -> dict[str, int]:
    cur = conn.execute(
        """
        SELECT action, COUNT(*) AS c
        FROM run_lock_events
        WHERE CAST(created_at AS TIMESTAMP) >= (CURRENT_TIMESTAMP - CAST(%s AS INTERVAL))
        GROUP BY action
        """,
        (f"{max(1, int(lookback_hours))} hours",),
    )
    counts: dict[str, int] = {}
    for row in cur.fetchall():
        counts[str(row["action"])] = int(row["c"])
    return counts


# ---------------------------------------------------------------------------
# Research CRUD
# ---------------------------------------------------------------------------


def upsert_company_research(
    conn,
    account_id: str,
    *,
    research_brief: str | None = None,
    research_profile: str | None = None,
    enrichment_json: str = "{}",
    research_status: str,
    model_used: str | None = None,
    prompt_hash: str | None = None,
) -> None:
    """Insert or update a company research record."""
    conn.execute(
        """
        INSERT INTO company_research
            (account_id, research_brief, research_profile, enrichment_json,
             research_status, researched_at, model_used, prompt_hash,
             created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (account_id) DO UPDATE SET
            research_brief   = EXCLUDED.research_brief,
            research_profile = EXCLUDED.research_profile,
            enrichment_json  = EXCLUDED.enrichment_json,
            research_status  = EXCLUDED.research_status,
            researched_at    = EXCLUDED.researched_at,
            model_used       = EXCLUDED.model_used,
            prompt_hash      = EXCLUDED.prompt_hash,
            updated_at       = CURRENT_TIMESTAMP
        """,
        (account_id, research_brief, research_profile, enrichment_json, research_status, model_used, prompt_hash),
    )
    conn.commit()


def get_company_research(conn, account_id: str) -> dict | None:
    """Return the company_research row or None."""
    row = conn.execute(
        "SELECT * FROM company_research WHERE account_id = %s",
        (account_id,),
    ).fetchone()
    return dict(row) if row else None


def get_accounts_needing_research(
    conn,
    run_date: str,
    score_run_id: str,
    max_accounts: int,
    min_tier: str,
    stale_days: int,
    current_prompt_hash: str,
) -> list[dict]:
    """
    Returns accounts that:
    1. Have a current score at min_tier or above
    2. Have no completed research, OR research older than stale_days,
       OR a different prompt_hash than current_prompt_hash
    3. Limited to max_accounts rows, ordered by signal_score DESC
    """
    tier_filter = ("high",) if min_tier == "high" else ("high", "medium")
    rows = conn.execute(
        """
        SELECT
            a.account_id,
            a.company_name,
            a.domain,
            s.score AS signal_score,
            s.tier AS signal_tier,
            s.delta_7d,
            s.top_reasons_json
        FROM account_scores s
        JOIN accounts a ON a.account_id = s.account_id
        LEFT JOIN company_research cr ON cr.account_id = a.account_id
        WHERE s.run_id = %s
          AND s.tier = ANY(%s)
          AND (
              cr.account_id IS NULL
              OR cr.research_status NOT IN ('completed', 'in_progress')
              OR cr.researched_at::timestamp < (CURRENT_TIMESTAMP - make_interval(days => %s))
              OR cr.prompt_hash IS DISTINCT FROM %s
          )
        ORDER BY s.score DESC
        LIMIT %s
        """,
        (score_run_id, list(tier_filter), stale_days, current_prompt_hash, max_accounts),
    ).fetchall()
    return [dict(r) for r in rows]


def mark_research_in_progress(conn, account_id: str) -> None:
    """Set research_status='in_progress' before making the API call."""
    conn.execute(
        """
        INSERT INTO company_research (account_id, research_status, created_at, updated_at)
        VALUES (%s, 'in_progress', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (account_id) DO UPDATE SET
            research_status = 'in_progress',
            updated_at = CURRENT_TIMESTAMP
        """,
        (account_id,),
    )
    conn.commit()


def upsert_contacts(conn, account_id: str, contacts: list[dict]) -> None:
    """Delete all existing contacts for account, then insert new ones."""
    conn.execute(
        "DELETE FROM contact_research WHERE account_id = %s",
        (account_id,),
    )
    for contact in contacts:
        identifier = contact.get("linkedin_url") or (contact.get("first_name", "") + contact.get("last_name", ""))
        contact_id = stable_hash(
            {"account_id": account_id, "identifier": identifier},
            prefix="contact",
            length=16,
        )
        conn.execute(
            """
            INSERT INTO contact_research
                (contact_id, account_id, first_name, last_name, title,
                 email, linkedin_url, management_level, year_joined, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (contact_id) DO NOTHING
            """,
            (
                contact_id,
                account_id,
                contact.get("first_name", ""),
                contact.get("last_name", ""),
                contact.get("title"),
                contact.get("email"),
                contact.get("linkedin_url"),
                contact.get("management_level"),
                contact.get("year_joined"),
            ),
        )
    conn.commit()


def get_contacts_for_account(conn, account_id: str) -> list[dict]:
    """Return all contacts for an account, ordered by management_level seniority."""
    rows = conn.execute(
        """
        SELECT * FROM contact_research
        WHERE account_id = %s
        ORDER BY CASE management_level
            WHEN 'C-Level' THEN 1
            WHEN 'VP' THEN 2
            WHEN 'Director' THEN 3
            WHEN 'Manager' THEN 4
            WHEN 'IC' THEN 5
            ELSE 6
        END
        """,
        (account_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def create_research_run(conn, run_date: str, score_run_id: str) -> str:
    """Insert a new research_runs row with status='running'. Returns research_run_id."""
    research_run_id = f"rr_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO research_runs
            (research_run_id, run_date, score_run_id, started_at, status)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP, 'running')
        """,
        (research_run_id, run_date, score_run_id),
    )
    conn.commit()
    return research_run_id


def finish_research_run(
    conn,
    research_run_id: str,
    status: str,
    accounts_attempted: int,
    accounts_completed: int,
    accounts_failed: int,
    accounts_skipped: int,
) -> None:
    """Update research_run with final counts and finished_at timestamp."""
    conn.execute(
        """
        UPDATE research_runs SET
            status = %s,
            accounts_attempted = %s,
            accounts_completed = %s,
            accounts_failed = %s,
            accounts_skipped = %s,
            finished_at = CURRENT_TIMESTAMP
        WHERE research_run_id = %s
        """,
        (status, accounts_attempted, accounts_completed, accounts_failed, accounts_skipped, research_run_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Account Labels (Web UI)
# ---------------------------------------------------------------------------


def insert_account_label(conn, account_id: str, label: str, reviewer: str = "web_ui", notes: str = "") -> str:
    import uuid

    label_id = f"lbl_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO account_labels (label_id, account_id, label, reviewer, notes)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (label_id) DO NOTHING
        """,
        (label_id, account_id, label, reviewer, notes),
    )
    conn.commit()
    return label_id


def delete_account_label(conn, label_id: str) -> None:
    conn.execute("DELETE FROM account_labels WHERE label_id = %s", (label_id,))
    conn.commit()


def get_labels_for_account(conn, account_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM account_labels WHERE account_id = %s ORDER BY created_at DESC",
        (account_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_accounts_paginated(
    conn,
    page: int = 1,
    per_page: int = 50,
    sort_by: str = "score",
    sort_dir: str = "desc",
    tier_filter: str = "",
    label_filter: str = "",
    search: str = "",
) -> tuple[list[dict], int]:
    """Return paginated accounts joined with latest scores and labels."""
    where_parts = []
    params: list = []

    if search:
        where_parts.append("(a.company_name ILIKE %s OR a.domain ILIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])

    if tier_filter:
        where_parts.append("best.tier = %s")
        params.append(tier_filter)

    if label_filter:
        where_parts.append(
            "EXISTS (SELECT 1 FROM account_labels al WHERE al.account_id = a.account_id AND al.label = %s)"
        )
        params.append(label_filter)

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    sort_map = {
        "score": "COALESCE(best.score, 0)",
        "company_name": "a.company_name",
        "domain": "a.domain",
        "tier": "best.tier",
    }
    order_col = sort_map.get(sort_by, "COALESCE(best.score, 0)")
    order_dir = "DESC" if sort_dir.lower() == "desc" else "ASC"

    count_sql = f"""
        SELECT COUNT(*) as total FROM (
            SELECT a.account_id
            FROM accounts a
            LEFT JOIN LATERAL (
                SELECT score, tier FROM account_scores
                WHERE account_id = a.account_id ORDER BY score DESC LIMIT 1
            ) best ON true
            {where_sql}
        ) sub
    """
    total = conn.execute(count_sql, params).fetchone()["total"]

    offset = (page - 1) * per_page
    data_params = list(params) + [per_page, offset]
    data_sql = f"""
        SELECT
            a.account_id, a.company_name, a.domain, a.source_type,
            COALESCE(best.score, 0) AS score,
            COALESCE(best.tier, 'low') AS tier,
            cr.research_status,
            (SELECT string_agg(al.label, ',') FROM account_labels al WHERE al.account_id = a.account_id) AS labels
        FROM accounts a
        LEFT JOIN LATERAL (
            SELECT score, tier
            FROM account_scores
            WHERE account_id = a.account_id ORDER BY score DESC LIMIT 1
        ) best ON true
        LEFT JOIN company_research cr ON cr.account_id = a.account_id
        {where_sql}
        ORDER BY {order_col} {order_dir}
        LIMIT %s OFFSET %s
    """
    rows = conn.execute(data_sql, data_params).fetchall()
    return [dict(r) for r in rows], total


def get_account_detail(conn, account_id: str) -> dict | None:
    """Full account detail with scores, signals, research, contacts, labels."""
    account = conn.execute("SELECT * FROM accounts WHERE account_id = %s", (account_id,)).fetchone()
    if not account:
        return None
    result = dict(account)

    scores = conn.execute(
        "SELECT product, score, tier FROM account_scores WHERE account_id = %s ORDER BY score DESC",
        (account_id,),
    ).fetchall()
    result["scores"] = [dict(r) for r in scores]

    signals = conn.execute(
        """SELECT signal_code, source, evidence_url, evidence_text, observed_at
           FROM signal_observations WHERE account_id = %s ORDER BY observed_at DESC LIMIT 50""",
        (account_id,),
    ).fetchall()
    result["signals"] = [dict(r) for r in signals]

    result["research"] = get_company_research(conn, account_id)
    result["contacts"] = get_contacts_for_account(conn, account_id)
    result["labels"] = get_labels_for_account(conn, account_id)
    return result


# ---------------------------------------------------------------------------
# Pipeline Runs (Web UI)
# ---------------------------------------------------------------------------


def create_ui_pipeline_run(conn, account_ids: list[str], stages: list[str]) -> str:
    import json as _json
    import uuid

    run_id = f"prun_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """INSERT INTO pipeline_runs (pipeline_run_id, account_ids_json, stages_json)
           VALUES (%s, %s, %s)""",
        (run_id, _json.dumps(account_ids), _json.dumps(stages)),
    )
    conn.commit()
    return run_id


def finish_ui_pipeline_run(conn, pipeline_run_id: str, status: str, result: dict) -> None:
    import json as _json

    conn.execute(
        """UPDATE pipeline_runs SET status = %s, result_json = %s, finished_at = CURRENT_TIMESTAMP
           WHERE pipeline_run_id = %s""",
        (status, _json.dumps(result), pipeline_run_id),
    )
    conn.commit()
