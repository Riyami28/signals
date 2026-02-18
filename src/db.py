from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import sqlite3
import uuid

from src.models import Account, AccountScore, ComponentScore, ReviewLabel, SignalObservation
from src.utils import load_csv_rows, normalize_domain, stable_hash, utc_now_iso

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
  attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
  matched_phrase TEXT NOT NULL COLLATE NOCASE,
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
"""


def get_connection(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    _run_schema_migrations(conn)
    conn.commit()


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(str(row["name"]) == column for row in rows)


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl_fragment: str) -> None:
    if _column_exists(conn, table, column):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_fragment}")


def _run_schema_migrations(conn: sqlite3.Connection) -> None:
    # Backfill newly introduced lineage columns in legacy SQLite databases.
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


def get_account_by_domain(conn: sqlite3.Connection, domain: str) -> sqlite3.Row | None:
    normalized = normalize_domain(domain)
    if not normalized:
        return None
    cur = conn.execute("SELECT * FROM accounts WHERE domain = ?", (normalized,))
    return cur.fetchone()


def upsert_account(conn: sqlite3.Connection, company_name: str, domain: str, source_type: str = "discovered") -> str:
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
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            account.account_id,
            account.company_name,
            account.domain,
            account.source_type,
            account.created_at,
        ),
    )
    conn.commit()
    return account.account_id


def seed_accounts(conn: sqlite3.Connection, seed_accounts_csv: Path) -> int:
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


def insert_signal_observation(conn: sqlite3.Connection, observation: SignalObservation) -> bool:
    try:
        conn.execute(
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def create_score_run(conn: sqlite3.Connection, run_date: str) -> str:
    run_id = f"run_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO score_runs (run_id, run_date, status, started_at)
        VALUES (?, ?, 'running', ?)
        """,
        (run_id, run_date, utc_now_iso()),
    )
    conn.commit()
    return run_id


def finish_score_run(
    conn: sqlite3.Connection,
    run_id: str,
    status: str,
    error_summary: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE score_runs
        SET status = ?, finished_at = ?, error_summary = ?
        WHERE run_id = ?
        """,
        (status, utc_now_iso(), error_summary or "", run_id),
    )
    conn.commit()


def fetch_observations_for_scoring(
    conn: sqlite3.Connection,
    run_date: str,
    lookback_days: int = 120,
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT *
        FROM signal_observations
        WHERE date(observed_at) <= date(?)
          AND date(observed_at) >= date(?, ?)
        """,
        (run_date, run_date, f"-{lookback_days} day"),
    )
    return list(cur.fetchall())


def replace_run_scores(
    conn: sqlite3.Connection,
    run_id: str,
    component_scores: list[ComponentScore],
    account_scores: list[AccountScore],
) -> None:
    conn.execute("DELETE FROM score_components WHERE run_id = ?", (run_id,))
    conn.execute("DELETE FROM account_scores WHERE run_id = ?", (run_id,))

    for component in component_scores:
        conn.execute(
            """
            INSERT INTO score_components (run_id, account_id, product, signal_code, component_score)
            VALUES (?, ?, ?, ?, ?)
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
            VALUES (?, ?, ?, ?, ?, ?, ?)
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


def get_score_delta_7d(conn: sqlite3.Connection, account_id: str, product: str, run_date: str) -> float:
    cur = conn.execute(
        """
        SELECT s.score
        FROM account_scores s
        JOIN score_runs r ON s.run_id = r.run_id
        WHERE s.account_id = ?
          AND s.product = ?
          AND date(r.run_date) <= date(?, '-7 day')
        ORDER BY date(r.run_date) DESC
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
        WHERE s.account_id = ?
          AND s.product = ?
          AND date(r.run_date) = date(?)
        ORDER BY r.started_at DESC
        LIMIT 1
        """,
        (account_id, product, run_date),
    )
    current_row = cur2.fetchone()
    if not current_row:
        return 0.0
    return round(float(current_row["score"]) - float(row["score"]), 2)


def get_latest_run_id_for_date(conn: sqlite3.Connection, run_date: str) -> str | None:
    cur = conn.execute(
        """
        SELECT run_id
        FROM score_runs
        WHERE date(run_date) = date(?)
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (run_date,),
    )
    row = cur.fetchone()
    return None if not row else str(row["run_id"])


def list_runs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    cur = conn.execute("SELECT * FROM score_runs ORDER BY started_at DESC")
    return list(cur.fetchall())


def fetch_scores_for_run(conn: sqlite3.Connection, run_id: str) -> list[sqlite3.Row]:
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
        WHERE s.run_id = ?
        ORDER BY s.score DESC
        """,
        (run_id,),
    )
    return list(cur.fetchall())


def insert_review_label(conn: sqlite3.Connection, label: ReviewLabel) -> bool:
    try:
        conn.execute(
            """
            INSERT INTO review_labels (review_id, run_id, account_id, decision, reviewer, notes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
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
        return True
    except sqlite3.IntegrityError:
        return False


def fetch_review_rows_for_date(conn: sqlite3.Connection, run_date: str) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT rl.*, r.run_date
        FROM review_labels rl
        JOIN score_runs r ON r.run_id = rl.run_id
        WHERE date(r.run_date) = date(?)
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def fetch_sources_for_account_window(
    conn: sqlite3.Connection,
    account_id: str,
    run_date: str,
    lookback_days: int = 30,
) -> list[str]:
    cur = conn.execute(
        """
        SELECT DISTINCT source
        FROM signal_observations
        WHERE account_id = ?
          AND date(observed_at) <= date(?)
          AND date(observed_at) >= date(?, ?)
        ORDER BY source
        """,
        (account_id, run_date, run_date, f"-{lookback_days} day"),
    )
    return [str(row["source"]) for row in cur.fetchall()]


def fetch_scored_sources_for_run_account(
    conn: sqlite3.Connection,
    run_id: str,
    account_id: str,
) -> list[str]:
    cur = conn.execute(
        """
        SELECT top_reasons_json
        FROM account_scores
        WHERE run_id = ?
          AND account_id = ?
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
    conn: sqlite3.Connection,
    run_date: str,
    rows: list[dict[str, float | int | str]],
) -> None:
    for row in rows:
        conn.execute(
            """
            INSERT INTO source_metrics (run_date, source, approved_rate, sample_size)
            VALUES (?, ?, ?, ?)
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


def fetch_source_metrics(conn: sqlite3.Connection, run_date: str) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT run_date, source, approved_rate, sample_size
        FROM source_metrics
        WHERE date(run_date) = date(?)
        ORDER BY source
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def fetch_recent_reviews(conn: sqlite3.Connection, run_date: str, days: int) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT rl.*, r.run_date
        FROM review_labels rl
        JOIN score_runs r ON r.run_id = rl.run_id
        WHERE date(r.run_date) <= date(?)
          AND date(r.run_date) >= date(?, ?)
        ORDER BY date(r.run_date) DESC
        """,
        (run_date, run_date, f"-{days} day"),
    )
    return list(cur.fetchall())


def account_exists(conn: sqlite3.Connection, account_id: str) -> bool:
    cur = conn.execute("SELECT 1 FROM accounts WHERE account_id = ? LIMIT 1", (account_id,))
    return cur.fetchone() is not None


def dump_run_summary(conn: sqlite3.Connection, run_id: str) -> dict[str, object]:
    cur = conn.execute(
        """
        SELECT
            COUNT(*) AS score_rows,
            COUNT(DISTINCT account_id) AS account_count
        FROM account_scores
        WHERE run_id = ?
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
    conn: sqlite3.Connection,
    source: str,
    account_id: str,
    endpoint: str,
) -> bool:
    cur = conn.execute(
        """
        SELECT 1
        FROM crawl_checkpoints
        WHERE source = ?
          AND account_id = ?
          AND endpoint = ?
          AND datetime(last_crawled_at) >= datetime('now', '-20 hours')
        LIMIT 1
        """,
        (source, account_id, endpoint),
    )
    return cur.fetchone() is not None


def mark_crawled(
    conn: sqlite3.Connection,
    source: str,
    account_id: str,
    endpoint: str,
) -> None:
    conn.execute(
        """
        INSERT INTO crawl_checkpoints (source, account_id, endpoint, last_crawled_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(source, account_id, endpoint)
        DO UPDATE SET last_crawled_at = excluded.last_crawled_at
        """,
        (source, account_id, endpoint, utc_now_iso()),
    )
    conn.commit()


def record_crawl_attempt(
    conn: sqlite3.Connection,
    source: str,
    account_id: str,
    endpoint: str,
    status: str,
    error_summary: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO crawl_attempts (source, account_id, endpoint, attempted_at, status, error_summary)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (source, account_id, endpoint, utc_now_iso(), status, (error_summary or "")[:500]),
    )
    conn.commit()


def fetch_crawl_attempt_summary(conn: sqlite3.Connection, run_date: str) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT source, status, COUNT(*) AS attempt_count
        FROM crawl_attempts
        WHERE date(attempted_at) = date(?)
        GROUP BY source, status
        ORDER BY source, status
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def fetch_latest_crawl_failures(conn: sqlite3.Connection, run_date: str, limit: int = 10) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT source, account_id, endpoint, status, error_summary, attempted_at
        FROM crawl_attempts
        WHERE date(attempted_at) = date(?)
          AND status IN ('http_error', 'exception')
        ORDER BY attempted_at DESC
        LIMIT ?
        """,
        (run_date, max(1, int(limit))),
    )
    return list(cur.fetchall())


def insert_external_discovery_event(
    conn: sqlite3.Connection,
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
    try:
        conn.execute(
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', '', '', '')
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
        return True
    except sqlite3.IntegrityError:
        return False


def fetch_pending_external_discovery_events(
    conn: sqlite3.Connection,
    run_date: str,
    limit: int = 500,
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT *
        FROM external_discovery_events
        WHERE processing_status = 'pending'
          AND date(observed_at) <= date(?)
        ORDER BY datetime(observed_at) ASC, event_id ASC
        LIMIT ?
        """,
        (run_date, max(1, int(limit))),
    )
    return list(cur.fetchall())


def mark_external_discovery_event_processed(
    conn: sqlite3.Connection,
    event_id: int,
    processed_run_id: str,
) -> None:
    conn.execute(
        """
        UPDATE external_discovery_events
        SET processing_status = 'processed',
            processed_run_id = ?,
            processed_at = ?,
            error_summary = ''
        WHERE event_id = ?
        """,
        ((processed_run_id or "").strip(), utc_now_iso(), int(event_id)),
    )
    conn.commit()


def mark_external_discovery_event_failed(
    conn: sqlite3.Connection,
    event_id: int,
    processed_run_id: str,
    error_summary: str,
) -> None:
    conn.execute(
        """
        UPDATE external_discovery_events
        SET processing_status = 'failed',
            processed_run_id = ?,
            processed_at = ?,
            error_summary = ?
        WHERE event_id = ?
        """,
        (
            (processed_run_id or "").strip(),
            utc_now_iso(),
            (error_summary or "")[:500],
            int(event_id),
        ),
    )
    conn.commit()


def insert_crawl_frontier(
    conn: sqlite3.Connection,
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
    try:
        conn.execute(
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, '', '', ?)
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
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def fetch_crawl_frontier_by_status(
    conn: sqlite3.Connection,
    run_date: str,
    status: str,
    limit: int = 500,
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT *
        FROM crawl_frontier
        WHERE run_date = ?
          AND status = ?
        ORDER BY priority DESC, first_seen_at ASC
        LIMIT ?
        """,
        (run_date, status, max(1, int(limit))),
    )
    return list(cur.fetchall())


def mark_crawl_frontier_status(
    conn: sqlite3.Connection,
    frontier_id: str,
    status: str,
    error_summary: str = "",
    bump_retry: bool = False,
) -> None:
    if bump_retry:
        conn.execute(
            """
            UPDATE crawl_frontier
            SET status = ?,
                retry_count = retry_count + 1,
                last_attempt_at = ?,
                last_error = ?
            WHERE frontier_id = ?
            """,
            (status, utc_now_iso(), (error_summary or "")[:500], frontier_id),
        )
    else:
        conn.execute(
            """
            UPDATE crawl_frontier
            SET status = ?,
                last_attempt_at = ?,
                last_error = ?
            WHERE frontier_id = ?
            """,
            (status, utc_now_iso(), (error_summary or "")[:500], frontier_id),
        )
    conn.commit()


def get_document_by_frontier_id(conn: sqlite3.Connection, frontier_id: str) -> sqlite3.Row | None:
    cur = conn.execute(
        """
        SELECT *
        FROM documents
        WHERE frontier_id = ?
        LIMIT 1
        """,
        (frontier_id,),
    )
    return cur.fetchone()


def upsert_document(
    conn: sqlite3.Connection,
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
) -> str:
    document_id = stable_hash({"canonical_url": canonical_url}, prefix="doc", length=16)
    now = utc_now_iso()
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        conn.commit()
    except sqlite3.IntegrityError:
        # Content hash collisions can happen when multiple URLs resolve to the same article.
        row = conn.execute(
            """
            SELECT document_id
            FROM documents
            WHERE canonical_url = ?
               OR content_sha256 = ?
            LIMIT 1
            """,
            ((canonical_url or "").strip(), (content_sha256 or "").strip()),
        ).fetchone()
        if row is not None:
            return str(row["document_id"])
        raise
    return document_id


def fetch_documents_for_run_by_frontier_status(
    conn: sqlite3.Connection,
    run_date: str,
    frontier_status: str,
    limit: int = 500,
) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT d.*, f.url_type, f.depth, f.priority, f.payload_json, f.frontier_id, f.source_event_id, f.source
        FROM documents d
        JOIN crawl_frontier f ON f.frontier_id = d.frontier_id
        WHERE f.run_date = ?
          AND f.status = ?
        ORDER BY f.priority DESC, d.updated_at ASC
        LIMIT ?
        """,
        (run_date, frontier_status, max(1, int(limit))),
    )
    return list(cur.fetchall())


def insert_document_mention(
    conn: sqlite3.Connection,
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
    try:
        conn.execute(
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        conn.commit()
        return mention_id, True
    except sqlite3.IntegrityError:
        return mention_id, False


def insert_observation_lineage(
    conn: sqlite3.Connection,
    obs_id: str,
    account_id: str,
    document_id: str,
    mention_id: str,
    source_event_id: str,
    run_date: str,
) -> bool:
    try:
        conn.execute(
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
            VALUES (?, ?, ?, ?, ?, ?, ?)
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
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def upsert_people_watchlist_entry(
    conn: sqlite3.Connection,
    account_id: str,
    person_name: str,
    role_title: str,
    role_weight: float,
    source_url: str,
    is_active: bool = True,
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    conn.commit()
    return watch_id


def insert_people_activity(
    conn: sqlite3.Connection,
    account_id: str,
    person_name: str,
    role_title: str,
    document_id: str,
    activity_type: str,
    summary: str,
    published_at: str,
    url: str,
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
    try:
        conn.execute(
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def fetch_story_evidence_rows(conn: sqlite3.Connection, run_date: str) -> list[sqlite3.Row]:
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
        WHERE f.run_date = ?
        ORDER BY d.evidence_quality DESC, d.relevance_score DESC, d.updated_at DESC
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def fetch_signal_lineage_rows(conn: sqlite3.Connection, run_date: str) -> list[sqlite3.Row]:
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
        WHERE ol.run_date = ?
        ORDER BY so.evidence_quality DESC, so.relevance_score DESC, so.confidence DESC
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def create_discovery_run(conn: sqlite3.Connection, run_date: str, score_run_id: str) -> str:
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
        VALUES (?, ?, ?, ?, 'running', 0, 0, 0, 0, '')
        """,
        (discovery_run_id, run_date, score_run_id, utc_now_iso()),
    )
    conn.commit()
    return discovery_run_id


def finish_discovery_run(
    conn: sqlite3.Connection,
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
        SET status = ?,
            source_events_processed = ?,
            observations_inserted = ?,
            total_candidates = ?,
            crm_eligible_candidates = ?,
            error_summary = ?
        WHERE discovery_run_id = ?
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
    conn: sqlite3.Connection,
    discovery_run_id: str,
    candidates: list[dict[str, object]],
    evidence_rows: list[dict[str, object]],
) -> None:
    conn.execute("DELETE FROM discovery_candidates WHERE discovery_run_id = ?", (discovery_run_id,))
    conn.execute("DELETE FROM discovery_evidence WHERE discovery_run_id = ?", (discovery_run_id,))

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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            INSERT OR REPLACE INTO discovery_evidence (
                discovery_run_id,
                account_id,
                signal_code,
                source,
                evidence_url,
                evidence_text,
                component_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
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


def get_latest_discovery_run_id_for_date(conn: sqlite3.Connection, run_date: str) -> str | None:
    cur = conn.execute(
        """
        SELECT discovery_run_id
        FROM discovery_runs
        WHERE date(run_date) = date(?)
          AND status = 'completed'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (run_date,),
    )
    row = cur.fetchone()
    return None if row is None else str(row["discovery_run_id"])


def fetch_discovery_candidates_for_run(conn: sqlite3.Connection, discovery_run_id: str) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT *
        FROM discovery_candidates
        WHERE discovery_run_id = ?
        ORDER BY rank_score DESC, score DESC, company_name ASC
        """,
        (discovery_run_id,),
    )
    return list(cur.fetchall())


def fetch_discovery_run(conn: sqlite3.Connection, discovery_run_id: str) -> sqlite3.Row | None:
    cur = conn.execute(
        """
        SELECT *
        FROM discovery_runs
        WHERE discovery_run_id = ?
        LIMIT 1
        """,
        (discovery_run_id,),
    )
    return cur.fetchone()
