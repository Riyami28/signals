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
    conn.commit()


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
                confidence,
                source_reliability,
                raw_payload_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
