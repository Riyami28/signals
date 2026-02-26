#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any

from src import db
from src.settings import load_settings

TABLE_ORDER = [
    "accounts",
    "score_runs",
    "score_components",
    "account_scores",
    "review_labels",
    "source_metrics",
    "crawl_checkpoints",
    "crawl_attempts",
    "external_discovery_events",
    "discovery_runs",
    "discovery_candidates",
    "discovery_evidence",
    "crawl_frontier",
    "documents",
    "document_mentions",
    "observation_lineage",
    "people_watchlist",
    "people_activity",
]


def _table_exists_sqlite(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _columns_for_table_sqlite(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(row["name"]) for row in rows]


def _truncate_postgres(pg_conn: Any) -> None:
    for table in reversed(TABLE_ORDER):
        pg_conn.execute(f"DELETE FROM {table}")
    pg_conn.commit()


def _sync_serial_sequences(pg_conn: Any) -> None:
    pg_conn.execute(
        """
        SELECT setval(
            pg_get_serial_sequence('crawl_attempts', 'attempt_id'),
            COALESCE((SELECT MAX(attempt_id) FROM crawl_attempts), 1),
            true
        )
        """
    )
    pg_conn.execute(
        """
        SELECT setval(
            pg_get_serial_sequence('external_discovery_events', 'event_id'),
            COALESCE((SELECT MAX(event_id) FROM external_discovery_events), 1),
            true
        )
        """
    )
    pg_conn.commit()


def migrate(sqlite_path: Path, batch_size: int, truncate_target: bool) -> None:
    settings = load_settings()
    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = db.get_connection(settings.pg_dsn)
    db.init_db(pg_conn)

    try:
        if truncate_target:
            _truncate_postgres(pg_conn)

        totals: dict[str, int] = {}
        for table in TABLE_ORDER:
            if not _table_exists_sqlite(sqlite_conn, table):
                totals[table] = 0
                continue

            columns = _columns_for_table_sqlite(sqlite_conn, table)
            if not columns:
                totals[table] = 0
                continue

            column_list = ", ".join(columns)
            placeholders = ", ".join("?" for _ in columns)
            insert_sql = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

            source_cursor = sqlite_conn.execute(f"SELECT {column_list} FROM {table}")
            copied = 0
            while True:
                rows = source_cursor.fetchmany(batch_size)
                if not rows:
                    break
                payload = [tuple(row[column] for column in columns) for row in rows]
                pg_conn.executemany(insert_sql, payload)
                copied += len(payload)
            pg_conn.commit()
            totals[table] = copied

        _sync_serial_sequences(pg_conn)

        total_rows = sum(totals.values())
        print(f"sqlite_path={sqlite_path} postgres_dsn={settings.pg_dsn}")
        print(f"tables_migrated={len(TABLE_ORDER)} rows_copied={total_rows}")
        for table in TABLE_ORDER:
            print(f"table={table} rows={totals.get(table, 0)}")
    finally:
        sqlite_conn.close()
        pg_conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Copy Signals data from SQLite to PostgreSQL.")
    parser.add_argument(
        "--sqlite-path",
        default="data/signals.db",
        help="Path to source SQLite DB (default: data/signals.db)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Row batch size for inserts (default: 500)",
    )
    parser.add_argument(
        "--truncate-target",
        action="store_true",
        help="Delete target Postgres table contents before migration.",
    )
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite_path).expanduser().resolve()
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite source not found: {sqlite_path}")

    migrate(
        sqlite_path=sqlite_path,
        batch_size=max(1, int(args.batch_size)),
        truncate_target=bool(args.truncate_target),
    )


if __name__ == "__main__":
    main()
