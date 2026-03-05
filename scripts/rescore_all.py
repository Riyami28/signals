"""Re-run scoring engine to pick up new firmographic signals."""
from __future__ import annotations

import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import db
from src.pipeline.score import run_scoring_stage
from src.settings import load_settings


def main():
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
    try:
        run_date = date.today()
        print(f"Running scoring for {run_date}...")
        run_id = run_scoring_stage(conn, settings, run_date)
        print(f"Scoring complete. run_id={run_id}")
        summary = db.dump_run_summary(conn, run_id)
        print(f"  accounts: {summary.get('account_count', 0)}, scores: {summary.get('score_rows', 0)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
