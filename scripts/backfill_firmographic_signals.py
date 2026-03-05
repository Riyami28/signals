"""One-time backfill: generate firmographic signals for already-enriched accounts."""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import db
from src.collectors.firmographic_google import _generate_firmographic_signals
from src.settings import load_settings


def main():
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)

    try:
        cursor = conn.cursor()
        # Find all accounts with enrichment_json
        cursor.execute("""
            SELECT cr.account_id, cr.enrichment_json
            FROM signals.company_research cr
            WHERE cr.enrichment_json IS NOT NULL
              AND cr.enrichment_json != '{}'
              AND cr.enrichment_json != ''
        """)
        rows = cursor.fetchall()
        print(f"Found {len(rows)} accounts with enrichment data")

        total_signals = 0
        accounts_with_signals = 0

        for row in rows:
            account_id = row["account_id"] if isinstance(row, dict) else row[0]
            enrichment_json = row["enrichment_json"] if isinstance(row, dict) else row[1]

            try:
                enrichment = json.loads(enrichment_json)
            except (json.JSONDecodeError, TypeError):
                continue

            # Check if signals already exist for this account from firmographic_google
            cursor.execute(
                "SELECT 1 FROM signals.signal_observations WHERE account_id = %s AND source = 'firmographic_google' LIMIT 1",
                (account_id,),
            )
            if cursor.fetchone():
                continue

            count = _generate_firmographic_signals(conn, account_id, enrichment)
            if count:
                total_signals += count
                accounts_with_signals += 1
                print(f"  {account_id}: {count} signals generated")

        conn.commit()
        print(f"\nDone: {total_signals} signals generated for {accounts_with_signals} accounts")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
