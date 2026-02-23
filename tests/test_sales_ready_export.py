"""Integration tests for src/export/csv_exporter.export_sales_ready using Postgres."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from src import db
from src.export.csv_exporter import export_sales_ready, _SALES_READY_COLUMNS


class TestExportSalesReady:
    """Tests for the unified sales-ready CSV export."""

    def _setup_scored_account(
        self,
        conn,
        domain: str,
        company_name: str,
        score: float,
        tier: str,
        delta_7d: float = 0.0,
        top_reasons_json: str = "[]",
    ) -> tuple[str, str]:
        """Helper to create an account with a score. Returns (account_id, run_id)."""
        account_id = db.upsert_account(
            conn,
            company_name=company_name,
            domain=domain,
            source_type="seed",
            commit=False,
        )
        run_id = db.create_score_run(conn, "2026-02-23")
        db.finish_score_run(conn, run_id, status="completed")
        # Insert account_score with valid product from CHECK constraint.
        conn.execute(
            """INSERT INTO account_scores (run_id, account_id, product, score, tier, delta_7d, top_reasons_json)
            VALUES (%s, %s, 'zopdev', %s, %s, %s, %s)""",
            (run_id, account_id, score, tier, delta_7d, top_reasons_json),
        )
        conn.commit()
        return account_id, run_id

    def _read_csv(self, path: Path) -> list[dict[str, str]]:
        """Read a CSV file and return rows as dicts."""
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)

    # -------------------------------------------------------------------

    def test_output_has_correct_columns_in_correct_order(self, tmp_path):
        conn = db.get_connection()
        db.init_db(conn)

        _, run_id = self._setup_scored_account(
            conn, "acme.com", "Acme Corp", 85.0, "high"
        )

        out = tmp_path / "sales_ready.csv"
        export_sales_ready(conn, run_id, out)
        conn.close()

        with open(out, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)

        assert header == list(_SALES_READY_COLUMNS)
        assert len(header) == len(_SALES_READY_COLUMNS)

    def test_only_high_and_medium_tier_accounts_are_included(self, tmp_path):
        conn = db.get_connection()
        db.init_db(conn)

        # Use the same run for all accounts.
        run_id = db.create_score_run(conn, "2026-02-23")
        db.finish_score_run(conn, run_id, status="completed")

        acme_id = db.upsert_account(conn, "Acme", "acme.com", "seed", commit=False)
        beta_id = db.upsert_account(conn, "Beta", "beta.com", "seed", commit=False)
        gamma_id = db.upsert_account(conn, "Gamma", "gamma.com", "seed", commit=False)
        conn.commit()

        conn.execute(
            """INSERT INTO account_scores (run_id, account_id, product, score, tier, delta_7d, top_reasons_json)
            VALUES (%s, %s, 'zopdev', 90.0, 'high', 0.0, '[]')""",
            (run_id, acme_id),
        )
        conn.execute(
            """INSERT INTO account_scores (run_id, account_id, product, score, tier, delta_7d, top_reasons_json)
            VALUES (%s, %s, 'zopdev', 50.0, 'medium', 0.0, '[]')""",
            (run_id, beta_id),
        )
        conn.execute(
            """INSERT INTO account_scores (run_id, account_id, product, score, tier, delta_7d, top_reasons_json)
            VALUES (%s, %s, 'zopdev', 5.0, 'low', 0.0, '[]')""",
            (run_id, gamma_id),
        )
        conn.commit()

        out = tmp_path / "sales_ready.csv"
        export_sales_ready(conn, run_id, out)
        conn.close()

        rows = self._read_csv(out)
        domains = {r["domain"] for r in rows}
        assert "acme.com" in domains
        assert "beta.com" in domains
        assert "gamma.com" not in domains

    def test_accounts_without_research_appear_with_status_skipped(self, tmp_path):
        conn = db.get_connection()
        db.init_db(conn)

        _, run_id = self._setup_scored_account(
            conn, "noresearch.com", "NoResearch Inc", 80.0, "high"
        )

        out = tmp_path / "sales_ready.csv"
        export_sales_ready(conn, run_id, out)
        conn.close()

        rows = self._read_csv(out)
        assert len(rows) == 1
        assert rows[0]["research_status"] == "skipped"

    def test_no_none_values_in_any_csv_cell(self, tmp_path):
        conn = db.get_connection()
        db.init_db(conn)

        _, run_id = self._setup_scored_account(
            conn, "clean.io", "CleanIO", 70.0, "medium"
        )

        out = tmp_path / "sales_ready.csv"
        export_sales_ready(conn, run_id, out)
        conn.close()

        rows = self._read_csv(out)
        assert len(rows) >= 1
        for row in rows:
            for col, val in row.items():
                assert val is not None, f"None found in column {col}"
                assert val != "None", f"String 'None' found in column {col}"

    def test_excluded_domains_are_not_in_output(self, tmp_path):
        conn = db.get_connection()
        db.init_db(conn)

        # Use the same run for all accounts.
        run_id = db.create_score_run(conn, "2026-02-23")
        db.finish_score_run(conn, run_id, status="completed")

        kept_id = db.upsert_account(conn, "Kept Inc", "kept.com", "seed", commit=False)
        excluded_id = db.upsert_account(conn, "Excluded Corp", "excluded.com", "seed", commit=False)
        conn.commit()

        for aid in (kept_id, excluded_id):
            conn.execute(
                """INSERT INTO account_scores (run_id, account_id, product, score, tier, delta_7d, top_reasons_json)
                VALUES (%s, %s, 'zopdev', 80.0, 'high', 0.0, '[]')""",
                (run_id, aid),
            )
        conn.commit()

        out = tmp_path / "sales_ready.csv"
        export_sales_ready(conn, run_id, out, excluded_domains={"excluded.com"})
        conn.close()

        rows = self._read_csv(out)
        domains = {r["domain"] for r in rows}
        assert "kept.com" in domains
        assert "excluded.com" not in domains

    def test_sorted_by_signal_score_desc(self, tmp_path):
        conn = db.get_connection()
        db.init_db(conn)

        run_id = db.create_score_run(conn, "2026-02-23")
        db.finish_score_run(conn, run_id, status="completed")

        low_id = db.upsert_account(conn, "Low Score", "low.com", "seed", commit=False)
        high_id = db.upsert_account(conn, "High Score", "high.com", "seed", commit=False)
        mid_id = db.upsert_account(conn, "Mid Score", "mid.com", "seed", commit=False)
        conn.commit()

        for aid, score in [(low_id, 55.0), (high_id, 95.0), (mid_id, 75.0)]:
            conn.execute(
                """INSERT INTO account_scores (run_id, account_id, product, score, tier, delta_7d, top_reasons_json)
                VALUES (%s, %s, 'zopdev', %s, 'high', 0.0, '[]')""",
                (run_id, aid, score),
            )
        conn.commit()

        out = tmp_path / "sales_ready.csv"
        export_sales_ready(conn, run_id, out)
        conn.close()

        rows = self._read_csv(out)
        scores = [float(r["signal_score"]) for r in rows]
        assert scores == sorted(scores, reverse=True)
        assert scores[0] == 95.0

    def test_delta_7d_formatted_with_sign(self, tmp_path):
        conn = db.get_connection()
        db.init_db(conn)

        _, run_id = self._setup_scored_account(
            conn, "delta.com", "Delta Corp", 80.0, "high", delta_7d=5.2
        )

        out = tmp_path / "sales_ready.csv"
        export_sales_ready(conn, run_id, out)
        conn.close()

        rows = self._read_csv(out)
        assert len(rows) == 1
        delta_val = rows[0]["delta_7d"]
        assert delta_val.startswith("+") or delta_val.startswith("-")
        assert "+5.2" in delta_val

    def test_negative_delta_formatted_correctly(self, tmp_path):
        conn = db.get_connection()
        db.init_db(conn)

        _, run_id = self._setup_scored_account(
            conn, "negdelta.com", "NegDelta Corp", 60.0, "medium", delta_7d=-3.7
        )

        out = tmp_path / "sales_ready.csv"
        export_sales_ready(conn, run_id, out)
        conn.close()

        rows = self._read_csv(out)
        assert len(rows) == 1
        assert "-3.7" in rows[0]["delta_7d"]
