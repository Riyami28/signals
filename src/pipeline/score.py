"""Scoring stage — run scoring engine and compute deltas."""

from __future__ import annotations

from datetime import date

from src import db
from src.discovery.config import classify_signal, load_signal_classes
from src.models import AccountScore
from src.scoring.engine import run_scoring
from src.scoring.rules import load_signal_rules, load_source_registry, load_thresholds
from src.settings import Settings


def baseline_score_7d(conn, account_id: str, product: str, run_date: str) -> float | None:
    cur = conn.execute(
        """
        SELECT s.score
        FROM account_scores s
        JOIN score_runs r ON r.run_id = s.run_id
        WHERE s.account_id = %s
          AND s.product = %s
          AND r.run_date::date <= (%s::date - INTERVAL '7 day')
        ORDER BY r.run_date::date DESC, r.started_at DESC
        LIMIT 1
        """,
        (account_id, product, run_date),
    )
    row = cur.fetchone()
    return None if row is None else float(row["score"])


def run_scoring_stage(conn, settings: Settings, run_date: date) -> str:
    run_date_str = run_date.isoformat()
    run_id = db.create_score_run(conn, run_date_str)

    rules = load_signal_rules(settings.signal_registry_path)
    thresholds = load_thresholds(settings.thresholds_path)
    source_registry = load_source_registry(settings.source_registry_path)
    signal_classes = load_signal_classes(settings.signal_classes_path)

    try:
        observations = db.fetch_observations_for_scoring(conn, run_date_str)
        result = run_scoring(
            run_id=run_id,
            run_date=run_date,
            observations=[dict(row) for row in observations],
            rules=rules,
            thresholds=thresholds,
            source_reliability_defaults=source_registry,
            delta_lookup=None,
        )

        # Keep account_scores exhaustive so downstream exports/metrics include silent accounts too.
        existing_scores = {(score.account_id, score.product) for score in result.account_scores}
        account_rows = conn.execute("SELECT account_id FROM accounts").fetchall()
        for row in account_rows:
            account_id = str(row["account_id"])
            for product in ("zopdev", "zopday", "zopnight"):
                if (account_id, product) in existing_scores:
                    continue
                result.account_scores.append(
                    AccountScore(
                        run_id=run_id,
                        account_id=account_id,
                        product=product,
                        score=0.0,
                        tier="low",
                        top_reasons_json="[]",
                        delta_7d=0.0,
                    )
                )

        signals_by_account_product: dict[tuple[str, str], set[str]] = {}
        for component in result.component_scores:
            key = (component.account_id, component.product)
            signals_by_account_product.setdefault(key, set()).add(component.signal_code)

        for score in result.account_scores:
            baseline = baseline_score_7d(conn, score.account_id, score.product, run_date_str)
            score.delta_7d = round(score.score - baseline, 2) if baseline is not None else 0.0
            has_primary = any(
                classify_signal(signal_code, signal_classes) == "primary"
                for signal_code in signals_by_account_product.get((score.account_id, score.product), set())
            )
            if score.tier in {"medium", "high"} and not has_primary:
                score.tier = "low"

        db.replace_run_scores(conn, run_id, result.component_scores, result.account_scores)
        db.finish_score_run(conn, run_id, status="completed", error_summary=None)
        return run_id
    except Exception as exc:
        db.finish_score_run(conn, run_id, status="failed", error_summary=str(exc)[:1000])
        raise
