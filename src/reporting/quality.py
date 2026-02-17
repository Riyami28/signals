from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

from src import db
from src.utils import load_csv_rows


def _decision_value(decision: str) -> int | None:
    normalized = decision.strip().lower()
    if normalized == "approved":
        return 1
    if normalized == "rejected":
        return 0
    return None


def compute_and_persist_source_metrics(conn, run_date: date) -> list[dict[str, float | int | str]]:
    rows = db.fetch_review_rows_for_date(conn, run_date.isoformat())

    counters: dict[str, dict[str, float]] = defaultdict(lambda: {"approved": 0.0, "total": 0.0})

    for row in rows:
        decision = _decision_value(str(row["decision"]))
        if decision is None:
            continue

        run_id = str(row["run_id"])
        account_id = str(row["account_id"])
        sources = db.fetch_scored_sources_for_run_account(conn, run_id=run_id, account_id=account_id)
        if not sources:
            sources = db.fetch_sources_for_account_window(conn, account_id=account_id, run_date=run_date.isoformat())
        if not sources:
            sources = ["unknown"]

        for source in set(sources):
            counters[source]["approved"] += float(decision)
            counters[source]["total"] += 1.0

        counters["__global__"]["approved"] += float(decision)
        counters["__global__"]["total"] += 1.0

    metric_rows: list[dict[str, float | int | str]] = []
    for source, values in sorted(counters.items()):
        total = int(values["total"])
        if total <= 0:
            continue
        approved_rate = round(values["approved"] / values["total"], 4)
        metric_rows.append(
            {
                "source": source,
                "approved_rate": approved_rate,
                "sample_size": total,
            }
        )

    db.upsert_source_metrics(conn, run_date.isoformat(), metric_rows)
    return metric_rows


def compute_promotion_readiness(conn, run_date: date) -> list[dict[str, float | int | str]]:
    review_rows = db.fetch_recent_reviews(conn, run_date.isoformat(), days=13)
    decision_rows = [row for row in review_rows if _decision_value(str(row["decision"])) is not None]

    windows = [
        ("week_1", run_date - timedelta(days=6), run_date),
        ("week_2", run_date - timedelta(days=13), run_date - timedelta(days=7)),
    ]

    output_rows: list[dict[str, float | int | str]] = []
    all_windows_ready = True

    for label, start, end in windows:
        sample = 0
        approved = 0
        for row in decision_rows:
            row_date = date.fromisoformat(str(row["run_date"]))
            if start <= row_date <= end:
                sample += 1
                approved += int(_decision_value(str(row["decision"])) or 0)

        approved_rate = round((approved / sample) if sample else 0.0, 4)
        meets_rate = approved_rate >= 0.70
        meets_sample = sample >= 50
        all_windows_ready = all_windows_ready and meets_rate and meets_sample

        output_rows.append(
            {
                "run_date": run_date.isoformat(),
                "window": label,
                "approved_rate": approved_rate,
                "sample_size": sample,
                "meets_rate": int(meets_rate),
                "meets_sample": int(meets_sample),
                "ready_for_promotion": 0,
            }
        )

    output_rows.append(
        {
            "run_date": run_date.isoformat(),
            "window": "summary",
            "approved_rate": 0,
            "sample_size": 0,
            "meets_rate": 0,
            "meets_sample": 0,
            "ready_for_promotion": int(all_windows_ready),
        }
    )

    return output_rows


def compute_icp_coverage(
    conn,
    run_id: str,
    reference_csv_path,
) -> tuple[list[dict[str, str | float]], dict[str, float | int]]:
    reference_rows = load_csv_rows(reference_csv_path)
    if not reference_rows:
        return [], {"total_accounts": 0, "high_or_medium_accounts": 0, "coverage_rate": 0.0}

    score_rows = conn.execute(
        """
        SELECT a.domain, a.company_name, s.product, s.score, s.tier
        FROM account_scores s
        JOIN accounts a ON a.account_id = s.account_id
        WHERE s.run_id = ?
        """,
        (run_id,),
    ).fetchall()

    by_domain_product: dict[str, dict[str, tuple[float, str]]] = defaultdict(dict)
    by_domain_name: dict[str, str] = {}
    for row in score_rows:
        domain = str(row["domain"])
        product = str(row["product"])
        score = float(row["score"])
        tier = str(row["tier"])
        by_domain_name[domain] = str(row["company_name"])
        by_domain_product[domain][product] = (score, tier)

    output_rows: list[dict[str, str | float]] = []
    covered = 0
    for row in reference_rows:
        domain = (row.get("domain", "") or "").strip().lower()
        if not domain:
            continue
        company_name = row.get("company_name", "") or by_domain_name.get(domain, domain)
        relationship_stage = row.get("relationship_stage", "") or "unknown"

        zopdev_score, zopdev_tier = by_domain_product.get(domain, {}).get("zopdev", (0.0, "none"))
        zopday_score, zopday_tier = by_domain_product.get(domain, {}).get("zopday", (0.0, "none"))
        zopnight_score, zopnight_tier = by_domain_product.get(domain, {}).get("zopnight", (0.0, "none"))
        max_score = max(zopdev_score, zopday_score, zopnight_score)
        max_tier = "high" if "high" in {zopdev_tier, zopday_tier, zopnight_tier} else (
            "medium" if "medium" in {zopdev_tier, zopday_tier, zopnight_tier} else "low"
        )

        if max_tier in {"high", "medium"}:
            covered += 1

        output_rows.append(
            {
                "company_name": company_name,
                "domain": domain,
                "relationship_stage": relationship_stage,
                "zopdev_score": round(zopdev_score, 2),
                "zopdev_tier": zopdev_tier,
                "zopday_score": round(zopday_score, 2),
                "zopday_tier": zopday_tier,
                "zopnight_score": round(zopnight_score, 2),
                "zopnight_tier": zopnight_tier,
                "max_score": round(max_score, 2),
                "max_tier": max_tier,
            }
        )

    total = len(output_rows)
    coverage_rate = round((covered / total) if total else 0.0, 4)
    summary = {"total_accounts": total, "high_or_medium_accounts": covered, "coverage_rate": coverage_rate}
    return output_rows, summary
