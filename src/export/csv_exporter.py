from __future__ import annotations

import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

from src import db
from src.models import EnrichmentData
from src.scoring.rules import legacy_tier_from_v2
from src.utils import normalize_domain, write_csv_rows

logger = logging.getLogger(__name__)


def date_suffix(run_date: date) -> str:
    return run_date.strftime("%Y%m%d")


def output_paths(out_dir: Path, run_date: date) -> dict[str, Path]:
    suffix = date_suffix(run_date)
    return {
        "review_queue": out_dir / f"review_queue_{suffix}.csv",
        "daily_scores": out_dir / f"daily_scores_{suffix}.csv",
        "source_quality": out_dir / f"source_quality_{suffix}.csv",
        "promotion_readiness": out_dir / f"promotion_readiness_{suffix}.csv",
        "ops_metrics": out_dir / f"ops_metrics_{suffix}.csv",
    }


def _parse_reasons(top_reasons_json: str) -> list[dict[str, Any]]:
    if not top_reasons_json:
        return []
    try:
        parsed = json.loads(top_reasons_json)
        if isinstance(parsed, list):
            return [row for row in parsed if isinstance(row, dict)]
    except json.JSONDecodeError:
        return []
    return []


def _legacy_tier(row: dict[str, Any]) -> str:
    tier_v2 = str(row.get("tier_v2", "") or "").strip().lower()
    legacy = str(row.get("tier", "") or "").strip().lower()
    # Backward compatibility: rows inserted by legacy paths may only set tier (high/medium/low),
    # while tier_v2 is left at the DB default tier_4.
    if tier_v2 == "tier_4" and legacy in {"high", "medium"}:
        return legacy
    if tier_v2:
        return legacy_tier_from_v2(tier_v2)
    return legacy


def export_daily_scores(conn, run_id: str, output_path: Path) -> int:
    rows = db.fetch_scores_for_run(conn, run_id)
    export_rows: list[dict[str, Any]] = []
    for row in rows:
        export_rows.append(
            {
                "run_date": row["run_date"],
                "account_id": row["account_id"],
                "company_name": row["company_name"],
                "domain": row["domain"],
                "product": row["product"],
                "score": row["score"],
                "tier": row["tier"],
                "tier_v2": row.get("tier_v2", ""),
                "delta_7d": row["delta_7d"],
                "velocity_7d": row.get("velocity_7d", 0.0),
                "velocity_14d": row.get("velocity_14d", 0.0),
                "velocity_30d": row.get("velocity_30d", 0.0),
                "velocity_category": row.get("velocity_category", "stable"),
                "top_reasons_json": row["top_reasons_json"],
                "confidence_band": row.get("confidence_band", "low"),
                "dimension_confidence_json": row.get("dimension_confidence_json", "{}"),
            }
        )

    write_csv_rows(
        output_path,
        export_rows,
        fieldnames=[
            "run_date",
            "account_id",
            "company_name",
            "domain",
            "product",
            "score",
            "tier",
            "tier_v2",
            "delta_7d",
            "velocity_7d",
            "velocity_14d",
            "velocity_30d",
            "velocity_category",
            "top_reasons_json",
            "confidence_band",
            "dimension_confidence_json",
        ],
    )
    return len(export_rows)


def export_review_queue(
    conn,
    run_id: str,
    output_path: Path,
    excluded_domains: set[str] | None = None,
) -> int:
    rows = db.fetch_scores_for_run(conn, run_id)
    queue_rows: list[dict[str, Any]] = []
    excluded = excluded_domains or set()
    best_by_account: dict[str, dict[str, Any]] = {}
    tier_rank = {"high": 2, "medium": 1}

    for row in rows:
        tier = _legacy_tier(row)
        if tier not in {"high", "medium"}:
            continue
        domain = normalize_domain(str(row["domain"] or ""))
        if domain in excluded:
            continue

        account_id = str(row["account_id"])
        score = float(row["score"])
        current = best_by_account.get(account_id)
        if current is not None:
            current_tier = str(current["tier"]).lower()
            current_score = float(current["score"])
            keep_existing = (tier_rank.get(current_tier, 0), current_score) >= (tier_rank.get(tier, 0), score)
            if keep_existing:
                continue
        best_by_account[account_id] = dict(row)

    selected_rows = sorted(
        best_by_account.values(),
        key=lambda row: (float(row["score"]), str(row["company_name"]).lower()),
        reverse=True,
    )

    for row in selected_rows:
        reasons = _parse_reasons(str(row["top_reasons_json"] or ""))
        reason_1 = reasons[0]["signal_code"] if len(reasons) > 0 else ""
        reason_2 = reasons[1]["signal_code"] if len(reasons) > 1 else ""
        reason_3 = reasons[2]["signal_code"] if len(reasons) > 2 else ""

        links = []
        for reason in reasons:
            url = str(reason.get("evidence_url", "")).strip()
            if url:
                links.append(url)

        conf_band = str(row.get("confidence_band", "low") or "low")
        needs_validation = conf_band == "low" and str(row["tier"]) in {"high", "medium"}

        queue_rows.append(
            {
                "run_date": row["run_date"],
                "account_id": row["account_id"],
                "company_name": row["company_name"],
                "domain": row["domain"],
                "product": row["product"],
                "score": row["score"],
                "tier": row["tier"],
                "velocity_7d": row.get("velocity_7d", 0.0),
                "velocity_14d": row.get("velocity_14d", 0.0),
                "velocity_30d": row.get("velocity_30d", 0.0),
                "velocity_category": row.get("velocity_category", "stable"),
                "confidence_band": conf_band,
                "needs_validation": "yes" if needs_validation else "",
                "top_reason_1": reason_1,
                "top_reason_2": reason_2,
                "top_reason_3": reason_3,
                "evidence_links": " | ".join(links),
            }
        )

    write_csv_rows(
        output_path,
        queue_rows,
        fieldnames=[
            "run_date",
            "account_id",
            "company_name",
            "domain",
            "product",
            "score",
            "tier",
            "velocity_7d",
            "velocity_14d",
            "velocity_30d",
            "velocity_category",
            "confidence_band",
            "needs_validation",
            "top_reason_1",
            "top_reason_2",
            "top_reason_3",
            "evidence_links",
        ],
    )
    return len(queue_rows)


def export_source_quality(conn, run_date: str, output_path: Path) -> int:
    rows = db.fetch_source_metrics(conn, run_date)
    export_rows: list[dict[str, Any]] = []
    for row in rows:
        export_rows.append(
            {
                "run_date": row["run_date"],
                "source": row["source"],
                "approved_rate": row["approved_rate"],
                "sample_size": row["sample_size"],
            }
        )

    write_csv_rows(
        output_path,
        export_rows,
        fieldnames=["run_date", "source", "approved_rate", "sample_size"],
    )
    return len(export_rows)


def export_promotion_readiness(rows: list[dict[str, Any]], output_path: Path) -> int:
    write_csv_rows(
        output_path,
        rows,
        fieldnames=[
            "run_date",
            "window",
            "approved_rate",
            "sample_size",
            "meets_rate",
            "meets_sample",
            "ready_for_promotion",
        ],
    )
    return len(rows)


def export_ops_metrics(conn, run_date: str, output_path: Path) -> int:
    rows = db.fetch_ops_metrics(conn, run_date)
    export_rows: list[dict[str, Any]] = []
    for row in rows:
        export_rows.append(
            {
                "run_date": row["run_date"],
                "recorded_at": row["recorded_at"],
                "metric": row["metric"],
                "value": row["value"],
                "meta_json": row["meta_json"],
            }
        )
    write_csv_rows(
        output_path,
        export_rows,
        fieldnames=["run_date", "recorded_at", "metric", "value", "meta_json"],
    )
    return len(export_rows)


_SALES_READY_COLUMNS = [
    "company_name",
    "domain",
    "website",
    "industry",
    "sub_industry",
    "country",
    "city",
    "state",
    "employees",
    "employee_range",
    "revenue_range",
    "company_linkedin_url",
    "signal_score",
    "signal_tier",
    "confidence_band",
    "delta_7d",
    "velocity_7d",
    "velocity_14d",
    "velocity_30d",
    "velocity_category",
    "top_signals",
    "evidence_links",
    "top_reason_1",
    "top_reason_2",
    "top_reason_3",
    "research_brief",
    "research_summary",
    "key_contacts",
    "conversation_starters",
    "research_status",
    "source_type",
    "first_seen_date",
    "last_signal_date",
]

_CONFIDENCE_THRESHOLD = 0.5


def _enrichment_field(enrichment: dict, field_name: str) -> str:
    """Extract a field from enrichment JSON, respecting confidence threshold."""
    conf_key = f"{field_name}_confidence"
    conf = enrichment.get(conf_key, 1.0)
    try:
        conf = float(conf)
    except (ValueError, TypeError):
        conf = 1.0
    if conf < _CONFIDENCE_THRESHOLD:
        return ""
    val = enrichment.get(field_name, "")
    if val is None:
        return ""
    if isinstance(val, list):
        return ", ".join(str(v) for v in val)
    return str(val).strip()


def _format_delta(delta: Any) -> str:
    """Format delta as +5.2 or -3.1."""
    try:
        val = float(delta or 0)
    except (ValueError, TypeError):
        return ""
    return f"{val:+.1f}"


def _extract_starters_from_profile(profile: str) -> str:
    """Extract conversation starters section from research profile."""
    if not profile:
        return ""
    match = re.search(r"##\s*Conversation\s*Starters?\s*\n(.*?)(?:\n##|\Z)", profile, re.DOTALL | re.IGNORECASE)
    if match:
        lines = match.group(1).strip().splitlines()
        items = []
        for line in lines:
            stripped = line.strip()
            m = re.match(r"^[-*•]\s*(.*)", stripped)
            if m and m.group(1).strip():
                items.append(m.group(1).strip())
        return "\n".join(items)
    return ""


def _iso_date(ts: Any) -> str:
    """Extract YYYY-MM-DD from a timestamp string."""
    if not ts:
        return ""
    s = str(ts).strip()
    if len(s) >= 10:
        return s[:10]
    return s


def export_sales_ready(
    conn,
    score_run_id: str,
    output_path: Path,
    excluded_domains: set[str] | None = None,
) -> int:
    """Export the unified sales-ready CSV. Returns number of rows written."""
    excluded = excluded_domains or set()

    # Fetch scores for this run.
    score_rows = db.fetch_scores_for_run(conn, score_run_id)

    # Dedupe to best product per account.
    tier_rank = {"high": 2, "medium": 1}
    best_by_account: dict[str, dict] = {}
    for row in score_rows:
        tier = _legacy_tier(row)
        if tier not in {"high", "medium"}:
            continue
        domain = normalize_domain(str(row.get("domain", "") or ""))
        if domain in excluded:
            continue
        account_id = str(row["account_id"])
        score = float(row.get("score", 0) or 0)
        current = best_by_account.get(account_id)
        if current:
            ct = str(current.get("tier", "")).lower()
            cs = float(current.get("score", 0) or 0)
            if (tier_rank.get(ct, 0), cs) >= (tier_rank.get(tier, 0), score):
                continue
        best_by_account[account_id] = dict(row)

    # Sort by signal_score DESC.
    sorted_rows = sorted(
        best_by_account.values(),
        key=lambda r: float(r.get("score", 0) or 0),
        reverse=True,
    )

    # Batch fetch last signal dates.
    account_ids = [str(r["account_id"]) for r in sorted_rows]
    last_signal_dates: dict[str, str] = {}
    if account_ids:
        signal_rows = conn.execute(
            """
            SELECT account_id, MAX(observed_at) AS last_observed
            FROM signal_observations
            WHERE account_id = ANY(%s)
            GROUP BY account_id
            """,
            (account_ids,),
        ).fetchall()
        for sr in signal_rows:
            last_signal_dates[str(sr["account_id"])] = _iso_date(sr["last_observed"])

    export_rows: list[dict[str, Any]] = []

    for row in sorted_rows:
        account_id = str(row["account_id"])

        # Research data (may be absent).
        research = db.get_company_research(conn, account_id)
        enrichment: dict = {}
        if research:
            try:
                raw_enrich = json.loads(research.get("enrichment_json", "{}") or "{}")
                validated = EnrichmentData.model_validate(raw_enrich)
                enrichment = validated.model_dump()

                # Validation logic
                fields_to_check = [
                    "website",
                    "industry",
                    "sub_industry",
                    "country",
                    "city",
                    "state",
                    "employees",
                    "employee_range",
                    "revenue_range",
                    "company_linkedin_url",
                    "funding_stage",
                    "total_funding",
                ]
                missing = []
                for field in fields_to_check:
                    val = enrichment.get(field)
                    if val is None or val == "" or val == []:
                        missing.append(field)

                if missing:
                    logger.warning("Enrichment data for %s is missing fields: %s", domain, ", ".join(missing))

                completeness = 1.0 - (len(missing) / len(fields_to_check)) if fields_to_check else 1.0

                # Record ops metric
                db.insert_ops_metric(
                    conn,
                    run_date=score_run_id.split("_")[0]
                    if "_" in score_run_id
                    else score_run_id,  # Or use today's date if safer, but since this exports via run_id we can use it. Wait, `insert_ops_metric` expects `run_date`
                    metric="enrichment_completeness",
                    value=completeness,
                    meta_json=json.dumps({"account_id": account_id}, ensure_ascii=True),
                )

            except Exception as exc:
                logger.warning("Failed to parse enrichment JSON for account %s: %s", account_id, exc)
                enrichment = {}

        # Contacts.
        contacts = db.get_contacts_for_account(conn, account_id)
        contact_lines = []
        for c in contacts[:5]:
            line = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
            title = c.get("title", "")
            if title:
                line += f" ({title})"
            linkedin = c.get("linkedin_url", "")
            if linkedin:
                line += f" \u2014 {linkedin}"
            contact_lines.append(line)

        # Top reasons.
        reasons = _parse_reasons(str(row.get("top_reasons_json", "") or ""))
        top_signals = [str(r.get("signal_code", "")) for r in reasons if r.get("signal_code")]
        evidence_urls: list[str] = []
        seen_urls: set[str] = set()
        for r in reasons:
            url = str(r.get("evidence_url", "")).strip()
            if url and url not in seen_urls:
                seen_urls.add(url)
                evidence_urls.append(url)

        def _reason_str(idx: int) -> str:
            if idx >= len(reasons):
                return ""
            r = reasons[idx]
            code = r.get("signal_code", "")
            source = r.get("source", "")
            return f"{code} via {source}" if source else code

        research_brief = (research or {}).get("research_brief", "") or ""
        research_profile = (research or {}).get("research_profile", "") or ""
        starters = _extract_starters_from_profile(research_profile)
        research_status = (research or {}).get("research_status", "skipped") or "skipped"

        export_rows.append(
            {
                "company_name": str(row.get("company_name", "") or ""),
                "domain": str(row.get("domain", "") or ""),
                "website": _enrichment_field(enrichment, "website"),
                "industry": _enrichment_field(enrichment, "industry"),
                "sub_industry": _enrichment_field(enrichment, "sub_industry"),
                "country": _enrichment_field(enrichment, "country"),
                "city": _enrichment_field(enrichment, "city"),
                "state": _enrichment_field(enrichment, "state"),
                "employees": _enrichment_field(enrichment, "employees"),
                "employee_range": _enrichment_field(enrichment, "employee_range"),
                "revenue_range": _enrichment_field(enrichment, "revenue_range"),
                "company_linkedin_url": _enrichment_field(enrichment, "company_linkedin_url"),
                "signal_score": str(row.get("score", "") or ""),
                "signal_tier": str(row.get("tier_v2", "") or row.get("tier", "") or ""),
                "delta_7d": _format_delta(row.get("delta_7d")),
                "velocity_7d": _format_delta(row.get("velocity_7d")),
                "velocity_14d": _format_delta(row.get("velocity_14d")),
                "velocity_30d": _format_delta(row.get("velocity_30d")),
                "velocity_category": str(row.get("velocity_category", "stable") or "stable"),
                "top_signals": "|".join(top_signals),
                "evidence_links": "|".join(evidence_urls),
                "top_reason_1": _reason_str(0),
                "top_reason_2": _reason_str(1),
                "top_reason_3": _reason_str(2),
                "research_brief": research_brief,
                "research_summary": research_profile,
                "key_contacts": "\n".join(contact_lines),
                "conversation_starters": starters,
                "research_status": research_status,
                "source_type": str(row.get("source_type", "") or ""),
                "first_seen_date": _iso_date(row.get("created_at", "")),
                "last_signal_date": last_signal_dates.get(account_id, ""),
            }
        )

    write_csv_rows(output_path, export_rows, fieldnames=_SALES_READY_COLUMNS)
    logger.info("export_sales_ready rows=%d path=%s", len(export_rows), output_path)
    return len(export_rows)
