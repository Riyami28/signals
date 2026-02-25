from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.utils import load_csv_rows, normalize_domain, write_csv_rows

TIER_ORDER = {"none": -1, "low": 0, "medium": 1, "high": 2}
PRIORITY_ORDER = {"p0": 0, "p1": 1, "p2": 2, "p3": 3}
PRODUCTS = ("zopdev", "zopday", "zopnight")
VALID_PRODUCTS = {"zopdev", "zopday", "zopnight", "shared", "all"}
VALID_STAGES = {"customer", "poc", "all"}


@dataclass(frozen=True)
class PlaybookSignal:
    relationship_stage: str
    product: str
    signal_code: str
    priority: str = "p2"
    recommended_source: str = ""
    action_hint: str = ""


DEFAULT_PLAYBOOK = [
    PlaybookSignal(
        relationship_stage="all",
        product="shared",
        signal_code="poc_stage_progression",
        priority="p0",
        recommended_source="first_party_csv",
        action_hint="capture procurement, legal, and security milestones",
    ),
    PlaybookSignal(
        relationship_stage="all",
        product="shared",
        signal_code="repo_added_deploy_attempted",
        priority="p1",
        recommended_source="first_party_csv",
        action_hint="capture product usage progression inside POC accounts",
    ),
    PlaybookSignal(
        relationship_stage="customer",
        product="zopdev",
        signal_code="compliance_initiative",
        priority="p0",
        recommended_source="news_csv",
        action_hint="track upcoming SOC2, ISO, or audit-driven initiatives",
    ),
    PlaybookSignal(
        relationship_stage="customer",
        product="zopday",
        signal_code="supply_chain_platform_rollout",
        priority="p0",
        recommended_source="first_party_csv",
        action_hint="track control tower or platform rollout milestones",
    ),
    PlaybookSignal(
        relationship_stage="customer",
        product="zopnight",
        signal_code="cost_reduction_mandate",
        priority="p0",
        recommended_source="first_party_csv",
        action_hint="track board-level cost and optimization mandates",
    ),
    PlaybookSignal(
        relationship_stage="poc",
        product="shared",
        signal_code="audit_viewed",
        priority="p1",
        recommended_source="first_party_csv",
        action_hint="track security/compliance intent from active evaluators",
    ),
    PlaybookSignal(
        relationship_stage="poc",
        product="zopdev",
        signal_code="devops_role_open",
        priority="p2",
        recommended_source="jobs_csv",
        action_hint="monitor build-vs-buy pressure via hiring trends",
    ),
    PlaybookSignal(
        relationship_stage="poc",
        product="zopnight",
        signal_code="vendor_consolidation_program",
        priority="p1",
        recommended_source="first_party_csv",
        action_hint="track consolidation pressure before budget cycles",
    ),
]


def _normalize_stage(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    return normalized if normalized in VALID_STAGES else "all"


def _normalize_product(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    return normalized if normalized in VALID_PRODUCTS else "shared"


def _normalize_priority(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    return normalized if normalized in PRIORITY_ORDER else "p2"


def load_icp_signal_playbook(path: Path | None = None) -> list[PlaybookSignal]:
    if path is None or not path.exists():
        return list(DEFAULT_PLAYBOOK)

    rows = load_csv_rows(path)
    signals: list[PlaybookSignal] = []
    for row in rows:
        signal_code = (row.get("signal_code", "") or "").strip()
        if not signal_code:
            continue
        signals.append(
            PlaybookSignal(
                relationship_stage=_normalize_stage(row.get("relationship_stage", "all")),
                product=_normalize_product(row.get("product", "shared")),
                signal_code=signal_code,
                priority=_normalize_priority(row.get("priority", "p2")),
                recommended_source=(row.get("recommended_source", "") or "").strip(),
                action_hint=(row.get("action_hint", "") or "").strip(),
            )
        )
    return signals or list(DEFAULT_PLAYBOOK)


def _best_score_for_domain(
    score_by_domain_product: dict[tuple[str, str], tuple[float, str]],
    domain: str,
) -> tuple[float, str]:
    best_score = 0.0
    best_tier = "none"
    for product in PRODUCTS:
        score, tier = score_by_domain_product.get((domain, product), (0.0, "none"))
        if score > best_score:
            best_score = score
            best_tier = tier
        elif score == best_score and TIER_ORDER.get(tier, -1) > TIER_ORDER.get(best_tier, -1):
            best_tier = tier
    return round(best_score, 2), best_tier


def compute_icp_signal_gaps(
    conn: Any,
    run_id: str,
    reference_csv_path: Path,
    playbook_path: Path | None = None,
) -> tuple[list[dict[str, Any]], dict[str, float | int]]:
    reference_rows = load_csv_rows(reference_csv_path)
    if not reference_rows:
        return [], {
            "total_accounts": 0,
            "expected_signals": 0,
            "observed_signals": 0,
            "coverage_rate": 0.0,
            "high_priority_gaps": 0,
            "accounts_with_full_coverage": 0,
        }

    playbook = load_icp_signal_playbook(playbook_path)

    account_reference: dict[str, dict[str, str]] = {}
    for row in reference_rows:
        domain = normalize_domain(row.get("domain", ""))
        if not domain or domain in account_reference:
            continue
        account_reference[domain] = {
            "domain": domain,
            "company_name": (row.get("company_name", "") or domain).strip(),
            "relationship_stage": _normalize_stage(row.get("relationship_stage", "all")),
        }

    if not account_reference:
        return [], {
            "total_accounts": 0,
            "expected_signals": 0,
            "observed_signals": 0,
            "coverage_rate": 0.0,
            "high_priority_gaps": 0,
            "accounts_with_full_coverage": 0,
        }

    company_name_by_domain: dict[str, str] = {}
    score_by_domain_product: dict[tuple[str, str], tuple[float, str]] = {}
    for row in conn.execute(
        """
        SELECT a.domain, a.company_name, s.product, s.score, s.tier
        FROM account_scores s
        JOIN accounts a ON a.account_id = s.account_id
        WHERE s.run_id = %s
        """,
        (run_id,),
    ).fetchall():
        domain = normalize_domain(str(row["domain"]))
        company_name_by_domain[domain] = str(row["company_name"])
        score_by_domain_product[(domain, str(row["product"]))] = (float(row["score"]), str(row["tier"]))

    component_by_domain_product_signal: dict[tuple[str, str, str], float] = {}
    component_by_domain_signal: dict[tuple[str, str], tuple[str, float]] = {}
    for row in conn.execute(
        """
        SELECT a.domain, c.product, c.signal_code, c.component_score
        FROM score_components c
        JOIN accounts a ON a.account_id = c.account_id
        WHERE c.run_id = %s
        """,
        (run_id,),
    ).fetchall():
        domain = normalize_domain(str(row["domain"]))
        product = str(row["product"])
        signal_code = str(row["signal_code"])
        component_score = round(float(row["component_score"]), 4)

        key = (domain, product, signal_code)
        prior = component_by_domain_product_signal.get(key, 0.0)
        if component_score > prior:
            component_by_domain_product_signal[key] = component_score

        shared_key = (domain, signal_code)
        existing = component_by_domain_signal.get(shared_key)
        if existing is None or component_score > existing[1]:
            component_by_domain_signal[shared_key] = (product, component_score)

    account_expected_counts: dict[str, int] = defaultdict(int)
    account_present_counts: dict[str, int] = defaultdict(int)
    output_rows: list[dict[str, Any]] = []

    for domain in sorted(account_reference):
        info = account_reference[domain]
        company_name = company_name_by_domain.get(domain, info["company_name"])
        relationship_stage = info["relationship_stage"]
        stage_rules = [rule for rule in playbook if rule.relationship_stage in {relationship_stage, "all"}]

        for rule in stage_rules:
            account_expected_counts[domain] += 1
            target_product = rule.product
            signal_code = rule.signal_code

            matched_product = ""
            component_score = 0.0
            current_score = 0.0
            current_tier = "none"

            if target_product in {"shared", "all"}:
                shared_match = component_by_domain_signal.get((domain, signal_code))
                if shared_match:
                    matched_product = shared_match[0]
                    component_score = round(shared_match[1], 4)
                current_score, current_tier = _best_score_for_domain(score_by_domain_product, domain)
            else:
                matched_product = target_product
                component_score = round(
                    component_by_domain_product_signal.get((domain, target_product, signal_code), 0.0),
                    4,
                )
                current_score, current_tier = score_by_domain_product.get((domain, target_product), (0.0, "none"))
                current_score = round(float(current_score), 2)

            present = int(component_score > 0)
            if present:
                account_present_counts[domain] += 1
            if not present:
                matched_product = ""

            output_rows.append(
                {
                    "run_id": run_id,
                    "company_name": company_name,
                    "domain": domain,
                    "relationship_stage": relationship_stage,
                    "target_product": target_product,
                    "matched_product": matched_product,
                    "signal_code": signal_code,
                    "priority": rule.priority,
                    "present": present,
                    "component_score": component_score,
                    "current_score": round(float(current_score), 2),
                    "current_tier": current_tier,
                    "recommended_source": rule.recommended_source,
                    "action_hint": rule.action_hint,
                }
            )

    output_rows.sort(
        key=lambda row: (
            int(row["present"]),
            PRIORITY_ORDER.get(str(row["priority"]), 99),
            str(row["relationship_stage"]),
            str(row["company_name"]).lower(),
            str(row["target_product"]),
            str(row["signal_code"]),
        )
    )

    expected_signals = len(output_rows)
    observed_signals = sum(int(row["present"]) for row in output_rows)
    total_accounts = len(account_reference)
    accounts_with_full_coverage = sum(
        1
        for domain in account_reference
        if account_expected_counts.get(domain, 0) > 0
        and account_present_counts.get(domain, 0) >= account_expected_counts.get(domain, 0)
    )
    high_priority_gaps = sum(
        1
        for row in output_rows
        if int(row["present"]) == 0 and str(row["priority"]).lower() in {"p0", "p1"}
    )

    summary = {
        "total_accounts": total_accounts,
        "expected_signals": expected_signals,
        "observed_signals": observed_signals,
        "coverage_rate": round((observed_signals / expected_signals) if expected_signals else 0.0, 4),
        "high_priority_gaps": high_priority_gaps,
        "accounts_with_full_coverage": accounts_with_full_coverage,
    }
    return output_rows, summary


def write_icp_signal_gap_report(path: Path, rows: list[dict[str, Any]]) -> None:
    write_csv_rows(
        path,
        rows,
        fieldnames=[
            "run_id",
            "company_name",
            "domain",
            "relationship_stage",
            "target_product",
            "matched_product",
            "signal_code",
            "priority",
            "present",
            "component_score",
            "current_score",
            "current_tier",
            "recommended_source",
            "action_hint",
        ],
    )
