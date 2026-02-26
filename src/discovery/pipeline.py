from __future__ import annotations

import json
from collections import defaultdict
from datetime import date
from typing import Any
from urllib.parse import urlparse

from rapidfuzz import fuzz

from src import db
from src.discovery.config import (
    count_cpg_pattern_groups,
    count_primary_signals,
    domain_family,
    has_primary_signal,
    is_placeholder_domain,
    load_account_profiles,
    load_discovery_blocklist,
    load_discovery_thresholds,
    load_icp_reference,
    load_signal_classes,
    resolve_account_profile,
)
from src.integrations.zoho_dedup import check_crm_dedup
from src.models import SignalObservation
from src.promotion_policy import PromotionPolicy, load_promotion_policy
from src.scoring.rules import load_keyword_lexicon, load_signal_rules, load_source_registry
from src.settings import Settings
from src.source_policy import load_source_execution_policy
from src.utils import classify_text, normalize_domain, stable_hash, utc_now_iso, write_csv_rows


def _flatten_lexicon(lexicon_by_source: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for _, group in lexicon_by_source.items():
        for row in group:
            signal_code = (row.get("signal_code", "") or "").strip()
            keyword = (row.get("keyword", "") or "").strip().lower()
            confidence = (row.get("confidence", "") or "").strip()
            key = (signal_code, keyword, confidence)
            if not signal_code or not keyword or key in seen:
                continue
            rows.append(row)
            seen.add(key)
    return rows


def _extract_domain_from_url(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    netloc = parsed.netloc or parsed.path
    return normalize_domain(netloc)


def _resolve_domain_and_company(
    conn,
    domain_hint: str,
    url: str,
    company_name_hint: str,
) -> tuple[str, str]:
    domain = normalize_domain(domain_hint)
    if not domain:
        domain = _extract_domain_from_url(url)
    if domain and is_placeholder_domain(domain):
        return "", ""

    company_name = (company_name_hint or "").strip()
    if domain:
        account = db.get_account_by_domain(conn, domain)
        if account is not None:
            existing_name = str(account["company_name"] or "").strip()
            if existing_name:
                company_name = existing_name
        if not company_name:
            company_name = domain
        return domain, company_name

    if not company_name:
        return "", ""

    rows = conn.execute("SELECT company_name, domain FROM accounts").fetchall()
    best_domain = ""
    best_score = 0
    normalized_hint = company_name.lower().strip()
    for row in rows:
        row_domain = normalize_domain(str(row["domain"]))
        if is_placeholder_domain(row_domain):
            continue
        candidate_name = str(row["company_name"] or "").strip().lower()
        if not candidate_name:
            continue
        score = fuzz.token_sort_ratio(normalized_hint, candidate_name)
        if score > best_score:
            best_score = score
            best_domain = normalize_domain(str(row["domain"]))
    if best_score >= 92 and best_domain:
        account = db.get_account_by_domain(conn, best_domain)
        resolved_name = company_name
        if account is not None:
            resolved_name = str(account["company_name"] or "").strip() or company_name
        return best_domain, resolved_name

    return "", ""


def ingest_external_events(conn, settings: Settings, run_date: date) -> dict[str, int | str]:
    run_date_str = run_date.isoformat()
    execution_policy = load_source_execution_policy(settings.source_execution_policy_path)
    webhook_policy = execution_policy.get("huginn_webhook")
    batch_limit = settings.discovery_event_batch_size
    if webhook_policy is not None and webhook_policy.batch_size > 0:
        batch_limit = min(batch_limit, webhook_policy.batch_size)
    pending_rows = db.fetch_pending_external_discovery_events(
        conn,
        run_date=run_date_str,
        limit=batch_limit,
    )

    lexicon = load_keyword_lexicon(settings.keyword_lexicon_path)
    flattened_lexicon = _flatten_lexicon(lexicon)
    rules = load_signal_rules(settings.signal_registry_path)
    source_registry = load_source_registry(settings.source_registry_path)

    processed = 0
    failed = 0
    matched_signals = 0
    inserted = 0
    marker = f"discover_ingest_{run_date_str}"

    for row in pending_rows:
        event_id = int(row["event_id"])
        source = str(row["source"] or "huginn_webhook").strip().lower() or "huginn_webhook"
        observed_at = str(row["observed_at"] or utc_now_iso())
        title = str(row["title"] or "")
        body = str(row["text"] or "")
        url = str(row["url"] or "")
        company_name_hint = str(row["company_name_hint"] or "")
        domain_hint = str(row["domain_hint"] or "")

        text = "\n".join([title, body]).strip()
        if not text:
            db.mark_external_discovery_event_failed(
                conn,
                event_id=event_id,
                processed_run_id=marker,
                error_summary="empty_text",
                commit=False,
            )
            failed += 1
            continue

        domain, company_name = _resolve_domain_and_company(
            conn=conn,
            domain_hint=domain_hint,
            url=url,
            company_name_hint=company_name_hint,
        )
        if not domain:
            db.mark_external_discovery_event_failed(
                conn,
                event_id=event_id,
                processed_run_id=marker,
                error_summary="unresolved_domain",
                commit=False,
            )
            failed += 1
            continue
        if is_placeholder_domain(domain):
            db.mark_external_discovery_event_failed(
                conn,
                event_id=event_id,
                processed_run_id=marker,
                error_summary="placeholder_domain",
                commit=False,
            )
            failed += 1
            continue

        account_id = db.upsert_account(
            conn,
            company_name=company_name,
            domain=domain,
            source_type="discovered",
            commit=False,
        )

        # CRM dedup: check if this account already exists in Zoho CRM.
        if settings.zoho_dedup_enabled:
            crm_status = check_crm_dedup(domain, company_name, settings)
            if crm_status != "new":
                db.update_crm_status(conn, account_id, crm_status, commit=False)
                db.mark_external_discovery_event_processed(
                    conn,
                    event_id=event_id,
                    processed_run_id=marker,
                    commit=False,
                )
                processed += 1
                continue

        matches = classify_text(text, flattened_lexicon)
        if not matches:
            db.mark_external_discovery_event_processed(
                conn,
                event_id=event_id,
                processed_run_id=marker,
                commit=False,
            )
            processed += 1
            continue

        source_reliability = source_registry.get(source, source_registry.get("huginn_webhook", 0.65))
        if source_reliability <= 0:
            db.mark_external_discovery_event_processed(
                conn,
                event_id=event_id,
                processed_run_id=marker,
                commit=False,
            )
            processed += 1
            continue

        payload = str(row["raw_payload_json"] or "{}")
        for signal_code, confidence, matched_keyword in matches:
            if signal_code not in rules:
                continue
            matched_signals += 1
            raw_hash = stable_hash(
                {
                    "event_id": event_id,
                    "signal_code": signal_code,
                    "matched_keyword": matched_keyword,
                    "payload": payload,
                },
                prefix="raw",
            )
            obs_id = stable_hash(
                {
                    "account_id": account_id,
                    "signal_code": signal_code,
                    "source": source,
                    "observed_at": observed_at,
                    "raw": raw_hash,
                },
                prefix="obs",
            )
            observation = SignalObservation(
                obs_id=obs_id,
                account_id=account_id,
                signal_code=signal_code,
                product="shared",
                source=source,
                observed_at=observed_at,
                evidence_url=url,
                evidence_text=text[:500],
                confidence=max(0.0, min(1.0, float(confidence))),
                source_reliability=max(0.0, min(1.0, float(source_reliability))),
                raw_payload_hash=raw_hash,
            )
            if db.insert_signal_observation(conn, observation, commit=False):
                inserted += 1

        db.mark_external_discovery_event_processed(
            conn,
            event_id=event_id,
            processed_run_id=marker,
            commit=False,
        )
        processed += 1

    conn.commit()
    return {
        "run_date": run_date_str,
        "events_seen": len(pending_rows),
        "events_processed": processed,
        "events_failed": failed,
        "signal_matches": matched_signals,
        "observations_inserted": inserted,
    }


def _parse_reasons(raw: str) -> list[dict[str, Any]]:
    text = (raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [row for row in parsed if isinstance(row, dict)]


def _max_reason_quality_scores(reasons: list[dict[str, Any]]) -> tuple[float, float]:
    max_evidence_quality = 0.0
    max_relevance_score = 0.0
    for reason in reasons:
        source_name = str(reason.get("source", ""))
        eq_raw = reason.get("evidence_quality", 0.0)
        rel_raw = reason.get("relevance_score", 0.0)
        try:
            eq = float(eq_raw or 0.0)
        except (TypeError, ValueError):
            eq = 0.0
        try:
            rel = float(rel_raw or 0.0)
        except (TypeError, ValueError):
            rel = 0.0
        if source_name == "first_party_csv" and eq <= 0:
            eq = 1.0
        if source_name == "first_party_csv" and rel <= 0:
            rel = 1.0
        max_evidence_quality = max(max_evidence_quality, eq)
        max_relevance_score = max(max_relevance_score, rel)
    return max_evidence_quality, max_relevance_score


def _evaluate_policy(
    row: dict[str, Any],
    policy: PromotionPolicy,
    max_evidence_quality: float,
    max_relevance_score: float,
) -> tuple[str, str]:
    if int(row.get("eligible_for_crm", 0) or 0) != 1:
        return "blocked", "not_eligible_for_crm"

    band = str(row.get("confidence_band", "")).strip().lower()
    if band in policy.auto_push_bands:
        if policy.require_strict_evidence_for_auto_push and (
            max_evidence_quality < float(policy.min_auto_push_evidence_quality)
            or max_relevance_score < float(policy.min_auto_push_relevance_score)
        ):
            return "blocked", "strict_evidence_gate_failed"
        return "auto_push", "meets_auto_push_policy"
    if band in policy.manual_review_bands:
        return "manual_review", "manual_review_band"
    return "blocked", "band_not_routable"


def _query_poc_progression_accounts(conn, run_date: str, lookback_days: int) -> set[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT account_id
        FROM signal_observations
        WHERE signal_code = 'poc_stage_progression'
          AND source = 'first_party_csv'
          AND observed_at::date <= %s::date
          AND observed_at::date >= (%s::date - make_interval(days => %s))
        """,
        (run_date, run_date, max(1, int(lookback_days))),
    ).fetchall()
    return {str(row["account_id"]) for row in rows}


def _select_with_diversity(
    candidates: list[dict[str, Any]],
    limit: int,
    family_counts: dict[str, int],
    max_per_family: int = 2,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for candidate in candidates:
        family = str(candidate.get("domain_family", ""))
        if family and family_counts.get(family, 0) >= max_per_family:
            continue
        selected.append(candidate)
        if family:
            family_counts[family] = family_counts.get(family, 0) + 1
        if len(selected) >= limit:
            break
    return selected


def score_discovery_candidates(
    conn,
    settings: Settings,
    run_date: date,
    score_run_id: str,
    source_events_processed: int = 0,
    observations_inserted: int = 0,
    enforce_quality_gates: bool = False,
    min_evidence_quality: float = 0.8,
    min_relevance_score: float = 0.65,
) -> dict[str, int | float | str]:
    run_date_str = run_date.isoformat()
    discovery_run_id = db.create_discovery_run(conn, run_date=run_date_str, score_run_id=score_run_id)

    try:
        thresholds = load_discovery_thresholds(settings.discovery_thresholds_path)
        signal_classes = load_signal_classes(settings.signal_classes_path)
        account_profiles = load_account_profiles(settings.account_profiles_path)
        icp_reference = load_icp_reference(settings.config_dir / "icp_reference_accounts.csv")
        blocklist = load_discovery_blocklist(settings.discovery_blocklist_path)
        progression_accounts = _query_poc_progression_accounts(
            conn,
            run_date=run_date_str,
            lookback_days=settings.discovery_lookback_days,
        )

        score_rows = db.fetch_scores_for_run(conn, score_run_id)
        component_rows = conn.execute(
            """
            SELECT account_id, product, signal_code, component_score
            FROM score_components
            WHERE run_id = %s
            """,
            (score_run_id,),
        ).fetchall()
        account_rows = conn.execute("SELECT account_id, company_name, domain, source_type FROM accounts").fetchall()
        account_meta = {
            str(row["account_id"]): {
                "company_name": str(row["company_name"] or ""),
                "domain": normalize_domain(str(row["domain"] or "")),
                "source_type": str(row["source_type"] or "seed"),
            }
            for row in account_rows
        }

        signals_by_account: dict[str, set[str]] = defaultdict(set)
        for row in component_rows:
            signals_by_account[str(row["account_id"])].add(str(row["signal_code"]))

        by_account: dict[str, dict[str, Any]] = {}
        for row in score_rows:
            account_id = str(row["account_id"])
            score = float(row["score"])
            tier = str(row["tier"])
            product = str(row["product"])
            reasons = _parse_reasons(str(row["top_reasons_json"] or ""))

            current = by_account.setdefault(
                account_id,
                {
                    "account_id": account_id,
                    "company_name": str(row["company_name"]),
                    "domain": normalize_domain(str(row["domain"])),
                    "best_score": score,
                    "best_tier": tier,
                    "best_product": product,
                    "reasons": reasons,
                    "signals": set(signals_by_account.get(account_id, set())),
                    "sources": {str(reason.get("source", "")) for reason in reasons if str(reason.get("source", ""))},
                },
            )
            current["signals"] = set(current["signals"]) | set(signals_by_account.get(account_id, set()))
            current["sources"] = set(current["sources"]) | {
                str(reason.get("source", "")) for reason in reasons if str(reason.get("source", ""))
            }
            if score > float(current["best_score"]):
                current["best_score"] = score
                current["best_tier"] = tier
                current["best_product"] = product
                current["reasons"] = reasons

        ranked_pool: list[dict[str, Any]] = []
        for account_id, row in by_account.items():
            domain = str(row["domain"])
            company_name = str(row["company_name"])
            if is_placeholder_domain(domain):
                continue
            signals = set(row["signals"])
            sources = set(row["sources"])
            score = round(float(row["best_score"]), 2)

            profile = resolve_account_profile(
                domain=domain,
                company_name=company_name,
                signal_codes=signals,
                account_profiles=account_profiles,
                icp_reference=icp_reference,
            )
            is_self = bool(profile.is_self)
            exclude = bool(profile.exclude_from_crm) or (domain in blocklist)
            relationship_stage = profile.relationship_stage
            vertical_tag = profile.vertical_tag

            primary_count = count_primary_signals(signals, signal_classes)
            has_primary = has_primary_signal(signals, signal_classes)
            group_count = count_cpg_pattern_groups(signals)
            source_count = len([source for source in sources if source])
            distinct_documents = {
                str(reason.get("document_id", ""))
                for reason in row["reasons"]
                if str(reason.get("document_id", "")).strip()
            }
            distinct_reason_sources = {
                str(reason.get("source", "")) for reason in row["reasons"] if str(reason.get("source", "")).strip()
            }
            corroboration_bonus = 1.5 if len(distinct_documents) >= 2 and len(distinct_reason_sources) >= 2 else 0.0
            has_poc_progression = int(account_id in progression_accounts)
            max_evidence_quality = 0.0
            max_relevance_score = 0.0
            for reason in row["reasons"]:
                source_name = str(reason.get("source", ""))
                eq_raw = reason.get("evidence_quality", 0.0)
                rel_raw = reason.get("relevance_score", 0.0)
                try:
                    eq = float(eq_raw or 0.0)
                except (TypeError, ValueError):
                    eq = 0.0
                try:
                    rel = float(rel_raw or 0.0)
                except (TypeError, ValueError):
                    rel = 0.0
                if source_name == "first_party_csv" and eq <= 0:
                    eq = 1.0
                if source_name == "first_party_csv" and rel <= 0:
                    rel = 1.0
                max_evidence_quality = max(max_evidence_quality, eq)
                max_relevance_score = max(max_relevance_score, rel)

            confidence_band = ""
            if score >= thresholds.high:
                confidence_band = "high"
            elif score >= thresholds.medium:
                confidence_band = "medium"
            elif score >= thresholds.explore:
                confidence_band = "explore"
            else:
                continue

            if confidence_band in {"high", "medium"} and not has_primary:
                confidence_band = "explore" if score >= thresholds.explore else ""
            if (
                enforce_quality_gates
                and confidence_band in {"high", "medium"}
                and (
                    max_evidence_quality < float(min_evidence_quality)
                    or max_relevance_score < float(min_relevance_score)
                )
            ):
                confidence_band = "explore" if score >= thresholds.explore else ""
            if not confidence_band:
                continue

            if group_count < 2:
                continue

            source_type = str(account_meta.get(account_id, {}).get("source_type", "seed"))
            novelty_score = 1.5 if source_type == "discovered" else 0.6
            if relationship_stage == "unknown":
                novelty_score += 0.5

            eligible_for_crm = (
                not is_self
                and not exclude
                and confidence_band in {"high", "medium"}
                and has_primary
                and group_count >= 2
                and (relationship_stage != "poc" or has_poc_progression == 1)
            )

            rank_score = round(
                score
                + (4.0 * group_count)
                + (1.8 * primary_count)
                + min(3, source_count)
                + novelty_score
                + corroboration_bonus
                + (1.0 if vertical_tag == "media" and "media_traffic_reliability_pressure" in signals else 0.0),
                4,
            )
            candidate = {
                "score_run_id": score_run_id,
                "run_date": run_date_str,
                "account_id": account_id,
                "company_name": company_name,
                "domain": domain,
                "best_product": str(row["best_product"]),
                "score": score,
                "tier": "high" if score >= thresholds.high else ("medium" if score >= thresholds.medium else "low"),
                "confidence_band": confidence_band,
                "cpg_like_group_count": group_count,
                "primary_signal_count": primary_count,
                "source_count": source_count,
                "has_poc_progression_first_party": has_poc_progression,
                "relationship_stage": relationship_stage,
                "vertical_tag": vertical_tag,
                "is_self": int(is_self),
                "exclude_from_crm": int(exclude),
                "eligible_for_crm": int(eligible_for_crm),
                "novelty_score": round(novelty_score, 4),
                "rank_score": rank_score,
                "reasons_json": json.dumps(row["reasons"]),
                "domain_family": domain_family(domain),
                "max_evidence_quality": round(max_evidence_quality, 4),
                "max_relevance_score": round(max_relevance_score, 4),
            }
            if not is_self and not exclude:
                ranked_pool.append(candidate)

        ranked_pool.sort(key=lambda item: (float(item["rank_score"]), float(item["score"])), reverse=True)
        by_band: dict[str, list[dict[str, Any]]] = {"high": [], "medium": [], "explore": []}
        for candidate in ranked_pool:
            by_band[str(candidate["confidence_band"])].append(candidate)

        family_counts: dict[str, int] = {}
        high_candidates = _select_with_diversity(by_band["high"], limit=10, family_counts=family_counts)
        medium_candidates = _select_with_diversity(by_band["medium"], limit=15, family_counts=family_counts)
        explore_candidates = _select_with_diversity(by_band["explore"], limit=5, family_counts=family_counts)

        selected = high_candidates + medium_candidates + explore_candidates
        selected.sort(key=lambda item: (float(item["rank_score"]), float(item["score"])), reverse=True)
        selected = selected[:30]

        evidence_rows: list[dict[str, Any]] = []
        for candidate in selected:
            reasons = _parse_reasons(str(candidate["reasons_json"]))
            for reason in reasons:
                evidence_rows.append(
                    {
                        "account_id": candidate["account_id"],
                        "signal_code": str(reason.get("signal_code", "")),
                        "source": str(reason.get("source", "")),
                        "evidence_url": str(reason.get("evidence_url", "")),
                        "evidence_text": str(reason.get("evidence_text", "")),
                        "component_score": float(reason.get("component_score", 0.0) or 0.0),
                    }
                )

        db.replace_discovery_candidates(
            conn,
            discovery_run_id=discovery_run_id,
            candidates=selected,
            evidence_rows=evidence_rows,
        )
        crm_eligible_count = sum(1 for row in selected if int(row["eligible_for_crm"]) == 1)
        db.finish_discovery_run(
            conn,
            discovery_run_id=discovery_run_id,
            status="completed",
            source_events_processed=source_events_processed,
            observations_inserted=observations_inserted,
            total_candidates=len(selected),
            crm_eligible_candidates=crm_eligible_count,
            error_summary="",
        )
        return {
            "discovery_run_id": discovery_run_id,
            "score_run_id": score_run_id,
            "total_candidates": len(selected),
            "high_candidates": len(high_candidates),
            "medium_candidates": len(medium_candidates),
            "explore_candidates": len(explore_candidates),
            "crm_eligible_candidates": crm_eligible_count,
        }
    except Exception as exc:
        db.finish_discovery_run(
            conn,
            discovery_run_id=discovery_run_id,
            status="failed",
            source_events_processed=source_events_processed,
            observations_inserted=observations_inserted,
            total_candidates=0,
            crm_eligible_candidates=0,
            error_summary=str(exc),
        )
        raise


def write_discovery_reports(
    conn,
    settings: Settings,
    run_date: date,
    discovery_run_id: str,
) -> dict[str, int | str]:
    rows = db.fetch_discovery_candidates_for_run(conn, discovery_run_id)
    policy = load_promotion_policy(settings.promotion_policy_path)
    run_date_str = run_date.strftime("%Y%m%d")

    queue_rows: list[dict[str, Any]] = []
    crm_rows: list[dict[str, Any]] = []
    manual_rows: list[dict[str, Any]] = []
    metrics_by_band: dict[str, int] = {"high": 0, "medium": 0, "explore": 0}
    metrics_by_decision: dict[str, int] = {"auto_push": 0, "manual_review": 0, "blocked": 0}

    for row in rows:
        reasons = _parse_reasons(str(row["reasons_json"] or ""))
        max_evidence_quality, max_relevance_score = _max_reason_quality_scores(reasons)
        policy_decision, policy_reason = _evaluate_policy(
            row=dict(row),
            policy=policy,
            max_evidence_quality=max_evidence_quality,
            max_relevance_score=max_relevance_score,
        )
        reason_codes = [str(reason.get("signal_code", "")) for reason in reasons if str(reason.get("signal_code", ""))]
        evidence_links = [
            str(reason.get("evidence_url", "")) for reason in reasons if str(reason.get("evidence_url", ""))
        ]
        queue_row = {
            "run_date": row["run_date"],
            "discovery_run_id": row["discovery_run_id"],
            "account_id": row["account_id"],
            "company_name": row["company_name"],
            "domain": row["domain"],
            "best_product": row["best_product"],
            "score": row["score"],
            "tier": row["tier"],
            "confidence_band": row["confidence_band"],
            "relationship_stage": row["relationship_stage"],
            "vertical_tag": row["vertical_tag"],
            "cpg_like_group_count": row["cpg_like_group_count"],
            "primary_signal_count": row["primary_signal_count"],
            "source_count": row["source_count"],
            "eligible_for_crm": row["eligible_for_crm"],
            "max_evidence_quality": round(max_evidence_quality, 4),
            "max_relevance_score": round(max_relevance_score, 4),
            "policy_decision": policy_decision,
            "policy_reason": policy_reason,
            "top_signals": " | ".join(reason_codes[:3]),
            "evidence_links": " | ".join(evidence_links[:3]),
        }
        queue_rows.append(queue_row)
        metrics_by_decision[policy_decision] = metrics_by_decision.get(policy_decision, 0) + 1

        band = str(row["confidence_band"])
        if band in metrics_by_band:
            metrics_by_band[band] += 1

        if policy_decision == "auto_push":
            crm_rows.append(queue_row)
        elif policy_decision == "manual_review":
            manual_rows.append(queue_row)

    metrics_rows = [
        {
            "run_date": run_date.isoformat(),
            "discovery_run_id": discovery_run_id,
            "metric": "total_candidates",
            "value": len(queue_rows),
        },
        {
            "run_date": run_date.isoformat(),
            "discovery_run_id": discovery_run_id,
            "metric": "crm_eligible_candidates",
            "value": len(crm_rows),
        },
        {
            "run_date": run_date.isoformat(),
            "discovery_run_id": discovery_run_id,
            "metric": "high_candidates",
            "value": metrics_by_band["high"],
        },
        {
            "run_date": run_date.isoformat(),
            "discovery_run_id": discovery_run_id,
            "metric": "medium_candidates",
            "value": metrics_by_band["medium"],
        },
        {
            "run_date": run_date.isoformat(),
            "discovery_run_id": discovery_run_id,
            "metric": "explore_candidates",
            "value": metrics_by_band["explore"],
        },
        {
            "run_date": run_date.isoformat(),
            "discovery_run_id": discovery_run_id,
            "metric": "auto_push_candidates",
            "value": metrics_by_decision["auto_push"],
        },
        {
            "run_date": run_date.isoformat(),
            "discovery_run_id": discovery_run_id,
            "metric": "manual_review_candidates",
            "value": metrics_by_decision["manual_review"],
        },
        {
            "run_date": run_date.isoformat(),
            "discovery_run_id": discovery_run_id,
            "metric": "blocked_candidates",
            "value": metrics_by_decision["blocked"],
        },
    ]

    queue_path = settings.out_dir / f"discovery_queue_{run_date_str}.csv"
    metrics_path = settings.out_dir / f"discovery_metrics_{run_date_str}.csv"
    crm_path = settings.out_dir / f"crm_candidates_{run_date_str}.csv"
    manual_path = settings.out_dir / f"manual_review_queue_{run_date_str}.csv"

    write_csv_rows(
        queue_path,
        queue_rows,
        fieldnames=[
            "run_date",
            "discovery_run_id",
            "account_id",
            "company_name",
            "domain",
            "best_product",
            "score",
            "tier",
            "confidence_band",
            "relationship_stage",
            "vertical_tag",
            "cpg_like_group_count",
            "primary_signal_count",
            "source_count",
            "eligible_for_crm",
            "max_evidence_quality",
            "max_relevance_score",
            "policy_decision",
            "policy_reason",
            "top_signals",
            "evidence_links",
        ],
    )
    write_csv_rows(
        metrics_path,
        metrics_rows,
        fieldnames=["run_date", "discovery_run_id", "metric", "value"],
    )
    write_csv_rows(
        crm_path,
        crm_rows,
        fieldnames=[
            "run_date",
            "discovery_run_id",
            "account_id",
            "company_name",
            "domain",
            "best_product",
            "score",
            "tier",
            "confidence_band",
            "relationship_stage",
            "vertical_tag",
            "cpg_like_group_count",
            "primary_signal_count",
            "source_count",
            "eligible_for_crm",
            "max_evidence_quality",
            "max_relevance_score",
            "policy_decision",
            "policy_reason",
            "top_signals",
            "evidence_links",
        ],
    )
    write_csv_rows(
        manual_path,
        manual_rows,
        fieldnames=[
            "run_date",
            "discovery_run_id",
            "account_id",
            "company_name",
            "domain",
            "best_product",
            "score",
            "tier",
            "confidence_band",
            "relationship_stage",
            "vertical_tag",
            "cpg_like_group_count",
            "primary_signal_count",
            "source_count",
            "eligible_for_crm",
            "max_evidence_quality",
            "max_relevance_score",
            "policy_decision",
            "policy_reason",
            "top_signals",
            "evidence_links",
        ],
    )
    return {
        "discovery_queue_rows": len(queue_rows),
        "crm_candidates_rows": len(crm_rows),
        "manual_review_rows": len(manual_rows),
        "metrics_rows": len(metrics_rows),
        "discovery_queue_path": str(queue_path),
        "crm_candidates_path": str(crm_path),
        "manual_review_path": str(manual_path),
        "discovery_metrics_path": str(metrics_path),
    }
