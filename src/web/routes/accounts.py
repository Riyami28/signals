"""Accounts API routes."""

from __future__ import annotations

import csv
import io
import json
import re
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from src import db
from src.export.dossier import render_dossier
from src.settings import load_settings

router = APIRouter(tags=["accounts"])

_ALLOWED_SORT_FIELDS = {"score", "company_name", "domain", "tier"}
_ALLOWED_SORT_DIRS = {"asc", "desc"}
_ALLOWED_TIERS = {"", "high", "medium", "low", "explore"}
_MAX_SEARCH_LENGTH = 200


def _get_conn():
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
    return conn


# Cache signal registry metadata for timeline enrichment
_signal_meta_cache: dict[str, dict] | None = None


def _get_signal_meta() -> dict[str, dict]:
    """Load signal registry as a lookup: signal_code → {dimension, category, base_weight}."""
    global _signal_meta_cache  # noqa: PLW0603
    if _signal_meta_cache is not None:
        return _signal_meta_cache

    settings = load_settings()
    meta: dict[str, dict] = {}
    path = settings.signal_registry_path
    if path.exists():
        with path.open("r", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                code = (row.get("signal_code") or "").strip()
                if not code:
                    continue
                try:
                    weight = int(row.get("base_weight", 0))
                except (ValueError, TypeError):
                    weight = 0
                try:
                    half_life = float(row.get("half_life_days", 30))
                except (ValueError, TypeError):
                    half_life = 30.0
                try:
                    min_conf = float(row.get("min_confidence", 0.5))
                except (ValueError, TypeError):
                    min_conf = 0.5
                meta[code] = {
                    "dimension": (row.get("dimension") or "").strip(),
                    "category": (row.get("category") or "").strip(),
                    "base_weight": weight,
                    "half_life_days": half_life,
                    "min_confidence": min_conf,
                }
    _signal_meta_cache = meta
    return meta


def _sanitize_search(q: str) -> str:
    """Strip control characters and truncate to max length."""
    cleaned = re.sub(r"[\x00-\x1f\x7f]", "", q).strip()
    return cleaned[:_MAX_SEARCH_LENGTH]


def _calculate_readiness_score(account_detail: dict) -> dict:
    """Calculate Account Readiness Score from 5 weighted components.

    Returns:
        {
            "score": 0-100,
            "status": "action_ready" | "review" | "needs_research",
            "components": {
                "signal_freshness": {
                    "label": "Signal Freshness",
                    "value": 0-100,
                    "weight_pct": 30,
                    "explanation": "% of signals within 14 days"
                },
                "source_diversity": {...},
                "evidence_quality": {...},
                "research_complete": {...},
                "contact_available": {...}
            }
        }
    """
    components = {}

    # 1. Signal Freshness (30%)
    signals = account_detail.get("signals", [])
    if signals:
        now = datetime.now(timezone.utc)
        fresh_count = 0
        for sig in signals:
            observed_at_str = sig.get("observed_at", "")
            if observed_at_str:
                try:
                    # Handle both ISO format and timezone-aware strings
                    if observed_at_str.endswith("Z"):
                        obs_dt = datetime.fromisoformat(observed_at_str.replace("Z", "+00:00"))
                    else:
                        obs_dt = datetime.fromisoformat(observed_at_str.split("+")[0])
                        # Make naive datetime timezone-aware
                        if obs_dt.tzinfo is None:
                            obs_dt = obs_dt.replace(tzinfo=timezone.utc)
                    age_days = (now - obs_dt).days
                    if age_days <= 14:
                        fresh_count += 1
                except (ValueError, TypeError):
                    pass
        signal_freshness = min(100, int((fresh_count / len(signals)) * 100)) if signals else 0
    else:
        signal_freshness = 0

    components["signal_freshness"] = {
        "label": "Signal Freshness",
        "value": signal_freshness,
        "weight_pct": 30,
        "explanation": f"{signal_freshness}% of signals within 14 days",
    }

    # 2. Source Diversity (25%)
    # Map distinct source count to 0-100: 1=20, 2=40, 3=60, 4=80, 5+=100
    distinct_sources = set()
    for sig in signals:
        source = sig.get("source", "").strip()
        if source:
            distinct_sources.add(source)

    source_count = len(distinct_sources)
    source_diversity_mapping = {0: 0, 1: 20, 2: 40, 3: 60, 4: 80}
    source_diversity = source_diversity_mapping.get(min(source_count, 4), 100)

    components["source_diversity"] = {
        "label": "Source Diversity",
        "value": source_diversity,
        "weight_pct": 25,
        "explanation": f"{source_count} distinct source(s)",
    }

    # 3. Evidence Quality (20%)
    # Average confidence across all signals
    if signals:
        confidences = []
        for sig in signals:
            conf = sig.get("confidence")
            if conf is not None:
                try:
                    confidences.append(float(conf))
                except (ValueError, TypeError):
                    pass
        evidence_quality = int(sum(confidences) / len(confidences) * 100) if confidences else 0
    else:
        evidence_quality = 0

    components["evidence_quality"] = {
        "label": "Evidence Quality",
        "value": evidence_quality,
        "weight_pct": 20,
        "explanation": f"Avg confidence: {evidence_quality / 100:.2f}" if evidence_quality > 0 else "No signals",
    }

    # 4. Research Complete (15%)
    # Binary: has enrichment OR research brief
    research = account_detail.get("research", {})
    research_complete = (
        100
        if (
            research
            and (
                research.get("enrichment_json")
                or research.get("research_brief")
                or research.get("research_status") == "completed"
            )
        )
        else 0
    )

    components["research_complete"] = {
        "label": "Research Complete",
        "value": research_complete,
        "weight_pct": 15,
        "explanation": "Has enrichment + brief" if research_complete == 100 else "Needs research",
    }

    # 5. Contact Available (10%)
    # Binary: has verified decision-maker contact
    contacts = account_detail.get("contacts", [])
    contact_available = 0
    for contact in contacts:
        # Consider contact available if they have a status (discovered, ranked, enriched, verified)
        if contact.get("status") in ["enriched", "verified"]:
            contact_available = 100
            break
    if contact_available == 0 and contacts:
        # If no verified contact, but has any contact, give partial credit
        contact_available = 50

    components["contact_available"] = {
        "label": "Contact Available",
        "value": contact_available,
        "weight_pct": 10,
        "explanation": f"{len(contacts)} contact(s)" if contacts else "No contacts",
    }

    # Calculate weighted average
    total_score = (
        (components["signal_freshness"]["value"] * 0.30)
        + (components["source_diversity"]["value"] * 0.25)
        + (components["evidence_quality"]["value"] * 0.20)
        + (components["research_complete"]["value"] * 0.15)
        + (components["contact_available"]["value"] * 0.10)
    )

    readiness_score = round(total_score)

    # Determine status
    if readiness_score >= 70:
        status = "action_ready"
    elif readiness_score >= 40:
        status = "review"
    else:
        status = "needs_research"

    return {"score": readiness_score, "status": status, "components": components}


@router.get("/accounts")
def list_accounts(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    sort: str = Query("score"),
    dir: str = Query("desc"),
    tier: str = Query(""),
    label: str = Query(""),
    q: str = Query(""),
    source: str = Query(""),
    readiness: str = Query(""),
):
    if sort not in _ALLOWED_SORT_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"invalid sort field, allowed: {sorted(_ALLOWED_SORT_FIELDS)}",
        )
    if dir.lower() not in _ALLOWED_SORT_DIRS:
        raise HTTPException(
            status_code=400,
            detail="invalid sort direction, allowed: asc, desc",
        )
    if tier and tier.lower() not in _ALLOWED_TIERS:
        raise HTTPException(
            status_code=400,
            detail=f"invalid tier filter, allowed: {sorted(_ALLOWED_TIERS - {''})}",
        )
    if readiness and readiness.lower() not in {
        "action_ready",
        "review",
        "needs_research",
    }:
        raise HTTPException(
            status_code=400,
            detail=("invalid readiness filter, allowed: action_ready, review, needs_research"),
        )

    safe_search = _sanitize_search(q)
    safe_label = label.strip()[:100]
    safe_source = source.strip()[:50]

    conn = _get_conn()
    try:
        rows, total = db.get_accounts_paginated(
            conn,
            page=page,
            per_page=per_page,
            sort_by=sort,
            sort_dir=dir,
            tier_filter=tier,
            label_filter=safe_label,
            search=safe_search,
            source_filter=safe_source,
        )

        # Calculate readiness score for each account
        for r in rows:
            account_id = str(r.get("account_id", ""))
            try:
                detail = db.get_account_detail(conn, account_id)
                if detail:
                    readiness_data = _calculate_readiness_score(detail)
                    r["readiness_score"] = readiness_data["score"]
                    r["readiness_status"] = readiness_data["status"]
                else:
                    r["readiness_score"] = 0
                    r["readiness_status"] = "needs_research"
            except Exception:
                r["readiness_score"] = 0
                r["readiness_status"] = "needs_research"

        # Filter by readiness if requested
        if readiness:
            rows = [r for r in rows if r.get("readiness_status", "") == readiness]
            total = len(rows)

        # Serialize datetimes
        for r in rows:
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()

        return {
            "items": rows,
            "total": total,
            "page": page,
            "pages": max(1, (total + per_page - 1) // per_page),
        }
    finally:
        conn.close()


@router.get("/accounts/{account_id}")
def get_account(account_id: str):
    conn = _get_conn()
    try:
        detail = db.get_account_detail(conn, account_id)
        if not detail:
            return {"error": "not found"}, 404

        # Calculate readiness score
        readiness_data = _calculate_readiness_score(detail)
        detail["readiness_score"] = readiness_data["score"]
        detail["readiness_status"] = readiness_data["status"]
        detail["readiness_components"] = readiness_data["components"]

        # Enrich signals with impact metadata from registry
        signal_meta = _get_signal_meta()
        if detail.get("signals") and isinstance(detail["signals"], list):
            for sig in detail["signals"]:
                code = sig.get("signal_code", "")
                meta = signal_meta.get(code, {})
                weight = meta.get("base_weight", 0)
                sig["base_weight"] = weight
                sig["dimension"] = meta.get("dimension", "")
                sig["category"] = meta.get("category", "")
                sig["half_life_days"] = meta.get("half_life_days", 30.0)
                sig["min_confidence"] = meta.get("min_confidence", 0.5)
                sig["impact"] = "high" if weight >= 18 else "medium" if weight >= 10 else "low"
            # Sort by component_score DESC (actual contribution), then base_weight, then date
            detail["signals"].sort(
                key=lambda s: (
                    float(s.get("component_score") or 0),
                    s.get("base_weight", 0),
                    s.get("observed_at", ""),
                ),
                reverse=True,
            )

        # Calculate dimension contributions (score * weight)
        # Import dimension weights from scoring engine
        from src.scoring.engine import DEFAULT_DIMENSION_WEIGHTS

        if detail.get("dimension_scores") and isinstance(detail["dimension_scores"], dict):
            dimension_contributions = {}
            for dim, raw_score in detail["dimension_scores"].items():
                if dim in DEFAULT_DIMENSION_WEIGHTS:
                    weight_config = DEFAULT_DIMENSION_WEIGHTS[dim]
                    weight = weight_config.weight
                    # Contribution = (dimension_score / 100) * weight * 100
                    # This puts it back on the 0-100 scale relative to each dimension's ceiling
                    contribution = (raw_score / 100.0) * weight * 100.0
                    dimension_contributions[dim] = {
                        "dimension_score": round(raw_score, 1),
                        "weight": weight,
                        "weight_pct": int(weight * 100),
                        "ceiling": weight_config.ceiling,
                        "contribution": round(contribution, 1),
                    }
            detail["dimension_contributions"] = dimension_contributions

        # Export dimension weights configuration for API transparency
        detail["dimension_weights"] = {
            dim: {
                "weight": weight_config.weight,
                "weight_pct": int(weight_config.weight * 100),
                "ceiling": weight_config.ceiling,
            }
            for dim, weight_config in DEFAULT_DIMENSION_WEIGHTS.items()
        }

        # Build per-dimension signal breakdown (top 5 signals per dimension)
        if detail.get("signals") and isinstance(detail["signals"], list):
            signals_by_dimension = {}
            for signal in detail["signals"]:
                dim = signal.get("dimension", "unknown")
                if dim not in signals_by_dimension:
                    signals_by_dimension[dim] = []
                # Extract evidence snippet (first 120 chars of evidence_text)
                evidence_text = signal.get("evidence_text", "")
                snippet = (evidence_text[:120] + "...") if len(evidence_text) > 120 else evidence_text
                signals_by_dimension[dim].append(
                    {
                        "signal_code": signal.get("signal_code", ""),
                        "source": signal.get("source", ""),
                        "component_score": float(signal.get("component_score") or 0),
                        "evidence_url": signal.get("evidence_url", ""),
                        "evidence_snippet": snippet,
                        "observed_at": signal.get("observed_at", ""),
                    }
                )

            # Sort by component_score and keep top 5 per dimension
            for dim in signals_by_dimension:
                signals_by_dimension[dim].sort(key=lambda s: s["component_score"], reverse=True)
                signals_by_dimension[dim] = signals_by_dimension[dim][:5]

            detail["signals_by_dimension"] = signals_by_dimension

        # Serialize datetimes
        _serialize_dates(detail)
        return detail
    finally:
        conn.close()


@router.get("/accounts/{account_id}/dossier")
def get_account_dossier(
    account_id: str,
    refresh: bool = Query(False),
):
    conn = _get_conn()
    try:
        if not db.account_exists(conn, account_id):
            raise HTTPException(status_code=404, detail="account not found")

        if not refresh:
            latest = db.get_latest_dossier(conn, account_id)
            if latest:
                _serialize_dates(latest)
                return latest

        detail = db.get_account_detail(conn, account_id)
        if not detail:
            raise HTTPException(status_code=404, detail="account not found")

        research = detail.get("research") if isinstance(detail.get("research"), dict) else {}
        enrichment: dict = {}
        enrichment_raw = str(research.get("enrichment_json", "") or "").strip()
        if enrichment_raw:
            try:
                parsed = json.loads(enrichment_raw)
                if isinstance(parsed, dict):
                    enrichment = parsed
            except json.JSONDecodeError:
                enrichment = {}

        scores = detail.get("scores") if isinstance(detail.get("scores"), list) else []
        score_row = scores[0] if scores and isinstance(scores[0], dict) else {}

        dossier = render_dossier(
            account=detail,
            research=research,
            enrichment=enrichment,
            contacts=detail.get("contacts") if isinstance(detail.get("contacts"), list) else [],
            scores=score_row,
            dimension_scores=detail.get("dimension_scores") if isinstance(detail.get("dimension_scores"), dict) else {},
            signals=detail.get("signals") if isinstance(detail.get("signals"), list) else [],
        )
        db.save_dossier(conn, dossier)
        _serialize_dates(dossier)
        return dossier
    finally:
        conn.close()


@router.get("/accounts/{account_id}/timeline")
def get_account_timeline(
    account_id: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    signal_code: str = Query(""),
    source: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
):
    """Return enriched scored timeline for an account.

    Each observation is enriched with scoring context:
    - ``dimension``: scoring dimension (trigger_intent, tech_fit, etc.)
    - ``category``: signal category (trigger_events, hiring, etc.)
    - ``base_weight``: signal importance weight from registry
    - ``component_score``: actual contribution to account score
    """
    conn = _get_conn()
    try:
        if not db.account_exists(conn, account_id):
            return {"error": "not found"}, 404

        # Load signal registry for enrichment
        signal_meta = _get_signal_meta()

        items, total = db.get_signal_timeline(
            conn,
            account_id,
            limit=limit,
            offset=offset,
            signal_code=signal_code,
            source=source,
            date_from=date_from,
            date_to=date_to,
        )

        # Enrich each item with dimension/category/weight from registry
        for item in items:
            code = item.get("signal_code", "")
            meta = signal_meta.get(code, {})
            item["dimension"] = meta.get("dimension", "")
            item["category"] = meta.get("category", "")
            item["base_weight"] = meta.get("base_weight", 0)

        _serialize_dates(items)
        return {
            "items": items,
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    finally:
        conn.close()


def _serialize_dates(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if hasattr(v, "isoformat"):
                obj[k] = v.isoformat()
            elif isinstance(v, (dict, list)):
                _serialize_dates(v)
    elif isinstance(obj, list):
        for item in obj:
            _serialize_dates(item)


# ---------------------------------------------------------------------------
# CSV Export
# ---------------------------------------------------------------------------


@router.get("/export/csv")
def export_accounts_csv(
    tier: str = Query(""),
    label: str = Query(""),
    q: str = Query(""),
    source: str = Query(""),
):
    """Export all scored accounts as a comprehensive CSV file.

    Includes: account info, scores, signals with evidence, contacts,
    research brief, conversation starters, and labels.
    """
    conn = _get_conn()
    try:
        rows, total = db.get_accounts_paginated(
            conn,
            page=1,
            per_page=10000,  # Export all
            sort_by="score",
            sort_dir="desc",
            tier_filter=tier,
            label_filter=label.strip()[:100],
            search=_sanitize_search(q) if q else "",
            source_filter=source.strip()[:50],
        )

        # Build CSV in memory
        output = io.StringIO()
        fieldnames = [
            # Account info
            "company_name",
            "domain",
            "industry",
            "country",
            "employees",
            "revenue_range",
            "linkedin_url",
            # Scoring
            "score",
            "tier",
            "velocity_7d",
            "velocity_14d",
            "velocity_30d",
            # Signals
            "signal_count",
            "signals",
            "evidence_urls",
            # Research
            "research_brief",
            "conversation_starters",
            "research_status",
            # Contacts
            "contact_1",
            "contact_2",
            "contact_3",
            # Labels
            "labels",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            account_id = str(row.get("account_id", ""))

            # --- Signals ---
            signals_summary = ""
            evidence_urls = ""
            try:
                detail = db.get_account_detail(conn, account_id)
                if detail and detail.get("signals"):
                    signal_list = detail["signals"]
                    real_signals = [
                        s for s in signal_list if not str(s.get("evidence_url", "")).startswith("internal://")
                    ]
                    signal_codes = [s.get("signal_code", "") for s in real_signals[:10]]
                    signals_summary = "; ".join(signal_codes)
                    urls = []
                    seen_urls: set[str] = set()
                    for s in real_signals[:10]:
                        url = str(s.get("evidence_url", "")).strip()
                        if url and url not in seen_urls:
                            seen_urls.add(url)
                            urls.append(url)
                    evidence_urls = "; ".join(urls)
            except Exception:
                pass

            # --- Research ---
            research_brief = ""
            conversation_starters = ""
            research_status = str(row.get("research_status", "") or "")
            industry = ""
            country = ""
            employees = ""
            revenue_range = ""
            linkedin_url = ""
            try:
                research = db.get_company_research(conn, account_id)
                if research:
                    research_brief = str(research.get("research_brief", "") or "")[:500]
                    research_status = str(research.get("research_status", "") or research_status)
                    enrich_raw = research.get("enrichment_json", "") or "{}"
                    enrichment = json.loads(enrich_raw) if isinstance(enrich_raw, str) else enrich_raw
                    industry = str(enrichment.get("industry", "") or "")
                    country = str(enrichment.get("country", "") or "")
                    employees = str(enrichment.get("employees", "") or enrichment.get("employee_range", "") or "")
                    revenue_range = str(enrichment.get("revenue_range", "") or "")
                    linkedin_url = str(enrichment.get("company_linkedin_url", "") or "")
                    # Conversation starters from research_profile
                    profile_raw = research.get("research_profile", "") or ""
                    if profile_raw:
                        try:
                            profile = json.loads(profile_raw) if isinstance(profile_raw, str) else profile_raw
                            starters = profile.get("conversation_starters", []) if isinstance(profile, dict) else []
                            if starters:
                                conversation_starters = " | ".join(str(s) for s in starters[:3])
                        except (json.JSONDecodeError, TypeError):
                            pass
            except Exception:
                pass

            # --- Contacts (top 3) ---
            contact_lines = ["", "", ""]
            try:
                contacts = db.get_contacts_for_account(conn, account_id)
                for i, c in enumerate(contacts[:3]):
                    name = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
                    title = c.get("title", "")
                    email = c.get("email", "")
                    li = c.get("linkedin_url", "")
                    parts = [name]
                    if title:
                        parts.append(title)
                    if email:
                        parts.append(email)
                    if li:
                        parts.append(li)
                    contact_lines[i] = " | ".join(parts)
            except Exception:
                pass

            writer.writerow(
                {
                    "company_name": row.get("company_name", ""),
                    "domain": row.get("domain", ""),
                    "industry": industry,
                    "country": country,
                    "employees": employees,
                    "revenue_range": revenue_range,
                    "linkedin_url": linkedin_url,
                    "score": row.get("score", 0),
                    "tier": row.get("tier", ""),
                    "velocity_7d": row.get("velocity_7d", 0),
                    "velocity_14d": row.get("velocity_14d", 0),
                    "velocity_30d": row.get("velocity_30d", 0),
                    "signal_count": row.get("signal_count", 0),
                    "signals": signals_summary,
                    "evidence_urls": evidence_urls,
                    "research_brief": research_brief,
                    "conversation_starters": conversation_starters,
                    "research_status": research_status,
                    "contact_1": contact_lines[0],
                    "contact_2": contact_lines[1],
                    "contact_3": contact_lines[2],
                    "labels": row.get("labels", ""),
                }
            )

        csv_content = output.getvalue()
        output.close()

        return StreamingResponse(
            io.BytesIO(csv_content.encode("utf-8")),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=signals_export.csv"},
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Scoring Rubric — static config served to the UI
# ---------------------------------------------------------------------------

_DIMENSION_DESCRIPTIONS = {
    "trigger_intent": "External events signalling active buying motion — funding rounds, exec changes, product launches, compliance deadlines.",
    "tech_fit": "Technology stack signals indicating readiness for DevOps/Platform/FinOps solutions — K8s, Terraform, cloud-native tooling.",
    "engagement_pql": "Product-qualified signals — community engagement, GitHub activity, documentation visits, trial/demo requests.",
    "firmographic": "Company profile fit — size, industry, revenue range, growth stage.",
    "hiring_growth": "Hiring patterns revealing infrastructure investment — DevOps, SRE, Platform Eng, FinOps roles being recruited.",
}

_DIMENSION_WEIGHTS = {
    "trigger_intent": {"weight": 0.35, "ceiling": 60.0},
    "tech_fit": {"weight": 0.20, "ceiling": 40.0},
    "engagement_pql": {"weight": 0.25, "ceiling": 50.0},
    "firmographic": {"weight": 0.10, "ceiling": 30.0},
    "hiring_growth": {"weight": 0.10, "ceiling": 30.0},
}


@router.get("/scoring/rubric")
def get_scoring_rubric():
    """Return the scoring configuration for UI transparency."""
    settings = load_settings()

    # Load thresholds
    tiers: list[dict] = []
    threshold_path = settings.project_root / "config" / "thresholds.csv"
    if threshold_path.exists():
        with threshold_path.open("r", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                key = (row.get("key") or "").strip()
                val = row.get("value", "")
                if key and key.startswith("tier_"):
                    tiers.append({"tier": key, "min_score": val})

    # Load execution policy to know which sources are actually enabled
    exec_policy: dict[str, bool] = {}
    exec_policy_path = settings.project_root / "config" / "source_execution_policy.csv"
    if exec_policy_path.exists():
        with exec_policy_path.open("r", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                src = (row.get("source") or "").strip()
                if src:
                    exec_policy[src] = (row.get("enabled", "true").strip().lower() == "true")

    # Load sources — only show those enabled in execution policy (or not in policy = CSV-based passive sources)
    # Explicitly exclude known-unintegrated sources regardless
    _UNINTEGRATED = {"bombora_api", "g2_api", "crunchbase", "story_hunt", "story_hunt_js", "gnews", "google_news_rss"}
    sources: list[dict] = []
    source_path = settings.source_registry_path
    if source_path.exists():
        with source_path.open("r", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                src = (row.get("source") or "").strip()
                if not src or src in _UNINTEGRATED:
                    continue
                # If source is in execution policy, respect its enabled flag
                if src in exec_policy and not exec_policy[src]:
                    continue
                try:
                    rel = float(row.get("reliability", 0))
                except (ValueError, TypeError):
                    rel = 0.0
                sources.append({"source": src, "reliability": rel})
    sources.sort(key=lambda s: s["reliability"], reverse=True)

    # Load signal definitions
    signals: list[dict] = []
    signal_path = settings.signal_registry_path
    if signal_path.exists():
        with signal_path.open("r", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                code = (row.get("signal_code") or "").strip()
                if code:
                    try:
                        weight = int(row.get("base_weight", 0))
                    except (ValueError, TypeError):
                        weight = 0
                    try:
                        hl = float(row.get("half_life_days", 30))
                    except (ValueError, TypeError):
                        hl = 30.0
                    signals.append(
                        {
                            "signal_code": code,
                            "dimension": (row.get("dimension") or "").strip(),
                            "category": (row.get("category") or "").strip(),
                            "base_weight": weight,
                            "half_life_days": hl,
                            "description": (row.get("description") or "").strip(),
                        }
                    )
    signals.sort(key=lambda s: s["base_weight"], reverse=True)

    return {
        "formula": "score = base_weight × confidence × source_reliability × recency_decay(half_life_days)",
        "anti_inflation": "Max 1 observation per source per signal; max 3 total per signal",
        "dimensions": [
            {
                "name": dim,
                "weight_pct": int(cfg["weight"] * 100),
                "ceiling": cfg["ceiling"],
                "description": _DIMENSION_DESCRIPTIONS.get(dim, ""),
            }
            for dim, cfg in _DIMENSION_WEIGHTS.items()
        ],
        "tiers": tiers,
        "sources": sources[:30],  # Top 30 by reliability
        "top_signals": signals[:20],  # Top 20 by base_weight
    }
