from __future__ import annotations

import csv
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from src import db
from src.settings import load_settings

_DATE_RE = re.compile(r"^(\d{8})$")
_MAX_RUN_HISTORY = 12


@dataclass(frozen=True)
class FileDescriptor:
    key: str
    label: str
    path: Path
    primary: bool
    description: str


_settings = load_settings()

_INPUT_FILES: list[FileDescriptor] = [
    FileDescriptor(
        key="watchlist_accounts",
        label="Watchlist Accounts (Primary Tracking List)",
        path=_settings.watchlist_accounts_path,
        primary=True,
        description="Primary list of companies to track. This is where the 1000 target companies are defined.",
    ),
    FileDescriptor(
        key="seed_accounts",
        label="Seed Accounts (Core Accounts)",
        path=_settings.seed_accounts_path,
        primary=False,
        description="Always-on baseline accounts that are tracked in addition to the watchlist.",
    ),
    FileDescriptor(
        key="account_source_handles",
        label="Account Source Handles",
        path=_settings.account_source_handles_path,
        primary=False,
        description="Per-domain source overrides (news query, careers URLs, board handles).",
    ),
    FileDescriptor(
        key="first_party_events",
        label="First-Party Events",
        path=_settings.raw_dir / "first_party_events.csv",
        primary=True,
        description="Direct product/CRM events. Fastest and highest-trust input source.",
    ),
    FileDescriptor(
        key="jobs",
        label="Jobs CSV",
        path=_settings.raw_dir / "jobs.csv",
        primary=False,
        description="Manual job posting snippets with optional explicit signal mapping.",
    ),
    FileDescriptor(
        key="news",
        label="News CSV",
        path=_settings.raw_dir / "news.csv",
        primary=False,
        description="Manual news snippets with optional explicit signal mapping.",
    ),
    FileDescriptor(
        key="technographics",
        label="Technographics CSV",
        path=_settings.raw_dir / "technographics.csv",
        primary=False,
        description="Manual architecture/tooling evidence snippets (Kubernetes, Terraform, etc.).",
    ),
    FileDescriptor(
        key="community",
        label="Community CSV",
        path=_settings.raw_dir / "community.csv",
        primary=False,
        description="Community/forum snippets with optional explicit signal mapping.",
    ),
    FileDescriptor(
        key="news_feeds",
        label="News Feeds CSV",
        path=_settings.raw_dir / "news_feeds.csv",
        primary=False,
        description="Per-account RSS feeds for automated ingestion.",
    ),
    FileDescriptor(
        key="review_input",
        label="Review Input CSV",
        path=_settings.raw_dir / "review_input.csv",
        primary=False,
        description="Analyst decisions applied back into quality metrics and source evaluation.",
    ),
]

_OUTPUT_PREFIX_BY_KEY: dict[str, str] = {
    "review_queue": "review_queue",
    "daily_scores": "daily_scores",
    "promotion_readiness": "promotion_readiness",
    "icp_coverage": "icp_coverage",
    "ops_metrics": "ops_metrics",
}

_OUTPUT_LABEL_BY_KEY: dict[str, str] = {
    "review_queue": "Review Queue",
    "daily_scores": "Daily Scores",
    "promotion_readiness": "Promotion Readiness",
    "icp_coverage": "ICP Coverage",
    "ops_metrics": "Ops Metrics",
}

_OUTPUT_DESC_BY_KEY: dict[str, str] = {
    "review_queue": "Main prioritized list to action now (who to contact and why).",
    "daily_scores": "Full scored matrix for every account and product lane.",
    "promotion_readiness": "Whether confidence bands are ready for CRM promotion.",
    "icp_coverage": "Coverage of known ICP reference accounts in high/medium tiers.",
    "ops_metrics": "Operational health: queue depth, precision, lag, lock activity.",
}

_TERM_GLOSSARY: list[dict[str, str]] = [
    {
        "term": "Watchlist Accounts",
        "details": "Primary company list to track. In this project, this should contain the target 1000 companies.",
    },
    {
        "term": "Seed Accounts",
        "details": "Always-on baseline companies tracked in addition to watchlist accounts.",
    },
    {
        "term": "Expected Total",
        "details": "Watchlist count + seed account count. This is the expected tracked-account total before scoring.",
    },
    {
        "term": "Accounts In DB",
        "details": "Count of accounts currently present in the `accounts` table.",
    },
    {
        "term": "Gap vs Expected",
        "details": "Expected Total minus Accounts In DB. A positive gap means tracked accounts are missing in DB.",
    },
    {
        "term": "Run Date",
        "details": "Business date assigned to pipeline outputs (format: YYYY-MM-DD).",
    },
    {
        "term": "Run ID",
        "details": "Unique identifier for one run execution in the `score_runs` table.",
    },
    {
        "term": "Started / Finished",
        "details": "Timestamps indicating when a run began and completed. Used to confirm completion and duration.",
    },
    {
        "term": "Exit Code",
        "details": "Process completion code. `0` means success; non-zero means failure.",
    },
    {
        "term": "Signal",
        "details": "A discrete buying indicator (example: `cost_reduction_mandate`, `poc_stage_progression`).",
    },
    {
        "term": "Component Score",
        "details": "Weighted contribution of one signal to an account/product score after confidence, reliability, and recency.",
    },
    {
        "term": "Daily Score",
        "details": "Final 0-100 score per account + product lane (`zopdev`, `zopday`, `zopnight`).",
    },
    {
        "term": "Tier",
        "details": "Band derived from score thresholds: `high`, `medium`, `low`.",
    },
    {
        "term": "Review Queue",
        "details": "Highest-priority candidates and reasons for human validation or outreach.",
    },
    {
        "term": "Promotion Readiness",
        "details": "Whether confidence bands satisfy policy to auto-push into CRM candidates.",
    },
    {
        "term": "ICP Coverage",
        "details": "How many reference ICP accounts are showing high/medium intent currently.",
    },
    {
        "term": "Ops Metrics",
        "details": "Pipeline health metrics (retry depth, queue size, lag, precision).",
    },
    {
        "term": "Live Crawl",
        "details": "If enabled, fetches live external sources (news/jobs/community/website scans).",
    },
    {
        "term": "Workers Per Source",
        "details": "Parallel company crawls per live source. Higher values increase throughput but use more network/CPU.",
    },
    {
        "term": "Confidence",
        "details": "Strength of pattern match (0-1). Can come from explicit CSV values or keyword matching.",
    },
    {
        "term": "Source Reliability",
        "details": "Trust multiplier from source registry (0-1).",
    },
]

_TERM_ACTION_NOTES: dict[str, str] = {
    "Watchlist Accounts": "This should be your target list size (for you: 1,000). If lower, update the watchlist input CSV.",
    "Seed Accounts": "These are baseline accounts tracked in addition to watchlist accounts. Keep this stable unless intentionally changed.",
    "Expected Total": "Use this as the minimum coverage expectation for tracked accounts each run.",
    "Accounts In DB": "Should be at least Expected Total. If lower, refresh sync or inspect ingestion failures.",
    "Gap vs Expected": "Healthy state is 0. Any value above 0 means missing tracked accounts in DB.",
    "Run Date": "Pick the business date you want outputs written under (used in output filenames).",
    "Run ID": "Use this ID to trace one exact execution in logs and troubleshooting.",
    "Started / Finished": "Confirms whether the run actually completed and how long it took.",
    "Exit Code": "0 means success. Non-zero means the run failed and needs investigation.",
    "Signal": "Signals are the raw indicators that feed scoring and prioritization decisions.",
    "Component Score": "Helps explain why a final score moved up or down for one account/product lane.",
    "Daily Score": "Primary ranking score used to prioritize who to review or contact first.",
    "Tier": "Use tier to bucket urgency: high first, then medium, then low.",
    "Review Queue": "Start here for daily analyst workflow and outbound prioritization.",
    "Promotion Readiness": "Use this to decide when scores are stable enough to sync into CRM candidate lists.",
    "ICP Coverage": "Use this to monitor whether known ICP accounts are represented in high/medium tiers.",
    "Ops Metrics": "Use this to detect pipeline quality/performance problems before they affect decisions.",
    "Live Crawl": "Enable only when you want fresh external evidence; runs may take longer.",
    "Workers Per Source": "Increase this for faster large-batch hunting (e.g., 1000-5000 companies). Leave blank to auto-size by machine/network pacing.",
    "Confidence": "Higher confidence means stronger text/evidence match for a signal.",
    "Source Reliability": "Higher reliability increases trust in evidence from that source.",
}

_EXTRA_TERM_GUIDE: list[dict[str, str]] = [
    {
        "term": "ICP (Ideal Customer Profile)",
        "details": "Reference profile of accounts that best match your product fit and conversion goals.",
        "how_to_read": "Use ICP coverage reports to verify high/medium intent is showing up in your core targets.",
    },
    {
        "term": "POC (Proof of Concept)",
        "details": "Structured evaluation stage before full purchase/rollout.",
        "how_to_read": "Signals mentioning legal/security/procurement progression usually indicate POC momentum.",
    },
    {
        "term": "CRM Candidate",
        "details": "Account that is strong enough to be pushed into CRM prioritization lists.",
        "how_to_read": "Check Promotion Readiness before auto-promoting to avoid noisy handoffs.",
    },
    {
        "term": "Signal Weight",
        "details": "Base influence assigned to a signal in scoring before decay/reliability adjustments.",
        "how_to_read": "Higher weight means that signal can move score/tier more strongly.",
    },
    {
        "term": "Signal Half-life",
        "details": "Days after which a signal's contribution decays by ~50%.",
        "how_to_read": "Short half-life means freshness matters more; stale evidence should be discounted faster.",
    },
    {
        "term": "Minimum Confidence",
        "details": "Confidence threshold required for a signal match to count.",
        "how_to_read": "If confidence is below this threshold, the signal should not contribute to score.",
    },
]

_OUTPUT_HOW_TO_USE_BY_KEY: dict[str, str] = {
    "review_queue": "Start here each day for action and outreach ordering.",
    "daily_scores": "Use this for full ranking and deeper drill-down by product lane.",
    "promotion_readiness": "Use this to decide if automation-to-CRM should proceed.",
    "icp_coverage": "Use this to verify strategic account coverage quality.",
    "ops_metrics": "Use this to catch pipeline reliability/performance issues early.",
}

_SIGNAL_DESCRIPTIONS: dict[str, str] = {
    "compliance_initiative": "Company is actively investing in a formal compliance program.",
    "audit_date_announced": "An audit timeline or milestone has been publicly stated.",
    "launch_or_scale_event": "A launch, scale-up, or major expansion event is underway.",
    "erp_s4_migration_milestone": "ERP modernization activity, especially SAP S/4 migration milestone.",
    "supply_chain_platform_rollout": "Supply-chain platform/control-tower rollout is underway.",
    "cloud_cost_spike": "Evidence of rising cloud spend pressure or usage spike.",
    "cost_reduction_mandate": "Explicit mandate to cut spend or improve margin through cost control.",
    "finops_tool_eval": "FinOps tooling is being evaluated or compared.",
    "vendor_consolidation_program": "Active program to reduce tool/vendor sprawl.",
    "governance_enforcement_need": "Need for policy, governance, or enforcement controls is explicit.",
    "env_spinup_requests": "Frequent requests for environments/clusters indicate platform friction.",
    "idp_golden_path_initiative": "Internal developer platform/golden path initiative is visible.",
    "devops_bottleneck_language": "Language indicates DevOps/infra bottlenecks slowing delivery.",
    "security_baseline_as_default": "Organization is pushing secure defaults/baselines by policy.",
    "cloud_connected": "First-party event indicates cloud account/project was connected.",
    "audit_viewed": "First-party event indicates audit/reporting surface was viewed.",
    "teammate_invited": "First-party collaboration event indicates additional stakeholder involvement.",
    "repo_added_deploy_attempted": "First-party setup/use event indicating deployment intent.",
    "sso_rbac_audit_controls_request": "Request for enterprise controls (SSO/RBAC/audit) appears.",
    "poc_stage_progression": "POC moves forward (security/procurement/legal/success milestones).",
    "devops_role_open": "Open role suggests active DevOps/SRE investment.",
    "finops_role_open": "Open role suggests active FinOps investment.",
    "platform_role_open": "Open role suggests platform engineering investment.",
    "kubernetes_detected": "Kubernetes usage or stack references are present.",
    "terraform_detected": "Terraform usage or IaC references are present.",
    "gitops_detected": "GitOps tooling/practices (ArgoCD/Flux) are present.",
    "tooling_sprawl_detected": "Multiple infra tools suggest stack complexity/sprawl.",
    "high_intent_phrase_production_fast": "Language indicates urgency to ship production-ready capability quickly.",
    "high_intent_phrase_devops_toil": "Language indicates operational toil or infra bottlenecks.",
    "high_intent_phrase_cost_control": "Language indicates urgent need for spend control.",
    "enterprise_modernization_program": "Broader modernization/transformation program is active.",
    "data_platform_initiative": "Data platform initiative is active or being planned.",
    "compliance_governance_messaging": "Messaging emphasizes governance/compliance outcomes.",
    "cloud_platform_messaging": "Messaging emphasizes cloud/platform modernization direction.",
    "media_traffic_reliability_pressure": "Traffic/reliability pressure is visible (outages, surges, performance risk).",
}


def _csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [{str(k): str(v or "") for k, v in row.items()} for row in reader]


def _csv_stats(path: Path) -> tuple[list[str], int]:
    if not path.exists():
        return [], 0
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = list(reader.fieldnames or [])
        row_count = sum(1 for _ in reader)
    return headers, row_count


def _csv_preview(path: Path, limit: int) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = list(reader.fieldnames or [])
        for idx, row in enumerate(reader):
            if idx >= limit:
                break
            rows.append({str(k): str(v or "") for k, v in row.items()})
    return headers, rows


def _file_metadata(descriptor: FileDescriptor) -> dict[str, Any]:
    headers, row_count = _csv_stats(descriptor.path)
    exists = descriptor.path.exists()
    modified = ""
    if exists:
        modified = datetime.fromtimestamp(descriptor.path.stat().st_mtime).isoformat(timespec="seconds")
    return {
        "key": descriptor.key,
        "label": descriptor.label,
        "description": descriptor.description,
        "path": str(descriptor.path),
        "primary": descriptor.primary,
        "exists": exists,
        "row_count": row_count,
        "columns": headers,
        "last_modified": modified,
    }


def _output_path(key: str, yyyymmdd: str) -> Path:
    prefix = _OUTPUT_PREFIX_BY_KEY[key]
    return _settings.out_dir / f"{prefix}_{yyyymmdd}.csv"


def _available_output_dates() -> list[str]:
    dates: set[str] = set()
    for path in _settings.out_dir.glob("*.csv"):
        stem = path.stem
        if "_" not in stem:
            continue
        maybe_date = stem.rsplit("_", 1)[-1]
        if _DATE_RE.match(maybe_date):
            dates.add(maybe_date)
    return sorted(dates, reverse=True)


def _default_output_date() -> str:
    available = _available_output_dates()
    if available:
        return available[0]
    return date.today().strftime("%Y%m%d")


def _output_metadata(output_date: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, label in _OUTPUT_LABEL_BY_KEY.items():
        path = _output_path(key, output_date)
        headers, row_count = _csv_stats(path)
        exists = path.exists()
        modified = ""
        if exists:
            modified = datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
        rows.append(
            {
                "key": key,
                "label": label,
                "description": _OUTPUT_DESC_BY_KEY.get(key, ""),
                "path": str(path),
                "exists": exists,
                "row_count": row_count,
                "columns": headers,
                "last_modified": modified,
            }
        )
    return rows


def _parse_key_values(text: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for token in text.strip().split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        parsed[key] = value
    return parsed


def _parse_iso(value: str) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _iso_for_ui(value: str) -> str:
    parsed = _parse_iso(value)
    if parsed is None:
        return value
    return parsed.isoformat(timespec="seconds")


def _db_dsn_with_timeout(timeout_seconds: int = 2) -> str:
    timeout = max(1, int(timeout_seconds))
    raw_dsn = str(_settings.pg_dsn or "").strip() or os.getenv("SIGNALS_PG_DSN", "").strip()
    if raw_dsn and "://" in raw_dsn:
        if "connect_timeout=" in raw_dsn:
            return raw_dsn
        separator = "&" if "?" in raw_dsn else "?"
        return f"{raw_dsn}{separator}connect_timeout={timeout}"

    host = os.getenv("SIGNALS_PG_HOST", "127.0.0.1").strip()
    port = os.getenv("SIGNALS_PG_PORT", "55432").strip()
    user = os.getenv("SIGNALS_PG_USER", "signals").strip()
    password = os.getenv("SIGNALS_PG_PASSWORD", "signals_dev_password").strip()
    database = os.getenv("SIGNALS_PG_DB", "signals").strip()
    return f"postgresql://{user}:{password}@{host}:{port}/{database}?connect_timeout={timeout}"


def _tracking_stats() -> dict[str, Any]:
    watchlist_rows = _csv_rows(_settings.watchlist_accounts_path)
    seed_rows = _csv_rows(_settings.seed_accounts_path)
    sample = [
        {
            "company_name": (row.get("company_name", "") or "").strip(),
            "domain": (row.get("domain", "") or "").strip(),
        }
        for row in watchlist_rows[:15]
    ]

    expected_total = len(watchlist_rows) + len(seed_rows)
    stats: dict[str, Any] = {
        "watchlist_count": len(watchlist_rows),
        "seed_count": len(seed_rows),
        "expected_total": expected_total,
        "db_total_accounts": None,
        "db_seed_accounts": None,
        "db_discovered_accounts": None,
        "db_gap": None,
        "db_available": False,
        "db_error": "",
        "watchlist_sample": sample,
    }

    try:
        conn = db.get_connection(_db_dsn_with_timeout())
        try:
            total_row = conn.execute("SELECT COUNT(*) AS c FROM accounts").fetchone()
            by_source = conn.execute(
                """
                SELECT source_type, COUNT(*) AS c
                FROM accounts
                GROUP BY source_type
                """
            ).fetchall()
            by_source_map = {str(row["source_type"]): int(row["c"] or 0) for row in by_source}
            db_total = int(total_row["c"] if total_row else 0)
            stats["db_total_accounts"] = db_total
            stats["db_seed_accounts"] = int(by_source_map.get("seed", 0))
            stats["db_discovered_accounts"] = int(by_source_map.get("discovered", 0))
            stats["db_gap"] = max(0, expected_total - db_total)
            stats["db_available"] = True
        finally:
            conn.close()
    except Exception as exc:
        stats["db_error"] = str(exc)

    return stats


def _tracked_company_rows() -> list[dict[str, str]]:
    watchlist_rows = _csv_rows(_settings.watchlist_accounts_path)
    seed_rows = _csv_rows(_settings.seed_accounts_path)
    rows: list[dict[str, str]] = []

    def append_rows(items: list[dict[str, str]], source: str) -> None:
        for row in items:
            company_name = (
                (row.get("company_name", "") or "").strip()
                or (row.get("account_name", "") or "").strip()
                or (row.get("name", "") or "").strip()
            )
            domain = (row.get("domain", "") or "").strip()
            country = (row.get("country", "") or "").strip()
            rows.append(
                {
                    "company_name": company_name,
                    "domain": domain,
                    "country": country,
                    "source": source,
                }
            )

    append_rows(watchlist_rows, "watchlist")
    append_rows(seed_rows, "seed")

    source_order = {"watchlist": 0, "seed": 1}
    return sorted(
        rows,
        key=lambda item: (
            int(source_order.get(str(item["source"]), 99)),
            str(item["company_name"]),
            str(item["domain"]),
        ),
    )


def _run_history(limit: int = _MAX_RUN_HISTORY) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        conn = db.get_connection(_db_dsn_with_timeout())
        try:
            all_runs = db.list_runs(conn)
        finally:
            conn.close()
    except Exception:
        return rows

    for run in all_runs[: max(1, int(limit))]:
        started_raw = str(run.get("started_at", "") or "")
        finished_raw = str(run.get("finished_at", "") or "")
        started = _parse_iso(started_raw)
        finished = _parse_iso(finished_raw)
        duration_seconds: float | None = None
        if started and finished:
            duration_seconds = max(0.0, (finished - started).total_seconds())
        rows.append(
            {
                "run_id": str(run.get("run_id", "") or ""),
                "run_date": str(run.get("run_date", "") or ""),
                "status": str(run.get("status", "") or ""),
                "started_at": _iso_for_ui(started_raw),
                "finished_at": _iso_for_ui(finished_raw),
                "duration_seconds": None if duration_seconds is None else round(duration_seconds, 2),
                "error_summary": str(run.get("error_summary", "") or "")[:180],
            }
        )
    return rows


def _latest_output_bundle(preferred_output_date: str | None = None) -> dict[str, Any]:
    candidate_dates: list[str] = []
    if preferred_output_date:
        candidate_dates.append(preferred_output_date)
    for maybe in _available_output_dates():
        if maybe not in candidate_dates:
            candidate_dates.append(maybe)

    for output_date in candidate_dates:
        metadata = _output_metadata(output_date)
        present = [row for row in metadata if bool(row.get("exists"))]
        if not present:
            continue
        missing = [str(row.get("label", "")) for row in metadata if not bool(row.get("exists"))]
        latest_modified = max(
            (
                _parse_iso(str(row.get("last_modified", "") or ""))
                for row in present
                if str(row.get("last_modified", "") or "").strip()
            ),
            default=None,
        )
        return {
            "available": True,
            "date": output_date,
            "status": "complete" if len(present) == len(metadata) else "partial",
            "files_present": len(present),
            "files_expected": len(metadata),
            "missing_labels": missing,
            "latest_modified": latest_modified.isoformat(timespec="seconds") if latest_modified is not None else "",
        }

    return {
        "available": False,
        "date": preferred_output_date or "",
        "status": "missing",
        "files_present": 0,
        "files_expected": len(_OUTPUT_PREFIX_BY_KEY),
        "missing_labels": [],
        "latest_modified": "",
    }


def _human_signal_name(signal_code: str) -> str:
    return signal_code.replace("_", " ").strip().title()


def _signal_glossary() -> list[dict[str, Any]]:
    rows = _csv_rows(_settings.signal_registry_path)
    glossary: list[dict[str, Any]] = []
    for row in rows:
        enabled = (row.get("enabled", "") or "").strip().lower() in {"1", "true", "yes", "on"}
        if not enabled:
            continue
        signal_code = (row.get("signal_code", "") or "").strip()
        if not signal_code:
            continue
        glossary.append(
            {
                "signal_code": signal_code,
                "signal_name": _human_signal_name(signal_code),
                "description": _SIGNAL_DESCRIPTIONS.get(signal_code, _human_signal_name(signal_code)),
                "product_scope": (row.get("product_scope", "") or "").strip(),
                "category": (row.get("category", "") or "").strip(),
                "base_weight": (row.get("base_weight", "") or "").strip(),
                "half_life_days": (row.get("half_life_days", "") or "").strip(),
                "min_confidence": (row.get("min_confidence", "") or "").strip(),
            }
        )
    return sorted(glossary, key=lambda item: (str(item["product_scope"]), str(item["signal_code"])))


def _term_glossary() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen_terms: set[str] = set()

    def add_term(term: str, details: str, how_to_read: str) -> None:
        key = term.strip()
        if not key or key in seen_terms:
            return
        seen_terms.add(key)
        rows.append(
            {
                "term": key,
                "details": details.strip(),
                "how_to_read": how_to_read.strip(),
            }
        )

    for row in _TERM_GLOSSARY:
        term = str(row.get("term", "") or "").strip()
        add_term(
            term=term,
            details=str(row.get("details", "") or ""),
            how_to_read=_TERM_ACTION_NOTES.get(
                term,
                "Use this with run history and output sheet descriptions to interpret pipeline state.",
            ),
        )
    for row in _EXTRA_TERM_GUIDE:
        add_term(
            term=str(row.get("term", "") or ""),
            details=str(row.get("details", "") or ""),
            how_to_read=str(row.get("how_to_read", "") or ""),
        )
    for key, label in _OUTPUT_LABEL_BY_KEY.items():
        add_term(
            term=f"{label} (Output Sheet)",
            details=_OUTPUT_DESC_BY_KEY.get(key, label),
            how_to_read=_OUTPUT_HOW_TO_USE_BY_KEY.get(
                key,
                "Use this output together with run history to confirm execution quality.",
            ),
        )
    return rows


class RunDailyRequest(BaseModel):
    run_date: str = Field(default_factory=lambda: date.today().isoformat())
    live_crawl: bool = False
    workers_per_source: int | None = None


app = FastAPI(title="signals-local-ui", version="0.2.0")


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Signals Local Console</title>
  <style>
    :root {
      --bg: #f4f1e8;
      --card: #fffef8;
      --ink: #122429;
      --muted: #5c6d73;
      --accent: #0f7a66;
      --accent-2: #c27a00;
      --line: #d8e2e0;
      --ok: #0f7a66;
      --warn: #9a5d00;
      --err: #b2182b;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 8% 10%, #fff8df 0%, transparent 32%),
        radial-gradient(circle at 92% 0%, #e5f6ef 0%, transparent 34%),
        var(--bg);
    }
    .wrap {
      max-width: 1280px;
      margin: 0 auto;
      padding: 24px;
      display: grid;
      gap: 16px;
    }
    .head {
      background: linear-gradient(118deg, #0d7a66, #114f60 70%);
      color: #f2fffb;
      border-radius: 16px;
      padding: 18px 20px;
      border: 1px solid rgba(255,255,255,0.12);
    }
    .head h1 {
      margin: 0 0 6px 0;
      font-size: 24px;
      letter-spacing: 0.2px;
    }
    .head p {
      margin: 0;
      opacity: 0.94;
    }
    .grid-2 {
      display: grid;
      gap: 16px;
      grid-template-columns: 1fr 1fr;
    }
    @media (max-width: 980px) {
      .grid-2 { grid-template-columns: 1fr; }
    }
    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 1px 0 rgba(0,0,0,0.03);
    }
    .card h2 {
      margin: 0 0 12px 0;
      font-size: 17px;
    }
    .subtle {
      margin: 0;
      color: var(--muted);
      font-size: 13px;
    }
    .run-controls {
      display: grid;
      gap: 10px;
      grid-template-columns: auto auto auto auto 1fr;
      align-items: center;
    }
    @media (max-width: 900px) {
      .run-controls { grid-template-columns: 1fr; }
    }
    .company-controls {
      display: grid;
      gap: 10px;
      grid-template-columns: minmax(220px, 1fr) auto auto auto auto;
      align-items: center;
      margin-bottom: 10px;
    }
    @media (max-width: 900px) {
      .company-controls { grid-template-columns: 1fr; }
    }
    input[type="date"], input[type="text"], select {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px 10px;
      background: #fff;
      color: var(--ink);
      min-height: 36px;
    }
    button {
      border: 0;
      border-radius: 10px;
      padding: 9px 12px;
      background: var(--accent);
      color: #fff;
      font-weight: 700;
      cursor: pointer;
      letter-spacing: 0.1px;
      min-height: 36px;
    }
    button:hover { filter: brightness(0.95); }
    button.alt {
      background: #ecf2ef;
      color: #173737;
      border: 1px solid var(--line);
    }
    .pill {
      display: inline-block;
      font-size: 12px;
      border-radius: 999px;
      padding: 2px 8px;
      background: #e8f7f2;
      color: #0d7a66;
      margin-left: 8px;
    }
    .kpis {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(5, minmax(120px, 1fr));
    }
    @media (max-width: 980px) {
      .kpis { grid-template-columns: repeat(2, minmax(120px, 1fr)); }
    }
    .kpi {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      background: #f9fcfb;
    }
    .kpi .label {
      color: var(--muted);
      font-size: 12px;
    }
    .kpi .value {
      margin-top: 4px;
      font-size: 20px;
      font-weight: 700;
      color: #13343b;
    }
    .kpi.warn .value { color: var(--warn); }
    .kpi.ok .value { color: var(--ok); }
    .kpi.err .value { color: var(--err); }
    .mini-list {
      margin-top: 10px;
      font-size: 12px;
      color: var(--muted);
      max-height: 120px;
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px;
      background: #fff;
    }
    .meta {
      margin: 10px 0 0 0;
      color: var(--muted);
      font-size: 12px;
      word-break: break-all;
    }
    .status-line {
      margin-top: 10px;
      font-size: 13px;
      font-weight: 700;
      color: #163a41;
    }
    .status-line.ok { color: var(--ok); }
    .status-line.err { color: var(--err); }
    .status-line.run { color: var(--warn); }
    .status-line.warn { color: var(--warn); }
    .run-badge {
      margin-top: 10px;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 9px 10px;
      font-size: 13px;
      font-weight: 700;
      color: #163a41;
      background: #edf4f2;
    }
    .run-badge.completed {
      color: var(--ok);
      background: #e8f7f2;
      border-color: #bee5d7;
    }
    .run-badge.failed {
      color: var(--err);
      background: #fdecee;
      border-color: #f4c1c9;
    }
    .run-badge.running {
      color: var(--warn);
      background: #fff4de;
      border-color: #f3d7a3;
    }
    .run-badge.none {
      color: var(--muted);
      background: #f4f6f6;
    }
    .run-badge.partial {
      color: var(--warn);
      background: #fff8ea;
      border-color: #ecd6ad;
    }
    .files, .table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    .files th, .files td, .table th, .table td {
      border-bottom: 1px solid var(--line);
      text-align: left;
      padding: 8px 6px;
      vertical-align: top;
    }
    .files th, .table th {
      color: var(--muted);
      font-weight: 700;
      position: sticky;
      top: 0;
      background: #f5fbf8;
    }
    .path {
      color: var(--muted);
      font-size: 12px;
      word-break: break-all;
      margin-top: 3px;
    }
    .desc {
      color: #4f646a;
      font-size: 12px;
      margin-top: 3px;
    }
    .scroll {
      overflow: auto;
      max-height: 280px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fff;
    }
    .preview-wrap {
      overflow: auto;
      max-height: 360px;
      border: 1px solid var(--line);
      border-radius: 10px;
    }
    .preview {
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
      background: #fff;
    }
    .preview th, .preview td {
      border-bottom: 1px solid #eef3f0;
      border-right: 1px solid #eef3f0;
      padding: 6px;
      white-space: nowrap;
      max-width: 240px;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .preview th { background: #f2f8f6; position: sticky; top: 0; }
    pre {
      margin: 0;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #0d1717;
      color: #dcf9ef;
      padding: 10px;
      max-height: 240px;
      overflow: auto;
      font-size: 12px;
    }
    .error {
      color: var(--err);
      font-weight: 700;
      margin-top: 6px;
    }
    .pager {
      display: flex;
      gap: 8px;
      justify-content: flex-end;
      margin-top: 8px;
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="head">
      <h1>Signals Local Console</h1>
      <p>Track watchlist coverage, run pipeline, see completion state, and understand every core term.</p>
    </div>

    <div class="card">
      <h2>Tracking Coverage</h2>
      <div class="kpis" id="kpiGrid"></div>
      <p id="coverageStatus" class="status-line"></p>
      <p id="trackingMeta" class="meta"></p>
      <div id="watchlistSample" class="mini-list"></div>
    </div>

    <div class="card">
      <h2>Tracked Companies</h2>
      <div class="company-controls">
        <input
          id="companyFilter"
          type="text"
          placeholder="Filter by company or domain"
          oninput="scheduleCompanyReload()"
        />
        <select id="companySource" onchange="loadCompanies(true)">
          <option value="watchlist" selected>Watchlist only</option>
          <option value="all">All lists</option>
          <option value="seed">Seed only</option>
        </select>
        <select id="companyLimit" onchange="onCompanyPageSizeChange()">
          <option value="100">100 / page</option>
          <option value="250" selected>250 / page</option>
          <option value="500">500 / page</option>
          <option value="5000">All (up to 5000)</option>
        </select>
        <button class="alt" onclick="showWatchlistTarget()">Show Full 1000 Watchlist</button>
        <button class="alt" onclick="loadCompanies(true)">Refresh List</button>
      </div>
      <p id="companyMeta" class="meta"></p>
      <div class="scroll" style="max-height: 340px;">
        <table class="table">
          <thead>
            <tr>
              <th>#</th>
              <th>Company</th>
              <th>Domain</th>
              <th>List</th>
            </tr>
          </thead>
          <tbody id="companyBody"></tbody>
        </table>
      </div>
      <div class="pager">
        <button class="alt" id="companyPrevBtn" onclick="pageCompanies(-1)">Prev</button>
        <button class="alt" id="companyNextBtn" onclick="pageCompanies(1)">Next</button>
      </div>
    </div>

    <div class="card">
      <h2>Run Pipeline</h2>
      <div class="run-controls">
        <label for="runDate">Run date</label>
        <input id="runDate" type="date" />
        <label><input id="liveCrawl" type="checkbox" /> Enable live crawl</label>
        <input id="workersPerSource" type="number" min="1" placeholder="Workers/source (auto)" title="Parallel workers per source. Leave blank for auto." />
        <div>
          <button id="runBtn" onclick="runDaily()">Run Daily</button>
          <button class="alt" onclick="refreshOverview()">Refresh</button>
        </div>
      </div>
      <div id="runStatus" class="status-line"></div>
      <div id="latestRunBadge" class="run-badge none">Latest recorded run: loading...</div>
      <p id="runSummary" class="meta"></p>
      <p id="latestRunMeta" class="meta"></p>
      <p id="outputBundleMeta" class="meta"></p>
      <pre id="runLog">No run launched from this browser session yet. Use "Latest recorded run" above to confirm completion time and status.</pre>
    </div>

    <div class="card">
      <h2>Recent Runs</h2>
      <div class="scroll">
        <table class="table">
          <thead>
            <tr>
              <th>Run Date</th>
              <th>Status</th>
              <th>Started</th>
              <th>Finished</th>
              <th>Duration (s)</th>
              <th>Run ID</th>
            </tr>
          </thead>
          <tbody id="runHistoryBody"></tbody>
        </table>
      </div>
    </div>

    <div class="grid-2">
      <div class="card">
        <h2>Input Sheets</h2>
        <div class="scroll">
          <table class="files">
            <thead>
              <tr><th>Sheet</th><th>Rows</th><th>Action</th></tr>
            </thead>
            <tbody id="inputBody"></tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <h2>Output Sheets</h2>
        <div class="meta" style="margin-top:0; margin-bottom: 8px;">
          Output date:
          <select id="outputDate" onchange="refreshOverview()"></select>
        </div>
        <div class="scroll">
          <table class="files">
            <thead>
              <tr><th>Sheet</th><th>Rows</th><th>Action</th></tr>
            </thead>
            <tbody id="outputBody"></tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="grid-2">
      <div class="card">
        <h2>Term Guide</h2>
        <div class="scroll">
          <table class="table">
            <thead>
              <tr><th>Term</th><th>Meaning</th><th>How to Use It</th></tr>
            </thead>
            <tbody id="termBody"></tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <h2>Signal Guide</h2>
        <input id="signalFilter" type="text" placeholder="Filter signals (code, product, category, weights, meaning)" oninput="renderSignalRows()" />
        <div class="scroll" style="margin-top:10px;">
          <table class="table">
            <thead>
              <tr>
                <th>Signal Code</th>
                <th>Product</th>
                <th>Category</th>
                <th>Weight</th>
                <th>Half-life (d)</th>
                <th>Min conf.</th>
                <th>Meaning</th>
              </tr>
            </thead>
            <tbody id="signalBody"></tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="card">
      <h2>CSV Preview</h2>
      <p id="previewMeta" class="meta">Select any input/output sheet to preview first 25 rows.</p>
      <div id="previewError" class="error"></div>
      <div class="preview-wrap">
        <table id="previewTable" class="preview"></table>
      </div>
    </div>
  </div>

  <script>
    const runDateInput = document.getElementById("runDate");
    const runBtn = document.getElementById("runBtn");
    const runStatus = document.getElementById("runStatus");
    const runSummary = document.getElementById("runSummary");
    const latestRunBadge = document.getElementById("latestRunBadge");
    const latestRunMeta = document.getElementById("latestRunMeta");
    const outputBundleMeta = document.getElementById("outputBundleMeta");
    const runLog = document.getElementById("runLog");

    const inputBody = document.getElementById("inputBody");
    const outputBody = document.getElementById("outputBody");
    const outputDate = document.getElementById("outputDate");

    const runHistoryBody = document.getElementById("runHistoryBody");
    const kpiGrid = document.getElementById("kpiGrid");
    const coverageStatus = document.getElementById("coverageStatus");
    const trackingMeta = document.getElementById("trackingMeta");
    const watchlistSample = document.getElementById("watchlistSample");
    const companyFilter = document.getElementById("companyFilter");
    const companySource = document.getElementById("companySource");
    const companyLimit = document.getElementById("companyLimit");
    const companyMeta = document.getElementById("companyMeta");
    const companyBody = document.getElementById("companyBody");
    const companyPrevBtn = document.getElementById("companyPrevBtn");
    const companyNextBtn = document.getElementById("companyNextBtn");

    const termBody = document.getElementById("termBody");
    const signalBody = document.getElementById("signalBody");
    const signalFilter = document.getElementById("signalFilter");

    const previewMeta = document.getElementById("previewMeta");
    const previewError = document.getElementById("previewError");
    const previewTable = document.getElementById("previewTable");

    let currentSignalRows = [];
    let companyPageSize = Number(companyLimit.value || 100);
    let companyOffset = 0;
    let companyTotal = 0;
    let companyFilterTimer = null;
    const completedRunStatuses = new Set(["completed", "success", "succeeded"]);
    const runningRunStatuses = new Set(["running", "in_progress"]);
    const sourceOptionLabels = {
      watchlist: "Watchlist only",
      all: "All lists",
      seed: "Seed only",
    };

    function fmtRows(value) {
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric.toLocaleString() : String(value || "");
    }

    function escapeHtml(value) {
      return String(value || "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
    }

    function isCompletedStatus(status) {
      return completedRunStatuses.has(String(status || "").toLowerCase());
    }

    function isRunningStatus(status) {
      return runningRunStatuses.has(String(status || "").toLowerCase());
    }

    function sourceCount(counts, key) {
      const raw = counts && counts[key] !== undefined ? counts[key] : 0;
      const parsed = Number(raw);
      return Number.isFinite(parsed) ? parsed : 0;
    }

    function updateCompanySourceLabels(counts) {
      for (const [value, label] of Object.entries(sourceOptionLabels)) {
        const option = [...companySource.options].find(item => item.value === value);
        if (!option) continue;
        option.textContent = `${label} (${fmtRows(sourceCount(counts, value))})`;
      }
    }

    function renderKpis(stats) {
      const dbGap = stats.db_gap === null || stats.db_gap === undefined ? "-" : fmtRows(stats.db_gap);
      const dbTotal = stats.db_total_accounts === null || stats.db_total_accounts === undefined ? "-" : fmtRows(stats.db_total_accounts);
      const gapClass = (stats.db_gap || 0) > 0 ? "warn" : "ok";
      const kpis = [
        { label: "Watchlist Targets", value: fmtRows(stats.watchlist_count), cls: "ok" },
        { label: "Seed Accounts", value: fmtRows(stats.seed_count), cls: "ok" },
        { label: "Expected Total", value: fmtRows(stats.expected_total), cls: "ok" },
        { label: "Accounts In DB", value: dbTotal, cls: stats.db_available ? "ok" : "warn" },
        { label: "Gap vs Expected", value: dbGap, cls: gapClass },
      ];
      kpiGrid.innerHTML = kpis.map(kpi => `
        <div class="kpi ${kpi.cls}">
          <div class="label">${escapeHtml(kpi.label)}</div>
          <div class="value">${escapeHtml(kpi.value)}</div>
        </div>
      `).join("");

      if (stats.db_available) {
        trackingMeta.textContent = `DB breakdown: seed=${fmtRows(stats.db_seed_accounts)} discovered=${fmtRows(stats.db_discovered_accounts)}.`;
      } else if (stats.db_error) {
        trackingMeta.textContent = `DB status unavailable: ${stats.db_error}`;
      } else {
        trackingMeta.textContent = "DB status unavailable.";
      }

      if (!stats.db_available) {
        coverageStatus.className = "status-line warn";
        coverageStatus.textContent = "Coverage check pending: database status is currently unavailable.";
      } else if ((stats.db_gap || 0) > 0) {
        coverageStatus.className = "status-line err";
        coverageStatus.textContent = `Coverage gap detected: ${fmtRows(stats.db_gap)} tracked accounts are missing from DB.`;
      } else {
        coverageStatus.className = "status-line ok";
        coverageStatus.textContent = `Coverage met: ${fmtRows(stats.watchlist_count)} watchlist + ${fmtRows(stats.seed_count)} seed = ${fmtRows(stats.expected_total)} expected (DB=${fmtRows(stats.db_total_accounts)}).`;
      }

      const sampleRows = (stats.watchlist_sample || []).map(row =>
        `${escapeHtml(row.company_name || "(no name)")} | ${escapeHtml(row.domain || "(no domain)")}`
      );
      watchlistSample.innerHTML = sampleRows.length
        ? sampleRows.join("<br/>")
        : "No watchlist sample available.";
    }

    function renderOutputBundle(bundle) {
      if (!bundle || !bundle.available) {
        outputBundleMeta.textContent = "Output bundle status: no output CSV bundle found yet.";
        return;
      }
      const missing = Array.isArray(bundle.missing_labels) ? bundle.missing_labels : [];
      const missingText = missing.length ? ` | missing=${missing.join(", ")}` : "";
      outputBundleMeta.textContent = [
        "Output bundle",
        `status=${String(bundle.status || "unknown").toUpperCase()}`,
        `date=${bundle.date || "-"}`,
        `files=${fmtRows(bundle.files_present)}/${fmtRows(bundle.files_expected)}`,
        `latest_file_update=${bundle.latest_modified || "-"}`,
      ].join(" | ") + missingText;
    }

    function renderRunHistory(rows, bundle) {
      if (!rows || rows.length === 0) {
        runHistoryBody.innerHTML = `<tr><td colspan="6">No runs found yet.</td></tr>`;
        if (bundle && bundle.available && String(bundle.status || "").toLowerCase() === "complete") {
          latestRunBadge.className = "run-badge completed";
          latestRunBadge.textContent = `Latest completed outputs: ${bundle.date || "-"} (files ${bundle.files_present}/${bundle.files_expected})`;
          latestRunMeta.textContent = `No DB run rows found. Completion inferred from output files updated at ${bundle.latest_modified || "-"}.`;
        } else if (bundle && bundle.available) {
          latestRunBadge.className = "run-badge partial";
          latestRunBadge.textContent = `Latest output bundle is partial: ${bundle.date || "-"} (${bundle.files_present}/${bundle.files_expected} files)`;
          latestRunMeta.textContent = "Run completion cannot be fully confirmed yet because output files are incomplete.";
        } else {
          latestRunBadge.className = "run-badge none";
          latestRunBadge.textContent = "Latest recorded run: none";
          latestRunMeta.textContent = "No recorded runs yet.";
        }
        return;
      }
      runHistoryBody.innerHTML = rows.map(row => `
        <tr>
          <td>${escapeHtml(row.run_date || "")}</td>
          <td>${escapeHtml(row.status || "")}</td>
          <td>${escapeHtml(row.started_at || "")}</td>
          <td>${escapeHtml(row.finished_at || "")}</td>
          <td>${row.duration_seconds === null || row.duration_seconds === undefined ? "" : escapeHtml(row.duration_seconds)}</td>
          <td title="${escapeHtml(row.error_summary || "")}">${escapeHtml(row.run_id || "")}</td>
        </tr>
      `).join("");

      const latest = rows[0];
      if (isCompletedStatus(latest.status)) {
        latestRunBadge.className = "run-badge completed";
      } else if (isRunningStatus(latest.status)) {
        latestRunBadge.className = "run-badge running";
      } else {
        latestRunBadge.className = "run-badge failed";
      }
      latestRunBadge.textContent = `Latest recorded run: ${String(latest.status || "unknown").toUpperCase()} | date=${latest.run_date || "-"}`;

      const latestCompleted = rows.find(row => isCompletedStatus(row.status));

      const duration = latest.duration_seconds === null || latest.duration_seconds === undefined
        ? ""
        : ` | duration=${latest.duration_seconds}s`;
      const base = [
        "Latest recorded run",
        `date=${latest.run_date || "-"}`,
        `status=${latest.status || "-"}`,
        `started=${latest.started_at || "-"}`,
        `finished=${latest.finished_at || "-"}`,
      ].join(" | ") + duration;
      if (!isCompletedStatus(latest.status) && latestCompleted) {
        latestRunMeta.textContent = `${base} | last_completed=${latestCompleted.run_date || "-"} finished=${latestCompleted.finished_at || "-"}`;
      } else {
        latestRunMeta.textContent = base;
      }
    }

    async function loadCompanies(resetOffset = false) {
      if (resetOffset) companyOffset = 0;
      const q = (companyFilter.value || "").trim();
      const source = companySource.value || "watchlist";
      const res = await fetch(
        `/api/tracked-companies?offset=${companyOffset}&limit=${companyPageSize}&source=${encodeURIComponent(source)}&q=${encodeURIComponent(q)}`
      );
      const data = await res.json();
      if (!res.ok) {
        companyBody.innerHTML = `<tr><td colspan="4">${escapeHtml(data.detail || "Failed to load companies.")}</td></tr>`;
        companyMeta.textContent = "Unable to load tracked companies.";
        companyPrevBtn.disabled = true;
        companyNextBtn.disabled = true;
        return;
      }

      const sourceCounts = data.source_counts || {};
      updateCompanySourceLabels(sourceCounts);
      companyOffset = Number(data.offset || 0);
      companyTotal = Number(data.total || 0);
      const rows = Array.isArray(data.rows) ? data.rows : [];
      if (rows.length === 0) {
        companyBody.innerHTML = `<tr><td colspan="4">No companies match this filter.</td></tr>`;
      } else {
        companyBody.innerHTML = rows.map((row, idx) => `
          <tr>
            <td>${escapeHtml(String(companyOffset + idx + 1))}</td>
            <td>${escapeHtml(row.company_name || "")}</td>
            <td>${escapeHtml(row.domain || "")}</td>
            <td>${escapeHtml(row.source || "")}</td>
          </tr>
        `).join("");
      }

      const start = companyTotal > 0 ? companyOffset + 1 : 0;
      const end = companyOffset + rows.length;
      const pageNumber = companyPageSize > 0 ? Math.floor(companyOffset / companyPageSize) + 1 : 1;
      const pageTotal = companyPageSize > 0 ? Math.max(1, Math.ceil(companyTotal / companyPageSize)) : 1;
      const filterNote = q ? ` | filter="${q}"` : "";
      const countsNote = ` | counts: watchlist=${fmtRows(sourceCount(sourceCounts, "watchlist"))} seed=${fmtRows(sourceCount(sourceCounts, "seed"))} all=${fmtRows(sourceCount(sourceCounts, "all"))}`;
      companyMeta.textContent = `Showing ${start}-${end} of ${companyTotal} tracked companies (source=${source}) | page ${pageNumber}/${pageTotal}${filterNote}${countsNote}`;
      companyPrevBtn.disabled = companyOffset <= 0;
      companyNextBtn.disabled = companyOffset + companyPageSize >= companyTotal;
    }

    function pageCompanies(direction) {
      if (direction < 0 && companyOffset <= 0) return;
      if (direction > 0 && companyOffset + companyPageSize >= companyTotal) return;
      companyOffset = Math.max(0, companyOffset + (direction * companyPageSize));
      loadCompanies(false);
    }

    function onCompanyPageSizeChange() {
      const parsed = Number(companyLimit.value || 100);
      companyPageSize = Number.isFinite(parsed) && parsed > 0 ? parsed : 100;
      loadCompanies(true);
    }

    function showWatchlistTarget() {
      companyFilter.value = "";
      companySource.value = "watchlist";
      companyLimit.value = "5000";
      companyPageSize = 5000;
      loadCompanies(true);
    }

    function scheduleCompanyReload() {
      if (companyFilterTimer) clearTimeout(companyFilterTimer);
      companyFilterTimer = setTimeout(() => loadCompanies(true), 250);
    }

    function renderTerms(rows) {
      termBody.innerHTML = rows.map(row => `
        <tr>
          <td><strong>${escapeHtml(row.term || "")}</strong></td>
          <td>${escapeHtml(row.details || "")}</td>
          <td>${escapeHtml(row.how_to_read || "")}</td>
        </tr>
      `).join("");
    }

    function renderSignalRows() {
      const needle = (signalFilter.value || "").trim().toLowerCase();
      const filtered = !needle ? currentSignalRows : currentSignalRows.filter(row => {
        const hay = [
          row.signal_code, row.signal_name, row.description, row.product_scope, row.category,
          row.base_weight, row.half_life_days, row.min_confidence,
        ].join(" ").toLowerCase();
        return hay.includes(needle);
      });
      signalBody.innerHTML = filtered.map(row => `
        <tr>
          <td><code>${escapeHtml(row.signal_code || "")}</code></td>
          <td>${escapeHtml(row.product_scope || "")}</td>
          <td>${escapeHtml(row.category || "")}</td>
          <td>${escapeHtml(row.base_weight || "-")}</td>
          <td>${escapeHtml(row.half_life_days || "-")}</td>
          <td>${escapeHtml(row.min_confidence || "-")}</td>
          <td>${escapeHtml(row.description || "")}</td>
        </tr>
      `).join("");
      if (filtered.length === 0) {
        signalBody.innerHTML = `<tr><td colspan="7">No signals match this filter.</td></tr>`;
      }
    }

    async function refreshOverview(preferredOutputDate = "") {
      const selectedDate = preferredOutputDate || outputDate.value || "";
      const qs = selectedDate ? `?output_date=${encodeURIComponent(selectedDate)}` : "";
      const res = await fetch(`/api/overview${qs}`);
      const data = await res.json();

      runDateInput.value = data.today;

      outputDate.innerHTML = "";
      for (const d of data.available_output_dates) {
        const option = document.createElement("option");
        option.value = d;
        option.textContent = d;
        if (d === data.selected_output_date) option.selected = true;
        outputDate.appendChild(option);
      }
      if (outputDate.options.length === 0) {
        const option = document.createElement("option");
        option.value = data.selected_output_date;
        option.textContent = data.selected_output_date;
        option.selected = true;
        outputDate.appendChild(option);
      }

      renderKpis(data.tracking_stats || {});
      renderRunHistory(data.run_history || [], data.output_bundle_status || {});
      renderOutputBundle(data.output_bundle_status || {});
      renderTerms(data.term_glossary || []);
      currentSignalRows = data.signal_glossary || [];
      renderSignalRows();
      await loadCompanies(true);

      inputBody.innerHTML = "";
      for (const item of data.inputs) {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>
            <strong>${escapeHtml(item.label)}</strong>
            ${item.primary ? '<span class="pill">primary</span>' : ""}
            <div class="desc">${escapeHtml(item.description || "")}</div>
            <div class="path">${escapeHtml(item.path)}</div>
          </td>
          <td>${item.exists ? fmtRows(item.row_count) : "missing"}</td>
          <td><button class="alt" onclick="preview('input','${item.key}')">Preview</button></td>
        `;
        inputBody.appendChild(tr);
      }

      outputBody.innerHTML = "";
      for (const item of data.outputs) {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>
            <strong>${escapeHtml(item.label)}</strong>
            <div class="desc">${escapeHtml(item.description || "")}</div>
            <div class="path">${escapeHtml(item.path)}</div>
          </td>
          <td>${item.exists ? fmtRows(item.row_count) : "missing"}</td>
          <td><button class="alt" onclick="preview('output','${item.key}')">Preview</button></td>
        `;
        outputBody.appendChild(tr);
      }
    }

    async function preview(area, key) {
      previewError.textContent = "";
      previewMeta.textContent = "Loading preview...";
      previewTable.innerHTML = "";
      const datePart = area === "output" ? `&output_date=${encodeURIComponent(outputDate.value)}` : "";
      const res = await fetch(`/api/preview?area=${encodeURIComponent(area)}&key=${encodeURIComponent(key)}${datePart}&limit=25`);
      const data = await res.json();
      if (!res.ok) {
        previewError.textContent = data.detail || "Preview failed.";
        previewMeta.textContent = "";
        return;
      }

      previewMeta.textContent = `${data.label} | ${data.path} | rows shown: ${data.rows.length}/${data.row_count}`;
      if (!data.columns || data.columns.length === 0) {
        previewTable.innerHTML = "<tr><td>No columns found.</td></tr>";
        return;
      }

      const thead = document.createElement("thead");
      const hr = document.createElement("tr");
      for (const col of data.columns) {
        const th = document.createElement("th");
        th.textContent = col;
        hr.appendChild(th);
      }
      thead.appendChild(hr);
      previewTable.appendChild(thead);

      const tbody = document.createElement("tbody");
      for (const row of data.rows) {
        const tr = document.createElement("tr");
        for (const col of data.columns) {
          const td = document.createElement("td");
          td.textContent = row[col] || "";
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
      previewTable.appendChild(tbody);
    }

    async function runDaily() {
      runBtn.disabled = true;
      runStatus.className = "status-line run";
      runStatus.textContent = "Running...";
      runSummary.textContent = "";
      runLog.textContent = "Running...";

      const started = Date.now();
      const ticker = setInterval(() => {
        const elapsed = Math.round((Date.now() - started) / 1000);
        runStatus.textContent = `Running... ${elapsed}s`;
      }, 500);

      const payload = {
        run_date: runDateInput.value,
        live_crawl: document.getElementById("liveCrawl").checked,
        workers_per_source: (() => {
          const raw = (document.getElementById("workersPerSource").value || "").trim();
          if (!raw) return null;
          const parsed = Number(raw);
          if (!Number.isFinite(parsed) || parsed < 1) return null;
          return Math.floor(parsed);
        })(),
      };
      try {
        const res = await fetch("/api/run-daily", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        const ok = res.ok && data.success;
        const duration = Number(data.duration_seconds || 0).toFixed(2);
        const finishedAt = data.finished_at || "";
        runStatus.className = ok ? "status-line ok" : "status-line err";
        runStatus.textContent = ok
          ? `Completed at ${finishedAt} in ${duration}s (exit=${data.exit_code}).`
          : `Failed at ${finishedAt} in ${duration}s (exit=${data.exit_code}).`;

        const summary = data.summary || {};
        const summaryParts = [];
        for (const key of ["ingested", "review_queue_rows", "daily_scores_rows", "icp_coverage", "sync_error"]) {
          if (summary[key] !== undefined) summaryParts.push(`${key}=${summary[key]}`);
        }
        runSummary.textContent = summaryParts.join(" | ");

        runLog.textContent = [
          `[command] ${data.command}`,
          `[started_at] ${data.started_at || ""}`,
          `[finished_at] ${data.finished_at || ""}`,
          `[duration_seconds] ${duration}`,
          "",
          "[stdout]",
          data.stdout || "",
          "",
          "[stderr]",
          data.stderr || ""
        ].join("\\n");

        const outputForRunDate = (payload.run_date || "").replaceAll("-", "");
        await refreshOverview(outputForRunDate);
      } catch (err) {
        runStatus.className = "status-line err";
        runStatus.textContent = "Run failed before completion.";
        runLog.textContent = String(err);
      } finally {
        clearInterval(ticker);
        runBtn.disabled = false;
      }
    }

    refreshOverview();
  </script>
</body>
</html>
"""


@app.get("/api/overview")
def overview(output_date: str | None = Query(default=None)) -> dict[str, Any]:
    today_iso = date.today().isoformat()
    available_dates = _available_output_dates()
    selected = output_date or _default_output_date()
    if output_date and output_date not in available_dates:
        available_dates = sorted(set(available_dates) | {output_date}, reverse=True)

    return {
        "today": today_iso,
        "selected_output_date": selected,
        "available_output_dates": available_dates,
        "tracking_stats": _tracking_stats(),
        "run_history": _run_history(limit=_MAX_RUN_HISTORY),
        "inputs": [_file_metadata(item) for item in _INPUT_FILES],
        "outputs": _output_metadata(selected),
        "output_bundle_status": _latest_output_bundle(selected),
        "term_glossary": _term_glossary(),
        "signal_glossary": _signal_glossary(),
    }


@app.get("/api/tracked-companies")
def tracked_companies(
    q: str = Query(default="", max_length=200),
    source: Literal["all", "watchlist", "seed"] = Query(default="watchlist"),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=10000),
) -> dict[str, Any]:
    filtered_rows = _tracked_company_rows()
    needle = q.strip().lower()
    if needle:
        filtered_rows = [
            row
            for row in filtered_rows
            if needle
            in " ".join(
                [
                    str(row.get("company_name", "")).lower(),
                    str(row.get("domain", "")).lower(),
                    str(row.get("country", "")).lower(),
                    str(row.get("source", "")).lower(),
                ]
            )
        ]
    source_counts = {
        "all": len(filtered_rows),
        "watchlist": sum(1 for row in filtered_rows if row["source"] == "watchlist"),
        "seed": sum(1 for row in filtered_rows if row["source"] == "seed"),
    }
    rows = filtered_rows if source == "all" else [row for row in filtered_rows if row["source"] == source]
    total = len(rows)
    page = rows[offset : offset + limit]
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "source": source,
        "query": q,
        "source_counts": source_counts,
        "rows": page,
    }


@app.get("/api/preview")
def preview(
    area: Literal["input", "output"] = Query(...),
    key: str = Query(...),
    output_date: str | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=200),
) -> dict[str, Any]:
    path: Path | None = None
    label = key
    if area == "input":
        match = next((item for item in _INPUT_FILES if item.key == key), None)
        if match is None:
            raise HTTPException(status_code=404, detail=f"Unknown input key: {key}")
        path = match.path
        label = match.label
    else:
        if key not in _OUTPUT_PREFIX_BY_KEY:
            raise HTTPException(status_code=404, detail=f"Unknown output key: {key}")
        selected = output_date or _default_output_date()
        path = _output_path(key, selected)
        label = _OUTPUT_LABEL_BY_KEY.get(key, key)

    if path is None or not path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")

    columns, rows = _csv_preview(path, limit=limit)
    _, row_count = _csv_stats(path)

    return {
        "area": area,
        "key": key,
        "label": label,
        "path": str(path),
        "columns": columns,
        "rows": rows,
        "row_count": row_count,
    }


@app.post("/api/run-daily")
def run_daily(payload: RunDailyRequest) -> dict[str, Any]:
    run_date = payload.run_date.strip()
    try:
        date.fromisoformat(run_date)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid run_date: {run_date}") from exc

    cmd = [sys.executable, "-m", "src.main", "run-daily", "--date", run_date]
    env = os.environ.copy()
    env["SIGNALS_ENABLE_LIVE_CRAWL"] = "1" if payload.live_crawl else "0"
    if payload.workers_per_source is not None and int(payload.workers_per_source) > 0:
        env["SIGNALS_LIVE_WORKERS_PER_SOURCE"] = str(int(payload.workers_per_source))

    started_at = datetime.now().isoformat(timespec="seconds")
    started_monotonic = time.monotonic()
    completed = subprocess.run(
        cmd,
        cwd=str(_settings.project_root),
        env=env,
        capture_output=True,
        text=True,
        timeout=3600,
        check=False,
    )
    finished_at = datetime.now().isoformat(timespec="seconds")
    duration_seconds = round(time.monotonic() - started_monotonic, 2)
    stdout = completed.stdout.strip()
    stderr = completed.stderr.strip()
    last_line = stdout.splitlines()[-1] if stdout else ""
    parsed = _parse_key_values(last_line)
    return {
        "success": completed.returncode == 0,
        "exit_code": int(completed.returncode),
        "command": " ".join(cmd),
        "run_date": run_date,
        "live_crawl": payload.live_crawl,
        "workers_per_source": payload.workers_per_source,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": duration_seconds,
        "summary": parsed,
        "stdout": stdout,
        "stderr": stderr,
    }
