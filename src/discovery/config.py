from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from src.utils import load_csv_rows, normalize_domain

logger = logging.getLogger(__name__)

DEFAULT_SECONDARY_SIGNALS = {
    "kubernetes_detected",
    "devops_role_open",
    "platform_role_open",
}

CPG_PATTERN_GROUPS: dict[str, set[str]] = {
    "rollout_change": {
        "erp_s4_migration_milestone",
        "supply_chain_platform_rollout",
        "launch_or_scale_event",
    },
    "cost_consolidation": {
        "cost_reduction_mandate",
        "vendor_consolidation_program",
    },
    "governance_compliance": {
        "governance_enforcement_need",
        "compliance_governance_messaging",
        "compliance_initiative",
        "audit_date_announced",
    },
    "poc_procurement_progression": {
        "poc_stage_progression",
    },
}

PLACEHOLDER_DOMAINS = {"example.com", "example.org", "example.net", "localhost"}

try:
    import tldextract  # type: ignore

    _OFFLINE_TLD_EXTRACTOR = tldextract.TLDExtract(cache_dir=None, suffix_list_urls=())
except Exception:  # pragma: no cover - optional dependency fallback.
    logger.warning("tldextract import failed, domain_family will use fallback parsing", exc_info=True)
    _OFFLINE_TLD_EXTRACTOR = None


def _to_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class SignalClassEntry:
    signal_code: str
    signal_class: str
    vertical_scope: str
    promotion_critical: bool


@dataclass(frozen=True)
class AccountProfile:
    domain: str
    relationship_stage: str
    vertical_tag: str
    is_self: bool
    exclude_from_crm: bool


@dataclass(frozen=True)
class DiscoveryThresholds:
    high: float
    medium: float
    explore: float
    low: float = 0.0


def load_signal_classes(path: Path) -> dict[str, SignalClassEntry]:
    rows = load_csv_rows(path)
    entries: dict[str, SignalClassEntry] = {}
    for row in rows:
        signal_code = (row.get("signal_code", "") or "").strip()
        if not signal_code:
            continue
        signal_class = (row.get("class", "primary") or "primary").strip().lower()
        if signal_class not in {"primary", "secondary"}:
            signal_class = "primary"
        entries[signal_code] = SignalClassEntry(
            signal_code=signal_code,
            signal_class=signal_class,
            vertical_scope=(row.get("vertical_scope", "all") or "all").strip().lower(),
            promotion_critical=_to_bool(row.get("promotion_critical", "false")),
        )

    # Safety fallback: if config is missing or incomplete, keep known noisy signals secondary.
    for signal_code in DEFAULT_SECONDARY_SIGNALS:
        entries.setdefault(
            signal_code,
            SignalClassEntry(
                signal_code=signal_code,
                signal_class="secondary",
                vertical_scope="all",
                promotion_critical=False,
            ),
        )
    return entries


def load_account_profiles(path: Path) -> dict[str, AccountProfile]:
    rows = load_csv_rows(path)
    profiles: dict[str, AccountProfile] = {}
    for row in rows:
        domain = normalize_domain(row.get("domain", ""))
        if not domain:
            continue
        profiles[domain] = AccountProfile(
            domain=domain,
            relationship_stage=(row.get("relationship_stage", "unknown") or "unknown").strip().lower(),
            vertical_tag=(row.get("vertical_tag", "unknown") or "unknown").strip().lower(),
            is_self=_to_bool(row.get("is_self", "false")),
            exclude_from_crm=_to_bool(row.get("exclude_from_crm", "false")),
        )
    return profiles


def load_icp_reference(path: Path) -> dict[str, str]:
    rows = load_csv_rows(path)
    by_domain: dict[str, str] = {}
    for row in rows:
        domain = normalize_domain(row.get("domain", ""))
        if not domain:
            continue
        stage = (row.get("relationship_stage", "unknown") or "unknown").strip().lower()
        by_domain[domain] = stage
    return by_domain


def load_discovery_thresholds(path: Path) -> DiscoveryThresholds:
    rows = load_csv_rows(path)
    values = {str(row.get("key", "")).strip().lower(): row.get("value", "") for row in rows}

    def _parse(key: str, default: float) -> float:
        raw = values.get(key, str(default))
        try:
            return float(raw)
        except (TypeError, ValueError):
            return default

    tier_1 = _parse("tier_1", 20.0)
    tier_2 = _parse("tier_2", 10.0)
    tier_3 = _parse("tier_3", 6.0)
    tier_4 = _parse("tier_4", 0.0)
    return DiscoveryThresholds(high=tier_1, medium=tier_2, explore=tier_3, low=tier_4)


def load_discovery_blocklist(path: Path) -> set[str]:
    rows = load_csv_rows(path)
    blocked: set[str] = set()
    for row in rows:
        domain = normalize_domain(row.get("domain", ""))
        if domain:
            blocked.add(domain)
    return blocked


def is_placeholder_domain(domain: str) -> bool:
    normalized = normalize_domain(domain)
    if not normalized:
        return True
    if normalized in PLACEHOLDER_DOMAINS:
        return True
    if normalized.endswith(".example"):
        return True
    return False


def classify_signal(signal_code: str, classes: dict[str, SignalClassEntry]) -> str:
    entry = classes.get(signal_code)
    if entry:
        return entry.signal_class
    return "secondary" if signal_code in DEFAULT_SECONDARY_SIGNALS else "primary"


def has_primary_signal(signal_codes: set[str], classes: dict[str, SignalClassEntry]) -> bool:
    return any(classify_signal(code, classes) == "primary" for code in signal_codes)


def count_primary_signals(signal_codes: set[str], classes: dict[str, SignalClassEntry]) -> int:
    return sum(1 for code in signal_codes if classify_signal(code, classes) == "primary")


def infer_vertical_tag(company_name: str, domain: str, signal_codes: set[str]) -> str:
    if "media_traffic_reliability_pressure" in signal_codes:
        return "media"

    normalized_name = (company_name or "").lower()
    normalized_domain = normalize_domain(domain)
    media_tokens = ("media", "news", "publishing", "ad")
    if any(token in normalized_name for token in media_tokens):
        return "media"

    cpg_tokens = ("consumer", "foods", "beverage", "retail", "fmcg")
    if any(token in normalized_name for token in cpg_tokens):
        return "cpg"
    if any(token in normalized_domain for token in ("consumer", "retail", "grocery", "foods")):
        return "cpg"

    return "unknown"


def resolve_account_profile(
    domain: str,
    company_name: str,
    signal_codes: set[str],
    account_profiles: dict[str, AccountProfile],
    icp_reference: dict[str, str],
) -> AccountProfile:
    normalized_domain = normalize_domain(domain)
    existing = account_profiles.get(normalized_domain)
    if existing:
        return existing

    relationship_stage = icp_reference.get(normalized_domain, "unknown")
    vertical_tag = infer_vertical_tag(company_name, normalized_domain, signal_codes)
    return AccountProfile(
        domain=normalized_domain,
        relationship_stage=relationship_stage,
        vertical_tag=vertical_tag,
        is_self=False,
        exclude_from_crm=False,
    )


def domain_family(domain: str) -> str:
    normalized_domain = normalize_domain(domain)
    if not normalized_domain:
        return ""

    try:
        if _OFFLINE_TLD_EXTRACTOR is None:
            raise RuntimeError("tldextract unavailable")
        extracted = _OFFLINE_TLD_EXTRACTOR(normalized_domain)
        if extracted.domain:
            return extracted.domain.lower()
    except Exception:
        logger.warning("tldextract failed for domain=%s", normalized_domain, exc_info=True)

    parts = normalized_domain.split(".")
    if len(parts) >= 2:
        return parts[-2]
    return normalized_domain


def count_cpg_pattern_groups(signal_codes: set[str]) -> int:
    hits = 0
    for _, signals in CPG_PATTERN_GROUPS.items():
        if signal_codes & signals:
            hits += 1
    return hits
