"""Crunchbase API client for firmographic enrichment."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from threading import Lock

import requests

logger = logging.getLogger(__name__)

_API_BASE = "https://api.crunchbase.com/api/v4"
_TIMEOUT_SECONDS = 15

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class FundingInfo:
    funding_stage: str = ""
    total_funding_usd: int = 0
    last_funding_date: str = ""
    last_funding_amount_usd: int = 0


@dataclass
class EmployeeInfo:
    current_count: int | None = None
    count_6mo_ago: int | None = None
    growth_rate: float | None = None


@dataclass
class CrunchbaseCompany:
    name: str = ""
    domain: str = ""
    founded_year: int | None = None
    funding: FundingInfo = field(default_factory=FundingInfo)
    employees: EmployeeInfo = field(default_factory=EmployeeInfo)
    enrichment_source: str = "crunchbase"


# ---------------------------------------------------------------------------
# Firmographic signal evaluation
# ---------------------------------------------------------------------------

FIRMOGRAPHIC_SIGNALS = {
    "funding_stage_series_b_plus": {
        "product_scope": "shared",
        "category": "firmographic",
        "base_weight": 15,
        "half_life_days": 90,
        "min_confidence": 0.7,
    },
    "funding_stage_series_a": {
        "product_scope": "shared",
        "category": "firmographic",
        "base_weight": 8,
        "half_life_days": 90,
        "min_confidence": 0.7,
    },
    "recent_funding_event": {
        "product_scope": "shared",
        "category": "firmographic",
        "base_weight": 12,
        "half_life_days": 60,
        "min_confidence": 0.65,
    },
    "employee_growth_positive": {
        "product_scope": "shared",
        "category": "firmographic",
        "base_weight": 10,
        "half_life_days": 45,
        "min_confidence": 0.6,
    },
    "employee_count_in_range": {
        "product_scope": "shared",
        "category": "firmographic",
        "base_weight": 8,
        "half_life_days": 90,
        "min_confidence": 0.7,
    },
}

_SERIES_B_PLUS = {"series_b", "series_c", "series_d", "series_e", "series_f", "ipo", "public"}
_SERIES_A = {"series_a"}
_EMPLOYEE_MIN = 100
_EMPLOYEE_MAX = 10_000
_GROWTH_THRESHOLD = 0.10  # 10% YoY
_RECENT_FUNDING_DAYS = 180


def evaluate_firmographic_signals(
    company: CrunchbaseCompany,
    as_of: date | None = None,
) -> list[dict]:
    """Evaluate firmographic signals from Crunchbase data.

    Returns a list of signal dicts compatible with the observation pipeline.
    """
    today = as_of or date.today()
    signals: list[dict] = []
    stage = company.funding.funding_stage.lower().replace(" ", "_")

    # Funding stage: Series B+
    if stage in _SERIES_B_PLUS:
        signals.append(
            _build_signal(
                signal_code="funding_stage_series_b_plus",
                domain=company.domain,
                confidence=0.85,
                evidence=f"Funding stage: {company.funding.funding_stage}",
                observed_at=today.isoformat(),
            )
        )

    # Funding stage: Series A
    if stage in _SERIES_A:
        signals.append(
            _build_signal(
                signal_code="funding_stage_series_a",
                domain=company.domain,
                confidence=0.85,
                evidence=f"Funding stage: {company.funding.funding_stage}",
                observed_at=today.isoformat(),
            )
        )

    # Recent funding event (within last 6 months)
    if company.funding.last_funding_date:
        try:
            funding_date = date.fromisoformat(company.funding.last_funding_date[:10])
            if (today - funding_date) <= timedelta(days=_RECENT_FUNDING_DAYS):
                amount_str = ""
                if company.funding.last_funding_amount_usd:
                    amount_str = f" (${company.funding.last_funding_amount_usd:,})"
                signals.append(
                    _build_signal(
                        signal_code="recent_funding_event",
                        domain=company.domain,
                        confidence=0.80,
                        evidence=f"Funding on {company.funding.last_funding_date}{amount_str}",
                        observed_at=company.funding.last_funding_date,
                    )
                )
        except (ValueError, TypeError):
            pass

    # Employee growth > 10% YoY
    if company.employees.growth_rate is not None and company.employees.growth_rate > _GROWTH_THRESHOLD:
        pct = round(company.employees.growth_rate * 100, 1)
        signals.append(
            _build_signal(
                signal_code="employee_growth_positive",
                domain=company.domain,
                confidence=0.70,
                evidence=f"Employee growth: {pct}%",
                observed_at=today.isoformat(),
            )
        )

    # Employee count in target range (100-10K)
    count = company.employees.current_count
    if count is not None and _EMPLOYEE_MIN <= count <= _EMPLOYEE_MAX:
        signals.append(
            _build_signal(
                signal_code="employee_count_in_range",
                domain=company.domain,
                confidence=0.80,
                evidence=f"Employee count: {count:,}",
                observed_at=today.isoformat(),
            )
        )

    return signals


def _build_signal(
    signal_code: str,
    domain: str,
    confidence: float,
    evidence: str,
    observed_at: str,
) -> dict:
    return {
        "signal_code": signal_code,
        "domain": domain,
        "source": "crunchbase",
        "confidence": confidence,
        "evidence_text": evidence,
        "observed_at": observed_at,
        "product": "shared",
    }


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------


class CrunchbaseClient:
    """Crunchbase API client with rate limiting."""

    def __init__(self, api_key: str, rate_limit: int = 50):
        self._api_key = api_key
        self._rate_limit = max(1, rate_limit)
        self._lock = Lock()
        self._request_times: list[float] = []

    def _wait_for_rate_limit(self) -> None:
        window = 60.0
        with self._lock:
            now = time.monotonic()
            cutoff = now - window
            self._request_times = [t for t in self._request_times if t > cutoff]
            if len(self._request_times) >= self._rate_limit:
                earliest = self._request_times[0]
                sleep_time = window - (now - earliest) + 0.1
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.monotonic()
                    self._request_times = [t for t in self._request_times if t > now - window]
            self._request_times.append(time.monotonic())

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        self._wait_for_rate_limit()
        headers = {"X-cb-user-key": self._api_key}
        all_params = params or {}
        resp = requests.get(
            f"{_API_BASE}{endpoint}",
            params=all_params,
            headers=headers,
            timeout=_TIMEOUT_SECONDS,
        )
        if resp.status_code != 200:
            logger.warning("crunchbase api error endpoint=%s status=%d", endpoint, resp.status_code)
            return {}
        return resp.json()

    def get_company(self, domain: str) -> CrunchbaseCompany | None:
        """Look up a company by domain and return firmographic data."""
        if not self._api_key or not domain:
            return None

        try:
            data = self._get(
                "/entities/organizations",
                params={
                    "field_ids": "short_description,founded_on,num_employees_enum,"
                    "funding_total,last_funding_type,last_funding_at,"
                    "last_funding_total,identifier",
                    "query": domain,
                    "limit": 1,
                },
            )
            if not data:
                return None

            entities = data.get("entities") or []
            if not entities:
                return None

            props = entities[0].get("properties") or {}
            identifier = props.get("identifier") or {}

            # Parse employee count from enum like "c_0101_0250"
            employee_count = _parse_employee_enum(props.get("num_employees_enum") or "")

            # Parse founded year
            founded_year = None
            founded_on = props.get("founded_on") or ""
            if founded_on and len(founded_on) >= 4:
                try:
                    founded_year = int(founded_on[:4])
                except (ValueError, TypeError):
                    pass

            funding = FundingInfo(
                funding_stage=(props.get("last_funding_type") or "").strip(),
                total_funding_usd=int(props.get("funding_total", {}).get("value_usd") or 0),
                last_funding_date=(props.get("last_funding_at") or "").strip(),
                last_funding_amount_usd=int(props.get("last_funding_total", {}).get("value_usd") or 0),
            )

            employees = EmployeeInfo(current_count=employee_count)

            return CrunchbaseCompany(
                name=(identifier.get("value") or "").strip(),
                domain=domain,
                founded_year=founded_year,
                funding=funding,
                employees=employees,
                enrichment_source="crunchbase",
            )
        except Exception:
            logger.warning("crunchbase get_company failed domain=%s", domain, exc_info=True)
            return None

    def get_employee_history(self, entity_id: str) -> list[dict]:
        """Get employee count history for growth rate calculation."""
        if not self._api_key or not entity_id:
            return []

        try:
            data = self._get(
                f"/entities/organizations/{entity_id}/cards/employees",
            )
            if not data:
                return []
            return data.get("cards", {}).get("employees") or []
        except Exception:
            logger.warning("crunchbase employee_history failed id=%s", entity_id, exc_info=True)
            return []


def _parse_employee_enum(enum_value: str) -> int | None:
    """Parse Crunchbase employee enum like 'c_0101_0250' into midpoint estimate."""
    if not enum_value:
        return None
    parts = enum_value.replace("c_", "").split("_")
    if len(parts) != 2:
        return None
    try:
        low = int(parts[0])
        high = int(parts[1])
        return (low + high) // 2
    except (ValueError, TypeError):
        return None


def compute_growth_rate(
    current_count: int | None,
    previous_count: int | None,
) -> float | None:
    """Compute employee growth rate. Returns None if data is insufficient."""
    if current_count is None or previous_count is None or previous_count <= 0:
        return None
    return (current_count - previous_count) / previous_count


def enrich_firmographics(
    domain: str,
    client: CrunchbaseClient | None,
) -> CrunchbaseCompany | None:
    """High-level function to fetch firmographic data for a domain."""
    if client is None:
        return None

    company = client.get_company(domain)
    if company is None:
        return None

    logger.info(
        "crunchbase_enrich domain=%s stage=%s employees=%s",
        domain,
        company.funding.funding_stage,
        company.employees.current_count,
    )
    return company
