"""Tests for src.integrations.crunchbase — Crunchbase client + firmographic signals."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.integrations.crunchbase import (
    CrunchbaseClient,
    CrunchbaseCompany,
    EmployeeInfo,
    FundingInfo,
    _parse_employee_enum,
    compute_growth_rate,
    enrich_firmographics,
    evaluate_firmographic_signals,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_response(status_code: int = 200, json_data: dict | None = None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    return resp


def _make_company(
    funding_stage: str = "",
    total_funding_usd: int = 0,
    last_funding_date: str = "",
    last_funding_amount_usd: int = 0,
    current_employees: int | None = None,
    growth_rate: float | None = None,
    domain: str = "example.com",
) -> CrunchbaseCompany:
    return CrunchbaseCompany(
        name="Example Inc",
        domain=domain,
        founded_year=2015,
        funding=FundingInfo(
            funding_stage=funding_stage,
            total_funding_usd=total_funding_usd,
            last_funding_date=last_funding_date,
            last_funding_amount_usd=last_funding_amount_usd,
        ),
        employees=EmployeeInfo(
            current_count=current_employees,
            growth_rate=growth_rate,
        ),
    )


# ---------------------------------------------------------------------------
# _parse_employee_enum
# ---------------------------------------------------------------------------


class TestParseEmployeeEnum:
    def test_valid_enum(self):
        assert _parse_employee_enum("c_0101_0250") == 175

    def test_large_range(self):
        assert _parse_employee_enum("c_1001_5000") == 3000

    def test_empty_string(self):
        assert _parse_employee_enum("") is None

    def test_invalid_format(self):
        assert _parse_employee_enum("unknown") is None


# ---------------------------------------------------------------------------
# compute_growth_rate
# ---------------------------------------------------------------------------


class TestComputeGrowthRate:
    def test_positive_growth(self):
        rate = compute_growth_rate(150, 100)
        assert rate == pytest.approx(0.5)

    def test_negative_growth(self):
        rate = compute_growth_rate(80, 100)
        assert rate == pytest.approx(-0.2)

    def test_zero_previous(self):
        assert compute_growth_rate(100, 0) is None

    def test_none_current(self):
        assert compute_growth_rate(None, 100) is None

    def test_none_previous(self):
        assert compute_growth_rate(100, None) is None


# ---------------------------------------------------------------------------
# evaluate_firmographic_signals
# ---------------------------------------------------------------------------


class TestEvaluateFirmographicSignals:
    def test_series_b_plus(self):
        company = _make_company(funding_stage="Series B")
        signals = evaluate_firmographic_signals(company, as_of=date(2026, 1, 15))
        codes = [s["signal_code"] for s in signals]
        assert "funding_stage_series_b_plus" in codes
        assert "funding_stage_series_a" not in codes

    def test_series_a(self):
        company = _make_company(funding_stage="Series A")
        signals = evaluate_firmographic_signals(company, as_of=date(2026, 1, 15))
        codes = [s["signal_code"] for s in signals]
        assert "funding_stage_series_a" in codes
        assert "funding_stage_series_b_plus" not in codes

    def test_ipo_is_b_plus(self):
        company = _make_company(funding_stage="IPO")
        signals = evaluate_firmographic_signals(company, as_of=date(2026, 1, 15))
        codes = [s["signal_code"] for s in signals]
        assert "funding_stage_series_b_plus" in codes

    def test_recent_funding(self):
        company = _make_company(
            last_funding_date="2025-12-01",
            last_funding_amount_usd=5_000_000,
        )
        signals = evaluate_firmographic_signals(company, as_of=date(2026, 1, 15))
        codes = [s["signal_code"] for s in signals]
        assert "recent_funding_event" in codes

    def test_old_funding_not_recent(self):
        company = _make_company(last_funding_date="2024-01-01")
        signals = evaluate_firmographic_signals(company, as_of=date(2026, 1, 15))
        codes = [s["signal_code"] for s in signals]
        assert "recent_funding_event" not in codes

    def test_employee_growth(self):
        company = _make_company(current_employees=500, growth_rate=0.15)
        signals = evaluate_firmographic_signals(company, as_of=date(2026, 1, 15))
        codes = [s["signal_code"] for s in signals]
        assert "employee_growth_positive" in codes

    def test_low_growth_not_flagged(self):
        company = _make_company(current_employees=500, growth_rate=0.05)
        signals = evaluate_firmographic_signals(company, as_of=date(2026, 1, 15))
        codes = [s["signal_code"] for s in signals]
        assert "employee_growth_positive" not in codes

    def test_employee_count_in_range(self):
        company = _make_company(current_employees=500)
        signals = evaluate_firmographic_signals(company, as_of=date(2026, 1, 15))
        codes = [s["signal_code"] for s in signals]
        assert "employee_count_in_range" in codes

    def test_employee_count_too_small(self):
        company = _make_company(current_employees=50)
        signals = evaluate_firmographic_signals(company, as_of=date(2026, 1, 15))
        codes = [s["signal_code"] for s in signals]
        assert "employee_count_in_range" not in codes

    def test_employee_count_too_large(self):
        company = _make_company(current_employees=50_000)
        signals = evaluate_firmographic_signals(company, as_of=date(2026, 1, 15))
        codes = [s["signal_code"] for s in signals]
        assert "employee_count_in_range" not in codes

    def test_all_signals_combined(self):
        company = _make_company(
            funding_stage="Series C",
            last_funding_date="2025-11-01",
            last_funding_amount_usd=20_000_000,
            current_employees=1500,
            growth_rate=0.25,
        )
        signals = evaluate_firmographic_signals(company, as_of=date(2026, 1, 15))
        codes = [s["signal_code"] for s in signals]
        assert "funding_stage_series_b_plus" in codes
        assert "recent_funding_event" in codes
        assert "employee_growth_positive" in codes
        assert "employee_count_in_range" in codes

    def test_signal_dict_structure(self):
        company = _make_company(funding_stage="Series B")
        signals = evaluate_firmographic_signals(company, as_of=date(2026, 1, 15))
        sig = signals[0]
        assert sig["source"] == "crunchbase"
        assert sig["product"] == "shared"
        assert sig["domain"] == "example.com"
        assert 0 < sig["confidence"] <= 1.0

    def test_empty_company_returns_no_signals(self):
        company = _make_company()
        signals = evaluate_firmographic_signals(company, as_of=date(2026, 1, 15))
        assert signals == []


# ---------------------------------------------------------------------------
# CrunchbaseClient — get_company
# ---------------------------------------------------------------------------


class TestCrunchbaseClientGetCompany:
    def test_returns_company_on_success(self):
        client = CrunchbaseClient(api_key="test-key", rate_limit=100)
        api_response = {
            "entities": [
                {
                    "properties": {
                        "identifier": {"value": "Acme Corp"},
                        "founded_on": "2018-03-15",
                        "num_employees_enum": "c_0101_0250",
                        "last_funding_type": "Series B",
                        "funding_total": {"value_usd": 50000000},
                        "last_funding_at": "2025-09-01",
                        "last_funding_total": {"value_usd": 20000000},
                    }
                }
            ]
        }
        with patch("src.integrations.crunchbase.requests.get", return_value=_mock_response(200, api_response)):
            company = client.get_company("acme.com")

        assert company is not None
        assert company.name == "Acme Corp"
        assert company.founded_year == 2018
        assert company.funding.funding_stage == "Series B"
        assert company.funding.total_funding_usd == 50000000
        assert company.funding.last_funding_date == "2025-09-01"
        assert company.employees.current_count == 175

    def test_returns_none_on_empty_result(self):
        client = CrunchbaseClient(api_key="test-key", rate_limit=100)
        with patch("src.integrations.crunchbase.requests.get", return_value=_mock_response(200, {"entities": []})):
            company = client.get_company("unknown.com")

        assert company is None

    def test_returns_none_on_api_error(self):
        client = CrunchbaseClient(api_key="test-key", rate_limit=100)
        with patch("src.integrations.crunchbase.requests.get", return_value=_mock_response(429)):
            company = client.get_company("example.com")

        assert company is None

    def test_returns_none_when_no_api_key(self):
        client = CrunchbaseClient(api_key="", rate_limit=100)
        assert client.get_company("example.com") is None

    def test_returns_none_when_empty_domain(self):
        client = CrunchbaseClient(api_key="test-key", rate_limit=100)
        assert client.get_company("") is None

    def test_returns_none_on_exception(self):
        client = CrunchbaseClient(api_key="test-key", rate_limit=100)
        with patch("src.integrations.crunchbase.requests.get", side_effect=ConnectionError("timeout")):
            company = client.get_company("example.com")

        assert company is None


# ---------------------------------------------------------------------------
# enrich_firmographics
# ---------------------------------------------------------------------------


class TestEnrichFirmographics:
    def test_returns_company_on_success(self):
        client = CrunchbaseClient(api_key="test-key", rate_limit=100)
        api_response = {
            "entities": [
                {
                    "properties": {
                        "identifier": {"value": "Test Co"},
                        "founded_on": "2020-01-01",
                        "num_employees_enum": "c_0051_0100",
                        "last_funding_type": "Seed",
                        "funding_total": {"value_usd": 2000000},
                        "last_funding_at": "2025-06-01",
                        "last_funding_total": {"value_usd": 2000000},
                    }
                }
            ]
        }
        with patch("src.integrations.crunchbase.requests.get", return_value=_mock_response(200, api_response)):
            company = enrich_firmographics("test.com", client)

        assert company is not None
        assert company.name == "Test Co"

    def test_returns_none_without_client(self):
        assert enrich_firmographics("example.com", None) is None
