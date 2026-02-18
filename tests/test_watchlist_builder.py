from __future__ import annotations

from src.discovery.watchlist_builder import (
    CountrySpec,
    _company_matches,
    _extract_registered_domain,
    _industry_matches,
    _rank_candidate,
)


def test_extract_registered_domain_filters_non_company_hosts():
    assert _extract_registered_domain("https://www.colgatepalmolive.com/en-us") == "colgatepalmolive.com"
    assert _extract_registered_domain("https://www.linkedin.com/company/example") == ""
    assert _extract_registered_domain("https://freshmart.example/news") == ""


def test_industry_filter_prefers_cpg_labels():
    assert _industry_matches("fast-moving consumer goods") is True
    assert _industry_matches("personal care product") is True
    assert _industry_matches("food service") is False
    assert _industry_matches("restaurant chain") is False


def test_company_filter_excludes_restaurant_brands():
    assert _company_matches("PepsiCo") is True
    assert _company_matches("McDonald's") is False
    assert _company_matches("CloudKitchens") is False


def test_rank_candidate_boosts_core_regions_and_scale():
    us = CountrySpec(name="United States", qid="Q30", region_group="us", priority=1.35)
    europe = CountrySpec(name="France", qid="Q142", region_group="europe", priority=1.05)

    us_score = _rank_candidate(
        country=us,
        industry_label="fast-moving consumer goods",
        sitelinks=80,
        revenue_usd=1_000_000_000,
        employees=50_000,
    )
    eu_score = _rank_candidate(
        country=europe,
        industry_label="fast-moving consumer goods",
        sitelinks=80,
        revenue_usd=1_000_000_000,
        employees=50_000,
    )

    assert us_score > eu_score
