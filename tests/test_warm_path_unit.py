"""Pure unit tests for warm_path.py helper functions.

These tests do NOT require a database connection — they exercise the
stateless helper functions directly.  They cover branches missed by the
integration tests in test_contacts_api.py.
"""

from __future__ import annotations

import pytest

from src.warm_path import (
    _extract_domain_keyword,
    _fuzzy_match_linkedin,
    _fuzzy_match_name,
    _normalize_linkedin_url,
)

# ── _normalize_linkedin_url ──────────────────────────────────────────────────


class TestNormalizeLinkedinUrl:
    def test_empty_string_returns_empty(self):
        """Empty / None input → empty string (covers early-return branch)."""
        assert _normalize_linkedin_url("") == ""
        assert _normalize_linkedin_url(None) == ""  # type: ignore[arg-type]

    def test_strips_protocol(self):
        assert _normalize_linkedin_url("https://linkedin.com/in/john") == "linkedin.com/in/john"
        assert _normalize_linkedin_url("http://linkedin.com/in/john") == "linkedin.com/in/john"

    def test_strips_query_params(self):
        """URL with '?' → query string removed (covers line 191 branch)."""
        assert _normalize_linkedin_url("linkedin.com/in/john?utm=123") == "linkedin.com/in/john"
        assert _normalize_linkedin_url("https://linkedin.com/in/john?a=1&b=2") == "linkedin.com/in/john"

    def test_strips_fragment(self):
        """URL with '#' → fragment removed (covers line 193 branch)."""
        assert _normalize_linkedin_url("linkedin.com/in/john#about") == "linkedin.com/in/john"

    def test_strips_trailing_slash(self):
        assert _normalize_linkedin_url("linkedin.com/in/john/") == "linkedin.com/in/john"
        assert _normalize_linkedin_url("linkedin.com/in/john///") == "linkedin.com/in/john"

    def test_lowercases(self):
        assert _normalize_linkedin_url("LinkedIn.com/in/JohnSmith") == "linkedin.com/in/johnsmith"

    def test_complex_url_all_components(self):
        """All normalizations applied at once."""
        url = "HTTPS://LinkedIn.com/in/John-Smith/?utm=abc#about"
        assert _normalize_linkedin_url(url) == "linkedin.com/in/john-smith"


# ── _fuzzy_match_linkedin ────────────────────────────────────────────────────


class TestFuzzyMatchLinkedin:
    def test_both_empty_returns_false(self):
        """Both empty → (False, 0.0) — covers the empty-guard branch."""
        is_match, score = _fuzzy_match_linkedin("", "")
        assert is_match is False
        assert score == 0.0

    def test_first_empty_returns_false(self):
        """First URL empty → (False, 0.0)."""
        is_match, score = _fuzzy_match_linkedin("", "linkedin.com/in/john")
        assert is_match is False
        assert score == 0.0

    def test_second_empty_returns_false(self):
        """Second URL empty → (False, 0.0)."""
        is_match, score = _fuzzy_match_linkedin("linkedin.com/in/john", "")
        assert is_match is False
        assert score == 0.0

    def test_exact_match_100(self):
        """Identical normalized URLs → (True, 100.0)."""
        url = "https://linkedin.com/in/johnsmith"
        is_match, score = _fuzzy_match_linkedin(url, url)
        assert is_match is True
        assert score == pytest.approx(100.0)

    def test_near_exact_with_query_params(self):
        """Same profile URL, one with utm params → exact match after normalization."""
        url1 = "https://linkedin.com/in/johnsmith"
        url2 = "https://linkedin.com/in/johnsmith?utm=xyz"
        is_match, score = _fuzzy_match_linkedin(url1, url2)
        assert is_match is True
        assert score == pytest.approx(100.0)

    def test_abhijeet_false_positive_rejected(self):
        """The original false positive: mehrotra vs malhotra must be rejected at 95% threshold."""
        url1 = "https://linkedin.com/in/abhijeetmehrotra"
        url2 = "https://linkedin.com/in/abhijeetmalhotra"
        is_match, score = _fuzzy_match_linkedin(url1, url2, threshold=95.0)
        # fuzz.ratio scores ~93.8% — below 95% threshold
        assert is_match is False
        assert score < 95.0

    def test_low_similarity_rejected(self):
        """Completely different URLs are rejected."""
        is_match, score = _fuzzy_match_linkedin(
            "linkedin.com/in/alice-smith",
            "linkedin.com/in/bob-jones",
        )
        assert is_match is False


# ── _fuzzy_match_name ────────────────────────────────────────────────────────


class TestFuzzyMatchName:
    def test_both_empty_returns_false(self):
        """Both empty names → (False, 0.0) — covers empty-guard branches."""
        is_match, score = _fuzzy_match_name("", "")
        assert is_match is False
        assert score == 0.0

    def test_first_empty_returns_false(self):
        is_match, score = _fuzzy_match_name("", "John Smith")
        assert is_match is False
        assert score == 0.0

    def test_second_empty_returns_false(self):
        is_match, score = _fuzzy_match_name("John Smith", "")
        assert is_match is False
        assert score == 0.0

    def test_exact_match_100(self):
        """Identical names → (True, 100.0)."""
        is_match, score = _fuzzy_match_name("John Smith", "John Smith")
        assert is_match is True
        assert score == pytest.approx(100.0)

    def test_case_insensitive_exact(self):
        is_match, score = _fuzzy_match_name("JOHN SMITH", "john smith")
        assert is_match is True
        assert score == pytest.approx(100.0)

    def test_single_word_name_strict_match(self):
        """Single-word names use token_set_ratio with threshold+10 (stricter)."""
        # "Alice" vs "Alice" — should match
        is_match, score = _fuzzy_match_name("Alice", "Alice")
        assert is_match is True

    def test_single_word_name_no_match(self):
        """Single-word names that are different should not match."""
        is_match, score = _fuzzy_match_name("Alice", "Bob")
        assert is_match is False

    def test_both_names_must_match_accepts_valid(self):
        """First AND last name both pass 85% threshold → accepted."""
        # "John Smith" vs "Jon Smith": first="john" vs "jon" ~85.7%, last="smith" vs "smith" 100%
        is_match, score = _fuzzy_match_name("John Smith", "Jon Smith")
        assert is_match is True

    def test_abhijeet_false_positive_rejected(self):
        """The original false positive: Mehrotra vs Malhotra rejected because last name <85%."""
        is_match, score = _fuzzy_match_name("Abhijeet Mehrotra", "Abhijeet Malhotra")
        # "mehrotra" vs "malhotra" scores ~75% — below 85% → rejected
        assert is_match is False

    def test_only_first_name_matches_rejected(self):
        """First name matches but last name doesn't → rejected."""
        is_match, score = _fuzzy_match_name("Robert Wilson", "Robert Thompson")
        # "wilson" vs "thompson" — very low score
        assert is_match is False

    def test_only_last_name_matches_rejected(self):
        """Last name matches but first name doesn't → rejected."""
        is_match, score = _fuzzy_match_name("Alice Smith", "Bob Smith")
        # "alice" vs "bob" — very low score
        assert is_match is False

    def test_middle_name_ignored(self):
        """Three-part name: only first and last token compared."""
        # "John Robert Smith" → first="john", last="smith"
        # "John Smith" → first="john", last="smith"
        is_match, score = _fuzzy_match_name("John Robert Smith", "John Smith")
        assert is_match is True


# ── _extract_domain_keyword ──────────────────────────────────────────────────


class TestExtractDomainKeyword:
    def test_standard_domain(self):
        assert _extract_domain_keyword("infosys.com") == "infosys"
        assert _extract_domain_keyword("tatadigital.com") == "tatadigital"

    def test_short_domain_skipped(self):
        """Domains with ≤3 char significant part are skipped."""
        assert _extract_domain_keyword("hul.co.in") == ""
        assert _extract_domain_keyword("ibm.com") == ""

    def test_www_stripped(self):
        assert _extract_domain_keyword("www.google.com") == "google"

    def test_empty_domain(self):
        assert _extract_domain_keyword("") == ""
        assert _extract_domain_keyword(None) == ""  # type: ignore[arg-type]
