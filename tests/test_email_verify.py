"""Tests for email verification module (Issue #35)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.integrations.email_verify import (
    EmailVerifier,
    VerifyStatus,
    _decide,
    filter_verified_contacts,
)

# ---------------------------------------------------------------------------
# _decide() — verification rules
# ---------------------------------------------------------------------------


class TestDecideRules:
    """Verify that _decide maps statuses to the correct accept/reject rules."""

    def test_valid_accepted_and_verified(self):
        r = _decide(VerifyStatus.valid, "a@b.com", "neverbounce", "valid")
        assert r.email_verified is True
        assert r.should_store is True
        assert r.flag_for_review is False

    def test_catch_all_accepted_but_not_verified(self):
        r = _decide(VerifyStatus.catch_all, "a@b.com", "neverbounce", "catchall")
        assert r.email_verified is False
        assert r.should_store is True
        assert r.flag_for_review is True

    def test_invalid_rejected(self):
        r = _decide(VerifyStatus.invalid, "bad@x.com", "zerobounce", "invalid")
        assert r.email_verified is False
        assert r.should_store is False
        assert r.flag_for_review is False

    def test_disposable_rejected(self):
        r = _decide(VerifyStatus.disposable, "d@temp.com", "neverbounce", "disposable")
        assert r.should_store is False

    def test_unknown_flagged_for_review(self):
        r = _decide(VerifyStatus.unknown, "u@x.com", "zerobounce", "unknown")
        assert r.should_store is True
        assert r.flag_for_review is True
        assert r.email_verified is False


# ---------------------------------------------------------------------------
# NeverBounce single verify (mocked)
# ---------------------------------------------------------------------------


class TestNeverBounceSingle:
    """Test NeverBounce single-email verification with mocked HTTP."""

    def _make_settings(self):
        s = MagicMock()
        s.email_verify_provider = "neverbounce"
        s.neverbounce_api_key = "nb-test-key"
        s.zerobounce_api_key = ""
        return s

    @patch("src.integrations.email_verify.requests.get")
    def test_valid_email(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"result": "valid"},
        )
        mock_get.return_value.raise_for_status = MagicMock()
        verifier = EmailVerifier(self._make_settings())
        result = verifier.verify("good@example.com")
        assert result.status == VerifyStatus.valid
        assert result.email_verified is True
        assert result.provider == "neverbounce"

    @patch("src.integrations.email_verify.requests.get")
    def test_invalid_email(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"result": "invalid"},
        )
        mock_get.return_value.raise_for_status = MagicMock()
        verifier = EmailVerifier(self._make_settings())
        result = verifier.verify("bad@example.com")
        assert result.status == VerifyStatus.invalid
        assert result.should_store is False

    @patch("src.integrations.email_verify.requests.get")
    def test_catchall_email(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"result": "catchall"},
        )
        mock_get.return_value.raise_for_status = MagicMock()
        verifier = EmailVerifier(self._make_settings())
        result = verifier.verify("info@catchall.com")
        assert result.status == VerifyStatus.catch_all
        assert result.flag_for_review is True

    @patch("src.integrations.email_verify.requests.get")
    def test_disposable_email(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"result": "disposable"},
        )
        mock_get.return_value.raise_for_status = MagicMock()
        verifier = EmailVerifier(self._make_settings())
        result = verifier.verify("temp@guerrilla.com")
        assert result.status == VerifyStatus.disposable
        assert result.should_store is False

    @patch("src.integrations.email_verify.requests.get")
    def test_api_error_returns_unknown(self, mock_get):
        mock_get.side_effect = ConnectionError("timeout")
        verifier = EmailVerifier(self._make_settings())
        result = verifier.verify("err@example.com")
        assert result.status == VerifyStatus.unknown
        assert result.raw_status == "error"


# ---------------------------------------------------------------------------
# ZeroBounce single verify (mocked)
# ---------------------------------------------------------------------------


class TestZeroBounceSingle:
    """Test ZeroBounce single-email verification with mocked HTTP."""

    def _make_settings(self):
        s = MagicMock()
        s.email_verify_provider = "zerobounce"
        s.neverbounce_api_key = ""
        s.zerobounce_api_key = "zb-test-key"
        return s

    @patch("src.integrations.email_verify.requests.get")
    def test_valid_email(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"status": "valid", "sub_status": ""},
        )
        mock_get.return_value.raise_for_status = MagicMock()
        verifier = EmailVerifier(self._make_settings())
        result = verifier.verify("good@example.com")
        assert result.status == VerifyStatus.valid
        assert result.provider == "zerobounce"

    @patch("src.integrations.email_verify.requests.get")
    def test_disposable_via_sub_status(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"status": "do_not_mail", "sub_status": "disposable"},
        )
        mock_get.return_value.raise_for_status = MagicMock()
        verifier = EmailVerifier(self._make_settings())
        result = verifier.verify("trash@temp.com")
        assert result.status == VerifyStatus.disposable
        assert result.should_store is False

    @patch("src.integrations.email_verify.requests.get")
    def test_catch_all(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"status": "catch-all", "sub_status": ""},
        )
        mock_get.return_value.raise_for_status = MagicMock()
        verifier = EmailVerifier(self._make_settings())
        result = verifier.verify("info@co.com")
        assert result.status == VerifyStatus.catch_all
        assert result.flag_for_review is True

    @patch("src.integrations.email_verify.requests.get")
    def test_spamtrap_mapped_to_invalid(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"status": "spamtrap", "sub_status": ""},
        )
        mock_get.return_value.raise_for_status = MagicMock()
        verifier = EmailVerifier(self._make_settings())
        result = verifier.verify("trap@co.com")
        assert result.status == VerifyStatus.invalid
        assert result.should_store is False


# ---------------------------------------------------------------------------
# No API key → graceful fallback
# ---------------------------------------------------------------------------


class TestNoApiKey:
    def test_no_key_returns_unknown(self):
        s = MagicMock()
        s.email_verify_provider = "neverbounce"
        s.neverbounce_api_key = ""
        s.zerobounce_api_key = ""
        verifier = EmailVerifier(s)
        assert verifier.is_configured is False
        result = verifier.verify("a@b.com")
        assert result.status == VerifyStatus.unknown
        assert result.raw_status == "no_api_key"

    def test_batch_no_key_returns_unknowns(self):
        s = MagicMock()
        s.email_verify_provider = "zerobounce"
        s.neverbounce_api_key = ""
        s.zerobounce_api_key = ""
        verifier = EmailVerifier(s)
        results = verifier.verify_batch(["a@b.com", "c@d.com"])
        assert len(results) == 2
        assert all(r.status == VerifyStatus.unknown for r in results)


# ---------------------------------------------------------------------------
# verify_with_retry
# ---------------------------------------------------------------------------


class TestVerifyWithRetry:
    @patch("src.integrations.email_verify.requests.get")
    def test_retry_on_unknown(self, mock_get):
        """First call returns unknown, retry returns valid."""
        responses = [
            MagicMock(json=lambda: {"result": "unknown"}),
            MagicMock(json=lambda: {"result": "valid"}),
        ]
        for r in responses:
            r.raise_for_status = MagicMock()
        mock_get.side_effect = responses

        s = MagicMock()
        s.email_verify_provider = "neverbounce"
        s.neverbounce_api_key = "key"
        s.zerobounce_api_key = ""
        verifier = EmailVerifier(s)
        result = verifier.verify_with_retry("maybe@example.com")
        assert result.status == VerifyStatus.valid
        assert mock_get.call_count == 2


# ---------------------------------------------------------------------------
# filter_verified_contacts
# ---------------------------------------------------------------------------


class TestFilterVerifiedContacts:
    def test_no_verifier_returns_unchanged(self):
        contacts = [{"first_name": "A", "email": "a@b.com"}]
        out = filter_verified_contacts(contacts, verifier=None)
        assert out == contacts

    def test_unconfigured_verifier_returns_unchanged(self):
        s = MagicMock()
        s.email_verify_provider = "neverbounce"
        s.neverbounce_api_key = ""
        s.zerobounce_api_key = ""
        verifier = EmailVerifier(s)
        contacts = [{"first_name": "A", "email": "a@b.com"}]
        out = filter_verified_contacts(contacts, verifier=verifier)
        assert out == contacts

    @patch("src.integrations.email_verify.requests.get")
    def test_invalid_email_cleared(self, mock_get):
        """Invalid emails should be set to empty string."""
        mock_get.return_value = MagicMock(
            json=lambda: {"result": "invalid"},
        )
        mock_get.return_value.raise_for_status = MagicMock()

        s = MagicMock()
        s.email_verify_provider = "neverbounce"
        s.neverbounce_api_key = "key"
        s.zerobounce_api_key = ""
        verifier = EmailVerifier(s)

        contacts = [
            {"first_name": "Bad", "email": "bad@x.com"},
        ]
        # Batch verify falls back to single when batch fails — mock single
        out = filter_verified_contacts(contacts, verifier=verifier)
        assert out[0]["email"] == ""
        assert out[0]["email_verified"] is False
        assert out[0]["verification_status"] == "invalid"

    @patch("src.integrations.email_verify.requests.get")
    def test_valid_email_marked_verified(self, mock_get):
        mock_get.return_value = MagicMock(
            json=lambda: {"result": "valid"},
        )
        mock_get.return_value.raise_for_status = MagicMock()

        s = MagicMock()
        s.email_verify_provider = "neverbounce"
        s.neverbounce_api_key = "key"
        s.zerobounce_api_key = ""
        verifier = EmailVerifier(s)

        contacts = [{"first_name": "Good", "email": "good@co.com"}]
        out = filter_verified_contacts(contacts, verifier=verifier)
        assert out[0]["email"] == "good@co.com"
        assert out[0]["email_verified"] is True
        assert out[0]["verification_status"] == "valid"

    def test_contacts_without_email_pass_through(self):
        s = MagicMock()
        s.email_verify_provider = "neverbounce"
        s.neverbounce_api_key = "key"
        s.zerobounce_api_key = ""
        verifier = EmailVerifier(s)

        contacts = [
            {"first_name": "NoEmail", "email": ""},
            {"first_name": "AlsoNo"},
        ]
        out = filter_verified_contacts(contacts, verifier=verifier)
        assert len(out) == 2
        # No verification_status added for contacts without emails
        assert "verification_status" not in out[0]
        assert "verification_status" not in out[1]


# ---------------------------------------------------------------------------
# Batch verify fallback
# ---------------------------------------------------------------------------


class TestBatchFallback:
    @patch("src.integrations.email_verify.requests.post")
    @patch("src.integrations.email_verify.requests.get")
    def test_batch_neverbounce_fallback_to_single(self, mock_get, mock_post):
        """If batch job creation fails, falls back to single verification."""
        mock_post.side_effect = ConnectionError("batch api down")
        # Single verify returns valid
        mock_get.return_value = MagicMock(
            json=lambda: {"result": "valid"},
        )
        mock_get.return_value.raise_for_status = MagicMock()

        s = MagicMock()
        s.email_verify_provider = "neverbounce"
        s.neverbounce_api_key = "key"
        s.zerobounce_api_key = ""
        verifier = EmailVerifier(s)
        results = verifier.verify_batch(["a@b.com", "c@d.com"])
        assert len(results) == 2
        assert all(r.status == VerifyStatus.valid for r in results)

    def test_batch_empty_list(self):
        s = MagicMock()
        s.email_verify_provider = "neverbounce"
        s.neverbounce_api_key = "key"
        s.zerobounce_api_key = ""
        verifier = EmailVerifier(s)
        results = verifier.verify_batch([])
        assert results == []
