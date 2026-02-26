"""
Email verification via NeverBounce or ZeroBounce.

Verifies email deliverability before CRM push.  Provider is selected by the
``SIGNALS_EMAIL_VERIFY_PROVIDER`` env-var (default: ``neverbounce``).

Verification statuses:
    valid       -> accept, email_verified=True
    catch_all   -> accept with warning, email_verified=False, flagged for review
    invalid     -> reject, do not store
    disposable  -> reject
    unknown     -> retry once, then flag for manual review
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Sequence

import requests

logger = logging.getLogger(__name__)

_TIMEOUT = 15  # seconds for HTTP calls


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


class VerifyStatus(str, Enum):
    valid = "valid"
    invalid = "invalid"
    catch_all = "catch_all"
    unknown = "unknown"
    disposable = "disposable"


@dataclass
class VerifyResult:
    email: str
    status: VerifyStatus
    email_verified: bool
    should_store: bool
    flag_for_review: bool
    provider: str
    raw_status: str = ""


def _decide(status: VerifyStatus, email: str, provider: str, raw: str) -> VerifyResult:
    """Apply the project's verification rules to a status code."""
    if status == VerifyStatus.valid:
        return VerifyResult(
            email=email,
            status=status,
            email_verified=True,
            should_store=True,
            flag_for_review=False,
            provider=provider,
            raw_status=raw,
        )
    if status == VerifyStatus.catch_all:
        return VerifyResult(
            email=email,
            status=status,
            email_verified=False,
            should_store=True,
            flag_for_review=True,
            provider=provider,
            raw_status=raw,
        )
    if status in (VerifyStatus.invalid, VerifyStatus.disposable):
        return VerifyResult(
            email=email,
            status=status,
            email_verified=False,
            should_store=False,
            flag_for_review=False,
            provider=provider,
            raw_status=raw,
        )
    # unknown
    return VerifyResult(
        email=email,
        status=status,
        email_verified=False,
        should_store=True,
        flag_for_review=True,
        provider=provider,
        raw_status=raw,
    )


# ---------------------------------------------------------------------------
# NeverBounce
# ---------------------------------------------------------------------------

_NB_STATUS_MAP: dict[str, VerifyStatus] = {
    "valid": VerifyStatus.valid,
    "invalid": VerifyStatus.invalid,
    "disposable": VerifyStatus.disposable,
    "catchall": VerifyStatus.catch_all,
    "unknown": VerifyStatus.unknown,
}


def _verify_neverbounce(email: str, api_key: str) -> VerifyResult:
    """Single-email verification via NeverBounce."""
    resp = requests.get(
        "https://api.neverbounce.com/v4/single/check",
        params={"key": api_key, "email": email},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    raw = str(data.get("result", "unknown"))
    status = _NB_STATUS_MAP.get(raw, VerifyStatus.unknown)
    return _decide(status, email, "neverbounce", raw)


def _verify_batch_neverbounce(
    emails: list[str],
    api_key: str,
) -> list[VerifyResult]:
    """Batch verification via NeverBounce jobs API.

    Flow:
      1. POST /v4/jobs/create  – submit the batch
      2. GET  /v4/jobs/status   – poll until complete
      3. GET  /v4/jobs/results  – fetch results
    """
    # --- create job ---
    create_resp = requests.post(
        "https://api.neverbounce.com/v4/jobs/create",
        json={
            "key": api_key,
            "input": [[e] for e in emails],
            "input_location": "supplied",
            "auto_start": True,
        },
        timeout=_TIMEOUT,
    )
    create_resp.raise_for_status()
    job_id = create_resp.json().get("job_id")
    if not job_id:
        raise RuntimeError("NeverBounce did not return a job_id")

    # --- poll for completion (simple blocking poll) ---
    import time

    for _ in range(60):
        status_resp = requests.get(
            "https://api.neverbounce.com/v4/jobs/status",
            params={"key": api_key, "job_id": job_id},
            timeout=_TIMEOUT,
        )
        status_resp.raise_for_status()
        job_status = status_resp.json().get("job_status")
        if job_status == "complete":
            break
        time.sleep(2)
    else:
        raise RuntimeError(f"NeverBounce job {job_id} did not complete in time")

    # --- fetch results ---
    results_resp = requests.get(
        "https://api.neverbounce.com/v4/jobs/results",
        params={"key": api_key, "job_id": job_id},
        timeout=_TIMEOUT,
    )
    results_resp.raise_for_status()
    results_data = results_resp.json().get("results", [])

    out: list[VerifyResult] = []
    for item in results_data:
        addr = item.get("data", {}).get("email", "") or item.get("email", "")
        raw = str(item.get("verification", {}).get("result", "unknown"))
        status = _NB_STATUS_MAP.get(raw, VerifyStatus.unknown)
        out.append(_decide(status, addr, "neverbounce", raw))
    return out


# ---------------------------------------------------------------------------
# ZeroBounce
# ---------------------------------------------------------------------------

_ZB_STATUS_MAP: dict[str, VerifyStatus] = {
    "valid": VerifyStatus.valid,
    "invalid": VerifyStatus.invalid,
    "catch-all": VerifyStatus.catch_all,
    "abuse": VerifyStatus.invalid,
    "do_not_mail": VerifyStatus.invalid,
    "spamtrap": VerifyStatus.invalid,
    "unknown": VerifyStatus.unknown,
}

_ZB_SUB_STATUS_DISPOSABLE = {"disposable"}


def _verify_zerobounce(email: str, api_key: str) -> VerifyResult:
    """Single-email verification via ZeroBounce."""
    resp = requests.get(
        "https://api.zerobounce.net/v2/validate",
        params={"api_key": api_key, "email": email},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    raw = str(data.get("status", "unknown")).lower()
    sub_status = str(data.get("sub_status", "")).lower()

    if sub_status in _ZB_SUB_STATUS_DISPOSABLE:
        status = VerifyStatus.disposable
    else:
        status = _ZB_STATUS_MAP.get(raw, VerifyStatus.unknown)

    return _decide(status, email, "zerobounce", raw)


def _verify_batch_zerobounce(
    emails: list[str],
    api_key: str,
) -> list[VerifyResult]:
    """Batch verification via ZeroBounce bulk API.

    Flow:
      1. POST /v2/sendfile  – upload CSV
      2. GET  /v2/filestatus – poll until complete
      3. GET  /v2/getfile    – download results
    """
    import csv
    import io
    import time

    # Build in-memory CSV
    buf = io.StringIO()
    writer = csv.writer(buf)
    for email in emails:
        writer.writerow([email])
    buf.seek(0)

    # --- upload ---
    upload_resp = requests.post(
        "https://bulkapi.zerobounce.net/v2/sendfile",
        data={"api_key": api_key, "email_address_column": 1},
        files={"file": ("emails.csv", buf.getvalue(), "text/csv")},
        timeout=30,
    )
    upload_resp.raise_for_status()
    file_id = upload_resp.json().get("file_id")
    if not file_id:
        raise RuntimeError("ZeroBounce did not return a file_id")

    # --- poll ---
    for _ in range(60):
        status_resp = requests.get(
            "https://bulkapi.zerobounce.net/v2/filestatus",
            params={"api_key": api_key, "file_id": file_id},
            timeout=_TIMEOUT,
        )
        status_resp.raise_for_status()
        file_status = status_resp.json().get("file_status", "")
        if file_status.lower() == "complete":
            break
        time.sleep(2)
    else:
        raise RuntimeError(f"ZeroBounce file {file_id} did not complete in time")

    # --- download results ---
    result_resp = requests.get(
        "https://bulkapi.zerobounce.net/v2/getfile",
        params={"api_key": api_key, "file_id": file_id},
        timeout=30,
    )
    result_resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(result_resp.text))
    out: list[VerifyResult] = []
    for row in reader:
        addr = row.get("Email Address", "") or row.get("email", "")
        raw = str(row.get("ZB Status", "unknown")).lower()
        sub = str(row.get("ZB Sub Status", "")).lower()
        if sub in _ZB_SUB_STATUS_DISPOSABLE:
            status = VerifyStatus.disposable
        else:
            status = _ZB_STATUS_MAP.get(raw, VerifyStatus.unknown)
        out.append(_decide(status, addr, "zerobounce", raw))
    return out


# ---------------------------------------------------------------------------
# Public API: EmailVerifier
# ---------------------------------------------------------------------------


class EmailVerifier:
    """Configurable email verifier supporting NeverBounce and ZeroBounce."""

    def __init__(self, settings) -> None:
        self.provider: str = getattr(settings, "email_verify_provider", "neverbounce")
        self._nb_key: str = getattr(settings, "neverbounce_api_key", "")
        self._zb_key: str = getattr(settings, "zerobounce_api_key", "")

    @property
    def _api_key(self) -> str:
        if self.provider == "zerobounce":
            return self._zb_key
        return self._nb_key

    @property
    def is_configured(self) -> bool:
        return bool(self._api_key)

    # -- single verify --

    def verify(self, email: str) -> VerifyResult:
        """Verify a single email address."""
        if not self._api_key:
            logger.warning("email verification skipped — no API key for %s", self.provider)
            return _decide(VerifyStatus.unknown, email, self.provider, "no_api_key")

        try:
            if self.provider == "zerobounce":
                return _verify_zerobounce(email, self._zb_key)
            return _verify_neverbounce(email, self._nb_key)
        except Exception:
            logger.warning("email verify failed for %s", email, exc_info=True)
            return _decide(VerifyStatus.unknown, email, self.provider, "error")

    # -- batch verify --

    def verify_batch(self, emails: Sequence[str]) -> list[VerifyResult]:
        """Verify a list of emails in batch.

        Falls back to single verification if batch fails.
        """
        if not self._api_key:
            logger.warning("batch email verification skipped — no API key for %s", self.provider)
            return [_decide(VerifyStatus.unknown, e, self.provider, "no_api_key") for e in emails]

        if not emails:
            return []

        try:
            if self.provider == "zerobounce":
                return _verify_batch_zerobounce(list(emails), self._zb_key)
            return _verify_batch_neverbounce(list(emails), self._nb_key)
        except Exception:
            logger.warning("batch verification failed, falling back to single", exc_info=True)
            return [self.verify(e) for e in emails]

    # -- verify with retry for unknowns --

    def verify_with_retry(self, email: str) -> VerifyResult:
        """Verify once; if unknown, retry once, then flag for review."""
        result = self.verify(email)
        if result.status == VerifyStatus.unknown and result.raw_status != "no_api_key":
            logger.info("retrying unknown email=%s", email)
            result = self.verify(email)
        return result


def filter_verified_contacts(
    contacts: list[dict],
    verifier: Optional[EmailVerifier] = None,
) -> list[dict]:
    """Verify emails in a contacts list and apply filtering rules.

    Returns a new list of contacts:
    - invalid/disposable emails are cleared (email set to "")
    - valid emails get email_verified=True
    - catch_all/unknown emails get email_verified=False, flag_for_review=True
    """
    if verifier is None or not verifier.is_configured:
        return contacts

    emails_to_verify: list[tuple[int, str]] = []
    for idx, contact in enumerate(contacts):
        email = contact.get("email", "")
        if email:
            emails_to_verify.append((idx, email))

    if not emails_to_verify:
        return contacts

    # Batch verify all non-empty emails
    raw_emails = [e for _, e in emails_to_verify]
    results = verifier.verify_batch(raw_emails)

    result_map: dict[str, VerifyResult] = {}
    for vr in results:
        result_map[vr.email] = vr

    out: list[dict] = []
    for contact in contacts:
        updated = dict(contact)
        email = contact.get("email", "")
        if email and email in result_map:
            vr = result_map[email]
            if not vr.should_store:
                # invalid / disposable → clear the email
                updated["email"] = ""
                updated["email_verified"] = False
                updated["verification_status"] = vr.status.value
                logger.info(
                    "rejected email=%s status=%s",
                    email,
                    vr.status.value,
                )
            else:
                updated["email_verified"] = vr.email_verified
                updated["verification_status"] = vr.status.value
                if vr.flag_for_review:
                    updated["flag_for_review"] = True
                    logger.info(
                        "flagged email=%s status=%s for review",
                        email,
                        vr.status.value,
                    )
        out.append(updated)
    return out
