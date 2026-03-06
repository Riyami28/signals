"""Conference/event attendance signal collector.

Ingests conference attendance, sponsorship, and speaking engagement data
from CSV. Each row maps a company (by domain) to a conference event,
creating signal observations that feed into the scoring engine.

CSV format (data/raw/conference_events.csv):
    domain, company_name, signal_code, event_name, event_type, event_url, observed_at, confidence
"""

from __future__ import annotations

import logging

from src import db
from src.models import SignalObservation
from src.settings import Settings
from src.utils import load_csv_rows, stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

VALID_SIGNAL_CODES = {
    "conference_attendance",
    "conference_sponsorship",
    "conference_speaking",
}

# Map event_type shorthand → signal_code (fallback when signal_code column is empty)
_EVENT_TYPE_MAP = {
    "attendance": "conference_attendance",
    "attending": "conference_attendance",
    "sponsor": "conference_sponsorship",
    "sponsorship": "conference_sponsorship",
    "speaking": "conference_speaking",
    "speaker": "conference_speaking",
    "keynote": "conference_speaking",
    "panel": "conference_speaking",
}


async def collect(
    conn,
    settings: Settings,
    lexicon_by_source: dict[str, list[dict[str, str]]],
    source_reliability: dict[str, float],
    db_pool=None,
    account_ids: list[str] | None = None,
) -> dict[str, int]:
    """Collect conference/event attendance signals from CSV."""
    del lexicon_by_source, db_pool, account_ids  # unused for CSV collector

    inserted = 0
    seen = 0

    source = "conference_event_csv"
    reliability = source_reliability.get(source, 0.85)
    if reliability <= 0:
        return {"inserted": 0, "seen": 0}

    csv_path = settings.raw_dir / "conference_events.csv"
    rows = load_csv_rows(csv_path)
    if not rows:
        logger.debug("conference_events: no rows in %s", csv_path)
        return {"inserted": 0, "seen": 0}

    for row in rows:
        domain = (row.get("domain") or "").strip()
        if not domain:
            continue

        # Resolve signal_code: explicit column > event_type mapping
        signal_code = (row.get("signal_code") or "").strip()
        if not signal_code:
            event_type = (row.get("event_type") or "").strip().lower()
            signal_code = _EVENT_TYPE_MAP.get(event_type, "")
        if signal_code not in VALID_SIGNAL_CODES:
            logger.debug("conference_events: skipping unknown signal_code=%s", signal_code)
            continue

        company_name = (row.get("company_name") or "").strip() or domain
        account_id = db.upsert_account(
            conn,
            company_name=company_name,
            domain=domain,
            source_type="discovered",
            commit=False,
        )

        try:
            confidence = float(row.get("confidence") or 0.75)
        except (ValueError, TypeError):
            confidence = 0.75

        observed_at = (row.get("observed_at") or "").strip() or utc_now_iso()
        event_name = (row.get("event_name") or "").strip()
        event_url = (row.get("event_url") or "").strip()

        # Build evidence text from event details
        evidence_parts = []
        if event_name:
            evidence_parts.append(event_name)
        event_type_raw = (row.get("event_type") or "").strip()
        if event_type_raw:
            evidence_parts.append(f"({event_type_raw})")
        evidence_text = " ".join(evidence_parts)

        payload = {"row": row}
        raw_hash = stable_hash(payload, prefix="raw")
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
            evidence_url=event_url,
            evidence_text=evidence_text[:500],
            confidence=max(0.0, min(1.0, confidence)),
            source_reliability=max(0.0, min(1.0, reliability)),
            raw_payload_hash=raw_hash,
        )

        seen += 1
        if db.insert_signal_observation(conn, observation, commit=False):
            inserted += 1

    conn.commit()
    logger.info("conference_events: inserted=%d seen=%d", inserted, seen)
    return {"inserted": inserted, "seen": seen}
