from __future__ import annotations

from src import db
from src.models import SignalObservation
from src.settings import Settings
from src.utils import load_csv_rows, stable_hash, utc_now_iso

VALID_PRODUCTS = {"zopdev", "zopday", "zopnight", "shared"}


def collect(
    conn,
    settings: Settings,
    lexicon_by_source: dict[str, list[dict[str, str]]],
    source_reliability: dict[str, float],
) -> dict[str, int]:
    del lexicon_by_source
    inserted = 0
    seen = 0

    source_default = "first_party_csv"
    default_reliability = source_reliability.get(source_default, 0.9)

    for row in load_csv_rows(settings.raw_dir / "first_party_events.csv"):
        domain = row.get("domain", "")
        signal_code = row.get("signal_code", "")
        if not domain or not signal_code:
            continue

        company_name = row.get("company_name", "") or domain
        account_id = db.upsert_account(conn, company_name=company_name, domain=domain, source_type="discovered", commit=False)

        product = (row.get("product", "shared") or "shared").strip().lower()
        if product not in VALID_PRODUCTS:
            product = "shared"

        source = row.get("source", "") or source_default
        reliability = source_reliability.get(source, default_reliability)
        if reliability <= 0:
            continue

        try:
            confidence = float(row.get("confidence", "0.8") or 0.8)
        except ValueError:
            confidence = 0.8

        observed_at = row.get("observed_at", "") or utc_now_iso()
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
            product=product,
            source=source,
            observed_at=observed_at,
            evidence_url=row.get("evidence_url", ""),
            evidence_text=row.get("evidence_text", "")[:500],
            confidence=max(0.0, min(1.0, confidence)),
            source_reliability=max(0.0, min(1.0, reliability)),
            raw_payload_hash=raw_hash,
        )
        seen += 1
        if db.insert_signal_observation(conn, observation, commit=False):
            inserted += 1

    conn.commit()
    return {"inserted": inserted, "seen": seen}
