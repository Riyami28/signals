from pathlib import Path

from src import db
from src.models import SignalObservation
from src.utils import stable_hash


def test_signal_observation_dedupe(tmp_path: Path):
    conn = db.get_connection(tmp_path / "signals.db")
    db.init_db(conn)

    account_id = db.upsert_account(conn, company_name="Acme", domain="acme.example", source_type="seed")
    payload = {"k": "v"}
    raw_hash = stable_hash(payload, prefix="raw")

    observation = SignalObservation(
        obs_id=stable_hash({"x": 1}, prefix="obs"),
        account_id=account_id,
        signal_code="devops_role_open",
        product="shared",
        source="jobs_csv",
        observed_at="2026-02-16T00:00:00Z",
        evidence_url="",
        evidence_text="devops engineer",
        confidence=0.7,
        source_reliability=0.75,
        raw_payload_hash=raw_hash,
    )

    first = db.insert_signal_observation(conn, observation)
    second = db.insert_signal_observation(conn, observation)

    assert first is True
    assert second is False
