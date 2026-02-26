from __future__ import annotations

from datetime import date
from typing import Any

from src.models import SignalObservation
from src.utils import utc_now_iso


def insert_signal_observation(conn: Any, observation: SignalObservation, commit: bool = True) -> bool:
    cur = conn.execute(
        """
        INSERT INTO signal_observations (
            obs_id,
            account_id,
            signal_code,
            product,
            source,
            observed_at,
            evidence_url,
            evidence_text,
            document_id,
            mention_id,
            evidence_sentence,
            evidence_sentence_en,
            matched_phrase,
            language,
            speaker_name,
            speaker_role,
            evidence_quality,
            relevance_score,
            confidence,
            source_reliability,
            raw_payload_hash
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING obs_id
        """,
        (
            observation.obs_id,
            observation.account_id,
            observation.signal_code,
            observation.product,
            observation.source,
            observation.observed_at,
            observation.evidence_url,
            observation.evidence_text,
            observation.document_id,
            observation.mention_id,
            observation.evidence_sentence,
            observation.evidence_sentence_en,
            observation.matched_phrase,
            observation.language,
            observation.speaker_name,
            observation.speaker_role,
            float(observation.evidence_quality),
            float(observation.relevance_score),
            float(observation.confidence),
            float(observation.source_reliability),
            observation.raw_payload_hash,
        ),
    )
    inserted = cur.fetchone() is not None
    if commit:
        conn.commit()
    return inserted
