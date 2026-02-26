from __future__ import annotations

import re
from dataclasses import dataclass

ROLE_PATTERNS: list[tuple[re.Pattern[str], str, float]] = [
    (re.compile(r"\bchief information officer\b|\bcio\b", re.I), "CIO", 1.0),
    (re.compile(r"\bchief technology officer\b|\bcto\b", re.I), "CTO", 1.0),
    (re.compile(r"\bchief data officer\b|\bcdo\b", re.I), "CDO", 0.95),
    (re.compile(r"\bchief information security officer\b|\bciso\b", re.I), "CISO", 1.0),
    (re.compile(r"\bhead of supply chain\b|\bsupply chain head\b", re.I), "Head Supply Chain", 0.95),
    (re.compile(r"\bhead of procurement\b|\bprocurement head\b", re.I), "Head Procurement", 0.95),
    (re.compile(r"\bvp engineering\b|\bvice president engineering\b", re.I), "VP Engineering", 0.9),
    (re.compile(r"\bhead of infrastructure\b|\bplatform engineering\b", re.I), "Head Infrastructure", 0.9),
]

NAME_PATTERN = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\b")
QUOTE_VERB_PATTERN = re.compile(r"\b(said|says|stated|noted|added|commented|explained|shared)\b", re.I)


@dataclass
class Speaker:
    name: str
    role: str
    role_weight: float


def infer_role(context: str) -> tuple[str, float]:
    text = (context or "").strip()
    for pattern, role, weight in ROLE_PATTERNS:
        if pattern.search(text):
            return role, weight
    return "", 0.0


def extract_speakers(text: str) -> list[Speaker]:
    normalized = text or ""
    speakers: list[Speaker] = []
    seen: set[tuple[str, str]] = set()

    for sentence in re.split(r"(?<=[.!?])\s+", normalized):
        if not sentence.strip():
            continue
        if not QUOTE_VERB_PATTERN.search(sentence):
            continue
        role, role_weight = infer_role(sentence)
        for match in NAME_PATTERN.findall(sentence):
            candidate = match.strip()
            if len(candidate.split()) < 2:
                continue
            key = (candidate.lower(), role.lower())
            if key in seen:
                continue
            seen.add(key)
            speakers.append(
                Speaker(
                    name=candidate,
                    role=role,
                    role_weight=role_weight,
                )
            )
    return speakers


def closest_speaker(sentence: str, speakers: list[Speaker]) -> Speaker | None:
    if not speakers:
        return None
    sentence_lower = (sentence or "").lower()
    for speaker in speakers:
        if speaker.name.lower() in sentence_lower:
            return speaker
    # Fall back to the strongest role-weighted speaker.
    ranked = sorted(speakers, key=lambda row: row.role_weight, reverse=True)
    return ranked[0] if ranked else None
