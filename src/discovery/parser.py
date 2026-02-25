from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import re
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from src.discovery import multilingual
from src.discovery.speaker_intel import Speaker, closest_speaker, extract_speakers
from src.utils import classify_text

logger = logging.getLogger(__name__)

try:
    import trafilatura
except Exception:  # pragma: no cover - optional dependency
    trafilatura = None  # type: ignore


LISTING_TOKENS = ("news", "press", "stories", "blog", "media", "announcements")
RELEVANCE_TERMS = (
    "cloud",
    "infrastructure",
    "infra",
    "devops",
    "sre",
    "kubernetes",
    "sap s/4hana",
    "erp modernization",
    "ecc sunset",
    "control tower",
    "demand planning",
    "warehouse digitization",
    "cost transformation",
    "margin improvement",
    "policy enforcement",
    "audit readiness",
    "risk controls",
    "procurement",
    "vendor consolidation",
    "go-live",
)
PARSER_VERSION = "story_parser_v1"


@dataclass
class ParsedDocument:
    title: str
    author: str
    published_at: str
    section: str
    language: str
    body_text: str
    body_text_en: str
    evidence_quality: float
    relevance_score: float
    outbound_links: list[str]
    is_listing: bool
    translation_status: str
    speakers: list[Speaker]


@dataclass
class ParsedMention:
    signal_code: str
    matched_phrase: str
    confidence: float
    evidence_sentence: str
    evidence_sentence_en: str
    language: str
    speaker_name: str
    speaker_role: str
    speaker_weight: float


def _extract_with_trafilatura(html: str) -> tuple[str, str]:
    if trafilatura is None:
        return "", ""
    try:
        text = trafilatura.extract(html, include_comments=False, include_tables=False, no_fallback=False) or ""
        meta = trafilatura.extract_metadata(html)
        title = ""
        if meta is not None:
            title = str(meta.title or "")
        return title.strip(), text.strip()
    except Exception:
        logger.debug("trafilatura extraction failed", exc_info=True)
        return "", ""


def _extract_with_bs4(html: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    for node in soup(["script", "style", "noscript"]):
        node.decompose()
    title = ""
    if soup.title and soup.title.text:
        title = soup.title.text.strip()
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return title, text


def _split_sentences(text: str) -> list[str]:
    parts = [chunk.strip() for chunk in re.split(r"(?<=[.!?])\s+", text or "") if chunk.strip()]
    return parts if parts else ([text.strip()] if text.strip() else [])


def _find_sentence_with_phrase(text: str, phrase: str) -> str:
    phrase_norm = (phrase or "").strip().lower()
    if not phrase_norm:
        return ""
    for sentence in _split_sentences(text):
        if phrase_norm in sentence.lower():
            return sentence
    return ""


def _extract_author(soup: BeautifulSoup, author_hint: str) -> str:
    if author_hint.strip():
        return author_hint.strip()
    meta_author = soup.find("meta", attrs={"name": "author"}) or soup.find("meta", attrs={"property": "author"})
    if meta_author:
        content = str(meta_author.get("content", "")).strip()
        if content:
            return content[:250]
    return ""


def _extract_published_at(soup: BeautifulSoup, published_at_hint: str) -> str:
    if published_at_hint.strip():
        return published_at_hint.strip()
    for key in ("article:published_time", "og:published_time", "publish_date", "date"):
        node = soup.find("meta", attrs={"property": key}) or soup.find("meta", attrs={"name": key})
        if node:
            content = str(node.get("content", "")).strip()
            if content:
                return content[:80]
    return ""


def _extract_section(url: str) -> str:
    path = (urlparse(url).path or "/").strip("/")
    if not path:
        return "home"
    return path.split("/")[0][:120]


def _is_listing_page(url: str, url_type: str, body_text: str, outbound_links: list[str]) -> bool:
    if url_type == "listing":
        return True
    path = (urlparse(url).path or "").lower()
    if any(token in path for token in LISTING_TOKENS) and len(body_text) < 1200 and len(outbound_links) >= 8:
        return True
    if len(body_text) < 400 and len(outbound_links) >= 10:
        return True
    return False


def _score_evidence_quality(body_text: str, author: str, published_at: str) -> float:
    body_len = len(body_text or "")
    if body_len >= 1800:
        score = 0.9
    elif body_len >= 1000:
        score = 0.84
    elif body_len >= 600:
        score = 0.76
    elif body_len >= 350:
        score = 0.62
    else:
        score = 0.4
    if author:
        score += 0.05
    if published_at:
        score += 0.05
    return max(0.0, min(1.0, score))


def _score_relevance(text_en: str) -> float:
    normalized = (text_en or "").lower()
    if not normalized:
        return 0.0
    hits = 0
    for token in RELEVANCE_TERMS:
        if token in normalized:
            hits += 1
    if hits == 0:
        return 0.0
    return max(0.0, min(1.0, hits / 7.0))


def extract_story_links(html: str, base_url: str, max_links: int = 25) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc.lower().replace("www.", "")
    discovered: list[str] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href", "")).strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        domain = parsed.netloc.lower().replace("www.", "")
        if domain != base_domain:
            continue
        url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
        if not url or url in seen:
            continue
        if url.rstrip("/") == base_url.rstrip("/"):
            continue
        seen.add(url)
        discovered.append(url)
        if len(discovered) >= max_links:
            break
    return discovered


def parse_document(
    html: str,
    url: str,
    url_type: str,
    language_hint: str = "",
    author_hint: str = "",
    published_at_hint: str = "",
) -> ParsedDocument:
    soup = BeautifulSoup(html or "", "html.parser")
    trafilatura_title, trafilatura_text = _extract_with_trafilatura(html or "")
    fallback_title, fallback_text = _extract_with_bs4(html or "")

    body_text = trafilatura_text if trafilatura_text else fallback_text
    title = trafilatura_title if trafilatura_title else fallback_title
    author = _extract_author(soup, author_hint=author_hint)
    published_at = _extract_published_at(soup, published_at_hint=published_at_hint)
    section = _extract_section(url)
    outbound_links = extract_story_links(html or "", base_url=url, max_links=25)

    normalized = multilingual.normalize_document_text(body_text, language_hint=language_hint)
    evidence_quality = _score_evidence_quality(body_text=body_text, author=author, published_at=published_at)
    relevance_score = _score_relevance(normalized.text_english)
    listing = _is_listing_page(
        url=url,
        url_type=url_type,
        body_text=body_text,
        outbound_links=outbound_links,
    )
    speakers = extract_speakers(f"{title}\n{normalized.text_english}")
    return ParsedDocument(
        title=(title or "")[:500],
        author=author,
        published_at=published_at,
        section=section,
        language=normalized.language,
        body_text=(normalized.text_original or body_text or "")[:200000],
        body_text_en=(normalized.text_english or body_text or "")[:200000],
        evidence_quality=evidence_quality,
        relevance_score=relevance_score,
        outbound_links=outbound_links,
        is_listing=listing,
        translation_status=normalized.translation_status,
        speakers=speakers,
    )


def extract_mentions(
    parsed: ParsedDocument,
    lexicon_rows: list[dict[str, str]],
) -> list[ParsedMention]:
    matches = classify_text(parsed.body_text_en, lexicon_rows)
    by_signal: dict[str, tuple[str, float]] = {}
    for signal_code, confidence, matched_keyword in matches:
        current = by_signal.get(signal_code)
        if current is None or confidence > current[1]:
            by_signal[signal_code] = (matched_keyword, float(confidence))

    mentions: list[ParsedMention] = []
    for signal_code, (matched_keyword, confidence) in by_signal.items():
        sentence_en = _find_sentence_with_phrase(parsed.body_text_en, matched_keyword)
        if not sentence_en:
            sentences = _split_sentences(parsed.body_text_en)
            sentence_en = sentences[0] if sentences else parsed.body_text_en[:400]
        sentence_native = _find_sentence_with_phrase(parsed.body_text, matched_keyword)
        if not sentence_native:
            sentence_native = sentence_en

        speaker = closest_speaker(sentence_en, parsed.speakers)
        mentions.append(
            ParsedMention(
                signal_code=signal_code,
                matched_phrase=matched_keyword,
                confidence=max(0.0, min(1.0, float(confidence))),
                evidence_sentence=sentence_native[:1500],
                evidence_sentence_en=sentence_en[:1500],
                language=parsed.language,
                speaker_name=(speaker.name if speaker else "")[:200],
                speaker_role=(speaker.role if speaker else "")[:120],
                speaker_weight=(speaker.role_weight if speaker else 0.0),
            )
        )
    return mentions


def parsed_document_to_json(parsed: ParsedDocument) -> str:
    payload: dict[str, Any] = {
        "parser_version": PARSER_VERSION,
        "translation_status": parsed.translation_status,
        "outbound_links": parsed.outbound_links,
        "speaker_count": len(parsed.speakers),
    }
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
