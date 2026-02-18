from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

try:
    from langdetect import detect as detect_language_raw
except Exception:  # pragma: no cover - optional dependency
    detect_language_raw = None  # type: ignore

try:
    from transformers import pipeline as hf_pipeline
except Exception:  # pragma: no cover - optional dependency
    hf_pipeline = None  # type: ignore


SUPPORTED_LANGS = {"en", "hi", "ar"}
MODEL_BY_LANG = {
    "hi": "Helsinki-NLP/opus-mt-hi-en",
    "ar": "Helsinki-NLP/opus-mt-ar-en",
}


@dataclass
class MultilingualResult:
    language: str
    text_original: str
    text_english: str
    translation_status: str


def detect_language(text: str, language_hint: str = "") -> str:
    hinted = (language_hint or "").strip().lower()
    if hinted in SUPPORTED_LANGS:
        return hinted
    if detect_language_raw is None:
        return "en"
    try:
        detected = str(detect_language_raw(text or "")).strip().lower()
    except Exception:
        return "en"
    if detected in SUPPORTED_LANGS:
        return detected
    return "en"


@lru_cache(maxsize=4)
def _load_translator(model_name: str):
    if hf_pipeline is None:
        return None
    try:
        return hf_pipeline("translation", model=model_name)
    except Exception:
        return None


def translate_to_english(text: str, language: str) -> tuple[str, str]:
    if language == "en":
        return text, "identity"
    model_name = MODEL_BY_LANG.get(language)
    if not model_name:
        return text, "unsupported_language"
    translator = _load_translator(model_name)
    if translator is None:
        return text, "translator_unavailable"
    try:
        # Keep chunking simple to avoid OOM on local setups.
        chunks = [text[i : i + 1200] for i in range(0, len(text), 1200)] or [text]
        translated_chunks: list[str] = []
        for chunk in chunks:
            output = translator(chunk, max_length=1200)
            if not output:
                continue
            first = output[0]
            translated_chunks.append(str(first.get("translation_text", "")).strip())
        translated = " ".join(part for part in translated_chunks if part)
        if translated.strip():
            return translated, "translated"
        return text, "translator_empty_output"
    except Exception:
        return text, "translator_error"


def normalize_document_text(text: str, language_hint: str = "") -> MultilingualResult:
    source_text = (text or "").strip()
    language = detect_language(source_text, language_hint=language_hint)
    translated, status = translate_to_english(source_text, language=language)
    return MultilingualResult(
        language=language,
        text_original=source_text,
        text_english=(translated or source_text).strip(),
        translation_status=status,
    )
