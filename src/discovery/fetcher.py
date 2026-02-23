from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
import logging
from typing import Any

from src.http_client import get as http_get
from src.settings import Settings

logger = logging.getLogger(__name__)

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except Exception:  # pragma: no cover - optional dependency
    PlaywrightTimeoutError = Exception  # type: ignore
    sync_playwright = None  # type: ignore


@dataclass
class FetchResult:
    ok: bool
    fetched_with: str
    final_url: str
    raw_html: str
    content_sha256: str
    error: str = ""


def _hash_content(text: str) -> str:
    return sha256((text or "").encode("utf-8", errors="ignore")).hexdigest()


def _extract_inline_html(payload_json: str) -> str:
    if not payload_json:
        return ""
    try:
        payload = json.loads(payload_json)
    except Exception:
        logger.debug("failed to parse payload JSON", exc_info=True)
        return ""
    if not isinstance(payload, dict):
        return ""
    raw_payload = payload.get("raw_payload_json")
    if isinstance(raw_payload, str):
        try:
            nested = json.loads(raw_payload)
        except Exception:
            logger.debug("failed to parse nested raw_payload_json", exc_info=True)
            nested = {}
        if isinstance(nested, dict):
            html = nested.get("html_content")
            if isinstance(html, str) and html.strip():
                return html
    html = payload.get("html_content")
    if isinstance(html, str) and html.strip():
        return html
    return ""


def _fetch_static(url: str, settings: Settings) -> FetchResult:
    response = http_get(url, settings)
    response.raise_for_status()
    html = response.text or ""
    return FetchResult(
        ok=bool(html.strip()),
        fetched_with="static_http",
        final_url=str(response.url or url),
        raw_html=html,
        content_sha256=_hash_content(html),
        error="" if html.strip() else "empty_html",
    )


def _fetch_with_js(url: str, settings: Settings, timeout_ms: int = 20000) -> FetchResult:
    if sync_playwright is None:
        return FetchResult(
            ok=False,
            fetched_with="js_render",
            final_url=url,
            raw_html="",
            content_sha256="",
            error="playwright_not_installed",
        )
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(user_agent=settings.http_user_agent)
            page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
            html = page.content() or ""
            final = page.url or url
            browser.close()
    except PlaywrightTimeoutError:
        return FetchResult(
            ok=False,
            fetched_with="js_render",
            final_url=url,
            raw_html="",
            content_sha256="",
            error="js_timeout",
        )
    except Exception as exc:  # pragma: no cover - highly environment-dependent
        return FetchResult(
            ok=False,
            fetched_with="js_render",
            final_url=url,
            raw_html="",
            content_sha256="",
            error=f"js_error:{str(exc)[:180]}",
        )
    return FetchResult(
        ok=bool(html.strip()),
        fetched_with="js_render",
        final_url=final,
        raw_html=html,
        content_sha256=_hash_content(html),
        error="" if html.strip() else "js_empty_html",
    )


def fetch_frontier_row(frontier_row: dict[str, Any], settings: Settings, allow_js_fallback: bool = True) -> FetchResult:
    payload_json = str(frontier_row.get("payload_json", "") or "")
    inline_html = _extract_inline_html(payload_json)
    candidate_url = str(frontier_row.get("url", "") or "")

    if inline_html.strip():
        return FetchResult(
            ok=True,
            fetched_with="inline_payload",
            final_url=candidate_url,
            raw_html=inline_html,
            content_sha256=_hash_content(inline_html),
            error="",
        )

    try:
        static_result = _fetch_static(candidate_url, settings)
        if static_result.ok:
            return static_result
    except Exception as exc:
        static_result = FetchResult(
            ok=False,
            fetched_with="static_http",
            final_url=candidate_url,
            raw_html="",
            content_sha256="",
            error=f"static_error:{str(exc)[:180]}",
        )

    if not allow_js_fallback:
        return static_result
    js_result = _fetch_with_js(candidate_url, settings)
    if js_result.ok:
        return js_result
    return static_result if static_result.ok else js_result
