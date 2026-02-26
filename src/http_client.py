from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib import robotparser
from urllib.parse import urlparse

import httpx

from src.settings import Settings
from src.utils import normalize_domain

logger = logging.getLogger(__name__)


class RobotsDisallowedError(RuntimeError):
    pass


@dataclass
class _RobotsCacheEntry:
    parser: robotparser.RobotFileParser
    fetched_at: datetime


# --- Sync state (kept for backward-compat sync ``get()``) ---
_sync_last_request: dict[str, float] = {}

# --- Async state ---
_domain_semaphores: dict[str, asyncio.Semaphore] = {}
_domain_last_request_async: dict[str, float] = {}
_robots_cache: dict[str, _RobotsCacheEntry] = {}


def _proxy_url(settings: Settings) -> str | None:
    proxy = settings.http_proxy_url.strip()
    return proxy or None


def _host_from_url(url: str) -> str:
    parsed = urlparse(url)
    return normalize_domain(parsed.netloc)


# ---------------------------------------------------------------------------
# Robots.txt helpers (async)
# ---------------------------------------------------------------------------


async def _fetch_robots_parser_async(
    client: httpx.AsyncClient,
    host: str,
    settings: Settings,
) -> robotparser.RobotFileParser:
    robots_url = f"https://{host}/robots.txt"
    parser = robotparser.RobotFileParser()
    try:
        response = await client.get(
            robots_url,
            timeout=min(settings.http_timeout_seconds, 10),
        )
        if response.status_code >= 400:
            parser.parse([])
            return parser
        parser.parse(response.text.splitlines())
        return parser
    except Exception:
        logger.warning("failed to fetch robots.txt for %s", host, exc_info=True)
        parser.parse([])
        return parser


async def _is_allowed_by_robots_async(
    client: httpx.AsyncClient,
    url: str,
    settings: Settings,
) -> bool:
    if not settings.respect_robots_txt:
        return True
    host = _host_from_url(url)
    if not host:
        return True

    now = datetime.now(timezone.utc)
    cached = _robots_cache.get(host)
    if cached and (now - cached.fetched_at) <= timedelta(hours=12):
        parser = cached.parser
    else:
        parser = await _fetch_robots_parser_async(client, host, settings)
        _robots_cache[host] = _RobotsCacheEntry(parser=parser, fetched_at=now)

    try:
        return bool(parser.can_fetch(settings.http_user_agent, url))
    except Exception:
        logger.warning("robots.txt check failed for %s, allowing", url, exc_info=True)
        return True


# ---------------------------------------------------------------------------
# Per-domain rate limiting (async)
# ---------------------------------------------------------------------------


def _get_domain_semaphore(host: str) -> asyncio.Semaphore:
    if host not in _domain_semaphores:
        _domain_semaphores[host] = asyncio.Semaphore(1)
    return _domain_semaphores[host]


async def _wait_for_rate_limit_async(host: str, settings: Settings) -> None:
    interval = max(0.0, float(settings.min_domain_request_interval_ms) / 1000.0)
    if interval <= 0 or not host:
        return

    sem = _get_domain_semaphore(host)
    async with sem:
        last = _domain_last_request_async.get(host)
        now = asyncio.get_event_loop().time()
        if last is not None:
            remaining = interval - (now - last)
            if remaining > 0:
                await asyncio.sleep(remaining)
        _domain_last_request_async[host] = asyncio.get_event_loop().time()


# ---------------------------------------------------------------------------
# Async public API
# ---------------------------------------------------------------------------


async def async_get(
    url: str,
    settings: Settings,
    client: httpx.AsyncClient | None = None,
    timeout_seconds: int | None = None,
) -> httpx.Response:
    """Async HTTP GET with robots.txt + per-domain rate limiting."""
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(
            headers={"User-Agent": settings.http_user_agent},
            proxy=_proxy_url(settings),
            follow_redirects=True,
        )

    try:
        if not await _is_allowed_by_robots_async(client, url, settings):
            raise RobotsDisallowedError(f"robots_disallowed url={url}")

        host = _host_from_url(url)
        await _wait_for_rate_limit_async(host, settings)

        response = await client.get(
            url,
            timeout=timeout_seconds or settings.http_timeout_seconds,
        )
        return response
    finally:
        if own_client:
            await client.aclose()


# ---------------------------------------------------------------------------
# Sync public API (backward compat — used by discovery/fetcher.py)
# ---------------------------------------------------------------------------


def _sleep_for_rate_limit_sync(host: str, settings: Settings) -> None:
    interval = max(0.0, float(settings.min_domain_request_interval_ms) / 1000.0)
    if interval <= 0 or not host:
        return
    last = _sync_last_request.get(host)
    now = time.monotonic()
    if last is not None:
        remaining = interval - (now - last)
        if remaining > 0:
            time.sleep(remaining)
    _sync_last_request[host] = time.monotonic()


def _fetch_robots_parser_sync(host: str, settings: Settings) -> robotparser.RobotFileParser:
    robots_url = f"https://{host}/robots.txt"
    parser = robotparser.RobotFileParser()
    try:
        response = httpx.get(
            robots_url,
            timeout=min(settings.http_timeout_seconds, 10),
            headers={"User-Agent": settings.http_user_agent},
        )
        if response.status_code >= 400:
            parser.parse([])
            return parser
        parser.parse(response.text.splitlines())
        return parser
    except Exception:
        logger.debug("failed to fetch robots.txt for %s", host, exc_info=True)
        parser.parse([])
        return parser


def _is_allowed_by_robots_sync(url: str, settings: Settings) -> bool:
    if not settings.respect_robots_txt:
        return True
    host = _host_from_url(url)
    if not host:
        return True
    now = datetime.now(timezone.utc)
    cached = _robots_cache.get(host)
    if cached and (now - cached.fetched_at) <= timedelta(hours=12):
        parser = cached.parser
    else:
        parser = _fetch_robots_parser_sync(host, settings)
        _robots_cache[host] = _RobotsCacheEntry(parser=parser, fetched_at=now)
    try:
        return bool(parser.can_fetch(settings.http_user_agent, url))
    except Exception:
        logger.debug("robots.txt check failed for %s, allowing", url, exc_info=True)
        return True


def get(url: str, settings: Settings, timeout_seconds: int | None = None) -> httpx.Response:
    """Sync HTTP GET — backward-compatible wrapper. Prefer ``async_get`` for collectors."""
    if not _is_allowed_by_robots_sync(url, settings):
        raise RobotsDisallowedError(f"robots_disallowed url={url}")

    host = _host_from_url(url)
    _sleep_for_rate_limit_sync(host, settings)

    response = httpx.get(
        url,
        timeout=timeout_seconds or settings.http_timeout_seconds,
        headers={"User-Agent": settings.http_user_agent},
    )
    return response
