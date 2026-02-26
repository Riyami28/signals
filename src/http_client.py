from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from urllib import robotparser
from urllib.parse import urlparse

import requests

from src.settings import Settings
from src.utils import normalize_domain

logger = logging.getLogger(__name__)


class RobotsDisallowedError(RuntimeError):
    pass


@dataclass
class _RobotsCacheEntry:
    parser: robotparser.RobotFileParser
    fetched_at: datetime


_domain_lock = Lock()
_last_request_monotonic: dict[str, float] = {}

_robots_lock = Lock()
_robots_cache: dict[str, _RobotsCacheEntry] = {}


def _proxy_dict(settings: Settings) -> dict[str, str] | None:
    proxy = settings.http_proxy_url.strip()
    if not proxy:
        return None
    return {"http": proxy, "https": proxy}


def _host_from_url(url: str) -> str:
    parsed = urlparse(url)
    return normalize_domain(parsed.netloc)


def _sleep_for_rate_limit(host: str, settings: Settings) -> None:
    interval_seconds = max(0.0, float(settings.min_domain_request_interval_ms) / 1000.0)
    if interval_seconds <= 0 or not host:
        return

    while True:
        with _domain_lock:
            last = _last_request_monotonic.get(host)
            now = time.monotonic()
            if last is None:
                _last_request_monotonic[host] = now
                return
            remaining = interval_seconds - (now - last)
            if remaining <= 0:
                _last_request_monotonic[host] = now
                return
        time.sleep(min(remaining, 0.25))


def _fetch_robots_parser(host: str, settings: Settings) -> robotparser.RobotFileParser:
    robots_url = f"https://{host}/robots.txt"
    parser = robotparser.RobotFileParser()

    try:
        response = requests.get(
            robots_url,
            timeout=min(settings.http_timeout_seconds, 10),
            headers={"User-Agent": settings.http_user_agent},
            proxies=_proxy_dict(settings),
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


def _is_allowed_by_robots(url: str, settings: Settings) -> bool:
    if not settings.respect_robots_txt:
        return True

    host = _host_from_url(url)
    if not host:
        return True

    now = datetime.now(timezone.utc)
    with _robots_lock:
        cached = _robots_cache.get(host)
        if cached and (now - cached.fetched_at) <= timedelta(hours=12):
            parser = cached.parser
        else:
            parser = _fetch_robots_parser(host, settings)
            _robots_cache[host] = _RobotsCacheEntry(parser=parser, fetched_at=now)

    try:
        return bool(parser.can_fetch(settings.http_user_agent, url))
    except Exception:
        logger.debug("robots.txt check failed for %s, allowing", url, exc_info=True)
        return True


def get(url: str, settings: Settings, timeout_seconds: int | None = None) -> requests.Response:
    if not _is_allowed_by_robots(url, settings):
        raise RobotsDisallowedError(f"robots_disallowed url={url}")

    host = _host_from_url(url)
    _sleep_for_rate_limit(host, settings)

    response = requests.get(
        url,
        timeout=timeout_seconds or settings.http_timeout_seconds,
        headers={"User-Agent": settings.http_user_agent},
        proxies=_proxy_dict(settings),
    )
    return response
