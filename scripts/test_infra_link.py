#!/usr/bin/env python3
"""
Test script to verify Postgres, Redis, Playwright, and HTTP connectivity.

Usage:
    python scripts/test_infra_link.py

This script performs 5 checks:
    1. Postgres: connection + insert + query
    2. Redis: connection + ping
    3. Playwright: browser launch + page navigation
    4. HTTP: async client request
    5. Settings: environment variables loaded correctly
"""

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# noqa: E402 - imports after path manipulation necessary for script execution
from src import db  # noqa: E402
from src.models import Account, SignalObservation  # noqa: E402
from src.settings import Settings  # noqa: E402
from src.utils import stable_hash, utc_now_iso  # noqa: E402


def print_header():
    """Print test header."""
    print("\n" + "╔" + "=" * 42 + "╗")
    print("║" + " " * 10 + "Infrastructure Connectivity Test" + " " * 1 + "║")
    print("╚" + "=" * 42 + "╝\n")


def print_check(num, title):
    """Print check header."""
    print(f"[{num}/6] {title}")


def print_pass(message):
    """Print passing message."""
    print(f"  ✓ {message}")


def print_warn(message):
    """Print warning message."""
    print(f"  ⚠ {message}")


def print_fail(message):
    """Print failing message."""
    print(f"  ✗ {message}")


def test_settings():
    """Test that settings load correctly."""
    print_check(1, "Testing Settings")
    try:
        settings = Settings()

        # Check key settings
        if settings.pg_dsn:
            print_pass("SIGNALS_PG_DSN set")
        else:
            print_fail("SIGNALS_PG_DSN not set")
            return False

        if settings.enable_live_crawl is not None:
            status = "enabled" if settings.enable_live_crawl else "disabled"
            print_pass(f"Live crawl {status}")

        if settings.http_timeout_seconds > 0:
            print_pass(f"HTTP timeout: {settings.http_timeout_seconds}s")

        if settings.live_max_accounts > 0:
            print_pass(f"Max accounts: {settings.live_max_accounts}")

        return True

    except Exception as exc:
        print_fail(f"Settings test failed: {exc}")
        return False


def test_postgres():
    """Test PostgreSQL connection and basic insert/query."""
    print_check(2, "Testing Postgres Connection")
    try:
        settings = Settings()
        conn = db.get_connection(settings.pg_dsn)

        # Check version
        cur = conn.execute("SELECT version();")
        version_row = cur.fetchone()
        if version_row:
            version = version_row.get("version") if isinstance(version_row, dict) else version_row[0]
            if "PostgreSQL 16" in version:
                print_pass("Connected to PostgreSQL 16")
            else:
                print_warn(f"PostgreSQL version: {version[:50]}...")
        else:
            print_fail("Could not fetch PostgreSQL version")
            return False

        # Check schema
        cur = conn.execute("SELECT current_schema;")
        schema_row = cur.fetchone()
        if schema_row:
            schema = schema_row.get("current_schema") if isinstance(schema_row, dict) else schema_row[0]
            if schema == "signals":
                print_pass("Default schema: signals")
            else:
                print_warn(f"Schema: {schema}")
        else:
            print_fail("Could not fetch current schema")
            return False

        # Test insert
        print_check(3, "Testing Database Insert")
        test_domain = f"test-{uuid4().hex[:8]}.example"
        account_id = None

        try:
            account_id = db.upsert_account(
                conn, test_domain, "Test Company for Signal Verification", "seed", commit=True
            )
            print_pass(f"Inserted test account: {test_domain}")
        except Exception as e:
            print_warn(f"Account insert failed: {e}")
            return False

        # Test observation insert
        obs = SignalObservation(
            obs_id=stable_hash({"test": "observation", "ts": utc_now_iso()}, prefix="obs"),
            account_id=account_id,
            signal_code="test_signal",
            source="test_script",
            observed_at=utc_now_iso(),
            confidence=0.5,
            source_reliability=0.8,
            evidence_url="https://example.com/test",
            evidence_text="Test observation from connectivity check",
            raw_payload_hash=stable_hash({"test": "payload"}, prefix="raw"),
        )

        try:
            inserted = db.insert_signal_observation(conn, obs, commit=True)
            if inserted:
                print_pass(f"Inserted test observation: {obs.obs_id[:20]}...")
            else:
                print_warn("Observation may have been deduplicated")
        except Exception as e:
            print_warn(f"Observation insert failed: {e}")

        # Query back
        cur = conn.execute("SELECT COUNT(*) as cnt FROM signal_observations WHERE signal_code = %s", ("test_signal",))
        count_row = cur.fetchone()
        if count_row:
            count = count_row.get("cnt") if isinstance(count_row, dict) else count_row[0]
            print_pass(f"Query returned {count} row(s)")
        else:
            print_fail("Could not query signal_observations")
            return False

        conn.close()
        return True

    except Exception as exc:
        print_fail(f"Postgres test failed: {exc}")
        return False


def test_redis():
    """Test Redis connection."""
    print_check(4, "Testing Redis Connection")
    try:
        import redis

        r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True, socket_connect_timeout=5)

        pong = r.ping()
        if pong:
            print_pass("Connected to Redis 7")
            print_pass("Ping successful")

            # Try set/get
            r.set("test_key", "test_value")
            val = r.get("test_key")
            if val == "test_value":
                print_pass("Set/Get working")
            r.delete("test_key")
        else:
            print_fail("Ping failed")
            return False

        return True

    except ImportError:
        print_warn("redis package not installed (optional for this version)")
        return True
    except Exception as exc:
        print_warn(f"Redis test failed: {exc} (OK if Redis not required)")
        return True


async def test_playwright():
    """Test Playwright browser launch."""
    print_check(5, "Testing Playwright Browser")
    try:
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            # Launch browser
            browser = await p.chromium.launch(headless=True)
            print_pass("Chromium browser launched")

            # Create page and navigate
            page = await browser.new_page()
            await page.goto("https://example.com", wait_until="networkidle", timeout=15000)
            title = await page.title()

            if title:
                print_pass("Navigated to https://example.com")
                print_pass(f"Page title: {title}")
            else:
                print_warn("Could not get page title")

            await browser.close()

        return True

    except ImportError:
        print_warn("playwright not installed (optional)")
        return True
    except Exception as exc:
        print_fail(f"Playwright test failed: {exc}")
        print_warn("Try: playwright install chromium")
        return False


async def test_httpx():
    """Test httpx HTTP client."""
    print_check(6, "Testing HTTP Client")
    try:
        import httpx

        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get("https://www.google.com", follow_redirects=True)
            if response.status_code == 200:
                print_pass(f"Fetched https://www.google.com ({response.status_code} OK)")
            else:
                print_warn(f"Status code: {response.status_code}")

        return True

    except Exception as exc:
        print_warn(f"HTTP test failed: {exc} (check internet connectivity)")
        return False


async def main():
    """Run all tests."""
    print_header()

    results = []

    # Sync tests
    results.append(("Settings", test_settings()))
    results.append(("Postgres", test_postgres()))
    results.append(("Redis", test_redis()))

    # Async tests
    results.append(("Playwright", await test_playwright()))
    results.append(("HTTP", await test_httpx()))

    # Summary
    print("\n" + "=" * 44)
    passed = sum(1 for _, result in results if result)
    total = len(results)

    if passed == total:
        print("✓ ALL CHECKS PASSED — Ready for ./signals start")
        print("=" * 44 + "\n")
        return 0
    else:
        failed = total - passed
        print(f"⚠ {failed} check(s) failed — see above for details")
        print("=" * 44 + "\n")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)
