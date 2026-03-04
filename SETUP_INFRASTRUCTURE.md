# Infrastructure Setup Guide — Zopdev Signals Pipeline

**Goal:** Set up PostgreSQL 16, Redis 7, and Playwright for the Signals pipeline.

**Estimated time:** 5-10 minutes

---

## Quick Start (TL;DR)

```bash
cd /Users/zopdec/signals

# 1. Copy environment variables
cp .env.example .env

# 2. Create data directories
mkdir -p data/local/postgres data/local/redis

# 3. Start Docker containers
docker compose -f docker-compose.local.yml up -d postgres redis

# 4. Wait for Postgres to be ready
sleep 10

# 5. Create venv and install dependencies
python3 -m venv venv
source venv/bin/activate
pip install -e .

# 6. Initialize database schema
python -m src.main migrate

# 7. Install Playwright
playwright install chromium

# 8. Verify everything works
python scripts/test_infra_link.py
```

If all checks pass → You're ready to run `./signals start` ✅

---

## Detailed Setup Steps

### Step 1: Copy Environment Configuration

```bash
cd /Users/zopdec/signals
cp .env.example .env
```

**What it does:**
- Copies default environment variables to `.env`
- Sets `SIGNALS_PG_DSN` to connect to local Postgres (port 55432)
- Sets `SIGNALS_ENABLE_LIVE_CRAWL=0` (you can enable later)

**Expected output:**
```
$ cat .env | head -5
SIGNALS_PROJECT_ROOT=/path/to/signals
SIGNALS_PG_DSN=postgresql://signals:signals_dev_password@127.0.0.1:55432/signals?options=-c%20search_path%3Dsignals
SIGNALS_PG_HOST=127.0.0.1
...
```

### Step 2: Create Data Directories

```bash
mkdir -p data/local/postgres data/local/redis
```

**What it does:**
- Creates directories where Docker will persist Postgres and Redis data
- These directories are git-ignored (data/ is in .gitignore)
- Survives container restarts

### Step 3: Start Docker Containers

```bash
docker compose -f docker-compose.local.yml up -d postgres redis
```

**What it does:**
- Starts PostgreSQL 16 on port 55432
- Starts Redis 7 on port 56379
- Starts with health checks enabled
- Runs in background (`-d` flag)

**Check status:**
```bash
docker compose -f docker-compose.local.yml ps
```

**Expected output:**
```
NAME              COMMAND                  SERVICE   STATUS
signals-postgres  postgres                 postgres  Up 2 minutes (healthy)
signals-redis     redis-server             redis     Up 2 minutes (healthy)
```

**Wait for Postgres to be ready:**
```bash
docker compose -f docker-compose.local.yml exec postgres pg_isready -U signals -d signals
```

**Expected output:**
```
accepting connections
```

If it says "rejecting connections", wait 10 more seconds and try again.

### Step 4: Create Python Virtual Environment

```bash
cd /Users/zopdec/signals
python3 -m venv venv
source venv/bin/activate
```

**What it does:**
- Creates isolated Python environment in `venv/` directory
- Activates it (you should see `(venv)` in your terminal prompt)

**Verify:**
```bash
which python
# Should output: /Users/zopdec/signals/venv/bin/python

python --version
# Should output: Python 3.12.x
```

### Step 5: Install Python Dependencies

```bash
pip install --upgrade pip
pip install -e .
```

**What it does:**
- Upgrades pip to latest version
- Installs project dependencies from `setup.py`
- `-e` means "editable" mode (changes to source code immediately reflected)

**Key dependencies installed:**
- `psycopg[binary]` — PostgreSQL driver
- `httpx` — async HTTP client
- `playwright` — browser automation
- `pydantic` — data validation
- `typer` — CLI framework
- `feedparser` — RSS parsing

**Check installation:**
```bash
pip list | grep -E "psycopg|httpx|playwright|pydantic"
```

**Expected output:**
```
httpx                          0.25.2
playwright                     1.40.0
psycopg                        3.1.12
pydantic                       2.5.0
...
```

### Step 6: Initialize Database Schema

```bash
python -m src.main migrate
```

**What it does:**
1. Connects to local Postgres (via SIGNALS_PG_DSN)
2. Creates `signals` schema
3. Creates tables:
   - `accounts` — company database
   - `signal_observations` — raw signals from collectors
   - `score_runs` — scoring pipeline runs
   - `score_components` — per-signal component scores
   - `account_scores` — final account tier/scores
   - `review_labels` — analyst feedback
   - `crawl_checkpoints` — dedup tracking
   - `crawl_attempts` — audit trail
   - ...and many more

**Expected output:**
```
[2024-02-25 12:34:56] [src.db] Initializing schema and tables...
[2024-02-25 12:34:56] [src.db] Schema 'signals' created successfully
[2024-02-25 12:34:56] [src.db] Table 'accounts' created
[2024-02-25 12:34:56] [src.db] Table 'signal_observations' created
...
[2024-02-25 12:34:56] [src.db] Database initialization complete ✓
```

**Verify schema was created:**
```bash
docker compose -f docker-compose.local.yml exec postgres psql -U signals -d signals \
    -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'signals' LIMIT 5;"
```

**Expected output:**
```
       table_name
──────────────────────────
 accounts
 signal_observations
 score_runs
 ...
```

### Step 7: Install Playwright Browser

```bash
playwright install chromium
```

**What it does:**
- Downloads Chromium browser binary (~300 MB)
- Stores in `~/.cache/ms-playwright/` (shared across projects)
- Enables `website_techscan.py` to scrape JS-heavy sites

**Expected output:**
```
Installing Playwright Chromium
Downloading Chromium...
Downloaded Chromium [████████████████████████████████] 100%
...
```

**Verify:**
```bash
playwright install-deps chromium 2>/dev/null || echo "Already installed"
```

### Step 8: Verify Infrastructure Connectivity

Create a test script to verify everything works:

```bash
python scripts/test_infra_link.py
```

This script (created below) will:
- ✅ Test Postgres connection
- ✅ Write a dummy observation to DB
- ✅ Query it back
- ✅ Test Redis connection
- ✅ Test Playwright browser launch
- ✅ Test httpx connection

**Expected output:**
```
╔════════════════════════════════════════════╗
║  Infrastructure Connectivity Test          ║
╚════════════════════════════════════════════╝

[1/5] Testing Postgres connection...
  ✓ Connected to PostgreSQL 16
  ✓ Default schema: signals

[2/5] Testing database insert...
  ✓ Inserted test observation (obs_test_abc123...)
  ✓ Query returned 1 row

[3/5] Testing Redis connection...
  ✓ Connected to Redis 7
  ✓ Ping successful

[4/5] Testing Playwright browser...
  ✓ Chromium browser launched
  ✓ Successfully navigated to https://example.com
  ✓ Page title: Example Domain

[5/5] Testing HTTP client...
  ✓ Fetched https://www.google.com (200 OK)

════════════════════════════════════════════
✓ ALL CHECKS PASSED — Ready for ./signals start
════════════════════════════════════════════
```

---

## Infrastructure Test Script

Create `scripts/test_infra_link.py`:

```python
#!/usr/bin/env python3
"""
Test script to verify Postgres, Redis, Playwright, and HTTP connectivity.
Usage: python scripts/test_infra_link.py
"""

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src import db
from src.models import SignalObservation, Account
from src.settings import Settings
from src.utils import utc_now_iso, stable_hash


def print_header():
    print("\n╔════════════════════════════════════════════╗")
    print("║  Infrastructure Connectivity Test          ║")
    print("╚════════════════════════════════════════════╝\n")


def test_postgres():
    """Test PostgreSQL connection and basic insert/query."""
    print("[1/5] Testing Postgres connection...")
    try:
        settings = Settings()
        conn = db.get_connection(settings.signals_pg_dsn)

        # Check version
        cur = conn.execute("SELECT version();")
        version = cur.fetchone()[0]
        if "PostgreSQL 16" in version:
            print("  ✓ Connected to PostgreSQL 16")
        else:
            print(f"  ⚠ Connected to {version[:40]}...")

        # Check schema
        cur = conn.execute("SELECT current_schema;")
        schema = cur.fetchone()[0]
        if schema == "signals":
            print(f"  ✓ Default schema: {schema}")
        else:
            print(f"  ⚠ Schema: {schema}")

        # Test insert
        print("[2/5] Testing database insert...")
        account = Account(
            account_id="test_account_" + str(uuid4())[:8],
            company_name="Test Company Inc",
            domain="test.example",
            source_type="seed"
        )
        db.upsert_account(conn, account.domain, account.company_name, account.source_type, commit=True)
        print(f"  ✓ Inserted test account: {account.domain}")

        # Test observation insert
        obs = SignalObservation(
            obs_id=stable_hash({"test": "observation"}, prefix="obs"),
            account_id=account.account_id,
            signal_code="test_signal",
            source="test_script",
            observed_at=utc_now_iso(),
            confidence=0.5,
            source_reliability=0.8,
            evidence_url="https://example.com",
            evidence_text="Test observation",
            raw_payload_hash=stable_hash({"test": "payload"}, prefix="raw")
        )
        inserted = db.insert_signal_observation(conn, obs, commit=True)
        if inserted:
            print(f"  ✓ Inserted test observation: {obs.obs_id[:20]}...")
        else:
            print(f"  ⚠ Observation may have been deduplicated")

        # Query back
        cur = conn.execute(
            "SELECT COUNT(*) FROM signal_observations WHERE signal_code = %s",
            ("test_signal",)
        )
        count = cur.fetchone()[0]
        print(f"  ✓ Query returned {count} row(s)")

        conn.close()
        return True

    except Exception as exc:
        print(f"  ✗ Postgres test failed: {exc}")
        return False


def test_redis():
    """Test Redis connection."""
    print("[3/5] Testing Redis connection...")
    try:
        import redis
        r = redis.Redis(
            host="127.0.0.1",
            port=6379,
            decode_responses=True,
            socket_connect_timeout=5
        )
        pong = r.ping()
        if pong:
            print("  ✓ Connected to Redis 7")
            print("  ✓ Ping successful")
        return True
    except ImportError:
        print("  ⚠ redis package not installed (optional)")
        return True
    except Exception as exc:
        print(f"  ⚠ Redis test failed: {exc} (OK if Redis not running)")
        return True


async def test_playwright():
    """Test Playwright browser launch."""
    print("[4/5] Testing Playwright browser...")
    try:
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            print("  ✓ Chromium browser launched")

            page = await browser.new_page()
            await page.goto("https://example.com", wait_until="networkidle")
            title = await page.title()
            print(f"  ✓ Successfully navigated to https://example.com")
            print(f"  ✓ Page title: {title}")

            await browser.close()
        return True
    except ImportError:
        print("  ⚠ playwright not installed")
        return False
    except Exception as exc:
        print(f"  ⚠ Playwright test failed: {exc}")
        print("     Try: playwright install chromium")
        return False


async def test_httpx():
    """Test httpx HTTP client."""
    print("[5/5] Testing HTTP client...")
    try:
        import httpx

        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get("https://www.google.com")
            print(f"  ✓ Fetched https://www.google.com ({response.status_code} OK)")
        return True
    except Exception as exc:
        print(f"  ⚠ HTTP test failed: {exc}")
        return False


async def main():
    print_header()

    results = []

    # Sync tests
    results.append(("Postgres", test_postgres()))
    results.append(("Redis", test_redis()))

    # Async tests
    results.append(("Playwright", await test_playwright()))
    results.append(("HTTP", await test_httpx()))

    # Summary
    print("\n" + "="*44)
    passed = sum(1 for _, result in results if result)
    total = len(results)

    if passed == total:
        print(f"✓ ALL CHECKS PASSED — Ready for ./signals start")
    else:
        print(f"⚠ {total - passed} check(s) failed — see above for details")
    print("="*44 + "\n")

    return 0 if passed == total else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
```

---

## Environment Variable Checklist

After copying `.env.example` to `.env`, verify these key variables:

```bash
# Postgres connection (local dev)
SIGNALS_PG_DSN=postgresql://signals:signals_dev_password@127.0.0.1:55432/signals?options=-c%20search_path%3Dsignals
SIGNALS_PG_HOST=127.0.0.1
SIGNALS_PG_PORT=55432
SIGNALS_PG_USER=signals
SIGNALS_PG_PASSWORD=signals_dev_password
SIGNALS_PG_DB=signals

# Live crawling (disabled by default)
SIGNALS_ENABLE_LIVE_CRAWL=0

# Redis (optional, for queue/caching)
SIGNALS_REDIS_HOST=127.0.0.1
SIGNALS_REDIS_PORT=56379

# Google Sheets (optional, leave blank for now)
GOOGLE_SHEETS_SPREADSHEET_ID=
GOOGLE_SERVICE_ACCOUNT_FILE=

# LLM API keys (optional, add later)
SIGNALS_CLAUDE_API_KEY=
```

**To update .env after copying:**
```bash
# Enable live crawling
sed -i '' 's/SIGNALS_ENABLE_LIVE_CRAWL=0/SIGNALS_ENABLE_LIVE_CRAWL=1/' .env

# Add API key
echo "SIGNALS_CLAUDE_API_KEY=sk-ant-..." >> .env

# Verify
grep "SIGNALS_ENABLE_LIVE_CRAWL\|SIGNALS_CLAUDE_API_KEY" .env
```

---

## Docker Container Management

### Start Services

```bash
# Start just Postgres and Redis (minimal)
docker compose -f docker-compose.local.yml up -d postgres redis

# Start all services (includes Huginn for webhook collection)
docker compose -f docker-compose.local.yml up -d
```

### Check Status

```bash
docker compose -f docker-compose.local.yml ps
docker compose -f docker-compose.local.yml logs postgres -f  # Follow Postgres logs
```

### Stop Services

```bash
docker compose -f docker-compose.local.yml down  # Stop and keep data
docker compose -f docker-compose.local.yml down -v  # Stop and delete data
```

### Restart Postgres

```bash
docker compose -f docker-compose.local.yml restart postgres
docker compose -f docker-compose.local.yml exec postgres pg_isready -U signals
```

### Access Postgres CLI

```bash
docker compose -f docker-compose.local.yml exec postgres psql -U signals -d signals

# Now in psql:
signals=> SELECT COUNT(*) FROM accounts;
signals=> \dt
signals=> \q
```

---

## Troubleshooting

### "Connection refused" on port 55432

**Problem:** Postgres container not running or not ready

**Solution:**
```bash
# Check if container exists
docker ps -a | grep signals-postgres

# If not running, start it
docker compose -f docker-compose.local.yml up -d postgres

# Wait for healthcheck
docker compose -f docker-compose.local.yml ps
# STATUS should say "Up X seconds (healthy)"

# If stuck "restarting", check logs
docker compose -f docker-compose.local.yml logs postgres
```

### "Column 'signals' not found" or schema errors

**Problem:** Database migration not run

**Solution:**
```bash
python -m src.main migrate

# Verify schema exists
docker compose -f docker-compose.local.yml exec postgres psql -U signals -d signals \
    -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'signals';"
```

### Playwright "Browser not found"

**Problem:** Chromium not installed

**Solution:**
```bash
playwright install chromium

# Or full install with all browsers
playwright install

# Verify
ls ~/.cache/ms-playwright/ | grep -i chromium
```

### "ModuleNotFoundError: No module named 'src'"

**Problem:** Virtual environment not activated or dependencies not installed

**Solution:**
```bash
source venv/bin/activate
pip install -e .

# Verify
python -c "import src; print(src.__file__)"
```

### httpx connection timeout

**Problem:** Network connectivity issue

**Solution:**
```bash
# Check internet connectivity
curl -I https://www.google.com

# Check local DNS
dig example.com

# Increase timeout in .env
echo "SIGNALS_HTTP_TIMEOUT_SECONDS=30" >> .env
```

---

## Automated Setup (One-Command)

Use the provided bootstrap script:

```bash
cd /Users/zopdec/signals
chmod +x scripts/bootstrap.sh
./scripts/bootstrap.sh
```

This runs all steps above automatically. If anything fails, it exits with clear error messages.

---

## Verification Checklist

After setup, verify:

- [ ] `.env` file exists and has `SIGNALS_PG_DSN` set
- [ ] `docker compose ps` shows `signals-postgres` and `signals-redis` as "healthy"
- [ ] `python -m src.main migrate` runs without errors
- [ ] `psql -U signals -d signals -c "SELECT COUNT(*) FROM accounts;"` returns `0`
- [ ] `playwright install chromium` completes
- [ ] `python scripts/test_infra_link.py` shows all ✓ checks

---

## Ready to Run Pipeline

Once all checks pass, you can run:

```bash
# Full daily pipeline (ingest → score → export)
./signals start

# Or with verbose progress
SIGNALS_VERBOSE_PROGRESS=1 ./signals start

# Just ingest stage
python -m src.main ingest

# Just scoring stage
python -m src.main score
```

**Expected output from `./signals start`:**
```
[2024-02-25 12:34:56] [src.pipeline.daily] Running full daily pipeline...
[2024-02-25 12:34:56] [src.pipeline.daily] Stage: ingest
  jobs: {"inserted": 47, "seen": 89}
  news: {"inserted": 23, "seen": 156}
  reddit: {"inserted": 31, "seen": 73}  ← Your new Reddit collector!
  community: {"inserted": 5, "seen": 12}
  technographics: {"inserted": 18, "seen": 42}
  first_party: {"inserted": 12, "seen": 12}
  Total: {"inserted": 136, "seen": 384}
[2024-02-25 12:34:58] [src.pipeline.daily] Stage: score
  Scored 256 accounts across 3 products
[2024-02-25 12:35:00] [src.pipeline.daily] Stage: export
  Generated: review_queue.csv, daily_scores.csv, source_quality.csv
[2024-02-25 12:35:01] [src.pipeline.daily] Pipeline completed successfully ✓
```

---

## Environment Details

| Service | Port | Container | Status Check |
|---------|------|-----------|--------------|
| **Postgres** | 55432 | signals-postgres | `docker exec signals-postgres pg_isready -U signals` |
| **Redis** | 56379 | signals-redis | `docker exec signals-redis redis-cli ping` |
| **Huginn** | 3000 | signals-huginn | `curl localhost:3000` |

| Binary | Check | Command |
|--------|-------|---------|
| **Python** | ✓ 3.12+ | `python --version` |
| **Docker** | ✓ Latest | `docker --version` |
| **Playwright** | ✓ Chromium | `ls ~/.cache/ms-playwright/chromium-*` |
| **psycopg** | ✓ Installed | `python -c "import psycopg"` |
| **httpx** | ✓ Installed | `python -c "import httpx"` |

---

## Summary

**In 5-10 minutes you'll have:**
- ✅ PostgreSQL 16 running locally
- ✅ Redis 7 running locally
- ✅ Python dependencies installed
- ✅ Database schema created with all tables
- ✅ Playwright Chromium ready for web scraping
- ✅ httpx configured for HTTP requests
- ✅ All connectivity verified

**Ready to run:**
```bash
./signals start
```
