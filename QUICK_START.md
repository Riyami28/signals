# Quick Start — Zopdev Signals Pipeline

Copy and paste these commands in your terminal to set up the entire infrastructure in 5 minutes.

---

## Step 1: Navigate to Project

```bash
cd /Users/zopdec/signals
```

---

## Step 2: Copy Environment File

```bash
cp .env.example .env
```

**Expected output:** `.env` file created in project root

---

## Step 3: Create Data Directories

```bash
mkdir -p data/local/postgres data/local/redis
```

---

## Step 4: Start Docker Containers

```bash
docker compose -f docker-compose.local.yml up -d postgres redis
```

**Check status:**
```bash
docker compose -f docker-compose.local.yml ps
```

**Expected output:**
```
NAME              STATUS
signals-postgres  Up X seconds (healthy)
signals-redis     Up X seconds (healthy)
```

---

## Step 5: Wait for Postgres (Optional but Recommended)

```bash
# Wait up to 60 seconds for Postgres to be ready
for i in {1..30}; do
  docker compose -f docker-compose.local.yml exec -T postgres \
    pg_isready -U signals -d signals >/dev/null 2>&1 && break
  sleep 2
done

# Verify
docker compose -f docker-compose.local.yml exec postgres \
  pg_isready -U signals -d signals
```

**Expected output:**
```
accepting connections
```

---

## Step 6: Create Python Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

**Verify activation:** You should see `(venv)` in your terminal prompt.

---

## Step 7: Install Python Dependencies

```bash
pip install --upgrade pip
pip install -e .
```

**Expected time:** 30-60 seconds

---

## Step 8: Initialize Database Schema

```bash
python -m src.main migrate
```

**Expected output:**
```
[timestamp] Initializing schema and tables...
[timestamp] Database initialization complete ✓
```

---

## Step 9: Install Playwright

```bash
playwright install chromium
```

**Expected time:** 1-2 minutes (downloads ~300MB)

---

## Step 10: Verify Infrastructure

```bash
python scripts/test_infra_link.py
```

**Expected output:**
```
╔════════════════════════════════════════════╗
║  Infrastructure Connectivity Test          ║
╚════════════════════════════════════════════╝

[1/6] Testing Settings
  ✓ SIGNALS_PG_DSN set
  ✓ Live crawl disabled
  ✓ HTTP timeout: 12s
  ✓ Max accounts: 1000

[2/3] Testing Postgres Connection
  ✓ Connected to PostgreSQL 16
  ✓ Default schema: signals

[3/3] Testing Database Insert
  ✓ Inserted test account: test-abc12345.example
  ✓ Inserted test observation: obs_xyz789...
  ✓ Query returned 1 row(s)

[4/6] Testing Redis Connection
  ✓ Connected to Redis 7
  ✓ Ping successful
  ✓ Set/Get working

[5/6] Testing Playwright Browser
  ✓ Chromium browser launched
  ✓ Navigated to https://example.com
  ✓ Page title: Example Domain

[6/6] Testing HTTP Client
  ✓ Fetched https://www.google.com (200 OK)

════════════════════════════════════════════
✓ ALL CHECKS PASSED — Ready for ./signals start
════════════════════════════════════════════
```

---

## All Done! 🎉

You're now ready to run the pipeline:

```bash
./signals start
```

Or with verbose output:

```bash
SIGNALS_VERBOSE_PROGRESS=1 ./signals start
```

---

## Common Commands

### Check Docker Status
```bash
docker compose -f docker-compose.local.yml ps
```

### View Postgres Logs
```bash
docker compose -f docker-compose.local.yml logs postgres -f
```

### Access Postgres CLI
```bash
docker compose -f docker-compose.local.yml exec postgres \
  psql -U signals -d signals
```

### Stop All Services
```bash
docker compose -f docker-compose.local.yml down
```

### Restart Everything
```bash
docker compose -f docker-compose.local.yml restart
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `Connection refused on port 55432` | Run `docker compose -f docker-compose.local.yml up -d postgres` and wait 10 seconds |
| `ModuleNotFoundError: No module named 'src'` | Run `source venv/bin/activate` to activate virtual environment |
| `Playwright not found` | Run `playwright install chromium` |
| `psycopg: connection failed` | Check Docker is running with `docker ps` |
| `column 'signals' not found` | Run `python -m src.main migrate` to initialize schema |

---

## Next Steps

After setup, customize `.env` for your use case:

### Enable Live Crawling (Optional)
```bash
# Edit .env
sed -i '' 's/SIGNALS_ENABLE_LIVE_CRAWL=0/SIGNALS_ENABLE_LIVE_CRAWL=1/' .env

# Verify
grep SIGNALS_ENABLE_LIVE_CRAWL .env
```

### Add Claude API Key (For LLM Research, Optional)
```bash
# Edit .env and add your key
echo "SIGNALS_CLAUDE_API_KEY=sk-ant-..." >> .env
```

### Add Google Sheets Integration (Optional)
```bash
# Copy your Google Service Account JSON to the project
cp /path/to/service-account.json .service-account.json

# Edit .env
echo "GOOGLE_SHEETS_SPREADSHEET_ID=..." >> .env
echo "GOOGLE_SERVICE_ACCOUNT_FILE=.service-account.json" >> .env
```

---

## Full Documentation

For detailed explanations and troubleshooting, see:
- `SETUP_INFRASTRUCTURE.md` — Comprehensive setup guide with all details
- `REDDIT_COLLECTOR_INTEGRATION.md` — How the Reddit collector integrates
- `REDDIT_COLLECTOR_STATUS.md` — Implementation details and architecture
- `CLAUDE.md` — Project architecture and patterns
