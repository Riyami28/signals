# Complete Setup Guide — Zopdev Signals with Reddit Collector

**Purpose:** Full end-to-end setup with actual data flow and Reddit collector testing

**Prerequisites:**
- macOS or Linux (Windows: use WSL2)
- Docker installed ([download](https://www.docker.com/products/docker-desktop))
- Python 3.12+ ([download](https://www.python.org/downloads/) or use `brew install python@3.12`)
- Git
- Internet connection

---

## Part 1: Docker & Database Setup (10 minutes)

### Step 1: Install Docker

**macOS:**
```bash
# Option A: Download Docker Desktop
# https://www.docker.com/products/docker-desktop

# Option B: Using Homebrew
brew install docker docker-compose

# Verify
docker --version
docker compose --version
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get update
sudo apt-get install docker.io docker-compose-plugin

# Add user to docker group
sudo usermod -aG docker $USER
newgrp docker

# Verify
docker --version
```

**Windows (WSL2):**
```bash
# Install Docker Desktop with WSL2 backend
# https://docs.docker.com/desktop/install/windows-install/
```

### Step 2: Create Data Directories

```bash
cd /Users/zopdec/signals
mkdir -p data/local/postgres data/local/redis
```

### Step 3: Start PostgreSQL & Redis

```bash
docker compose -f docker-compose.local.yml up -d postgres redis

# Verify containers are running
docker compose -f docker-compose.local.yml ps
```

**Expected output:**
```
NAME              STATUS
signals-postgres  Up 5 seconds (healthy)
signals-redis     Up 5 seconds (healthy)
```

### Step 4: Verify Database Connection

```bash
# Wait a few seconds for Postgres to be fully ready
sleep 10

# Check connection
docker compose -f docker-compose.local.yml exec postgres \
  pg_isready -U signals -d signals

# Expected: "accepting connections"
```

---

## Part 2: Python Environment (5 minutes)

### Step 1: Install Python 3.12+

**macOS:**
```bash
# Using Homebrew
brew install python@3.12

# Verify
python3.12 --version
# Expected: Python 3.12.x or higher
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get install python3.12 python3.12-venv python3.12-dev

# Verify
python3.12 --version
```

**macOS/Linux (Using pyenv):**
```bash
# Install pyenv
curl https://pyenv.run | bash

# Install Python 3.12
pyenv install 3.12.0
pyenv global 3.12.0

# Verify
python3 --version  # Should show 3.12.x
```

### Step 2: Create Virtual Environment

```bash
cd /Users/zopdec/signals

# Create venv with Python 3.12
python3.12 -m venv venv

# Activate
source venv/bin/activate

# Verify (you should see (venv) in your prompt)
which python
# Should output: /Users/zopdec/signals/venv/bin/python

python --version
# Should output: Python 3.12.x
```

### Step 3: Install Dependencies

```bash
# Make sure venv is activated
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install project dependencies
pip install -e .

# Verify key packages installed
pip list | grep -E "psycopg|httpx|playwright|pydantic|typer"
```

**Expected packages:**
- psycopg (PostgreSQL driver)
- httpx (HTTP client)
- playwright (browser automation)
- pydantic (data validation)
- typer (CLI framework)
- feedparser (RSS parsing)

---

## Part 3: Database Initialization (2 minutes)

### Step 1: Initialize Schema

```bash
# Make sure venv is activated
source venv/bin/activate

# Run migration
python -m src.main migrate

# Expected output:
# [timestamp] Initializing schema and tables...
# [timestamp] Table 'accounts' created
# [timestamp] Table 'signal_observations' created
# ... more tables ...
# [timestamp] Database initialization complete ✓
```

### Step 2: Verify Schema Was Created

```bash
# Check tables were created
docker compose -f docker-compose.local.yml exec postgres \
  psql -U signals -d signals -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'signals';"

# Expected: Should show 20+ tables
```

### Step 3: Install Playwright

```bash
# Chromium browser for web scraping
playwright install chromium

# This downloads ~300MB, takes 1-2 minutes
# Verify it installed
ls ~/.cache/ms-playwright/ | grep chromium
```

---

## Part 4: Verify Infrastructure (2 minutes)

```bash
# Make sure venv is activated
source venv/bin/activate

# Run comprehensive test
python scripts/test_infra_link.py

# Expected output:
# ╔════════════════════════════════════════════╗
# ║  Infrastructure Connectivity Test          ║
# ╚════════════════════════════════════════════╝
#
# [1/6] Testing Settings
#   ✓ SIGNALS_PG_DSN set
#   ...
#
# [2/6] Testing Postgres Connection
#   ✓ Connected to PostgreSQL 16
#   ...
#
# ... more tests ...
#
# ════════════════════════════════════════════
# ✓ ALL CHECKS PASSED — Ready for ./signals start
# ════════════════════════════════════════════
```

If all checks pass, **you're ready to run the pipeline!** 🎉

---

## Part 5: Running the Full Pipeline (Real Data Test)

### Step 1: Create Seed Data (Optional but Recommended)

Create a test file `config/test_accounts.csv` with real companies:

```csv
company_name,domain,source_type
Microsoft,microsoft.com,seed
Google,google.com,seed
Amazon,amazon.com,seed
Stripe,stripe.com,seed
GitLab,gitlab.com,seed
Atlassian,atlassian.com,seed
```

The pipeline will automatically ingest these on first run.

### Step 2: Enable Live Crawling (Optional)

Edit `.env` to enable fetching live data:

```bash
# Edit .env
sed -i '' 's/SIGNALS_ENABLE_LIVE_CRAWL=0/SIGNALS_ENABLE_LIVE_CRAWL=1/' .env

# Verify
grep SIGNALS_ENABLE_LIVE_CRAWL .env
# Should show: SIGNALS_ENABLE_LIVE_CRAWL=1
```

### Step 3: Run the Full Pipeline

```bash
# Make sure venv is activated
source venv/bin/activate

# Run with verbose output to see what's happening
SIGNALS_VERBOSE_PROGRESS=1 ./signals start

# Expected output shows:
# [INGEST STAGE]
#   jobs: {"inserted": 47, "seen": 89}
#   news: {"inserted": 23, "seen": 156}
#   reddit: {"inserted": 31, "seen": 73}     ← Your Reddit collector!
#   community: {"inserted": 5, "seen": 12}
#   technographics: {"inserted": 18, "seen": 42}
#   first_party: {"inserted": 12, "seen": 12}
#
# [SCORE STAGE]
#   Scored 256 accounts
#
# [EXPORT STAGE]
#   Generated review_queue.csv, daily_scores.csv
```

### Step 4: Check Output Files

```bash
# View generated CSV files
ls -lh data/out/

# Expected files:
# -rw-r--r--  review_queue.csv          (accounts ready for sales)
# -rw-r--r--  daily_scores.csv          (all accounts with scores)
# -rw-r--r--  source_quality.csv        (signal source metrics)
```

### Step 5: Inspect the Data

```bash
# View review queue (highest-value accounts)
head -10 data/out/review_queue.csv

# View daily scores
head -10 data/out/daily_scores.csv

# Check how many observations Reddit collector inserted
docker compose -f docker-compose.local.yml exec postgres \
  psql -U signals -d signals -c \
  "SELECT source, COUNT(*) as count FROM signal_observations GROUP BY source ORDER BY count DESC;"

# Expected output includes:
# reddit_api | 147
# jobs_csv   | 89
# news_csv   | 234
# ... etc
```

---

## Part 6: Reddit Collector Deep-Dive (Testing Specific Collector)

### Test 1: Run Ingest Only (See Reddit Collector in Action)

```bash
source venv/bin/activate

# Run just the ingest stage
python -m src.main ingest

# Watch for reddit_collector output:
# [2024-03-03 12:34:56] [src.collectors.reddit_collector] Fetching reddit signals...
# [2024-03-03 12:34:57] [src.pipeline.ingest] reddit: {"inserted": 31, "seen": 73}
```

### Test 2: Check Reddit Observations in Database

```bash
docker compose -f docker-compose.local.yml exec postgres \
  psql -U signals -d signals << 'EOF'

-- Count Reddit observations
SELECT
  source,
  COUNT(*) as count,
  COUNT(DISTINCT signal_code) as unique_signals
FROM signal_observations
WHERE source LIKE 'reddit%'
GROUP BY source;

-- View sample Reddit observations
SELECT
  account_id,
  signal_code,
  source,
  observed_at,
  confidence,
  evidence_text
FROM signal_observations
WHERE source LIKE 'reddit%'
LIMIT 5;

-- Check signal distribution
SELECT
  signal_code,
  COUNT(*) as count,
  AVG(confidence) as avg_confidence
FROM signal_observations
WHERE source LIKE 'reddit%'
GROUP BY signal_code
ORDER BY count DESC;

EOF
```

### Test 3: Verify Reddit Signals Are Being Scored

```bash
docker compose -f docker-compose.local.yml exec postgres \
  psql -U signals -d signals << 'EOF'

-- Check if reddit observations contributed to scores
SELECT
  product,
  COUNT(DISTINCT account_id) as accounts_scored,
  SUM(component_score) as total_contribution
FROM score_components
WHERE signal_code IN (
  SELECT DISTINCT signal_code
  FROM signal_observations
  WHERE source LIKE 'reddit%'
)
GROUP BY product;

-- View top accounts influenced by Reddit signals
SELECT
  a.company_name,
  a.domain,
  COUNT(DISTINCT so.obs_id) as reddit_signals,
  ROUND(SUM(sc.component_score)::numeric, 2) as reddit_contribution,
  acs.score as total_score,
  acs.tier_v2
FROM accounts a
LEFT JOIN signal_observations so ON a.account_id = so.account_id AND so.source LIKE 'reddit%'
LEFT JOIN score_components sc ON sc.account_id = a.account_id AND sc.signal_code IN (
  SELECT DISTINCT signal_code FROM signal_observations WHERE source LIKE 'reddit%'
)
LEFT JOIN account_scores acs ON acs.account_id = a.account_id AND acs.product = sc.product
WHERE so.obs_id IS NOT NULL
GROUP BY a.account_id, a.company_name, a.domain, acs.score, acs.tier_v2
ORDER BY reddit_signals DESC
LIMIT 10;

EOF
```

---

## Part 7: Monitoring & Debugging

### Check Pipeline Logs

```bash
# View logs of last run
tail -50 /var/log/signals.log

# Or run with logging to see real-time
SIGNALS_LOG_LEVEL=DEBUG ./signals start 2>&1 | tee pipeline_run.log
```

### Access PostgreSQL Directly

```bash
# Connect to database
docker compose -f docker-compose.local.yml exec postgres \
  psql -U signals -d signals

# Now you're in psql interactive shell:
signals=> SELECT COUNT(*) FROM accounts;
signals=> SELECT COUNT(*) FROM signal_observations;
signals=> SELECT DISTINCT source FROM signal_observations ORDER BY source;
signals=> \q
```

### Check Docker Logs

```bash
# Postgres logs
docker compose -f docker-compose.local.yml logs postgres

# Redis logs
docker compose -f docker-compose.local.yml logs redis

# Follow logs in real-time
docker compose -f docker-compose.local.yml logs -f postgres
```

### Restart Services

```bash
# Restart Postgres (if needed)
docker compose -f docker-compose.local.yml restart postgres

# Wait for it to be healthy
docker compose -f docker-compose.local.yml exec postgres \
  pg_isready -U signals -d signals
```

---

## Part 8: Customization

### Add Custom Seed Accounts

Edit `config/seed_accounts.csv` (or create if doesn't exist):

```csv
company_name,domain,source_type
Your Company A,yourcompanya.com,seed
Your Company B,yourcompanyb.com,seed
```

On next pipeline run, these will be ingested.

### Add Custom Keywords for Reddit

Edit `config/keyword_lexicon.csv` and add rows like:

```csv
source,signal_code,keyword,confidence
community,cost_optimization,cost reduction,0.6
community,cost_optimization,kubernetes cost,0.7
community,hiring_mention,hiring,0.5
community,platform_mention,platform engineering,0.65
```

On next run, Reddit posts matching these keywords will be scored differently.

### Adjust Reddit Collector Settings

Edit `.env`:

```bash
# Max accounts to crawl per source
SIGNALS_LIVE_MAX_ACCOUNTS=1000

# HTTP timeout for Reddit API
SIGNALS_HTTP_TIMEOUT_SECONDS=15

# Workers per source (concurrent requests)
SIGNALS_LIVE_WORKERS_PER_SOURCE=auto
```

### Monitor Reddit Collector Performance

```bash
# Count observations by source (Reddit vs others)
docker compose -f docker-compose.local.yml exec postgres \
  psql -U signals -d signals -c \
  "SELECT source, COUNT(*) as observations FROM signal_observations GROUP BY source ORDER BY observations DESC;"

# Time how long ingest takes
time python -m src.main ingest

# Profile which collectors are slowest
SIGNALS_VERBOSE_PROGRESS=1 ./signals start 2>&1 | grep -E "stage=|duration"
```

---

## Complete System Flow (What Happens on `./signals start`)

```
1. LOAD CONFIGURATION
   ├─ Load signal_registry.csv (signal definitions + base weights)
   ├─ Load source_registry.csv (source reliability scores)
   ├─ Load keyword_lexicon.csv (keywords → signals)
   ├─ Load source_execution_policy.csv (which sources enabled)
   └─ Load settings from .env (database, timeouts, workers)

2. INGEST STAGE (Collectors run sequentially)
   ├─ jobs.collect()
   │  ├─ Phase 1: Load jobs.csv → classify → insert observations
   │  ├─ Phase 2: Fetch Greenhouse/Lever/Ashby APIs → classify → insert
   │  └─ Return {"inserted": 47, "seen": 89}
   ├─ news.collect()
   │  ├─ Phase 1: Load news.csv → classify → insert
   │  ├─ Phase 2: Fetch Google News RSS → classify → insert
   │  └─ Return {"inserted": 23, "seen": 156}
   ├─ reddit_collector.collect()  ← Your Reddit collector!
   │  ├─ Phase 1: Load community.csv (Reddit data) → classify → insert
   │  ├─ Phase 2: Search Reddit RSS API → classify → insert
   │  └─ Return {"inserted": 31, "seen": 73}
   ├─ [... other collectors ...]
   └─ TOTAL: {"inserted": 136, "seen": 384}

3. SCORE STAGE
   ├─ For each observation:
   │  ├─ Get signal rule from signal_registry
   │  ├─ Calculate component = base_weight × confidence × source_reliability × recency_decay
   │  └─ Store ComponentScore
   ├─ Aggregate components by (account, product, dimension)
   ├─ Normalize dimensions and apply weights
   ├─ Classify into tiers (tier_1 ≥80, tier_2 ≥60, tier_3 ≥40, tier_4 <40)
   └─ Store AccountScore

4. EXPORT STAGE
   ├─ Write review_queue.csv (tier_1 + tier_2 accounts)
   ├─ Write daily_scores.csv (all accounts with scores)
   └─ Write source_quality.csv (signal counts by source)

5. QUALITY STAGE
   ├─ Calculate ICP coverage (% of known customers scored)
   ├─ Calculate precision (true positive rate)
   └─ Log metrics

6. OPS STAGE
   └─ Log operational metrics to database
```

---

## Troubleshooting

### Docker Issues

| Error | Solution |
|-------|----------|
| `docker: command not found` | Install Docker Desktop or use `brew install docker` |
| `Cannot connect to Docker daemon` | Start Docker Desktop (macOS) or `sudo systemctl start docker` (Linux) |
| `Port 55432 already in use` | Change port in docker-compose.local.yml or kill existing process: `lsof -i :55432 \| grep LISTEN \| awk '{print $2}' \| xargs kill -9` |
| `Postgres not ready` | Wait longer: `sleep 20` then check: `docker compose ps` |

### Python Issues

| Error | Solution |
|-------|----------|
| `ModuleNotFoundError: No module named 'src'` | Activate venv: `source venv/bin/activate` |
| `Python 3.12 not found` | Install: `brew install python@3.12` or use pyenv |
| `psycopg: connection failed` | Check `.env` has correct `SIGNALS_PG_DSN` |
| `ImportError: No module named 'playwright'` | Install: `pip install -e .` |

### Database Issues

| Error | Solution |
|-------|----------|
| `database "signals" does not exist` | Run: `python -m src.main migrate` |
| `relation "accounts" does not exist` | Same as above |
| `column "signals" does not exist` | Same as above, schema not created |

---

## Quick Command Reference

```bash
# Activate venv (do this first!)
source venv/bin/activate

# Start Docker services
docker compose -f docker-compose.local.yml up -d postgres redis

# Stop Docker services
docker compose -f docker-compose.local.yml down

# Initialize database
python -m src.main migrate

# Run full pipeline
./signals start

# Run just ingest (collectors)
python -m src.main ingest

# Run just scoring
python -m src.main score

# Run just export
python -m src.main export

# View database
docker compose -f docker-compose.local.yml exec postgres psql -U signals -d signals

# Check Docker status
docker compose -f docker-compose.local.yml ps

# View Docker logs
docker compose -f docker-compose.local.yml logs postgres -f

# Run tests
python scripts/test_infra_link.py

# View output
ls -la data/out/
```

---

## Success Indicators

After setup, you should see:

✅ Docker containers running (`docker compose ps` shows healthy)
✅ PostgreSQL accessible (`psql -U signals -d signals -c "SELECT 1"` returns 1)
✅ Python venv activated (prompt shows `(venv)`)
✅ All packages installed (`pip list` shows psycopg, httpx, playwright, etc.)
✅ Database schema created (`psql -U signals -d signals -c "\dt"` shows 20+ tables)
✅ Pipeline runs without errors (`./signals start` completes)
✅ CSV files generated (`ls data/out/` shows review_queue.csv, daily_scores.csv)
✅ Reddit observations in database (`psql -U signals -d signals -c "SELECT COUNT(*) FROM signal_observations WHERE source LIKE 'reddit%'"` returns > 0)

---

## Next Steps

1. ✅ Follow steps 1-7 above (30-45 minutes total)
2. ✅ Run pipeline: `./signals start`
3. ✅ Check Reddit observations: Query database for source='reddit_*'
4. ✅ Review output CSVs in `data/out/`
5. ✅ Customize keywords in `config/keyword_lexicon.csv`
6. ✅ Add your own companies to `config/seed_accounts.csv`
7. ✅ Enable Google Sheets export if needed
8. ✅ Schedule daily runs via cron

---

## Summary

You now have a **production-ready Zopdev Signals pipeline** with:

✅ PostgreSQL 16 database running in Docker
✅ Redis 7 caching layer
✅ Python 3.12+ environment
✅ All collectors integrated (including Reddit)
✅ Real-time signal ingestion
✅ Automated scoring
✅ CSV export for sales
✅ Full verification tests

**The Reddit collector is working identically to all other collectors** — same architecture, same patterns, same data flow.

**Ready to run:** `./signals start`
