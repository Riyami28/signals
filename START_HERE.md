# START HERE — Zopdev Signals Infrastructure Setup

**Status:** ✅ All files ready. Choose your setup method below.

---

## 🚀 Fastest Way (One Command)

This runs all setup steps automatically:

```bash
bash /Users/zopdec/signals/RUN_INFRASTRUCTURE_SETUP.sh
```

**What it does in 5-10 minutes:**
1. ✓ Copies .env configuration
2. ✓ Creates data directories
3. ✓ Starts Docker (Postgres 16, Redis 7)
4. ✓ Creates Python virtual environment
5. ✓ Installs dependencies (psycopg, httpx, playwright, etc.)
6. ✓ Initializes database schema
7. ✓ Installs Playwright Chromium
8. ✓ Verifies all connections work

**Expected output:**
```
✓ SETUP COMPLETE
✓ ALL CHECKS PASSED — Ready for ./signals start
```

---

## 📋 Step-by-Step Way (Manual Control)

Copy and paste these commands one by one:

```bash
# 1. Navigate to project
cd /Users/zopdec/signals

# 2. Copy environment configuration
cp .env.example .env

# 3. Create data directories
mkdir -p data/local/postgres data/local/redis

# 4. Start Docker
docker compose -f docker-compose.local.yml up -d postgres redis

# 5. Wait for Postgres (optional but recommended)
sleep 10

# 6. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 7. Install dependencies
pip install --upgrade pip
pip install -e .

# 8. Initialize database
python -m src.main migrate

# 9. Install Playwright
playwright install chromium

# 10. Verify everything works
python scripts/test_infra_link.py
```

**Time required:** 5-10 minutes
**What you get:** Same result as one-command setup

---

## 📚 Detailed Understanding Way (Learn Everything)

Read the comprehensive guides in order:

1. **QUICK_START.md** (5 min read)
   - Copy-paste commands
   - What each step does
   - Expected outputs

2. **SETUP_INFRASTRUCTURE.md** (20 min read)
   - Why each component is needed
   - How to verify everything
   - Troubleshooting guide
   - Docker management
   - Environment variables

3. **INFRASTRUCTURE_SETUP_COMPLETE.md** (10 min read)
   - Overview of all files created
   - Component details
   - Running the pipeline
   - Next steps

---

## ✅ After Setup Completes

When you see `✓ ALL CHECKS PASSED`, you're ready to run the pipeline:

```bash
# Make sure you're in the virtual environment
source venv/bin/activate

# Run the full pipeline
./signals start
```

**Expected output:**
```
[timestamp] Pipeline stage: ingest
  jobs: {"inserted": 47, "seen": 89}
  news: {"inserted": 23, "seen": 156}
  reddit: {"inserted": 31, "seen": 73}     ← Your new Reddit collector!
  community: {"inserted": 5, "seen": 12}
  technographics: {"inserted": 18, "seen": 42}
  first_party: {"inserted": 12, "seen": 12}

[timestamp] Pipeline stage: score
  Scored 256 accounts

[timestamp] Pipeline stage: export
  Generated CSV files

✓ Pipeline completed successfully
```

---

## 📂 Files Created for You

### Setup & Verification
- **RUN_INFRASTRUCTURE_SETUP.sh** — One-command setup script
- **QUICK_START.md** — Copy-paste commands
- **SETUP_INFRASTRUCTURE.md** — Detailed guide
- **INFRASTRUCTURE_SETUP_COMPLETE.md** — Complete overview
- **scripts/test_infra_link.py** — Verification test script

### Already In Repo
- **docker-compose.local.yml** — Docker services definition
- **.env.example** — Environment template
- **scripts/bootstrap.sh** — Original bootstrap script
- **src/main.py** — Pipeline CLI with `migrate`, `start`, `score` commands

---

## 🎯 Three Setup Paths

| Method | Time | Best For | Command |
|--------|------|----------|---------|
| **Automatic** | 5-10 min | Getting started fast | `bash RUN_INFRASTRUCTURE_SETUP.sh` |
| **Manual** | 5-10 min | Understanding each step | Follow QUICK_START.md |
| **Detailed** | 15-20 min | Learning everything | Read all docs then setup |

---

## 🔍 What Gets Set Up

### Infrastructure (via Docker)
- **PostgreSQL 16** on port 55432
  - Database: `signals`
  - User: `signals`
  - Password: `signals_dev_password`
- **Redis 7** on port 56379 (optional, for caching/queue)
- **Huginn** on port 3000 (optional, for webhook collection)

### Python Environment
- **Virtual environment** in `venv/`
- **Dependencies:** psycopg, httpx, playwright, pydantic, typer, feedparser, etc.
- **Playwright Chromium** (~300MB, downloaded to ~/.cache/ms-playwright/)

### Database
- **Schema:** `signals`
- **Tables:** accounts, signal_observations, score_runs, score_components, account_scores, review_labels, crawl_checkpoints, etc.
- **Indexes:** Deduplication on (account_id, signal_code, source, observed_at, raw_payload_hash)

### Configuration
- **.env file** with all environment variables
- **config/signal_registry.csv** — Signal definitions
- **config/source_registry.csv** — Source reliability scores
- **config/source_execution_policy.csv** — Collector execution policy
- **config/keyword_lexicon.csv** — Signal keywords

---

## ⚠️ Before You Start

### Required
- ✅ **Python 3.12+** → Check: `python3 --version`
- ✅ **Docker** → Check: `docker --version`
- ✅ **Internet connection** → For downloading dependencies and Playwright

### Optional (Add Later)
- Google Sheets API credentials (for export)
- Claude API key (for LLM research)
- Redis client library (only if using Redis features)

---

## 🆘 Troubleshooting Quick Links

| Issue | Quick Fix |
|-------|-----------|
| Docker not running | Open Docker Desktop |
| Port 55432 in use | Kill existing process: `lsof -i :55432` |
| venv not activating | Run: `source venv/bin/activate` (not `source ./venv/activate`) |
| psycopg errors | Make sure you're in venv and have run `pip install -e .` |
| Playwright errors | Run: `playwright install chromium` |
| "Connection refused" | Docker container not ready; wait 10 seconds and retry |

For more issues, see **SETUP_INFRASTRUCTURE.md** Troubleshooting section.

---

## 📞 Next Steps

### 1. Choose Your Setup Method
- **Option A (Recommended):** `bash RUN_INFRASTRUCTURE_SETUP.sh`
- **Option B (Manual):** Follow QUICK_START.md commands
- **Option C (Learning):** Read all documentation then setup

### 2. Wait for Completion
- Watch for `✓ ALL CHECKS PASSED` message
- Takes 5-10 minutes total

### 3. Customize (Optional)
Edit `.env` to:
- Enable live crawling: `SIGNALS_ENABLE_LIVE_CRAWL=1`
- Add API keys: `SIGNALS_CLAUDE_API_KEY=sk-ant-...`
- Configure Google Sheets export
- Change database connection if needed

### 4. Run the Pipeline
```bash
./signals start
```

### 5. Check Output
```bash
ls -la data/out/
# You'll see: review_queue.csv, daily_scores.csv, source_quality.csv
```

---

## 📖 Documentation Map

**For Getting Started:**
- `START_HERE.md` ← You are here
- `QUICK_START.md` ← Copy-paste commands
- `RUN_INFRASTRUCTURE_SETUP.sh` ← One-command setup

**For Understanding:**
- `SETUP_INFRASTRUCTURE.md` ← Detailed breakdown
- `INFRASTRUCTURE_SETUP_COMPLETE.md` ← Component overview
- `CLAUDE.md` ← Architecture & patterns

**For Reddit Collector:**
- `REDDIT_COLLECTOR_INTEGRATION.md` ← How it fits in pipeline
- `REDDIT_COLLECTOR_STATUS.md` ← Implementation details
- `src/collectors/reddit_collector.py` ← The code

---

## ✨ You're Ready!

Everything has been prepared. Choose your setup method above and you'll be running the pipeline in 5-10 minutes.

**Questions?** Check the appropriate documentation:
- Quick issues → `QUICK_START.md` Troubleshooting
- Deep issues → `SETUP_INFRASTRUCTURE.md` Troubleshooting
- Architecture questions → `CLAUDE.md`

**Let's go! 🚀**
