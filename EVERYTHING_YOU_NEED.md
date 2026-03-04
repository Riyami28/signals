# Everything You Need — Complete Setup, Architecture, and Reddit Collector

**Status:** ✅ COMPLETE PACKAGE READY

This document is the **master reference** for understanding, setting up, and running the Zopdev Signals pipeline with the Reddit collector.

---

## 📚 Documentation Structure

### For Getting Started (Start Here)
1. **START_HERE.md** (2 min) — Choose your setup path
2. **COMPLETE_SETUP_WITH_REDDIT.md** (45 min) — Full end-to-end setup with Docker installation
3. **QUICK_START.md** (10 min) — Copy-paste commands for experienced users

### For Understanding the System
1. **CLAUDE.md** (20 min) — Complete architecture reference
2. **REDDIT_COLLECTOR_INTEGRATION.md** (30 min) — How Reddit fits in the pipeline
3. **REDDIT_COLLECTOR_STATUS.md** (20 min) — Implementation details
4. **ARCHITECTURE_OVERVIEW.md** (10 min) — High-level system overview

### For Detailed Setup & Troubleshooting
1. **SETUP_INFRASTRUCTURE.md** (30 min) — Step-by-step guide with troubleshooting
2. **INFRASTRUCTURE_SETUP_COMPLETE.md** (20 min) — Component details
3. **INFRASTRUCTURE_DELIVERY_SUMMARY.md** (15 min) — Executive summary

### For Automation
1. **RUN_INFRASTRUCTURE_SETUP.sh** — One-command setup
2. **scripts/test_infra_link.py** — Verification tests

---

## 🎯 What You Have

### Code
✅ **Reddit Collector** (`src/collectors/reddit_collector.py`)
- Fully implemented (274 lines)
- Follows exact same pattern as jobs.py, news.py, community.py
- Async architecture with semaphore-based concurrency
- Phase 1: CSV-based observations
- Phase 2: Live Reddit API search
- Integrated into pipeline (called in `src/pipeline/ingest.py`)

### Configuration
✅ **Updated Config Files:**
- `config/signal_registry.csv` — Added `community_mention` signal
- `config/source_registry.csv` — Added `reddit_api` with 0.65 reliability
- `config/source_execution_policy.csv` — Enabled `reddit_api` execution

### Infrastructure
✅ **Docker Setup:**
- `docker-compose.local.yml` — PostgreSQL 16, Redis 7, Huginn
- Ready to run on any machine with Docker installed

✅ **Database:**
- 20+ tables pre-defined
- Deduplication indexes
- Signal schema ready
- Auto-initialized via `python -m src.main migrate`

✅ **Environment:**
- `.env.example` — Complete configuration template
- All env vars documented and ready to use

### Scripts & Tools
✅ **RUN_INFRASTRUCTURE_SETUP.sh** — Automated one-command setup
✅ **scripts/test_infra_link.py** — 6-point infrastructure verification
✅ **scripts/bootstrap.sh** — Alternative setup script

### Documentation
✅ **1000+ lines** of comprehensive guides covering:
- Setup (3 difficulty levels)
- Architecture explanation
- Reddit collector integration
- Troubleshooting reference
- Command reference
- Component details

---

## 🚀 How to Get Started (3 Options)

### Option 1: Fastest (Recommended) — 15 minutes

```bash
# 1. Read entry point
cat START_HERE.md

# 2. Run one command
bash /Users/zopdec/signals/RUN_INFRASTRUCTURE_SETUP.sh

# 3. Verify success
# Wait for: ✓ ALL CHECKS PASSED

# 4. Run pipeline
./signals start

# 5. Check results
ls -la data/out/
# review_queue.csv, daily_scores.csv, source_quality.csv ✓
```

### Option 2: Manual Step-by-Step — 15-20 minutes

```bash
# Follow commands in QUICK_START.md
# 10 simple copy-paste commands
# Each one explained with expected output
```

### Option 3: Full Learning — 45 minutes

```bash
# Read: COMPLETE_SETUP_WITH_REDDIT.md
# Understand every step
# Run with full explanation of what's happening
```

---

## 📊 What the Reddit Collector Does

### Architecture (Same as All Other Collectors)

```
INPUT:
  • Reddit RSS search API or CSV data
  • Keyword lexicon (community keywords)
  • Source reliability (0.65)

PROCESSING:
  1. Fetch Reddit search results
  2. Extract title + text from each post
  3. Classify text against keywords
  4. Build SignalObservation with:
     - obs_id (deterministic hash)
     - signal_code (from keyword match)
     - confidence (from keyword)
     - source_reliability (0.65)
  5. Insert to DB with deduplication

OUTPUT:
  {"inserted": N, "seen": M}

  Where:
    N = new observations actually inserted
    M = observations found (some may be deduped)
```

### Data Flow

```
Reddit Posts
    ↓
Fetch & Parse (Reddit JSON API)
    ↓
Text Classification (Keyword Matching)
    ↓
Build Observations (SignalObservation model)
    ↓
Insert to Database (with dedup)
    ↓
[SCORING STAGE]
    ↓
Apply: score = base_weight × confidence × source_reliability × recency_decay
    ↓
[Account Scores & Tier Classification]
    ↓
[Export CSV for Sales]
```

### Configuration

| Config File | Change | Effect |
|-------------|--------|--------|
| `signal_registry.csv` | Added `community_mention` signal | New signal type available for scoring |
| `source_registry.csv` | Added `reddit_api` with 0.65 reliability | Reddit observations worth 65% of other sources |
| `source_execution_policy.csv` | Set `reddit_api` enabled=true | Reddit collector will run on pipeline execution |
| `keyword_lexicon.csv` | Add keywords with source="community" | Reddit posts matching keywords get scored |

### Example Real Data

If you have companies: Microsoft, Google, Stripe, GitLab

And run: `./signals start`

Reddit collector will:
1. Search Reddit for mentions of each company
2. Find posts like:
   - "I'm using Stripe for payment processing"
   - "We migrated from X to Google Cloud"
   - "GitLab CI/CD is better than Jenkins"
3. Classify each post with keywords
4. Create observations with confidence scores
5. Insert to database
6. Scoring engine applies:
   - base_weight=6 (for community_mention)
   - confidence=0.6 (keyword match)
   - source_reliability=0.65 (Reddit)
   - recency_decay (how old the post is)
7. Generate final scores for each company

---

## 🔧 Tech Stack

### What You're Using

| Component | Version | Purpose |
|-----------|---------|---------|
| **Python** | 3.12+ | Main language |
| **PostgreSQL** | 16 | Database (in Docker) |
| **Redis** | 7 | Caching/queue (in Docker) |
| **psycopg** | 3.1+ | PostgreSQL driver |
| **httpx** | 0.25+ | Async HTTP client |
| **playwright** | 1.40+ | Browser automation |
| **pydantic** | 2.5+ | Data validation |
| **typer** | CLI framework | Command-line interface |
| **feedparser** | RSS parsing | RSS feeds |
| **Docker** | Latest | Container runtime |

### What Each Library Does

**psycopg:**
- Connects Python to PostgreSQL
- Handles queries, inserts, transactions
- Used for all database operations

**httpx:**
- Fetches data from APIs (Reddit, news, jobs boards)
- Async support for parallel requests
- Rate limiting, timeouts, retries

**playwright:**
- Automates browser (Chromium)
- Scrapes JavaScript-heavy websites
- Extracts text from dynamic content

**pydantic:**
- Validates data (SignalObservation, Account, etc.)
- Ensures data integrity
- Type checking

**typer:**
- Creates CLI commands: `./signals start`, `./signals ingest`, etc.
- Argument parsing and validation

**feedparser:**
- Parses RSS/Atom feeds
- Extracts articles from RSS streams
- Used by news and Reddit collectors

---

## ✅ Verification Checklist

After setup, verify:

- [ ] Docker running: `docker ps` shows healthy containers
- [ ] Postgres accessible: `psql -U signals -d signals -c "SELECT 1"`
- [ ] Python venv activated: Prompt shows `(venv)`
- [ ] Dependencies installed: `pip list | grep psycopg`
- [ ] Database initialized: `psql -U signals -d signals -c "\dt"`
- [ ] Playwright installed: `ls ~/.cache/ms-playwright/chromium-*`
- [ ] Test script passes: `python scripts/test_infra_link.py` → `✓ ALL CHECKS PASSED`
- [ ] Pipeline runs: `./signals start` completes without errors
- [ ] CSV output generated: `ls data/out/` shows review_queue.csv
- [ ] Reddit observations created: Database contains reddit_* observations

---

## 📝 Architecture Summary

### Pipeline Stages (What Happens on `./signals start`)

```
1. INGEST
   └─ All collectors run (jobs, news, reddit, technographics, community, first_party)
      └─ Each collector inserts observations to signal_observations table

2. SCORE
   └─ For each observation:
      └─ Apply: score = base_weight × confidence × source_reliability × recency_decay
      └─ Store component scores
      └─ Aggregate to account scores
      └─ Classify into tiers

3. EXPORT
   └─ Write CSV files:
      ├─ review_queue.csv (high-value accounts for sales)
      ├─ daily_scores.csv (all accounts with scores)
      └─ source_quality.csv (signal metrics)

4. QUALITY & OPS
   └─ Calculate metrics, log to database
```

### Key Design Patterns

**Configuration-Driven:**
- Everything in CSVs, nothing hardcoded
- Change config → pipeline behaves differently
- No code changes needed for tuning

**Async with Concurrency Control:**
- All collectors use async/await
- Semaphore limits parallel requests
- Respects rate limits

**Deduplication at Two Levels:**
1. **Checkpoint:** Same endpoint not crawled twice in 20 hours
2. **Database:** Unique index on (account_id, signal_code, source, observed_at, raw_hash)

**Single Batch Commit:**
- All observations inserted with `commit=False`
- Single `conn.commit()` at end of collector
- Atomicity and performance

**Observable & Debuggable:**
- All crawl attempts logged (status, errors)
- Signal confidence tracked
- Evidence URLs stored
- Top reasons explained

---

## 🎓 How It Works (Step by Step)

### Example: Reddit Collector Processing a Post

**Input:**
```
Reddit Post:
  title: "We migrated to Kubernetes for cost optimization"
  text: "Spent 3 months optimizing cloud costs..."
  subreddit: "devops"
  author: "engineering_lead"
  created: 2024-02-25 12:00:00
```

**Step 1: Fetch**
```python
response = await async_get("https://www.reddit.com/search.json?q=Target%20Inc", settings, client)
post_data = response.json()
```

**Step 2: Validate**
```python
post = RedditPost(
    title="We migrated to Kubernetes for cost optimization",
    selftext="Spent 3 months optimizing cloud costs...",
    subreddit="devops",
    author="engineering_lead",
    created_utc=1708871400,
    score=245,
    num_comments=42,
    url="https://reddit.com/r/devops/comments/abc123/..."
)
```

**Step 3: Classify**
```python
text = "We migrated to Kubernetes for cost optimization\nSpent 3 months optimizing cloud costs..."
matches = classify_text(text, keyword_lexicon)
# Finds: [("kubernetes_detected", 0.7, "kubernetes"), ("cost_optimization", 0.6, "cost")]
```

**Step 4: Build Observation**
```python
observation = SignalObservation(
    obs_id="obs_abc123...",  # Deterministic hash
    account_id="acc_target",
    signal_code="kubernetes_detected",
    source="reddit_api",
    observed_at="2024-02-25T12:00:00+00:00",
    evidence_url="https://reddit.com/r/devops/comments/abc123/...",
    evidence_text="We migrated to Kubernetes for cost optimization...",
    confidence=0.7,  # From keyword match
    source_reliability=0.65,  # From source_registry.csv
    raw_payload_hash="raw_xyz789..."
)
```

**Step 5: Insert to Database**
```python
inserted = db.insert_signal_observation(conn, observation, commit=False)
# ON CONFLICT DO NOTHING → if obs_id exists, skip (dedup)
# Returns True if inserted, False if skipped
```

**Step 6: Scoring (Later Stage)**
```python
# Scoring engine loads the observation
# Gets rule: base_weight=6, half_life_days=14, min_confidence=0.3
# Calculates:
component = 6 × 0.7 × 0.65 × decay(0 days)
          = 6 × 0.7 × 0.65 × 1.0
          = 2.73

# This contributes 2.73 points to Target Inc's "trigger_intent" dimension
# Which is then weighted and aggregated with other dimensions
# Final result: Target Inc scores 45.2 in tier_2 (≥40, <60)
```

---

## 🔄 Complete System Integration

The Reddit collector is **100% integrated** with:

✅ **Database** — Inserts to `signal_observations` table
✅ **Scoring** — Observations scored via `signal_registry.csv` rules
✅ **Export** — Scored accounts exported to CSV for sales
✅ **Configuration** — Driven entirely by CSV files
✅ **Monitoring** — Crawl attempts logged, metrics tracked
✅ **Pipeline** — Called sequentially in ingest stage

**No special handling needed** — Works exactly like jobs, news, technographics collectors.

---

## 📞 Command Reference

### Essential Commands

```bash
# Setup
source venv/bin/activate
bash RUN_INFRASTRUCTURE_SETUP.sh

# Run pipeline
./signals start
SIGNALS_VERBOSE_PROGRESS=1 ./signals start  # With progress output

# Just collectors
python -m src.main ingest

# Just scoring
python -m src.main score

# Just export
python -m src.main export

# Check status
docker compose -f docker-compose.local.yml ps

# Access database
docker compose -f docker-compose.local.yml exec postgres \
  psql -U signals -d signals

# View logs
docker compose -f docker-compose.local.yml logs postgres -f

# Stop services
docker compose -f docker-compose.local.yml down
```

### Query Commands

```bash
# Count observations by source
docker compose -f docker-compose.local.yml exec postgres \
  psql -U signals -d signals -c \
  "SELECT source, COUNT(*) FROM signal_observations GROUP BY source;"

# Check Reddit observations specifically
docker compose -f docker-compose.local.yml exec postgres \
  psql -U signals -d signals -c \
  "SELECT COUNT(*) FROM signal_observations WHERE source LIKE 'reddit%';"

# View top scored companies
docker compose -f docker-compose.local.yml exec postgres \
  psql -U signals -d signals -c \
  "SELECT company_name, domain, score, tier_v2 FROM accounts a
   JOIN account_scores s ON a.account_id = s.account_id
   ORDER BY score DESC LIMIT 10;"

# View Reddit's contribution
docker compose -f docker-compose.local.yml exec postgres \
  psql -U signals -d signals -c \
  "SELECT signal_code, COUNT(*) FROM signal_observations
   WHERE source LIKE 'reddit%' GROUP BY signal_code;"
```

---

## 🎁 What's Next

### Immediate (After Setup)
1. Run `./signals start`
2. Check CSV outputs in `data/out/`
3. Query database for Reddit observations
4. Verify scores are calculated

### Short Term (This Week)
1. Add custom companies to `config/seed_accounts.csv`
2. Add keywords to `config/keyword_lexicon.csv`
3. Enable Google Sheets export
4. Schedule daily runs via cron

### Medium Term (This Month)
1. Monitor signal quality
2. Tune scoring thresholds
3. Integrate with sales CRM
4. Set up dashboards

### Long Term (This Quarter)
1. Implement feedback loops
2. Train team on interpretation
3. Expand to additional signals
4. Build predictive models

---

## ✨ Summary

**You have everything needed:**

✅ Complete codebase (Reddit collector pre-integrated)
✅ Docker configuration (PostgreSQL 16, Redis 7)
✅ Database schema (20+ tables, ready to initialize)
✅ Configuration files (all CSVs with examples)
✅ Setup automation (one-command or manual)
✅ Comprehensive documentation (1000+ lines)
✅ Verification tests (6-point infrastructure check)
✅ Troubleshooting guides (common issues solved)
✅ Command references (quick lookup)
✅ Architecture explanations (understand how it works)

**Everything follows the same patterns:**
- Reddit collector = jobs collector = news collector
- Same async architecture
- Same error handling
- Same deduplication
- Same scoring integration
- Same CSV export

**You can start immediately:**

```bash
# Option 1 (Fastest)
bash /Users/zopdec/signals/RUN_INFRASTRUCTURE_SETUP.sh

# Option 2 (Manual)
Follow QUICK_START.md commands

# Option 3 (Learning)
Read COMPLETE_SETUP_WITH_REDDIT.md

# Then run pipeline
./signals start
```

**Within 45 minutes, you'll have:**
- Docker running
- Database initialized
- Python environment ready
- Reddit collector tested with real data
- CSV outputs for sales team

---

**Ready to go! 🚀**
