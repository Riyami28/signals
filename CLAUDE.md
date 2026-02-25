# CLAUDE.md — Signals Codebase Guide

## What This Is

Buying signal tracker for enterprise SaaS. Ingests signals from jobs, news, RSS, technographics, community sources. Scores companies for 3 products: **zopdev** (DevOps), **zopday** (platform eng), **zopnight** (FinOps). Exports tiered review queues for sales.

## Quick Commands

```bash
# Local dev
make setup              # Full bootstrap (venv, deps, Docker, DB, seed)
make dev                # Start UI at localhost:8788

# Pipeline
./signals start         # Full daily pipeline (streaming)
./signals run --date 2026-02-25  # Non-streaming run
./signals company example.com    # Single company
./signals hunt example.com       # Deep research on one company
./signals conviction --top 15    # Top accounts with reasons
./signals sources                # Source quality report
./signals ui --port 8788         # Web UI only

# Testing
pytest -q                          # All tests
pytest tests/test_scoring.py -v    # Single file
SIGNALS_TEST_PG_DSN=postgresql://signals:signals_dev_password@127.0.0.1:55432/signals_test pytest

# Docker
docker compose -f docker-compose.local.yml up -d     # Start Postgres + Redis + Huginn
docker compose -f docker-compose.local.yml down       # Stop
docker exec signals-postgres psql -U signals -d signals  # Direct DB access
```

## Project Structure

```
src/
├── cli.py                  # Typer CLI (./signals wrapper) — 8 user-facing commands
├── main.py                 # Pipeline orchestrator — 30 internal commands (@app.command)
├── db.py                   # PostgreSQL data layer — 89 functions, ALL db operations
├── settings.py             # Pydantic Settings — env var config
├── models.py               # Data models: Account, SignalObservation, ComponentScore, AccountScore, ReviewLabel
├── http_client.py          # HTTP client with robots.txt + rate limiting
├── utils.py                # Core utilities: normalize_domain, stable_hash, classify_text, load_csv_rows
├── notifier.py             # Alerts: Google Chat webhook + email
├── logging_config.py       # Structured logging (exists but mostly unused)
├── source_policy.py        # Per-source concurrency/timeout config
├── promotion_policy.py     # Tier promotion rules
│
├── collectors/             # Signal ingestion (all have `collect()` entry point)
│   ├── jobs.py             # Greenhouse/Lever/careers page job crawling
│   ├── news.py             # News articles + Google News RSS
│   ├── community.py        # Reddit/community signals
│   ├── technographics.py   # Tech stack detection (K8s, Terraform, etc.)
│   └── first_party.py      # CRM/CS product events
│
├── scoring/
│   ├── engine.py           # recency_decay(), classify_tier(), run_scoring()
│   ├── rules.py            # load_signal_rules(), load_source_registry(), load_thresholds()
│   └── explain.py          # Top-reasons ranking for score transparency
│
├── discovery/              # Autonomous account discovery
│   ├── pipeline.py         # External event ingestion + scoring
│   ├── hunt.py             # LLM deep research orchestration (HuntProfile)
│   ├── config.py           # Signal classification, account profiles, blocklist
│   ├── parser.py           # Event payload parsing + multilingual
│   ├── frontier.py         # URL canonicalization + crawl queue
│   ├── fetcher.py          # Fetch with Playwright fallback
│   ├── webhook.py          # FastAPI webhook at POST /v1/discovery/events
│   ├── watchlist_builder.py # CPG watchlist from Wikidata
│   ├── speaker_intel.py    # Speaker/quote extraction patterns
│   └── multilingual.py     # Language detection
│
├── research/               # LLM-powered company research
│   ├── orchestrator.py     # Two-pass: extraction → scoring
│   ├── client.py           # LLM clients (Anthropic Claude / MiniMax)
│   ├── parser.py           # Evidence extraction from LLM responses
│   ├── prompts.py          # build_extraction_prompt(), build_scoring_prompt()
│   ├── enrichment.py       # Clearbit/Hunter enrichment waterfall
│   └── web_scraper.py      # Playwright web scraping
│
├── export/
│   └── csv_exporter.py     # CSV output: review_queue, daily_scores, source_quality
│
├── reporting/
│   ├── calibration.py      # ICP coverage + threshold tuning
│   ├── evals.py            # evaluate_run_output_quality()
│   ├── improvement.py      # Threshold self-improvement loop
│   ├── icp_playbook.py     # Signal gap analysis
│   └── quality.py          # Source quality metrics
│
├── review/
│   └── import_reviews.py   # Import analyst labels from CSV/Sheets
│
├── sync/
│   └── google_sheets.py    # Push data to Google Sheets
│
├── ui/
│   └── local_app.py        # Legacy local UI (DEPRECATED — use src/web/)
│
└── web/                    # Current web UI (FastAPI + Alpine.js)
    ├── app.py              # FastAPI app factory, CORS, static files
    ├── pipeline_runner.py  # Background pipeline execution for UI
    ├── routes/
    │   ├── accounts.py     # GET /api/accounts, GET /api/accounts/{id}
    │   ├── labels.py       # POST /api/labels, DELETE /api/labels/{id}, GET /api/labels/{id}
    │   ├── pipeline.py     # POST /api/pipeline/run, GET /api/pipeline/stream/{id}
    │   └── research.py     # GET /api/research/{account_id}
    └── static/
        ├── index.html      # Dashboard (Alpine.js)
        ├── app.js          # Frontend logic
        └── styles.css      # Dark theme styles

config/                     # CSV-driven configuration (edit without code changes)
├── signal_registry.csv     # Signal definitions: base_weight, half_life_days, min_confidence
├── thresholds.csv          # Tier boundaries: high≥20, medium≥10
├── source_registry.csv     # Source reliability scores (0.0–1.0)
├── keyword_lexicon.csv     # Keyword → signal_code mappings
├── seed_accounts.csv       # Initial 19 seed companies
├── watchlist_accounts.csv  # 143K target companies (from Wikidata)
├── account_source_handles.csv  # Per-account source query templates (578K rows)
├── icp_reference_accounts.csv  # Known customers for calibration
├── icp_signal_playbook.csv     # Expected signals by stage/product
├── signal_classes.csv      # Signal grouping (primary, platform, hiring)
├── account_profiles.csv    # Profile exclusions
├── discovery_thresholds.csv
├── discovery_blocklist.csv
├── promotion_policy.csv
└── source_execution_policy.csv  # Per-source concurrency budgets

tests/                      # pytest suite (requires Postgres for integration tests)
├── conftest.py
├── test_calibration.py
├── test_cli.py
├── test_collectors.py
├── test_db.py
├── test_discovery.py
├── test_hunt.py
├── test_icp_playbook.py
├── test_local_ui.py
├── test_output_quality.py
├── test_pipeline.py
├── test_quality.py
├── test_research_orchestrator.py
├── test_research_parser.py
├── test_research_prompts.py
├── test_review_import.py
├── test_sales_ready_export.py
├── test_scoring.py
├── test_sync_google_sheets.py
├── test_utils.py
└── test_watchlist_builder.py

scripts/
├── bootstrap.sh            # Full local setup automation
├── local_stack_up.sh       # Docker compose up
├── local_stack_down.sh     # Docker compose down
├── local_stack_status.sh   # Docker status check
├── run_daily_live_monitor.sh   # Real-time pipeline monitor
├── run_discovery_daily.sh      # Autonomous discovery loop
├── run_local_autonomous.sh     # Local testing helper
├── postgres_update_watchlist.sh # Bulk watchlist import
├── migrate_sqlite_to_postgres.py
├── check_migrations.sh     # CI migration safety check
└── test_smallset_autonomous.sh  # Small-scale autonomous test
```

## Database

**PostgreSQL 16** on port `55432`. Data lives in the `signals` schema (not `public`).

**Connection:** `SIGNALS_PG_DSN` env var or components: `SIGNALS_PG_HOST`, `SIGNALS_PG_PORT`, `SIGNALS_PG_USER`, `SIGNALS_PG_PASSWORD`, `SIGNALS_PG_DB`. Must include `?options=-c%20search_path%3Dsignals` for correct schema.

**Key tables** (in `signals` schema):
| Table | Purpose |
|-------|---------|
| `accounts` | company_name, domain, source_type (seed/discovered) |
| `signal_observations` | obs_id, account_id, signal_code, product, source, confidence, evidence_url |
| `score_runs` | run_id, run_date, status |
| `score_components` | run_id, account_id, product, signal_code, component_score |
| `account_scores` | run_id, account_id, product, score, tier, top_reasons_json, delta_7d |
| `review_labels` | review_id, run_id, account_id, decision, reviewer |
| `account_labels` | Web UI labels (qualified, SQL, MQL, etc.) |
| `crawl_checkpoints` | Same-day dedup for source crawls |
| `crawl_attempts` | Per-source crawl attempt tracking |
| `external_discovery_events` | Huginn webhook payloads |
| `crawl_frontier` | Discovery URL queue |
| `documents` | Crawled document storage |
| `document_mentions` | Entity mentions in documents |
| `observation_lineage` | Signal → document → mention tracing |
| `retry_queue` | Failed task retry with backoff |
| `ops_metrics` | Pipeline operational metrics |
| `company_research` | LLM research results (JSON) |
| `contact_research` | Enriched contact data |
| `discovery_runs` | Discovery pipeline run tracking |
| `discovery_candidates` | CRM handoff candidates |
| `pipeline_runs` | Web UI pipeline run tracking |
| `people_watchlist` | Person-level tracking |
| `people_activity` | Person activity signals |

**Key db.py functions** (most used):
- `get_connection(pg_dsn)` — returns psycopg connection
- `init_db(conn)` — create tables + run migrations
- `upsert_account(conn, domain, company_name, source_type)` → account dict
- `insert_signal_observation(conn, observation)` → bool
- `create_score_run(conn, run_date)` → run_id
- `replace_run_scores(conn, run_id, component_scores, account_scores)`
- `get_accounts_paginated(conn, page, per_page, sort_by, sort_dir, tier_filter, label_filter, search)` → (rows, total)
- `get_account_detail(conn, account_id)` → dict
- `select_accounts_for_live_crawl(conn, settings, run_date)` → accounts list
- `try_advisory_lock(conn, lock_name, owner_id)` / `release_advisory_lock(...)` — concurrency guard
- `enqueue_retry_task(...)` / `fetch_due_retry_tasks(...)` — retry queue
- `was_crawled_today(conn, account_id, run_date_str, source)` — dedup check

## Pipeline Stages

The daily pipeline (`./signals start` or `python -m src.main run-daily`) runs these stages in order:

```
1. INGEST     → All collectors in parallel (jobs, news, RSS, technographics, community)
2. SCORE      → Weighted scoring with recency decay
3. EXPORT     → CSV files: review_queue, daily_scores, source_quality
4. PREPARE    → Merge daily review_queue into review_input.csv
5. SYNC       → Push to Google Sheets (best effort)
6. IMPORT     → Read analyst decisions back
7. QUALITY    → ICP coverage + precision metrics
8. OPS        → Ingest lag, retry depth, lock events
```

**Scoring formula:** `score = confidence × source_reliability × recency_decay(half_life) × base_weight`

**Anti-inflation:** max 1 obs per source/signal, max 3 total per signal.

**Tiers:** high ≥ 20, medium ≥ 10, low < 10 (configurable in `config/thresholds.csv`).

## Environment Variables

**Required for local dev:**
```
SIGNALS_PROJECT_ROOT=/path/to/signals
SIGNALS_PG_DSN=postgresql://signals:signals_dev_password@127.0.0.1:55432/signals?options=-c%20search_path%3Dsignals
```

**Live crawling:**
```
SIGNALS_ENABLE_LIVE_CRAWL=1
SIGNALS_HTTP_TIMEOUT_SECONDS=12
SIGNALS_LIVE_MAX_ACCOUNTS=1000
```

**LLM research:**
```
SIGNALS_CLAUDE_API_KEY=sk-ant-...
SIGNALS_CLAUDE_MODEL=claude-sonnet-4-5    # or claude-opus-4-6
SIGNALS_LLM_PROVIDER=minimax              # or anthropic
SIGNALS_MINIMAX_API_KEY=...
```

**Google Sheets sync:**
```
GOOGLE_SHEETS_SPREADSHEET_ID=...
GOOGLE_SERVICE_ACCOUNT_FILE=/path/to/sa.json
```

**Alerts:**
```
SIGNALS_GCHAT_WEBHOOK_URL=...
SIGNALS_ALERT_EMAIL_TO=...
```

## Architecture Patterns

- **Advisory locks** (`try_advisory_lock`) prevent concurrent `run-daily` and `run-autonomous-loop`
- **Crawl checkpoints** (`was_crawled_today`) prevent redundant same-day API fetches
- **Retry queue** with backoff (1m, 5m, 15m) → quarantine after 3 attempts
- **Stage watchdog** with per-stage timeout (default 1800s)
- **Content dedup** via `stable_hash()` on payloads
- **Entity resolution** via `normalize_domain()` + `rapidfuzz` fuzzy matching
- **All collectors** expose a single `collect(conn, settings, run_date, ...)` function
- **Products:** `zopdev`, `zopday`, `zopnight`, `shared` — separate scores per product, shared observations

## Key Utilities (src/utils.py)

- `normalize_domain(value)` — strip www/protocol, lowercase, TLD extract
- `stable_hash(payload, prefix, length)` — deterministic content hash for dedup
- `classify_text(text, lexicon_rows)` — keyword matching against `keyword_lexicon.csv`
- `load_csv_rows(path)` — CSV to list of dicts
- `load_account_source_handles(path)` — per-account source query lookup
- `write_csv_rows(path, rows, fieldnames)` — write CSV output
- `utc_now_iso()` — current UTC timestamp string

## Docker Services

| Service | Container | Port | Purpose |
|---------|-----------|------|---------|
| PostgreSQL 16 | signals-postgres | 55432 | Primary database |
| Redis 7 | signals-redis | 56379 | Queue/cache |
| Huginn | signals-huginn | 3000 | Webhook event collection |

## CI/CD

GitHub Actions at `.github/workflows/ci.yml`:
- Runs on all branches + PRs
- PostgreSQL 16 service container
- Steps: checkout → Python 3.12 → pip install → migration safety check → pytest

**Known:** 11 tests currently failing on main (SQL placeholder bugs, Typer interface mismatches, missing ResearchClient attribute). See GitHub Issue #3.

## Current Issues

See https://github.com/talvinder/signals/issues for the full backlog. Key items:
- **#1** Bootstrap script for new dev onboarding
- **#2** DB migration system + search_path fix
- **#3** Fix 11 failing CI tests
- **#4** Security: CORS + API auth
- **#5** Structured logging
- **#6** Test coverage (currently ~35%, target 70%)
- **#7-8** Refactor db.py (2,889 lines) and main.py (2,275 lines)
- **#9** Async HTTP collectors
- **#10** Remove unused deps (openai) and dead code (src/ui/local_app.py)

## Common Patterns When Editing

**Adding a new signal type:**
1. Add row to `config/signal_registry.csv` (signal_code, base_weight, half_life_days, min_confidence)
2. Add keywords to `config/keyword_lexicon.csv`
3. Scoring engine picks it up automatically

**Adding a new collector:**
1. Create `src/collectors/my_source.py` with `collect(conn, settings, run_date, ...)` function
2. Register in `src/main.py` `ingest` command's collector list
3. Add source reliability to `config/source_registry.csv`

**Adding a new API endpoint:**
1. Create route in `src/web/routes/`
2. Register router in `src/web/app.py` `create_app()`
3. Add frontend call in `src/web/static/app.js`

**Adding a new DB table:**
1. Add `CREATE TABLE IF NOT EXISTS` to `SCHEMA_SQL` in `src/db.py`
2. Add CRUD functions in `src/db.py`
3. Note: tables created in default schema; data lives in `signals` schema

## Do NOT

- Run `git push --force` to main
- Commit `.env` files (gitignored for a reason — contains credentials)
- Use SQLite paths — this is Postgres-only now
- Modify `config/watchlist_accounts.csv` by hand (143K rows, use scripts)
- Skip the `?options=-c%20search_path%3Dsignals` in the PG DSN
