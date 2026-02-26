# Zopdev Signals

Local-first buying signal tracker for `zop.dev`, `zopday`, and `zopnight`.

## Prerequisites

- **Python 3.12+** — `python3 --version`
- **Docker Desktop** — running and accessible
- **git** and **gh** CLI

## Setup (New Clone)

```bash
git clone https://github.com/talvinder/signals.git
cd signals
make setup        # Creates venv, installs deps, starts Docker, initialises DB
make dev          # Starts web UI at http://localhost:8788
```

The `make setup` command runs `scripts/bootstrap.sh` which:
1. Creates `.venv` and installs all Python dependencies
2. Installs Playwright Chromium (used for JS-fallback crawling)
3. Copies `.env.example` → `.env` (edit to add API keys)
4. Starts Docker services: Postgres (55432), Redis (56379), Huginn (3000)
5. Waits for Postgres health check
6. Runs `python -m src.main migrate` to apply DB migrations
7. Loads seed accounts from `config/seed_accounts.csv`

### After setup, add your API keys to `.env`:
```
SIGNALS_CLAUDE_API_KEY=sk-ant-...    # Required for LLM research stage
SIGNALS_CLAUDE_MODEL=claude-sonnet-4-6
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `docker: command not found` | Install Docker Desktop and make sure it's running |
| `psycopg.OperationalError: connection refused` | Run `make setup` or `docker compose -f docker-compose.local.yml up -d` |
| Tests skipped (not failing) | DB not running — start Docker first |
| `ImportError` on `src.*` | Activate venv: `source .venv/bin/activate` |
| Schema mismatch errors | Run `make migrate` to apply pending migrations |

## Account Watchlist

A **1,000-company starter watchlist** is committed at `config/watchlist_accounts.csv` and seeds automatically on the first pipeline run.

For a full **~143K company watchlist** (production), regenerate from Wikidata once after setup:

```bash
./signals build-cpg-watchlist --limit 143000
# Then run the pipeline — watchlist is seeded on first run
./signals start
```

> The full watchlist is not committed to git (too large). Each developer generates it locally. The 1K starter set is sufficient for development and testing.

## Quick Start (One Command)

After `make setup`, run the full pipeline:

```bash
./signals start
```

This runs the daily pipeline with live terminal progress so you can see:

- which account is being processed
- which source is running
- scoring/export stage transitions
- final run completion (`run_exit_code=0` on success)

Useful variants:

```bash
# explicit date
./signals start --date 2026-02-22

# force higher parallelism for large backlogs
./signals start --workers-per-source 32

# run for one company only
./signals company conagrabrands.com

# fast one-company hunt (ingest + score + citations)
./signals hunt conagrabrands.com

# show scored companies with reason citations
./signals conviction --date 2026-02-22 --top 15

# source depth + quality report
./signals sources --date 2026-02-22

# run web UI
./signals ui
```

## Overview

This repo implements a daily pipeline that:

- Ingests signals from local CSV drops and live external sources into PostgreSQL.
- Scores accounts for `zopdev`, `zopday`, and `zopnight`.
- Exports review and reporting CSVs.
- Optionally syncs outputs to Google Sheets.
- Tracks quality and promotion readiness.
- Produces an ICP calibration report for known customer/POC accounts.
- Supports large watchlists via `config/watchlist_accounts.csv` (auto-seeded at bootstrap).

## Pipeline

`run-daily` executes:

1. `ingest` (all collectors)
2. `score`
3. `export`
4. `prepare-review-input` (merge today’s review queue into `data/raw/review_input.csv`)
5. `sync-sheet` (best effort; non-blocking if not configured)
6. `import-reviews`
7. quality + promotion report refresh
8. ICP coverage report generation

## Sources

Current source families:

- Manual/seeded inputs
  - `jobs_csv`
  - `news_csv`
  - `technographics_csv`
  - `community_csv`
  - `first_party_csv`
  - `rss_feed` (custom RSS from `data/raw/news_feeds.csv`)

- Live crawled/enriched sources
  - `greenhouse_api`
  - `lever_api`
  - `careers_live`
  - `google_news_rss`
  - `reddit_rss`
  - `website_scan`

Notes:

- Live discovery is controlled per account via `config/account_source_handles.csv`.
- Crawls are checkpointed (`crawl_checkpoints` table) to avoid repeated same-day endpoint fetches.
- Run `./signals sources --date YYYY-MM-DD` to inspect source attempts, source reliability, evidence coverage, and weak-source recommendations.

## Scoring Behavior

Scores are computed from registry-driven signals in `config/signal_registry.csv` with recency decay and source reliability.

Important anti-inflation rules in the current engine:

- Max 1 contribution per source for the same signal.
- Max 3 total observations counted per signal.

This prevents repeated postings from one source from artificially saturating account scores.

Tiering comes from `config/thresholds.csv`.

## ICP Calibration

Use `config/icp_reference_accounts.csv` to track known customer/POC coverage.

- This file is used for evaluation and reporting only.
- It does not directly increase or force account scores.

Generated output:

- `data/out/icp_coverage_YYYYMMDD.csv`

## ICP Signal Gap Tracking

Use `config/icp_signal_playbook.csv` to define expected signals by `relationship_stage` and `product`.

The playbook is compared against current scored components for ICP accounts to find missing high-priority signals.

Generated output:

- `data/out/icp_signal_gaps_YYYYMMDD.csv`

## Discovery (Huginn Connector)

Discovery ingestion accepts webhook events at `POST /v1/discovery/events` and converts them into signal observations.

- Auth header: `X-Discovery-Token` (set via `SIGNALS_DISCOVERY_WEBHOOK_TOKEN`)
- Event payload fields:
  - `source`
  - `source_event_id`
  - `observed_at`
  - `title`
  - `text`
  - `url`
  - `company_name_hint`
  - `domain_hint`
  - `raw_payload`

Discovery scoring uses:

- fixed tiers from `config/thresholds.csv` (`high>=20`, `medium>=10`)
- signal class gating from `config/signal_classes.csv`
- account profile exclusions from `config/account_profiles.csv`
- blocklist from `config/discovery_blocklist.csv`
- candidate mix thresholds from `config/discovery_thresholds.csv`

## CPG Watchlist Expansion

Use the built-in generator to produce a real-company CPG watchlist (Wikidata-backed, no placeholder domains):

- output: `config/watchlist_accounts.csv`
- optional source-handle merge: `config/account_source_handles.csv`

Notes:

- Existing curated rows in `config/account_source_handles.csv` are preserved.
- New generated rows get a default buying-signal news query template.
- `seed_accounts.csv` and `watchlist_accounts.csv` are both auto-seeded into `accounts`.

## CLI

Primary commands (recommended):

```bash
# one-command operator run (real-time stream)
./signals start

# same monitor mode with explicit controls
./signals watch --date 2026-02-22 --live-max-accounts 1000 --workers-per-source 24 --poll-interval-seconds 2

# non-streaming run
./signals run --date 2026-02-22 --live --workers-per-source 24

# one-company targeted run
./signals company conagrabrands.com --workers-per-source 8

# fastest single-company flow for trust-building
./signals hunt conagrabrands.com

# conviction report (scores + reasons + evidence URLs)
./signals conviction --date 2026-02-22 --top 15

# source depth + quality report (attempts, success/error mix, evidence coverage)
./signals sources --date 2026-02-22

# web UI
./signals ui --port 8788
```

Advanced low-level commands are still available via:

```bash
python -m src.main --help
```

## Runtime Configuration

Configure via `.env` (see `.env.example`):

- `SIGNALS_PG_DSN` or `SIGNALS_PG_HOST` + `SIGNALS_PG_PORT` + `SIGNALS_PG_USER` + `SIGNALS_PG_PASSWORD` + `SIGNALS_PG_DB`
- `SIGNALS_TEST_PG_DSN` (optional; defaults to `postgresql://signals:signals_dev_password@127.0.0.1:55432/signals_test` for pytest isolation)
- `SIGNALS_ENABLE_LIVE_CRAWL`
- `SIGNALS_HTTP_TIMEOUT_SECONDS`
- `SIGNALS_HTTP_USER_AGENT`
- `SIGNALS_HTTP_PROXY_URL` (approved proxy only; no evasion)
- `SIGNALS_RESPECT_ROBOTS_TXT`
- `SIGNALS_MIN_DOMAIN_REQUEST_INTERVAL_MS`
- `SIGNALS_LIVE_MAX_ACCOUNTS`
- `SIGNALS_LIVE_WORKERS_PER_SOURCE` (`auto` by default; or set an integer like `24`)
- `SIGNALS_LIVE_TARGET_DOMAINS` (optional comma-separated domain filter for live crawl)
- `SIGNALS_AUTO_DISCOVER_JOB_HANDLES`
- `SIGNALS_LIVE_MAX_JOBS_PER_SOURCE`
- `SIGNALS_STAGE_TIMEOUT_SECONDS`
- `SIGNALS_RETRY_ATTEMPT_LIMIT`
- `SIGNALS_GCHAT_WEBHOOK_URL`
- `SIGNALS_ALERT_EMAIL_TO` + `SIGNALS_ALERT_EMAIL_FROM`
- `SIGNALS_ALERT_SMTP_HOST` + `SIGNALS_ALERT_SMTP_PORT` + `SIGNALS_ALERT_SMTP_USER` + `SIGNALS_ALERT_SMTP_PASSWORD`
- `SIGNALS_ALERT_RETRY_DEPTH_THRESHOLD`
- `SIGNALS_ALERT_MIN_HIGH_PRECISION`
- `SIGNALS_ALERT_MIN_MEDIUM_PRECISION`
- `SIGNALS_OPS_METRICS_LOOKBACK_DAYS`
- `SIGNALS_WATCHLIST_QUERY_WORKERS`
- `SIGNALS_WATCHLIST_COUNTRY_TIMEOUT_SECONDS`

## Optional ML Dependencies (`hunt_ml`)

`hunt_ml` is optional and not required for normal ingest/score/export runs.

```bash
pip install -e ".[hunt_ml]"
```

This extra installs heavyweight packages (`transformers`, `torch`, `spacy`, `sentencepiece`) and can add ~1GB+ to environment size.
Only install it if you are enabling multilingual discovery enrichment that depends on Hugging Face pipelines.
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_FILE`

## Output Files

Typical outputs in `data/out/`:

- `review_queue_YYYYMMDD.csv`
- `daily_scores_YYYYMMDD.csv`
- `source_quality_YYYYMMDD.csv`
- `source_depth_YYYYMMDD.csv`
- `promotion_readiness_YYYYMMDD.csv`
- `icp_coverage_YYYYMMDD.csv`
- `icp_signal_gaps_YYYYMMDD.csv`
- `discovery_queue_YYYYMMDD.csv`
- `discovery_metrics_YYYYMMDD.csv`
- `crm_candidates_YYYYMMDD.csv`
- `manual_review_queue_YYYYMMDD.csv`
- `story_evidence_YYYYMMDD.csv`
- `signal_lineage_YYYYMMDD.csv`
- `hunt_quality_metrics_YYYYMMDD.csv`
- `ops_metrics_YYYYMMDD.csv`

## Key Config Files

- `config/signal_registry.csv`
- `config/source_registry.csv`
- `config/thresholds.csv`
- `config/keyword_lexicon.csv`
- `config/seed_accounts.csv`
- `config/watchlist_accounts.csv`
- `config/account_source_handles.csv`
- `config/icp_reference_accounts.csv`
- `config/icp_signal_playbook.csv`
- `config/signal_classes.csv`
- `config/account_profiles.csv`
- `config/discovery_thresholds.csv`
- `config/discovery_blocklist.csv`
- `config/promotion_policy.csv`
- `config/profile_scenarios.csv`
- `config/signal_universe_stackrank.csv`
- `config/source_execution_policy.csv`

## Scheduler Example

```cron
0 6 * * * cd /Users/raramuri/Projects/zopdev/signals && ./signals run >> data/out/daily.log 2>&1
```

Discovery-specific daily runner installed in this environment:

```cron
15 6 * * * /Users/raramuri/Projects/zopdev/signals/scripts/run_discovery_daily.sh
```

Local autonomous runtime helper:

```bash
scripts/run_local_autonomous.sh
```

## Local Stack (Docker)

Bring up PostgreSQL + Redis + Huginn locally:

```bash
scripts/local_stack_up.sh
scripts/local_stack_status.sh
scripts/postgres_update_watchlist.sh
```

One-time migration from legacy SQLite data:

```bash
python scripts/migrate_sqlite_to_postgres.py --sqlite-path data/signals.db --truncate-target
```

Default local ports:
- `Huginn`: `3000`
- `PostgreSQL`: `55432`
- `Redis`: `56379`

Stop stack:

```bash
scripts/local_stack_down.sh
```

## Notes

- Runtime database is PostgreSQL (SQLite fallback has been removed).
- Default `SIGNALS_LIVE_MAX_ACCOUNTS` is now `1000` (override in `.env` if needed).
- Crawlers respect `robots.txt` by default and enforce per-domain request intervals.
- Proxy support is for approved network egress/reliability only, not scraping-evasion.
- If Google Sheets is not configured, `sync-sheet` will fail with a clear error and the rest of `run-daily` still completes.
- Review labels from `review_input` are required for meaningful source quality and promotion metrics.
- `run-daily` and `run-autonomous-loop` are single-flight guarded via PostgreSQL advisory locks.
- Failed watchdog stages are routed to retry queue with backoff (`1m`, `5m`, `15m`) and then quarantine.

## Architecture References

- `docs/autonomous_discovery_architecture.md`
- `config/signal_universe_stackrank.csv`
