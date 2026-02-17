# Zopdev Signals

Local-first buying signal tracker for `zop.dev`, `zopday`, and `zopnight`.

## Overview

This repo implements a daily pipeline that:

- Ingests signals from local CSV drops and live external sources into SQLite.
- Scores accounts for `zopdev`, `zopday`, and `zopnight`.
- Exports review and reporting CSVs.
- Optionally syncs outputs to Google Sheets.
- Tracks quality and promotion readiness.
- Produces an ICP calibration report for known customer/POC accounts.

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

## Commands

```bash
python -m src.main ingest --all
python -m src.main score --date 2026-02-16
python -m src.main export --date 2026-02-16
python -m src.main prepare-review-input --date 2026-02-16
python -m src.main sync-sheet --date 2026-02-16
python -m src.main import-reviews --date 2026-02-16
python -m src.main run-daily --date 2026-02-16
python -m src.main icp-report --date 2026-02-16
python -m src.main crawl-diagnostics --date 2026-02-16
python -m src.main calibrate-thresholds --date 2026-02-16
python -m src.main tune-profile --date 2026-02-16
```

## Runtime Configuration

Configure via `.env` (see `.env.example`):

- `SIGNALS_ENABLE_LIVE_CRAWL`
- `SIGNALS_HTTP_TIMEOUT_SECONDS`
- `SIGNALS_HTTP_USER_AGENT`
- `SIGNALS_LIVE_MAX_ACCOUNTS`
- `SIGNALS_AUTO_DISCOVER_JOB_HANDLES`
- `SIGNALS_LIVE_MAX_JOBS_PER_SOURCE`
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_FILE`

## Output Files

Typical outputs in `data/out/`:

- `review_queue_YYYYMMDD.csv`
- `daily_scores_YYYYMMDD.csv`
- `source_quality_YYYYMMDD.csv`
- `promotion_readiness_YYYYMMDD.csv`
- `icp_coverage_YYYYMMDD.csv`

## Key Config Files

- `config/signal_registry.csv`
- `config/source_registry.csv`
- `config/thresholds.csv`
- `config/keyword_lexicon.csv`
- `config/seed_accounts.csv`
- `config/account_source_handles.csv`
- `config/icp_reference_accounts.csv`
- `config/profile_scenarios.csv`

## Scheduler Example

```cron
0 6 * * * cd /Users/raramuri/Projects/zopdev/signals && /usr/bin/env python -m src.main run-daily >> data/out/daily.log 2>&1
```

## Notes

- SQLite is configured with WAL and busy timeout for better write resilience.
- If Google Sheets is not configured, `sync-sheet` will fail with a clear error and the rest of `run-daily` still completes.
- Review labels from `review_input` are required for meaningful source quality and promotion metrics.
