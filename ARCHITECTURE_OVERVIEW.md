# Zopdev Signals: Architecture & Pipeline Overview

This document provides a comprehensive breakdown of the **Zopdev Signals** repository, explaining the folder structure, core tech stack, and the data pipelines that power the system.

## 1. High-Level Folder Structure

The project is organized to separate configuration, raw data, and executable logic.

| Directory | Purpose |
| :--- | :--- |
| `config/` | **The Brain.** Contains CSV files that define scoring rules (`thresholds.csv`), source reliability (`source_registry.csv`), and signal definitions (`signal_registry.csv`). |
| `src/` | **The Engine.** The core Python source code. |
| `data/` | **Storage.** `data/raw/` for initial inputs; `data/out/` for generated reports and CSV outputs. |
| `docs/` | **Knowledge Base.** Detailed architectural documents and operational guides. |
| `scripts/` | **Tooling.** Shell scripts for database setup, migrations, and local environment management. |
| `tests/` | **Quality Assurance.** Pytest suite for validating collectors and scoring logic. |

---

## 2. Source Code Breakdown (`src/`)

The `src` directory is divided into modules based on their responsibility in the data lifecycle:

### A. Data Gathering (`src/collectors/`)
This module contains "Collectors" that fetch raw information from the web:
- `jobs.py`: Scrapes Greenhouse, Lever, and other job boards for hiring signals.
- `news.py` & `gnews_collector.py`: Fetches articles from Google News and RSS feeds.
- `community.py`: Targeted Reddit and community forum monitoring.
- `website_techscan.py`: Uses Playwright to visit company sites and detect technologies (Technographics).

### B. The Processing Pipeline (`src/pipeline/`)
Orchestrates the lifecycle of a lead:
- `daily.py`: The standard batch pipeline (Ingest -> Score -> Export).
- `autonomous.py`: A continuous, "always-on" loop for high-frequency signal tracking.
- `ingest.py`: Normalizes raw data from all collectors into a unified database format.

### C. Scoring & Logic (`src/scoring/` & `src/promotion_policy.py`)
- Calculates a numeric score for every account based on the observations found.
- Applies **Recency Decay**: Newer signals are worth more than old ones.
- **Anti-Inflation:** Prevents a company from getting an infinite score just because they posted 100 identical job ads.

### D. Discovery (`src/discovery/`)
- Focused on "fishing" for new companies that are NOT yet in the system.
- Uses Wikidata and other public sources to build and expand the "Watchlist".

### E. Integrations (`src/integrations/`)
- **Anthropic/Claude:** Used for "Deep Research" to summarize why a company is a good fit.
- **Google Sheets:** Syncs the final results into a shared spreadsheet for sales teams.

---

## 3. The Data Pipelines

The system runs three main types of loops to keep data fresh:

### 1. The Daily Pipeline (Standard)
**Frequency:** Once per day (via Cron or `./signals start`).
1. **Ingest:** Runs all collectors for the known watchlist.
2. **Score:** Re-calculates scores for every account in the database.
3. **Export:** Generates `daily_scores_YYYYMMDD.csv` and other reports in `data/out/`.

### 2. The Autonomous Loop (High Scale)
**Frequency:** Continuous/Every 15-60 minutes.
- Uses PostgreSQL advisory locks to ensure multiple instances don't clash.
- Designed for 1,000+ accounts where a single daily batch is too slow.
- Processes signals incrementally as they arrive.

### 3. The Discovery Loop
**Frequency:** Weekly or on-demand.
- Mines Wikidata for companies matching the "CPG" (Consumer Packaged Goods) profile.
- Adds newly discovered domains to the main tracking watchlist.

---

## 4. Tech Stack Summary

- **Language:** Python 3.12+ (Type-hinted, Pydantic-driven).
- **Database:** PostgreSQL (with `psycopg3` and connection pooling).
- **Web Interface:** FastAPI + Uvicorn (serves the dashboard at port 8788).
- **Crawling:** Playwright (for JS-heavy sites), Trafilatura (for text extraction).
- **Infrastructure:** Docker & Docker Compose (running Postgres, Redis, and Huginn).
- **AI:** Claude 3.5 Sonnet (Anthropic API).

---

## 5. Summary of Signal Logic

1. **Observation:** A raw event (e.g., "Hiring for SAP Manager").
2. **Signal Mapping:** Map the event to a Signal in `config/signal_registry.csv` (e.g., `erp_modernization`).
3. **Weighting:** Multiply by Source Reliability (e.g., Job Board = 1.0, Reddit = 0.6).
4. **Promotion:** If `Total Score > Threshold`, move account to "Promoted" for sales review.
