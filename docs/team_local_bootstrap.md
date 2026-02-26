# Team Local Bootstrap

## Goal
Bring each team machine to a consistent local runtime using Docker, Python, and the Makefile.

## Prerequisites

| Requirement | Check |
|---|---|
| Python 3.12+ | `python3 --version` |
| Docker Desktop (running) | `docker info` |
| git + gh CLI | `gh auth status` |

---

## One-Command Setup (Recommended)

```bash
git clone https://github.com/talvinder/signals.git
cd signals
make setup        # full bootstrap (see steps below)
make dev          # starts web UI at http://localhost:8788
```

`make setup` runs `scripts/bootstrap.sh` which:

| Step | What happens |
|---|---|
| 1 | Checks Python 3.12+ |
| 2 | Creates `.venv`, runs `pip install -e .` |
| 3 | Installs Playwright Chromium |
| 4 | Copies `.env.example` → `.env` (skips if `.env` exists) |
| 5 | Starts Docker: Postgres (55432), Redis (56379), Huginn (3000) |
| 6 | Waits for Postgres health check |
| 7 | Runs `python -m src.main migrate` — schema only, no account data |
| 8 | Runs `pytest -q` smoke test |

> **Account seeding** — seed companies and the watchlist are loaded automatically on the **first** `./signals start` or `make dev` run. Schema creation and account seeding are intentionally separate.

---

## Manual Steps (if `make setup` can't run)

```bash
# 1. venv + deps
python3 -m venv .venv && source .venv/bin/activate
pip install -e .

# 2. env
cp .env.example .env          # then edit with your API keys

# 3. Docker
docker compose -f docker-compose.local.yml up -d
scripts/local_stack_status.sh  # wait until postgres shows healthy

# 4. Schema
python -m src.main migrate

# 5. First pipeline run (seeds accounts + watchlist)
./signals start --date $(date +%Y-%m-%d)
```

---

## Watchlist

A **1,000-company starter watchlist** is committed at `config/watchlist_accounts.csv`.
It seeds automatically on the first `./signals start`.

For a full **~143K company watchlist** (production use), regenerate from Wikidata:

```bash
./signals build-cpg-watchlist --limit 143000
# accounts load on next pipeline run automatically
./signals start
```

See [`docs/watchlist.md`](watchlist.md) for the full seeding flow and UI ingestion path.

---

## Key API Keys (edit `.env`)

```
SIGNALS_CLAUDE_API_KEY=sk-ant-...     # required for LLM research stage
SIGNALS_CLAUDE_MODEL=claude-sonnet-4-6
```

All other keys (Zoho, Apollo, Crunchbase, etc.) are optional until the relevant Epic (#13, #14) work starts.

---

## Ongoing Ops

```bash
make test                                     # run test suite
make lint                                     # ruff check + format check
make migrate                                  # apply pending schema migrations
./signals start                               # full daily pipeline
python -m src.main retry-failures --limit 50  # process retry queue
scripts/run_local_autonomous.sh               # continuous local loop
```

---

## Smoke Verification

After setup, confirm:

```bash
# DB has tables + accounts
docker exec signals-postgres psql -U signals -d signals \
  -c "SELECT COUNT(*) FROM signals.accounts;"

# Pipeline runs cleanly
./signals start --date $(date +%Y-%m-%d)
```

Expected artifacts in `data/out/`:
- `review_queue_YYYYMMDD.csv`
- `daily_scores_YYYYMMDD.csv`
- `source_quality_YYYYMMDD.csv`
- `promotion_readiness_YYYYMMDD.csv`

---

## Exit Criteria (Team Soak)

- ≥98% successful pipeline runs.
- No unrecovered quarantines older than 24h.
- Medium-tier accounts stay in review queue and do not leak into sales-ready output.
