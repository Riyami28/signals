# Team Local Bootstrap (Docker Standard)

## Goal
Bring each team machine to a consistent local runtime using Docker and shared scripts.

## Prerequisites
- Docker + Docker Compose installed.
- Python `3.12+` and project `.venv` created.
- Repo cloned to local machine.

## Steps
1. Copy baseline env:
   `cp .env.example .env`
2. Update machine-specific env values (DB creds, webhook token, alert webhook).
3. Start stack:
   `scripts/local_stack_up.sh`
4. Confirm stack health:
   `scripts/local_stack_status.sh`
5. Initialize/refresh watchlist into Postgres:
   `scripts/postgres_update_watchlist.sh`
6. Run one smoke cycle:
   `python -m src.main run-daily --date YYYY-MM-DD`
7. Verify artifacts:
   - `data/out/review_queue_YYYYMMDD.csv`
   - `data/out/discovery_queue_YYYYMMDD.csv`
   - `data/out/crm_candidates_YYYYMMDD.csv`
   - `data/out/manual_review_queue_YYYYMMDD.csv`
   - `data/out/ops_metrics_YYYYMMDD.csv`

## Ongoing Ops
- Continuous local loop:
  `scripts/run_local_autonomous.sh`
- Process retries:
  `python -m src.main retry-failures --limit 50`
- Alert smoke test:
  `python -m src.main alert-test --title "signals smoke" --body "team local bootstrap"`

## Exit Criteria (Team Soak)
- >=98% successful runs.
- No unrecovered quarantines older than 24h.
- Medium band remains in `manual_review_queue` and does not leak into `crm_candidates`.
