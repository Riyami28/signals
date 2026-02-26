# Bad Migration Runbook

## Background: How Migrations Work

Schema changes use a two-layer system:

| Layer | What it does | When it runs |
|---|---|---|
| `SCHEMA_SQL` in `src/db.py` | `CREATE TABLE IF NOT EXISTS` for all tables | Every `init_db()` call (idempotent) |
| `migrations/*.sql` | Numbered forward-only SQL files | `python -m src.main migrate` or `make migrate` |
| `_run_column_migrations()` | `ALTER TABLE ADD COLUMN IF NOT EXISTS` for legacy backfill | Every `init_db()` call (idempotent) |

Applied versions are tracked in the `schema_version` table. `migrate` is idempotent — running it twice is safe.

> **Important:** `migrate` is schema-only. It does **not** seed accounts. Account seeding happens via `_bootstrap()` on every `run-daily`/`ingest` run.

---

## Symptoms

- App fails after a schema change with `column does not exist` or `relation does not exist`.
- `init_db` raises during startup.
- CI migration checks fail (`scripts/check_migrations.sh`).
- `psycopg.ProgrammingError` or `InternalError` in pipeline logs.

---

## Immediate Checks

```bash
# 1. Run CI migration safety check
scripts/check_migrations.sh

# 2. See which migrations have been applied
docker exec signals-postgres psql -U signals -d signals \
  -c "SELECT * FROM signals.schema_version ORDER BY version;"

# 3. Check what migration files exist
ls -1 migrations/

# 4. Validate DB connectivity and table existence
python -m src.main score --date $(date +%Y-%m-%d)

# 5. Check recent errors
tail -n 200 data/out/alerts.log 2>/dev/null || true
```

---

## Recovery Steps

### Case 1 — Missing migration (version not applied)

```bash
# Apply pending migrations
make migrate
# or
python -m src.main migrate

# Verify
docker exec signals-postgres psql -U signals -d signals \
  -c "SELECT * FROM signals.schema_version ORDER BY version;"
```

### Case 2 — Migration file is broken / errored mid-apply

1. **Do not edit the failed migration file** — fix by creating a new one.
2. Identify the partial state:
   ```bash
   docker exec signals-postgres psql -U signals -d signals \
     -c "\d signals.<affected_table>"
   ```
3. Write a new migration file `migrations/NNN_fix_<description>.sql` that corrects the state.
4. Re-run `make migrate`.

### Case 3 — Schema out of sync with code (missing column)

```bash
# Check if _run_column_migrations covers it (legacy backfill)
grep -n "_ensure_column" src/db.py

# If not covered, add a new migration file:
# migrations/NNN_add_<column>_to_<table>.sql
# Content: ALTER TABLE <table> ADD COLUMN IF NOT EXISTS <col> <type> DEFAULT <val>;

make migrate
```

### Case 4 — Need to restore from backup

```bash
scripts/backup_restore_drill.sh   # restore to last known-good state
make migrate                       # re-apply any migrations since backup
pytest -q                          # validate
```

---

## Forbidden Patterns (CI Blocks These)

`scripts/check_migrations.sh` fails CI if any `.sql` file or `src/` code contains:

- `DROP TABLE` — use soft-deletes or archive tables instead
- `DROP COLUMN` — mark as deprecated in comments, remove in a later cycle

These are enforced to keep migrations forward-only and reversible via backup.

---

## Adding a New Migration

```bash
# 1. Find the next version number
ls migrations/

# 2. Create the file
# migrations/002_add_dimension_to_signals.sql

# 3. Structure:
# -- Migration NNN: short description
# -- Applied by: python -m src.main migrate
#
# ALTER TABLE signal_observations ADD COLUMN IF NOT EXISTS dimension TEXT NOT NULL DEFAULT '';
#
# INSERT INTO schema_version (version, description, applied_at)
# VALUES (NNN, 'add_dimension_to_signals', NOW()::TEXT)
# ON CONFLICT (version) DO NOTHING;

# 4. Test locally
make migrate

# 5. Verify idempotency
make migrate   # should print: migrations_applied=0 status=already_up_to_date
```

---

## Validation Checklist

- [ ] `scripts/check_migrations.sh` passes
- [ ] `python -m src.main migrate` prints `already_up_to_date`
- [ ] `pytest -q` — all tests pass
- [ ] `python -m src.main run-daily --date YYYY-MM-DD` completes with `exit_code=0`
