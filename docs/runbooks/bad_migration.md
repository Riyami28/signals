# Bad Migration Runbook

## Symptoms
- App starts failing after schema change.
- `init_db` or runtime queries fail on missing/invalid columns.
- CI migration checks fail.

## Immediate Checks
1. Run migration checks:
   `scripts/check_migrations.sh`
2. Validate DB connectivity:
   `python -m src.main score --date YYYY-MM-DD`
3. Inspect latest errors:
   `tail -n 200 data/out/alerts.log`

## Recovery Steps
1. Take backup before remediation:
   `scripts/backup_restore_drill.sh`
2. Roll forward with fixed migration if possible (preferred).
3. If rollback required, restore from last known-good backup in staging/local test DB.
4. Re-run validation:
   `./.venv/bin/pytest -q`
   `python -m src.main run-daily --date YYYY-MM-DD`

## Validation
- Migration checks pass.
- Postgres integration tests pass.
- Daily pipeline runs successfully and outputs are generated.
