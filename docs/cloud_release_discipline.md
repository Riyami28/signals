# Cloud Release Discipline

## Staging Baseline
- Use production-like Postgres version and schema.
- Validate env parity using `config/staging.env.example`.
- Run full integration suite before deploy.

## Mandatory Pre-Deploy Checks
1. Migration safety:
   `scripts/check_migrations.sh`
2. Backup hook:
   `scripts/backup_restore_drill.sh`
3. Integration tests:
   `./.venv/bin/pytest -q`

## Deployment Gate
Deploy only if all are true:
- CI Postgres integration checks pass.
- Migration checks pass.
- Backup/restore drill succeeds.
- No critical alerts in the previous 24h.

## Post-Deploy Validation
1. Run `ops-metrics`:
   `python -m src.main ops-metrics --date YYYY-MM-DD`
2. Confirm `retry_queue_size` and `quarantine_size` are stable.
3. Confirm alert routing is operational:
   `python -m src.main alert-test --title "post-deploy health" --body "cloud validation"`
