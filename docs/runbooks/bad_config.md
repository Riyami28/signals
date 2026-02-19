# Bad Config Runbook

## Symptoms
- Sudden output drift after config edits.
- Discovery routing anomalies (`auto_push` drops unexpectedly).
- Collectors disabled unexpectedly.

## Immediate Checks
1. Validate modified config files:
   - `config/promotion_policy.csv`
   - `config/source_execution_policy.csv`
   - `config/discovery_thresholds.csv`
   - `config/signal_classes.csv`
2. Check for malformed CSV rows:
   `python -m src.main run-daily --date YYYY-MM-DD` and inspect failure stage.

## Recovery Steps
1. Restore known-good config snapshot (git checkout specific files/commit).
2. If only promotion policy changed:
   `scripts/rollback_promotion_policy.sh`
3. Re-run pipeline:
   `python -m src.main run-daily --date YYYY-MM-DD`
4. Replay failed events if needed:
   `python -m src.main replay-discovery-events --date YYYY-MM-DD --only-failed`

## Validation
- `run-daily` exits successfully.
- `data/out/discovery_metrics_YYYYMMDD.csv` and `ops_metrics_YYYYMMDD.csv` regenerate.
- Alert log (`data/out/alerts.log`) shows no new critical failures.
