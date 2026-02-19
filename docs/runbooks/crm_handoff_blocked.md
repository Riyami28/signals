# CRM Handoff Blocked Runbook

## Scope
Current handoff path is CSV-based (`crm_candidates_YYYYMMDD.csv`). Direct CRM writeback is intentionally deferred.

## Symptoms
- `crm_candidates_YYYYMMDD.csv` missing or empty unexpectedly.
- `manual_review_queue_YYYYMMDD.csv` unexpectedly large.
- Promotion policy blocked too many high-confidence candidates.

## Immediate Checks
1. Confirm discovery outputs exist:
   `ls -la data/out/*{discovery_queue,crm_candidates,manual_review_queue}*.csv`
2. Confirm policy config:
   `cat config/promotion_policy.csv`
3. Recompute reports:
   `python -m src.main discover-report --date YYYY-MM-DD`

## Recovery Steps
1. If strict policy is too aggressive, rollback policy file:
   `scripts/rollback_promotion_policy.sh`
2. Rebuild reports after rollback:
   `python -m src.main discover-report --date YYYY-MM-DD`
3. If pipeline stage failed, run retries:
   `python -m src.main retry-failures --limit 50`
4. Regenerate daily outputs:
   `python -m src.main run-daily --date YYYY-MM-DD`

## Validation
- `crm_candidates_YYYYMMDD.csv` contains only `policy_decision=auto_push`.
- `manual_review_queue_YYYYMMDD.csv` contains `policy_decision=manual_review`.
- `ops_metrics_YYYYMMDD.csv` includes acceptable `handoff_success_rate`.
