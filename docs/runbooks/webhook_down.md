# Webhook Down Runbook

## Symptoms
- `serve-discovery-webhook` is not reachable.
- `discover-ingest` shows low/zero `events_seen`.
- `external_discovery_events` ingestion stops increasing.

## Immediate Checks
1. Confirm webhook process is running:
   `ps aux | rg "serve-discovery-webhook"`
2. Confirm port is listening:
   `lsof -i :8787`
3. Verify auth token:
   `echo $SIGNALS_DISCOVERY_WEBHOOK_TOKEN`
4. Verify endpoint health from local host:
   `curl -i http://127.0.0.1:8787/v1/discovery/events`

## Recovery Steps
1. Restart webhook service:
   `python -m src.main serve-discovery-webhook --host 127.0.0.1 --port 8787`
2. Replay failed discovery events:
   `python -m src.main replay-discovery-events --date YYYY-MM-DD --only-failed`
3. Re-run ingestion and discovery:
   `python -m src.main discover-ingest --date YYYY-MM-DD`
   `python -m src.main run-hunt --date YYYY-MM-DD --profile light`
4. Process queued retries:
   `python -m src.main retry-failures --limit 50`

## Validation
- `discover-ingest` shows `events_seen > 0`.
- `data/out/discovery_queue_YYYYMMDD.csv` is regenerated.
- `data/out/ops_metrics_YYYYMMDD.csv` shows stable `ingest_lag_seconds`.
