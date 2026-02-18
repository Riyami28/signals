#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ ! -d ".venv" ]; then
  echo "Missing .venv. Create it and install dependencies first."
  exit 1
fi

source .venv/bin/activate

export SIGNALS_ENABLE_LIVE_CRAWL="${SIGNALS_ENABLE_LIVE_CRAWL:-1}"

WEBHOOK_HOST="${SIGNALS_WEBHOOK_HOST:-127.0.0.1}"
WEBHOOK_PORT="${SIGNALS_WEBHOOK_PORT:-8787}"
INGEST_INTERVAL_MINUTES="${SIGNALS_INGEST_INTERVAL_MINUTES:-15}"
SCORE_INTERVAL_MINUTES="${SIGNALS_SCORE_INTERVAL_MINUTES:-60}"
DISCOVERY_INTERVAL_MINUTES="${SIGNALS_DISCOVERY_INTERVAL_MINUTES:-180}"
HUNT_PROFILE="${SIGNALS_HUNT_PROFILE:-light}"

python -m src.main serve-discovery-webhook --host "$WEBHOOK_HOST" --port "$WEBHOOK_PORT" --log-level warning &
WEBHOOK_PID=$!

cleanup() {
  kill "$WEBHOOK_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

python -m src.main run-autonomous-loop \
  --ingest-interval-minutes "$INGEST_INTERVAL_MINUTES" \
  --score-interval-minutes "$SCORE_INTERVAL_MINUTES" \
  --discovery-interval-minutes "$DISCOVERY_INTERVAL_MINUTES" \
  --hunt-profile "$HUNT_PROFILE"
