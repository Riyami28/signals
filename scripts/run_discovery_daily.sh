#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/raramuri/Projects/zopdev/signals"
PYTHON_BIN="$ROOT/.venv/bin/python"
LOG_PATH="$ROOT/data/out/discovery_daily.log"

mkdir -p "$ROOT/data/out"
cd "$ROOT"

SIGNALS_ENABLE_LIVE_CRAWL=0 "$PYTHON_BIN" -m src.main run-hunt --profile light >> "$LOG_PATH" 2>&1
