#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

source .venv/bin/activate
python -m src.main build-cpg-watchlist --limit "${SIGNALS_WATCHLIST_LIMIT:-1000}"
