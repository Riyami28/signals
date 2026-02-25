#!/usr/bin/env bash
# Bootstrap script for the Signals project.
# Run once after cloning to set up the full local development environment.
#
# Usage: ./scripts/bootstrap.sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# ── Helpers ─────────────────────────────────────────────────────────────────

info()  { echo "[bootstrap] $*"; }
error() { echo "[bootstrap] ERROR: $*" >&2; exit 1; }

require_command() {
    command -v "$1" >/dev/null 2>&1 || error "$1 is required but not installed. See README.md prerequisites."
}

# ── 1. Prerequisites check ──────────────────────────────────────────────────

info "Checking prerequisites..."
require_command python3
require_command docker
require_command git

PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
PYTHON_MAJOR=$(echo "$PYTHON_VERSION" | cut -d. -f1)
PYTHON_MINOR=$(echo "$PYTHON_VERSION" | cut -d. -f2)
if [ "$PYTHON_MAJOR" -lt 3 ] || { [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 12 ]; }; then
    error "Python 3.12+ is required (found $PYTHON_VERSION)"
fi
info "Python $PYTHON_VERSION ✓"

# ── 2. Virtual environment ──────────────────────────────────────────────────

if [ ! -d ".venv" ]; then
    info "Creating virtual environment..."
    python3 -m venv .venv
fi

info "Activating virtual environment..."
# shellcheck disable=SC1091
source .venv/bin/activate

# ── 3. Install dependencies ─────────────────────────────────────────────────

info "Installing Python dependencies..."
pip install --quiet --upgrade pip
pip install --quiet -e .

# ── 4. Playwright browser ───────────────────────────────────────────────────

info "Installing Playwright Chromium browser..."
playwright install chromium 2>/dev/null || info "  (Playwright install failed — OK if not using JS fallback)"

# ── 5. Environment file ─────────────────────────────────────────────────────

if [ ! -f ".env" ]; then
    info "Copying .env.example → .env"
    cp .env.example .env
    # Set the project root to this directory
    sed -i.bak "s|SIGNALS_PROJECT_ROOT=.*|SIGNALS_PROJECT_ROOT=$ROOT_DIR|" .env
    rm -f .env.bak
    info "  Edit .env to add API keys (SIGNALS_CLAUDE_API_KEY, etc.)"
else
    info ".env already exists — skipping copy"
fi

# ── 6. Docker stack ─────────────────────────────────────────────────────────

info "Starting Docker services (Postgres + Redis + Huginn)..."
mkdir -p data/local/postgres data/local/redis
docker compose -f docker-compose.local.yml up -d

# ── 7. Wait for Postgres ────────────────────────────────────────────────────

info "Waiting for Postgres to be ready..."
MAX_WAIT=60
ELAPSED=0
until docker compose -f docker-compose.local.yml exec -T postgres \
        pg_isready -U signals -d signals >/dev/null 2>&1; do
    if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
        error "Postgres did not become ready within ${MAX_WAIT}s"
    fi
    sleep 2
    ELAPSED=$((ELAPSED + 2))
done
info "Postgres is ready ✓"

# ── 8. Initialize DB schema ─────────────────────────────────────────────────

info "Initialising database schema..."
python -m src.main migrate || python3 -m src.main migrate

# ── 9. Seed data ────────────────────────────────────────────────────────────

info "Loading seed accounts..."
python -m src.main seed-accounts 2>/dev/null \
    || python3 -m src.main seed-accounts 2>/dev/null \
    || info "  seed-accounts command not found — skipping (data will be seeded on first run)"

# ── 10. Smoke test ──────────────────────────────────────────────────────────

info "Running smoke test (pytest -q --tb=short)..."
pytest -q --tb=short || {
    info "  Some tests failed — check output above. Environment is still usable."
}

# ── Done ────────────────────────────────────────────────────────────────────

info ""
info "Bootstrap complete!"
info ""
info "Next steps:"
info "  make dev          → Start the web UI at http://localhost:8788"
info "  ./signals start   → Run the full pipeline"
info "  make test         → Run the test suite"
info ""
info "Before running the pipeline, set your API keys in .env:"
info "  SIGNALS_CLAUDE_API_KEY=sk-ant-..."
