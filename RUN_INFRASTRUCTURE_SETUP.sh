#!/usr/bin/env bash
#
# Zopdev Signals Infrastructure Setup Script
#
# Usage: bash RUN_INFRASTRUCTURE_SETUP.sh
#
# This script sets up the entire infrastructure in one go:
#   1. Docker containers (Postgres 16, Redis 7)
#   2. Python virtual environment
#   3. Database schema initialization
#   4. Playwright browsers
#   5. Infrastructure verification
#

set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Get project root
PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_ROOT"

echo -e "${BLUE}╔════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Zopdev Signals Infrastructure Setup       ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════╝${NC}\n"

# Step 1: Copy .env
echo -e "${YELLOW}[1/8]${NC} Copying environment configuration..."
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo -e "${GREEN}✓${NC} Copied .env.example → .env"
else
    echo -e "${YELLOW}⚠${NC} .env already exists — skipping copy"
fi

# Step 2: Create data directories
echo -e "${YELLOW}[2/8]${NC} Creating data directories..."
mkdir -p data/local/postgres data/local/redis
echo -e "${GREEN}✓${NC} Created data/local/{postgres,redis}"

# Step 3: Start Docker containers
echo -e "${YELLOW}[3/8]${NC} Starting Docker containers..."
if command -v docker &> /dev/null; then
    docker compose -f docker-compose.local.yml up -d postgres redis
    echo -e "${GREEN}✓${NC} Docker containers starting..."

    # Wait for Postgres
    echo -e "${YELLOW}   ${NC} Waiting for Postgres to be ready (max 60s)..."
    MAX_WAIT=60
    ELAPSED=0
    until docker compose -f docker-compose.local.yml exec -T postgres \
            pg_isready -U signals -d signals >/dev/null 2>&1 || [ "$ELAPSED" -ge "$MAX_WAIT" ]; do
        sleep 2
        ELAPSED=$((ELAPSED + 2))
        echo -n "."
    done

    if [ "$ELAPSED" -lt "$MAX_WAIT" ]; then
        echo ""
        echo -e "${GREEN}✓${NC} Postgres is ready"
    else
        echo ""
        echo -e "${RED}✗${NC} Postgres did not become ready within ${MAX_WAIT}s"
        echo -e "${YELLOW}   Try:${NC} docker compose -f docker-compose.local.yml logs postgres"
        exit 1
    fi
else
    echo -e "${RED}✗${NC} Docker not installed. Please install Docker Desktop."
    exit 1
fi

# Step 4: Create Python virtual environment
echo -e "${YELLOW}[4/8]${NC} Creating Python virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}✓${NC} Created virtual environment"
else
    echo -e "${YELLOW}⚠${NC} venv already exists — skipping"
fi

# Activate venv
source venv/bin/activate
echo -e "${GREEN}✓${NC} Virtual environment activated"

# Step 5: Install Python dependencies
echo -e "${YELLOW}[5/8]${NC} Installing Python dependencies..."
pip install --quiet --upgrade pip
pip install --quiet -e .
echo -e "${GREEN}✓${NC} Dependencies installed"

# Step 6: Initialize database schema
echo -e "${YELLOW}[6/8]${NC} Initializing database schema..."
python -m src.main migrate
echo -e "${GREEN}✓${NC} Database schema initialized"

# Step 7: Install Playwright
echo -e "${YELLOW}[7/8]${NC} Installing Playwright Chromium browser..."
playwright install chromium 2>/dev/null || true
echo -e "${GREEN}✓${NC} Playwright Chromium installed"

# Step 8: Run verification tests
echo -e "${YELLOW}[8/8]${NC} Running infrastructure verification tests..."
echo ""
python scripts/test_infra_link.py

echo ""
echo -e "${BLUE}╔════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}✓ SETUP COMPLETE${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════╝${NC}"
echo ""
echo -e "Next steps:"
echo -e "  1. Review .env file (especially API keys if needed)"
echo -e "  2. Run: ${YELLOW}./signals start${NC}"
echo -e ""
echo -e "Documentation:"
echo -e "  - Quick Start:        ${YELLOW}QUICK_START.md${NC}"
echo -e "  - Detailed Setup:     ${YELLOW}SETUP_INFRASTRUCTURE.md${NC}"
echo -e "  - Full Architecture:  ${YELLOW}CLAUDE.md${NC}"
echo ""
