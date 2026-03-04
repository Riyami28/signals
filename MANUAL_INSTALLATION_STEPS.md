# Manual Installation Steps — What YOU Need to Do

**Current System Status:**
- ❌ Docker NOT installed
- ❌ Python 3.12+ NOT available (only 3.9.6)
- ✅ .env file created
- ✅ venv exists
- ✅ Some packages partially installed

**Blockers:** Cannot proceed without Docker and Python 3.12+

---

## Step 1: Install Docker (Required)

### macOS

**Option A: Docker Desktop (Recommended)**
1. Download: https://www.docker.com/products/docker-desktop
2. Install: Drag Docker.app to Applications
3. Start: Click Docker app
4. Verify: `docker --version`

**Option B: Homebrew**
```bash
brew install docker docker-compose
```

### Linux (Ubuntu/Debian)
```bash
sudo apt-get update
sudo apt-get install docker.io docker-compose-plugin

sudo usermod -aG docker $USER
newgrp docker

docker --version  # Verify
```

### Verify Installation
```bash
docker --version
docker compose --version
docker run hello-world
```

---

## Step 2: Install Python 3.12+ (Required)

### macOS

**Option A: Homebrew (Easiest)**
```bash
brew install python@3.12

# Verify
python3.12 --version  # Should output: Python 3.12.x
```

**Option B: Download Installer**
1. Go to: https://www.python.org/downloads/
2. Download Python 3.12.x
3. Run installer
4. Verify: `python3.12 --version`

**Option C: pyenv**
```bash
# Install pyenv
curl https://pyenv.run | bash

# Install Python 3.12
pyenv install 3.12.0
pyenv global 3.12.0

# Verify
python3 --version
```

### Linux (Ubuntu/Debian)
```bash
sudo apt-get install python3.12 python3.12-venv python3.12-dev

python3.12 --version  # Verify
```

---

## Step 3: Recreate Virtual Environment (After Installing Python 3.12)

```bash
cd /Users/zopdec/signals

# Remove old venv
rm -rf venv

# Create new venv with Python 3.12
python3.12 -m venv venv

# Activate
source venv/bin/activate

# Verify
python --version  # Should show: Python 3.12.x

which python  # Should show: /Users/zopdec/signals/venv/bin/python
```

---

## Step 4: Install Project Dependencies

```bash
# Make sure venv is activated
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install project
pip install -e .

# Verify installation
pip list | grep -E "psycopg|httpx|playwright|pydantic|typer"

# Expected output:
# httpx               0.28.1
# pydantic            2.12.5
# pydantic-settings   2.11.0
# playwright          1.40.0
# psycopg             3.1.12
# typer               0.9.0
```

---

## Step 5: Start Docker Services

```bash
# Make sure Docker is running
docker --version

# Start PostgreSQL and Redis
docker compose -f docker-compose.local.yml up -d postgres redis

# Verify
docker compose -f docker-compose.local.yml ps

# Expected output:
# NAME               STATUS
# signals-postgres   Up X seconds (healthy)
# signals-redis      Up X seconds (healthy)
```

---

## Step 6: Initialize Database

```bash
# Make sure venv is activated
source venv/bin/activate

# Run migration
python -m src.main migrate

# Expected output:
# [timestamp] Initializing schema and tables...
# [timestamp] Table 'accounts' created
# [timestamp] Table 'signal_observations' created
# ... more tables ...
# [timestamp] Database initialization complete ✓
```

---

## Step 7: Install Playwright

```bash
# Install Chromium browser
playwright install chromium

# Takes 1-2 minutes (~300MB download)
```

---

## Step 8: Verify Everything Works

```bash
# Make sure venv is activated
source venv/bin/activate

# Run tests
python scripts/test_infra_link.py

# Expected output:
# ╔════════════════════════════════════════════╗
# ║  Infrastructure Connectivity Test          ║
# ╚════════════════════════════════════════════╝
#
# [1/6] Testing Settings
#   ✓ SIGNALS_PG_DSN set
#   ...
#
# [6/6] Testing HTTP Client
#   ✓ Fetched https://www.google.com (200 OK)
#
# ════════════════════════════════════════════
# ✓ ALL CHECKS PASSED — Ready for ./signals start
# ════════════════════════════════════════════
```

---

## Step 9: Run the Pipeline

```bash
# Make sure venv is activated
source venv/bin/activate

# Run pipeline
./signals start

# With verbose output
SIGNALS_VERBOSE_PROGRESS=1 ./signals start

# Expected output:
# [INGEST STAGE]
#   jobs: {"inserted": 47, "seen": 89}
#   news: {"inserted": 23, "seen": 156}
#   reddit: {"inserted": 31, "seen": 73}  ← Your Reddit collector!
#   ...
#
# [SCORE STAGE]
#   Scored 256 accounts
#
# [EXPORT STAGE]
#   Generated CSV files
```

---

## Step 10: Check Results

```bash
# View CSV outputs
ls -lh data/out/

# Expected files:
# review_queue.csv
# daily_scores.csv
# source_quality.csv

# Query database for Reddit observations
docker compose -f docker-compose.local.yml exec postgres \
  psql -U signals -d signals -c \
  "SELECT source, COUNT(*) FROM signal_observations GROUP BY source;"

# Expected output includes:
# reddit_api | 147
```

---

## Checklist Before Each Run

Before running `./signals start`, verify:

- [ ] Docker running: `docker ps`
- [ ] Postgres healthy: `docker compose ps | grep postgres`
- [ ] venv activated: `echo $VIRTUAL_ENV` shows `/Users/zopdec/signals/venv`
- [ ] Python 3.12+: `python --version` shows 3.12.x
- [ ] Dependencies installed: `pip list | grep psycopg`

---

## Quick Command Copy-Paste (After Installing Docker & Python 3.12)

```bash
# 1. Recreate venv
rm -rf venv
python3.12 -m venv venv
source venv/bin/activate

# 2. Install deps
pip install --upgrade pip && pip install -e .

# 3. Start Docker
docker compose -f docker-compose.local.yml up -d postgres redis

# 4. Initialize DB
python -m src.main migrate

# 5. Install Playwright
playwright install chromium

# 6. Test setup
python scripts/test_infra_link.py

# 7. Run pipeline
./signals start

# 8. Check results
ls -lh data/out/
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `python3.12: command not found` | Install Python 3.12 (see Step 2) |
| `docker: command not found` | Install Docker (see Step 1) |
| `ModuleNotFoundError: No module named 'psycopg'` | Run `pip install -e .` in activated venv |
| `ERROR: Package requires Python >=3.12` | You're using wrong Python; activate venv with Python 3.12 |
| `Connection refused` on 55432 | Start Docker: `docker compose up -d postgres redis` |
| `Postgres not healthy` | Wait 10 seconds, containers still starting |
| `pg_isready: command not found` | Use Docker: `docker compose exec postgres pg_isready -U signals` |

---

## Summary

**You must manually:**
1. ✅ Install Docker
2. ✅ Install Python 3.12+

**After that, you can run:**
1. Recreate venv with Python 3.12
2. Install dependencies
3. Start Docker
4. Initialize database
5. Run pipeline

**This will take 20-30 minutes total** (most time is download/installation)

Once complete, you'll have:
- PostgreSQL 16 running
- Redis 7 running
- Full Signals pipeline with Reddit collector
- Ready to process real data

---

## Next Action

1. **Install Docker** first
2. **Install Python 3.12** second
3. **Come back here** and follow the copy-paste commands above
4. **Run pipeline**: `./signals start`

**You've got everything documented. Just need those two system-level installations!**
