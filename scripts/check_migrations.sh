#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ -d ".venv" ]; then
  source .venv/bin/activate
fi

echo "[check_migrations] forward-only static checks"
if rg -n "DROP\s+TABLE|DROP\s+COLUMN" scripts/sql src/db.py >/dev/null 2>&1; then
  echo "Found forbidden destructive migration pattern (DROP TABLE/DROP COLUMN)."
  exit 1
fi

echo "[check_migrations] idempotency check"
python - <<'PY'
from src import db
from src.settings import load_settings

settings = load_settings()
conn = db.get_connection(settings.pg_dsn)
try:
    db.init_db(conn)
    db.init_db(conn)
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM information_schema.tables WHERE table_schema = current_schema()"
    ).fetchone()
    count = int(row["c"] if row else 0)
    if count <= 0:
        raise RuntimeError("no_tables_found_after_init")
    print(f"idempotency_check=ok tables={count}")
finally:
    conn.close()
PY

echo "[check_migrations] ok"
