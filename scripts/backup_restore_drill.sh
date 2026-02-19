#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v pg_dump >/dev/null 2>&1; then
  echo "pg_dump is required."
  exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "psql is required."
  exit 1
fi

PRIMARY_DSN="${SIGNALS_PG_DSN:-}"
TEST_DSN="${SIGNALS_TEST_PG_DSN:-}"

if [ -z "$PRIMARY_DSN" ] || [ -z "$TEST_DSN" ]; then
  echo "Set SIGNALS_PG_DSN and SIGNALS_TEST_PG_DSN before running backup/restore drill."
  exit 1
fi

STAMP="$(date +%Y%m%d_%H%M%S)"
DUMP_PATH="/tmp/signals_backup_${STAMP}.sql"

echo "[backup_restore_drill] dumping primary DB to $DUMP_PATH"
pg_dump "$PRIMARY_DSN" --no-owner --no-privileges --format=plain > "$DUMP_PATH"

echo "[backup_restore_drill] resetting test DB schema"
psql "$TEST_DSN" -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"

echo "[backup_restore_drill] restoring dump into test DB"
psql "$TEST_DSN" -v ON_ERROR_STOP=1 -f "$DUMP_PATH" >/dev/null

echo "[backup_restore_drill] validation query"
psql "$TEST_DSN" -v ON_ERROR_STOP=1 -c "SELECT COUNT(*) AS table_count FROM information_schema.tables WHERE table_schema='public';"

echo "[backup_restore_drill] ok dump_path=$DUMP_PATH"
