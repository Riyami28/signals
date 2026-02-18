#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

PGHOST="${SIGNALS_PG_HOST:-127.0.0.1}"
PGPORT="${SIGNALS_PG_PORT:-55432}"
PGUSER="${SIGNALS_PG_USER:-signals}"
PGDATABASE="${SIGNALS_PG_DB:-signals}"
PGPASSWORD_VALUE="${SIGNALS_PG_PASSWORD:-signals_dev_password}"

WATCHLIST_CSV="${SIGNALS_WATCHLIST_CSV:-$ROOT_DIR/config/watchlist_accounts.csv}"
HANDLES_CSV="${SIGNALS_HANDLES_CSV:-$ROOT_DIR/config/account_source_handles.csv}"

if [[ ! -f "$WATCHLIST_CSV" ]]; then
  echo "Missing watchlist csv: $WATCHLIST_CSV" >&2
  exit 1
fi

if [[ ! -f "$HANDLES_CSV" ]]; then
  echo "Missing handles csv: $HANDLES_CSV" >&2
  exit 1
fi

export PGPASSWORD="$PGPASSWORD_VALUE"

psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -f scripts/sql/postgres_init_signals.sql

psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "TRUNCATE signals.stage_watchlist;"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "\\copy signals.stage_watchlist FROM '$WATCHLIST_CSV' CSV HEADER"

psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "TRUNCATE signals.stage_handles;"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "\\copy signals.stage_handles FROM '$HANDLES_CSV' CSV HEADER"

psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -f scripts/sql/postgres_upsert_watchlist.sql

psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "
SELECT
  (SELECT COUNT(*) FROM signals.accounts) AS accounts,
  (SELECT COUNT(*) FROM signals.account_metadata) AS metadata_rows,
  (SELECT COUNT(*) FROM signals.account_source_handles) AS handle_rows,
  (SELECT COUNT(*) FROM signals.account_metadata WHERE country <> '') AS metadata_with_country,
  (SELECT COUNT(*) FROM signals.account_metadata WHERE region_group <> '') AS metadata_with_region,
  (SELECT COUNT(*) FROM signals.account_metadata WHERE industry_label <> '') AS metadata_with_industry;
"
