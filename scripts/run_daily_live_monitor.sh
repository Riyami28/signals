#!/usr/bin/env bash
set -euo pipefail

RUN_DATE="${1:-$(date +%F)}"
LIVE_MAX_ACCOUNTS="${2:-1000}"
STAGE_TIMEOUT_SECONDS="${3:-900}"
POLL_INTERVAL_SECONDS="${4:-5}"
LIVE_WORKERS_PER_SOURCE="${5:-${SIGNALS_LIVE_WORKERS_PER_SOURCE:-}}"

if [[ ! -x "./.venv/bin/python" ]]; then
  echo "error: ./.venv/bin/python not found. Create the virtualenv first."
  exit 1
fi

mkdir -p output
STAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DATE_SUFFIX="${RUN_DATE//-/}"
LOG_PATH="output/run_daily_live_${RUN_DATE_SUFFIX}_${STAMP}.log"

CMD=(
  "./.venv/bin/python"
  "-m"
  "src.main"
  "run-daily"
  "--date"
  "$RUN_DATE"
  "--live-max-accounts"
  "$LIVE_MAX_ACCOUNTS"
  "--stage-timeout-seconds"
  "$STAGE_TIMEOUT_SECONDS"
)
if [[ -n "${LIVE_WORKERS_PER_SOURCE}" ]] && [[ "${LIVE_WORKERS_PER_SOURCE}" != "auto" ]]; then
  CMD+=("--live-workers-per-source" "${LIVE_WORKERS_PER_SOURCE}")
fi

echo "run_date=$RUN_DATE live_max_accounts=$LIVE_MAX_ACCOUNTS stage_timeout_seconds=$STAGE_TIMEOUT_SECONDS poll_interval_seconds=$POLL_INTERVAL_SECONDS"
echo "live_workers_per_source=${LIVE_WORKERS_PER_SOURCE:-auto}"
echo "log_path=$LOG_PATH"
echo "command=${CMD[*]}"

BASELINE="$("./.venv/bin/python" - "$RUN_DATE" <<'PY'
import sys

from src import db

run_date = str(sys.argv[1])
conn = db.get_connection()
try:
    max_attempt_row = conn.execute(
        "SELECT MAX(attempt_id) AS max_id FROM crawl_attempts WHERE date(attempted_at) = date(?)",
        (run_date,),
    ).fetchone()
    max_obs_row = conn.execute(
        "SELECT MAX(observed_at) AS max_obs FROM signal_observations WHERE date(observed_at) = date(?)",
        (run_date,),
    ).fetchone()
    max_attempt_id = int(max_attempt_row["max_id"] or 0) if max_attempt_row is not None else 0
    max_obs = str(max_obs_row["max_obs"] or "") if max_obs_row is not None else ""
    print(f"{max_attempt_id}\t{max_obs}")
finally:
    conn.close()
PY
)"

LAST_ATTEMPT_ID="${BASELINE%%$'\t'*}"
if [[ "$BASELINE" == *$'\t'* ]]; then
  LAST_OBSERVED_AT="${BASELINE#*$'\t'}"
else
  LAST_OBSERVED_AT=""
fi
LAST_LOG_LINE_COUNT=0
MONITOR_PID=""

cleanup() {
  if [[ -n "${MONITOR_PID:-}" ]]; then
    kill "$MONITOR_PID" >/dev/null 2>&1 || true
    wait "$MONITOR_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup INT TERM EXIT

monitor_loop() {
  while true; do
    local ts_utc
    ts_utc="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"

    local monitor_output
    monitor_output="$("./.venv/bin/python" - "$RUN_DATE" "$LAST_ATTEMPT_ID" "$LAST_OBSERVED_AT" <<'PY'
import json
import sys

from src import db

run_date = str(sys.argv[1])
last_attempt_id = int(sys.argv[2] or "0")
last_observed_at = str(sys.argv[3] or "")

try:
    conn = db.get_connection()
except Exception as exc:  # pragma: no cover - terminal helper only
    print(f"STATE\t{last_attempt_id}\t{last_observed_at}")
    print(f"monitor_error=connection_failed error={exc}")
    raise SystemExit(0)

try:
    attempts_by_source = conn.execute(
        """
        SELECT source, COUNT(*) AS attempts, COUNT(DISTINCT account_id) AS unique_accounts
        FROM crawl_attempts
        WHERE date(attempted_at) = date(?)
        GROUP BY source
        ORDER BY source
        """,
        (run_date,),
    ).fetchall()
    total_attempts = int(sum(int(row["attempts"] or 0) for row in attempts_by_source))

    obs_count_row = conn.execute(
        "SELECT COUNT(*) AS c FROM signal_observations WHERE date(observed_at) = date(?)",
        (run_date,),
    ).fetchone()
    observations_today = int(obs_count_row["c"] or 0) if obs_count_row is not None else 0

    latest_attempt_row = conn.execute(
        "SELECT MAX(attempt_id) AS max_id FROM crawl_attempts WHERE date(attempted_at) = date(?)",
        (run_date,),
    ).fetchone()
    latest_attempt_id = int(latest_attempt_row["max_id"] or 0) if latest_attempt_row is not None else 0

    latest_obs_row = conn.execute(
        "SELECT MAX(observed_at) AS max_obs FROM signal_observations WHERE date(observed_at) = date(?)",
        (run_date,),
    ).fetchone()
    latest_observed_at = str(latest_obs_row["max_obs"] or "") if latest_obs_row is not None else ""

    new_attempts_rows = conn.execute(
        """
        SELECT
          ca.attempt_id,
          ca.source,
          ca.status,
          ca.attempted_at,
          ca.error_summary,
          a.company_name,
          a.domain
        FROM crawl_attempts ca
        LEFT JOIN accounts a ON a.account_id = ca.account_id
        WHERE date(ca.attempted_at) = date(?)
          AND ca.attempt_id > ?
        ORDER BY ca.attempt_id ASC
        LIMIT 8
        """,
        (run_date, last_attempt_id),
    ).fetchall()
    new_attempts_count_row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM crawl_attempts
        WHERE date(attempted_at) = date(?)
          AND attempt_id > ?
        """,
        (run_date, last_attempt_id),
    ).fetchone()
    new_attempts_count = int(new_attempts_count_row["c"] or 0) if new_attempts_count_row is not None else 0

    if last_observed_at:
        new_signals_rows = conn.execute(
            """
            SELECT
              so.observed_at,
              so.source,
              so.signal_code,
              a.company_name,
              a.domain
            FROM signal_observations so
            LEFT JOIN accounts a ON a.account_id = so.account_id
            WHERE date(so.observed_at) = date(?)
              AND so.observed_at > ?
            ORDER BY so.observed_at ASC
            LIMIT 8
            """,
            (run_date, last_observed_at),
        ).fetchall()
        new_signals_count_row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM signal_observations
            WHERE date(observed_at) = date(?)
              AND observed_at > ?
            """,
            (run_date, last_observed_at),
        ).fetchone()
    else:
        new_signals_rows = conn.execute(
            """
            SELECT
              so.observed_at,
              so.source,
              so.signal_code,
              a.company_name,
              a.domain
            FROM signal_observations so
            LEFT JOIN accounts a ON a.account_id = so.account_id
            WHERE date(so.observed_at) = date(?)
            ORDER BY so.observed_at ASC
            LIMIT 8
            """,
            (run_date,),
        ).fetchall()
        new_signals_count_row = conn.execute(
            "SELECT COUNT(*) AS c FROM signal_observations WHERE date(observed_at) = date(?)",
            (run_date,),
        ).fetchone()
    new_signals_count = int(new_signals_count_row["c"] or 0) if new_signals_count_row is not None else 0

    latest_score_run = conn.execute(
        """
        SELECT run_id, status, started_at, finished_at
        FROM score_runs
        WHERE date(run_date) = date(?)
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (run_date,),
    ).fetchone()
    score_line = "score=none"
    if latest_score_run is not None:
        run_id = str(latest_score_run["run_id"] or "")
        row_counts = conn.execute(
            """
            SELECT
              COUNT(*) AS total_rows,
              SUM(CASE WHEN tier = 'high' THEN 1 ELSE 0 END) AS high_rows,
              SUM(CASE WHEN tier = 'medium' THEN 1 ELSE 0 END) AS medium_rows
            FROM account_scores
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
        rows = int(row_counts["total_rows"] or 0) if row_counts is not None else 0
        high = int(row_counts["high_rows"] or 0) if row_counts is not None else 0
        medium = int(row_counts["medium_rows"] or 0) if row_counts is not None else 0
        score_line = (
            "score="
            + json.dumps(
                {
                    "run_id": run_id,
                    "status": str(latest_score_run["status"] or ""),
                    "started_at": str(latest_score_run["started_at"] or ""),
                    "finished_at": str(latest_score_run["finished_at"] or ""),
                    "rows": rows,
                    "high_rows": high,
                    "medium_rows": medium,
                },
                ensure_ascii=True,
            )
        )

    print(f"STATE\t{latest_attempt_id}\t{latest_observed_at}")
    snapshot = {
        "run_date": run_date,
        "crawl_total_attempts": total_attempts,
        "observations_today": observations_today,
        "by_source": [
            {
                "source": str(row["source"] or ""),
                "attempts": int(row["attempts"] or 0),
                "unique_accounts": int(row["unique_accounts"] or 0),
            }
            for row in attempts_by_source
        ],
    }
    print(f"snapshot={json.dumps(snapshot, ensure_ascii=True)}")
    if new_attempts_count > 0:
        print(f"new_attempts={new_attempts_count}")
        for row in new_attempts_rows:
            company = str(row["company_name"] or "").strip() or "-"
            domain = str(row["domain"] or "").strip() or "-"
            error = str(row["error_summary"] or "").strip()
            error_part = f" error={error}" if error else ""
            print(
                "attempt="
                + json.dumps(
                    {
                        "attempt_id": int(row["attempt_id"] or 0),
                        "source": str(row["source"] or ""),
                        "status": str(row["status"] or ""),
                        "attempted_at": str(row["attempted_at"] or ""),
                        "company_name": company,
                        "domain": domain,
                        "error_summary": error,
                    },
                    ensure_ascii=True,
                )
            )
    if new_signals_count > 0:
        print(f"new_signals={new_signals_count}")
        for row in new_signals_rows:
            print(
                "signal="
                + json.dumps(
                    {
                        "observed_at": str(row["observed_at"] or ""),
                        "source": str(row["source"] or ""),
                        "signal_code": str(row["signal_code"] or ""),
                        "company_name": str(row["company_name"] or ""),
                        "domain": str(row["domain"] or ""),
                    },
                    ensure_ascii=True,
                )
            )
    print(score_line)
except Exception as exc:  # pragma: no cover - terminal helper only
    print(f"STATE\t{last_attempt_id}\t{last_observed_at}")
    print(f"monitor_error=query_failed error={exc}")
finally:
    conn.close()
PY
)"

    if [[ -n "$monitor_output" ]]; then
      local state_line
      state_line="$(printf '%s\n' "$monitor_output" | head -n 1)"
      if [[ "$state_line" == $'STATE\t'* ]]; then
        local remainder
        remainder="${state_line#STATE$'\t'}"
        LAST_ATTEMPT_ID="${remainder%%$'\t'*}"
        if [[ "$remainder" == *$'\t'* ]]; then
          LAST_OBSERVED_AT="${remainder#*$'\t'}"
        fi
      fi
      printf '%s\n' "$monitor_output" | tail -n +2 | while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        echo "[${ts_utc:-unknown}] $line"
      done
    fi

    if [[ -f "$LOG_PATH" ]]; then
      local current_log_lines
      current_log_lines="$(wc -l < "$LOG_PATH" | tr -d ' ')"
      if [[ "$current_log_lines" =~ ^[0-9]+$ ]] && (( current_log_lines > LAST_LOG_LINE_COUNT )); then
        local start_line
        start_line=$((LAST_LOG_LINE_COUNT + 1))
        sed -n "${start_line},${current_log_lines}p" "$LOG_PATH" | while IFS= read -r log_line; do
          [[ -z "$log_line" ]] && continue
          echo "[${ts_utc:-unknown}] log=$log_line"
        done
        LAST_LOG_LINE_COUNT="$current_log_lines"
      fi
    fi
    sleep "$POLL_INTERVAL_SECONDS"
  done
}

monitor_loop &
MONITOR_PID="$!"

set +e
SIGNALS_VERBOSE_PROGRESS=1 "${CMD[@]}" >"$LOG_PATH" 2>&1
RUN_EXIT_CODE=$?
set -e

if [[ -n "${MONITOR_PID:-}" ]]; then
  kill "$MONITOR_PID" >/dev/null 2>&1 || true
  wait "$MONITOR_PID" >/dev/null 2>&1 || true
  MONITOR_PID=""
fi

TS_UTC_FINAL="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"
echo "[$TS_UTC_FINAL] run_exit_code=$RUN_EXIT_CODE"
echo "[$TS_UTC_FINAL] log_path=$LOG_PATH"
echo "[$TS_UTC_FINAL] ----- log_tail -----"
tail -n 40 "$LOG_PATH" || true
exit "$RUN_EXIT_CODE"
