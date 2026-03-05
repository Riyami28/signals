from __future__ import annotations

import json
import uuid

from src.utils import utc_now_iso

# ---------------------------------------------------------------------------
# Pipeline Runs (Web UI)
# ---------------------------------------------------------------------------


def create_ui_pipeline_run(conn, run_id: str, account_ids: list[str], stages: list[str]) -> str:
    """Insert a new pipeline_runs row using the caller-supplied run_id.

    Previously this function generated its own UUID, which led to a mismatch:
    pipeline_runner.py created run_id A, this function inserted run_id B,
    and finish_ui_pipeline_run tried to UPDATE run_id A — so the DB row
    stayed stuck as 'running' forever.
    """
    conn.execute(
        """INSERT INTO pipeline_runs (pipeline_run_id, account_ids_json, stages_json)
           VALUES (%s, %s, %s)""",
        (run_id, json.dumps(account_ids), json.dumps(stages)),
    )
    conn.commit()
    return run_id


def finish_ui_pipeline_run(conn, pipeline_run_id: str, status: str, result: dict) -> None:
    conn.execute(
        """UPDATE pipeline_runs SET status = %s, result_json = %s, finished_at = CURRENT_TIMESTAMP
           WHERE pipeline_run_id = %s""",
        (status, json.dumps(result), pipeline_run_id),
    )
    conn.commit()
