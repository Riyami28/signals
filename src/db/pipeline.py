from __future__ import annotations

import json
import uuid

from src.utils import utc_now_iso

# ---------------------------------------------------------------------------
# Pipeline Runs (Web UI)
# ---------------------------------------------------------------------------


def create_ui_pipeline_run(conn, account_ids: list[str], stages: list[str]) -> str:
    run_id = f"prun_{uuid.uuid4().hex[:12]}"
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
