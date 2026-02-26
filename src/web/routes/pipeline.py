"""Pipeline execution API with SSE streaming."""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src import db
from src.settings import load_settings
from src.web.pipeline_runner import ACTIVE_QUEUES, run_pipeline_async

logger = logging.getLogger(__name__)

router = APIRouter(tags=["pipeline"])


class PipelineRunRequest(BaseModel):
    account_ids: list[str] = []
    batch_id: str = ""
    stages: list[str] = ["ingest", "score", "research", "export"]


@router.post("/pipeline/run")
async def start_pipeline(body: PipelineRunRequest):
    account_ids = list(body.account_ids)

    # If batch_id is provided, resolve account_ids from batch and validate
    if body.batch_id:
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        try:
            db.init_db(conn)
            batch = db.get_upload_batch(conn, body.batch_id)
            if not batch:
                raise HTTPException(status_code=404, detail=f"Batch {body.batch_id} not found")
            batch_companies = db.get_batch_companies(conn, body.batch_id)
            if not batch_companies:
                raise HTTPException(status_code=400, detail=f"Batch {body.batch_id} has no companies")

            # Seed batch companies into accounts table and collect account_ids
            for company in batch_companies:
                domain = (company.get("domain") or "").strip()
                if not domain:
                    continue
                company_name = company.get("company_name") or domain
                acct_id = db.upsert_account(conn, company_name, domain, source_type="discovered")
                db.link_batch_company_account(conn, company["id"], acct_id)
                if acct_id not in account_ids:
                    account_ids.append(acct_id)
            conn.commit()
            db.update_batch_status(conn, body.batch_id, "processing")
        finally:
            conn.close()

    pipeline_run_id = await run_pipeline_async(account_ids, body.stages, batch_id=body.batch_id)
    return {"pipeline_run_id": pipeline_run_id, "batch_id": body.batch_id or None}


@router.get("/pipeline/stream/{pipeline_run_id}")
async def stream_pipeline(pipeline_run_id: str):
    """SSE endpoint — streams pipeline progress events."""

    async def event_generator():
        queue = ACTIVE_QUEUES.get(pipeline_run_id)
        if not queue:
            yield f"data: {json.dumps({'type': 'error', 'message': 'unknown run'})}\n\n"
            return

        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=120)
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("type") == "done":
                    break
            except asyncio.TimeoutError:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
