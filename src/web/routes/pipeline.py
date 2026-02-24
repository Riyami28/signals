"""Pipeline execution API with SSE streaming."""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src.web.pipeline_runner import run_pipeline_async, ACTIVE_QUEUES

logger = logging.getLogger(__name__)

router = APIRouter(tags=["pipeline"])


class PipelineRunRequest(BaseModel):
    account_ids: list[str] = []
    stages: list[str] = ["ingest", "score", "research", "export"]


@router.post("/pipeline/run")
async def start_pipeline(body: PipelineRunRequest):
    pipeline_run_id = await run_pipeline_async(body.account_ids, body.stages)
    return {"pipeline_run_id": pipeline_run_id}


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
