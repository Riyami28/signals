"""FastAPI web application for the Signals pipeline UI."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.web.routes import accounts, labels, pipeline, research

logger = logging.getLogger(__name__)

_STATIC_DIR = Path(__file__).parent / "static"


def create_app() -> FastAPI:
    app = FastAPI(title="Signals Pipeline UI", version="0.2.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        start = time.monotonic()
        response = await call_next(request)
        duration = round(time.monotonic() - start, 4)
        logger.info(
            "http_request method=%s path=%s status=%d duration_seconds=%.4f",
            request.method,
            request.url.path,
            response.status_code,
            duration,
        )
        return response

    # API routes
    app.include_router(accounts.router, prefix="/api")
    app.include_router(labels.router, prefix="/api")
    app.include_router(pipeline.router, prefix="/api")
    app.include_router(research.router, prefix="/api")

    # Static files
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    @app.get("/")
    def index():
        return FileResponse(str(_STATIC_DIR / "index.html"))

    return app
