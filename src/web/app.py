"""FastAPI web application for the Signals pipeline UI."""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from src import db, notifier
from src.settings import load_settings
from src.web.routes import accounts, batches, contacts, labels, pipeline, research, upload

logger = logging.getLogger(__name__)

_STATIC_DIR = Path(__file__).parent / "static"
_RESCORE_POLL_INTERVAL = 30  # seconds


def _rescore_worker(settings: object) -> None:
    """Daemon thread: poll retry_queue for rescore_account tasks every 30s."""
    from src.scoring.engine import rescore_account

    while True:
        time.sleep(_RESCORE_POLL_INTERVAL)
        try:
            conn = db.get_connection(settings.pg_dsn)
            try:
                tasks = db.fetch_due_retry_tasks(conn, limit=10)
                for task in tasks:
                    if task["task_type"] != "rescore_account":
                        continue
                    db.mark_retry_task_running(conn, task["task_id"])
                    try:
                        payload = json.loads(task["payload_json"])
                        result = rescore_account(conn, payload["account_id"], settings)
                        if result.get("tier_changes"):
                            for product, change in result["tier_changes"].items():
                                notifier.send_alert(
                                    settings,
                                    title=f"Tier Change — {payload.get('domain', payload['account_id'])}",
                                    body=(
                                        f"Product: {product}\n"
                                        f"Tier: {change['from']} → {change['to']}\n"
                                        f"Score: {change['score']}"
                                    ),
                                    severity="info",
                                )
                        db.mark_retry_task_completed(conn, task["task_id"])
                    except Exception as exc:
                        logger.exception("rescore_worker task_id=%s error=%s", task["task_id"], exc)
                        db.reschedule_retry_task(conn, task["task_id"], str(exc))
            finally:
                conn.close()
        except Exception:
            logger.exception("rescore_worker poll error")


def create_app() -> FastAPI:
    settings = load_settings()
    app = FastAPI(title="Signals Pipeline UI", version="0.2.0")

    # Initialize DB schema once at startup (not on every request)
    try:
        conn = db.get_connection(settings.pg_dsn)
        try:
            db.init_db(conn)
            logger.info("db_initialized at startup")
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("db_init_at_startup failed: %s", exc)

    # --- Configurable CORS ---
    origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["Authorization", "Content-Type"],
    )

    is_dev = settings.env.lower() == "development"
    api_key = settings.api_key

    @app.middleware("http")
    async def security_middleware(request: Request, call_next):
        # --- API key authentication ---
        if not is_dev and api_key and request.url.path.startswith("/api/"):
            auth_header = request.headers.get("authorization", "")
            if not auth_header.startswith("Bearer ") or auth_header[7:] != api_key:
                logger.warning("auth_rejected path=%s", request.url.path)
                return JSONResponse(status_code=401, content={"detail": "invalid or missing API key"})

        # --- Request logging + security headers ---
        start = time.monotonic()
        response = await call_next(request)
        duration = round(time.monotonic() - start, 4)

        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval'; style-src 'self' 'unsafe-inline'"
        )

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
    app.include_router(batches.router, prefix="/api")
    app.include_router(contacts.router, prefix="/api")
    app.include_router(labels.router, prefix="/api")
    app.include_router(pipeline.router, prefix="/api")
    app.include_router(research.router, prefix="/api")
    app.include_router(batches.router, prefix="/api")
    app.include_router(upload.router, prefix="/api")

    # Background rescore worker
    @app.on_event("startup")
    def start_rescore_worker() -> None:
        import os

        if os.environ.get("PYTEST_CURRENT_TEST"):
            return  # Skip background worker during test runs to prevent DB deadlocks
        t = threading.Thread(target=_rescore_worker, args=(settings,), daemon=True, name="rescore-worker")
        t.start()
        logger.info("rescore_worker started poll_interval=%ds", _RESCORE_POLL_INTERVAL)

    # Static files
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    @app.get("/")
    def index():
        return FileResponse(str(_STATIC_DIR / "index.html"))

    return app
