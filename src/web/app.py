"""FastAPI web application for the Signals pipeline UI."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from src import db
from src.settings import load_settings
from src.web.routes import accounts, batches, contacts, labels, pipeline, research, upload

logger = logging.getLogger(__name__)

_STATIC_DIR = Path(__file__).parent / "static"


def create_app() -> FastAPI:
    settings = load_settings()
    app = FastAPI(title="Signals Pipeline UI", version="0.2.0")

    # Initialize DB schema once at startup (not on every request)
    try:
        conn = db.get_connection(settings.pg_dsn)
        db.init_db(conn)
        conn.close()
        logger.info("db_initialized at startup")
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

    # Static files
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    @app.get("/")
    def index():
        return FileResponse(str(_STATIC_DIR / "index.html"))

    return app


# Create and export app for uvicorn/gunicorn
app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8788)
