"""Server commands — FastAPI web/webhook/UI launchers."""

from __future__ import annotations

import typer


def serve_discovery_webhook_impl(host: str, port: int, log_level: str) -> None:
    try:
        import uvicorn  # type: ignore
    except Exception as exc:
        raise typer.BadParameter("uvicorn is required. Install project dependencies first.") from exc

    from src.discovery.webhook import app as discovery_app

    if discovery_app is None:
        raise typer.BadParameter("fastapi is required. Install project dependencies first.")

    uvicorn.run(discovery_app, host=host, port=port, log_level=log_level)


def serve_local_ui_impl(host: str, port: int, log_level: str) -> None:
    try:
        import uvicorn  # type: ignore
    except Exception as exc:
        raise typer.BadParameter("uvicorn is required. Install project dependencies first.") from exc

    from src.ui.local_app import app as local_ui_app

    uvicorn.run(local_ui_app, host=host, port=port, log_level=log_level)


def serve_web_impl(host: str, port: int, log_level: str) -> None:
    """Launch the Signals pipeline web UI."""
    try:
        import uvicorn  # type: ignore
    except Exception as exc:
        raise typer.BadParameter("uvicorn is required. Install project dependencies first.") from exc

    from src.web.app import create_app

    web_app = create_app()
    uvicorn.run(web_app, host=host, port=port, log_level=log_level)
