"""Structured logging configuration for the Signals pipeline."""

from __future__ import annotations

import logging
import sys


def configure_logging(level: str = "INFO") -> None:
    """Call once at process start from main.py."""
    fmt = "%(asctime)s %(levelname)-8s %(name)s %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        stream=sys.stdout,
        force=True,
    )
