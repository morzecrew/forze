"""Structured logging built on structlog.

Standard keys: ``scope``, ``source``, ``logger``, ``event``, ``level``, ``timestamp``.

Use :func:`configure` at startup. For request-scoped context (e.g. correlation_id),
use :func:`bound_context` or :func:`bind_context` at the start of each request.
"""

from .config import (
    LogLevelName,
    get_config,
    level_no,
    normalize_level,
)
from .context import bind_context, bound_context, clear_context
from .facade import configure, getLogger, register_unhandled_exception_handler, reset
from .logger import Logger

# ----------------------- #

__all__ = [
    "LogLevelName",
    "Logger",
    "bound_context",
    "bind_context",
    "clear_context",
    "configure",
    "get_config",
    "getLogger",
    "level_no",
    "normalize_level",
    "register_unhandled_exception_handler",
    "reset",
]
