"""Structured logging facade built on structlog.

Standard keys: ``scope``, ``source``, ``logger``, ``event``, ``level``, ``timestamp``.

Use :func:`configure` at startup. For request-scoped context (e.g. correlation_id),
use :func:`bound_context` or :func:`bind_context` at the start of each request.
"""

from ._core import (
    LogLevelName,
    bound_context,
    bind_context,
    clear_context,
    configure,
    get_config,
    getLogger,
    normalize_level,
    render_message,
    reset,
    safe_preview,
)
from .logger import Logger

__all__ = [
    "LogLevelName",
    "Logger",
    "bound_context",
    "bind_context",
    "clear_context",
    "configure",
    "get_config",
    "getLogger",
    "normalize_level",
    "render_message",
    "reset",
    "safe_preview",
]
