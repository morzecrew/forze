"""Context: log depth, sections, and request-scoped bindings."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

import structlog

# ----------------------- #
# Depth (for section indentation)

_log_depth: ContextVar[int] = ContextVar("forze_log_v2_depth", default=0)


def get_depth() -> int:
    return _log_depth.get()


@contextmanager
def log_section() -> Iterator[None]:
    """Increase indentation depth inside a logical logging section."""
    depth = _log_depth.get()
    token = _log_depth.set(depth + 1)

    try:
        yield

    finally:
        _log_depth.reset(token)


# ----------------------- #
# Request-scoped context (correlation_id, etc.)


def bind_context(**kwargs: Any) -> Any:
    """Bind key-value pairs to the context-local context (e.g. correlation_id).

    Use at the start of a request (e.g. FastAPI middleware) so all loggers
    in that request get the context. Returns tokens for reset_contextvars if needed.
    """

    return structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    """Clear the context-local context. Call at the start of each request."""

    structlog.contextvars.clear_contextvars()


@contextmanager
def bound_context(**kwargs: Any) -> Iterator[None]:
    """Context manager to bind key-value pairs for the duration of the block.

    Use for request-scoped context (correlation_id, request_id, etc.).
    """
    with structlog.contextvars.bound_contextvars(**kwargs):
        yield
