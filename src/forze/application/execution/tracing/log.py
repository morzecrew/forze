"""Optional debug logging for runtime traces."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from forze.application._logger import logger

if TYPE_CHECKING:
    from ..deps.container import Deps

# ----------------------- #

_TRUTHY_ENV = frozenset({"1", "true", "yes"})


def _runtime_trace_log_from_env() -> bool:
    value = os.environ.get("FORZE_RUNTIME_TRACE_LOG", "").strip().lower()
    return value in _TRUTHY_ENV


# ....................... #


def log_runtime_trace(deps: Deps[Any]) -> None:
    """Log ``deps.runtime_trace().format_lines()`` at DEBUG when ``FORZE_RUNTIME_TRACE_LOG`` is set."""

    if not _runtime_trace_log_from_env():
        return

    trace = deps.runtime_trace()

    if trace is None or not trace.events:
        return

    logger.debug("Runtime trace (%s events):\n%s", len(trace.events), trace.format_lines())
