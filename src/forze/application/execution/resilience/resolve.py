"""Resolve the active resilience executor, with a shared default fallback.

Integration adapters call :func:`resolve_resilience_executor` so resilience works
out of the box even when an app has not registered :class:`ResilienceDepsModule`.
Apps override behavior by registering the module (the registered executor wins).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from forze.application.contracts.resilience import (
    ResilienceExecutorDepKey,
    ResilienceExecutorPort,
)

from .executor import InProcessResilienceExecutor
from .policies import builtin_default_policies

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #

_DEFAULT_EXECUTOR: ResilienceExecutorPort = InProcessResilienceExecutor(
    policies=builtin_default_policies(),
)
"""Process-wide default executor used when no app executor is registered."""


# ....................... #


def default_resilience_executor() -> ResilienceExecutorPort:
    """Return the shared process-default resilience executor."""

    return _DEFAULT_EXECUTOR


# ....................... #


def resolve_resilience_executor(ctx: ExecutionContext) -> ResilienceExecutorPort:
    """Return the app-registered executor, or the shared default if none exists."""

    if ctx.deps.exists(ResilienceExecutorDepKey):
        return ctx.deps.provide(ResilienceExecutorDepKey)

    return _DEFAULT_EXECUTOR
