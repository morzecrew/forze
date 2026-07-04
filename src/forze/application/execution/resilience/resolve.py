"""Resolve the active resilience executor, with a shared default fallback.

Integration adapters call :func:`resolve_resilience_executor` so resilience works
out of the box even when an app has not registered :class:`ResilienceDepsModule`.
Apps override behavior by registering the module (the registered executor wins).
"""

import asyncio
from typing import TYPE_CHECKING
from weakref import WeakKeyDictionary

from forze.application.contracts.resilience import (
    ResilienceExecutorDepKey,
    ResilienceExecutorPort,
)

from .executor import InProcessResilienceExecutor
from .policies import builtin_default_policies

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #

_default_executors: "WeakKeyDictionary[asyncio.AbstractEventLoop, ResilienceExecutorPort]" = (
    WeakKeyDictionary()
)
"""Per-event-loop default executors. Bulkhead waiter futures are loop-affine, so a single
process-wide instance shared across loops (a multi-loop app, or sequential pytest loops)
would ``set_result`` a waiter on a foreign/closed loop — a ``RuntimeError``. Each running
loop gets its own default; entries drop when the loop is collected."""

_OFF_LOOP_FALLBACK: ResilienceExecutorPort = InProcessResilienceExecutor(
    policies=builtin_default_policies(),
)
"""Used only when there is no running loop (rare — e.g. an attrs ``factory=`` at eager,
off-loop construction). Binds to the first loop that parks a waiter, matching the historical
single-instance behavior; loop-bound callers get a per-loop instance instead."""


# ....................... #


def default_resilience_executor() -> ResilienceExecutorPort:
    """Return a process-default resilience executor bound to the running event loop.

    Keyed per loop so a bulkhead never wakes a waiter parked on a different loop (see
    ``_default_executors``). Called without a running loop, it returns the shared off-loop
    fallback.
    """

    try:
        loop = asyncio.get_running_loop()

    except RuntimeError:
        return _OFF_LOOP_FALLBACK

    executor = _default_executors.get(loop)

    if executor is None:
        executor = InProcessResilienceExecutor(policies=builtin_default_policies())
        _default_executors[loop] = executor

    return executor


# ....................... #


def resolve_resilience_executor(ctx: "ExecutionContext") -> ResilienceExecutorPort:
    """Return the app-registered executor, or the per-loop default if none exists."""

    if ctx.deps.exists(ResilienceExecutorDepKey):
        return ctx.deps.provide(ResilienceExecutorDepKey)

    return default_resilience_executor()
