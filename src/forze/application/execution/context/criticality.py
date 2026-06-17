"""Task-scoped request criticality for prioritized load shedding.

Like the invocation deadline, criticality is a deliberately **module-level**
ContextVar — read by components with no access to the execution context (the
process-wide resilience executor / bulkhead). A boundary binds it once per
invocation (user-facing vs background / prefetch) and everything downstream on
the same task inherits it for free, exactly mirroring
:mod:`~forze.application.execution.context.deadline`.

The default is :attr:`Criticality.NORMAL`, so a request that never binds a value
is treated uniformly — and prioritized shedding is a no-op when every request
shares a tier, which is why the bulkhead's behavior is unchanged until both a
strategy opts in (``prioritized=True``) and callers bind distinct tiers.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from enum import IntEnum
from typing import Generator

# ----------------------- #


class Criticality(IntEnum):
    """Request importance tier; a higher value is shed later under overload."""

    BEST_EFFORT = 0
    """Background, prefetch, or bulk work — shed first."""

    DEGRADED = 10
    """Degradable features — shed before normal interactive traffic."""

    NORMAL = 20
    """Default interactive traffic (the unbound default)."""

    CRITICAL = 30
    """User-facing critical path — shed last."""


# ....................... #

_criticality_var: ContextVar[Criticality] = ContextVar(
    "forze_request_criticality",
    default=Criticality.NORMAL,
)


# ....................... #


def current_criticality() -> Criticality:
    """The criticality bound for the current task (``NORMAL`` when unbound)."""

    return _criticality_var.get()


# ....................... #


def set_criticality(criticality: Criticality) -> Token[Criticality]:
    """Set the request criticality; reset with :func:`reset_criticality`.

    Engine fast path: a raw ContextVar set/reset pair avoids the
    ``@contextmanager`` generator overhead on the per-operation hot path
    (mirrors :func:`~forze.application.execution.context.deadline.set_deadline`).
    Prefer :func:`bind_criticality` outside the engine.
    """

    return _criticality_var.set(criticality)


# ....................... #


def reset_criticality(token: Token[Criticality]) -> None:
    """Reset the criticality to its state before :func:`set_criticality`."""

    _criticality_var.reset(token)


# ....................... #


@contextmanager
def bind_criticality(criticality: Criticality | None) -> Generator[None]:
    """Bind the request criticality for the block.

    ``criticality=None`` is a no-op passthrough, so a boundary can forward an
    optional per-request tier without branching.
    """

    if criticality is None:
        yield
        return

    token = set_criticality(criticality)

    try:
        yield

    finally:
        reset_criticality(token)
