"""Task-scoped invocation deadline.

The deadline is a deliberately **module-level** (immortal) ContextVar — the
CPython-blessed shape, mirroring
:mod:`~forze.application.execution.context.active_operation` — because it is
read by components with no access to the execution context (the process-wide
resilience executor, port wrappers). A boundary binds it once per invocation;
everything downstream on the same task — nested scopes, dispatched
operations, resilience strategies — inherits it for free.

Values are **absolute** :func:`time.monotonic` instants. Binding is
tighten-only: a nested bind can shorten the budget but never extend it past
the enclosing deadline (gRPC-style propagation).
"""

import time
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Generator

# ----------------------- #

_deadline_var: ContextVar[float | None] = ContextVar(
    "forze_invocation_deadline",
    default=None,
)

# ....................... #


def current_deadline() -> float | None:
    """The absolute :func:`time.monotonic` deadline, or ``None`` when unbound."""

    return _deadline_var.get()


# ....................... #


def remaining_time() -> float | None:
    """Seconds left until the deadline (clamped at ``0.0``), or ``None`` when unbound."""

    deadline = _deadline_var.get()

    if deadline is None:
        return None

    return max(0.0, deadline - time.monotonic())


# ....................... #


@contextmanager
def bind_deadline(timeout: float | None) -> Generator[None]:
    """Bind an invocation deadline of *timeout* seconds from now.

    Tighten-only: when a deadline is already bound, the effective deadline is
    the earlier of the two — a nested bind never extends the enclosing budget.
    ``timeout=None`` is a no-op passthrough, so boundaries can forward an
    optional per-request timeout without branching.
    """

    if timeout is None:
        yield
        return

    requested = time.monotonic() + timeout
    existing = _deadline_var.get()
    effective = requested if existing is None else min(existing, requested)

    token = _deadline_var.set(effective)

    try:
        yield

    finally:
        _deadline_var.reset(token)
