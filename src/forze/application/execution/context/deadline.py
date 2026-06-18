"""Task-scoped invocation deadline.

The deadline is a deliberately **module-level** (immortal) ContextVar — the
CPython-blessed shape, mirroring
:mod:`~forze.application.execution.context.active_operation` — because it is
read by components with no access to the execution context (the process-wide
resilience executor, port wrappers). A boundary binds it once per invocation;
everything downstream on the same task — nested scopes, dispatched
operations, resilience strategies — inherits it for free.

Values are **absolute** monotonic instants from the ambient time seam
(:func:`~forze.base.primitives.monotonic`), so a bound simulation clock makes
deadlines advance in virtual time. Binding is tighten-only: a nested bind can
shorten the budget but never extend it past the enclosing deadline (gRPC-style
propagation).
"""

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Generator

from forze.base.primitives import monotonic

# ----------------------- #

_deadline_var: ContextVar[float | None] = ContextVar(
    "forze_invocation_deadline",
    default=None,
)

# ....................... #


def current_deadline() -> float | None:
    """The absolute monotonic deadline (ambient time seam), or ``None`` when unbound."""

    return _deadline_var.get()


# ....................... #


def remaining_time() -> float | None:
    """Seconds left until the deadline (clamped at ``0.0``), or ``None`` when unbound."""

    deadline = _deadline_var.get()

    if deadline is None:
        return None

    return max(0.0, deadline - monotonic())


# ....................... #


def set_deadline(timeout: float) -> Token[float | None]:
    """Set a deadline of *timeout* seconds from now; reset with :func:`reset_deadline`.

    Tighten-only, like :func:`bind_deadline`. Engine fast path: a raw
    ContextVar set/reset pair avoids the ``@contextmanager`` generator
    overhead on the per-operation hot path (mirrors
    ``InvocationContext.set_read_only``). Prefer :func:`bind_deadline`
    outside the engine.
    """

    requested = monotonic() + timeout
    existing = _deadline_var.get()
    effective = requested if existing is None else min(existing, requested)

    return _deadline_var.set(effective)


# ....................... #


def reset_deadline(token: Token[float | None]) -> None:
    """Reset the deadline to its state before :func:`set_deadline`."""

    _deadline_var.reset(token)


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

    token = set_deadline(timeout)

    try:
        yield

    finally:
        reset_deadline(token)
