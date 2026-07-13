"""Task-scoped invocation deadline (ambient time seam).

The deadline is a deliberately **module-level** (immortal) ContextVar — the
CPython-blessed shape — because it is read by components with no access to the
execution context (the process-wide resilience executor, port wrappers, the CPU
offload seam). A boundary binds it once per invocation; everything downstream on
the same task — nested scopes, dispatched operations, resilience strategies,
offloaded work — inherits it for free.

Values are **absolute** monotonic instants from the ambient time seam
(:func:`~forze.base.primitives.monotonic`), so a bound simulation clock makes
deadlines advance in virtual time. Binding is tighten-only: a nested bind can
shorten the budget but never extend it past the enclosing deadline (gRPC-style
propagation).

Lives in ``forze.base.primitives`` (the lowest layer) so base-level seams such
as the CPU offload primitive can read it without importing upward;
:mod:`forze.application.execution.context.deadline` re-exports these names for
the application layer.
"""

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar, Token

from .datetime import monotonic

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

DEFAULT_DRIVER_DEADLINE_GRACE = 0.1
"""Slack (seconds) a driver-side timeout backstop adds to the remaining budget.

Keeps the driver bound (Postgres ``statement_timeout`` / Mongo CSOT) slightly **looser** than
the authoritative :func:`asyncio.timeout` armed at the invocation boundary, so that fires
first and classifies the expiry (deadline / commit-ambiguous). The driver bound only ever
fires as a backstop — cleanly cancelling an in-flight query the asyncio cancellation could
not — and, being positive, avoids the ``statement_timeout``/``maxTimeMS`` trap where **0
means unlimited**."""


def driver_deadline_budget(grace: float = DEFAULT_DRIVER_DEADLINE_GRACE) -> float | None:
    """Seconds for a driver-side timeout backstop: remaining budget + *grace*, or ``None``.

    ``None`` when no invocation deadline is bound (the caller then sets no driver timeout).
    See :data:`DEFAULT_DRIVER_DEADLINE_GRACE` for why it is looser than the asyncio deadline
    and always positive."""

    remaining = remaining_time()

    return None if remaining is None else remaining + grace


# ....................... #


def set_deadline(timeout: float) -> Token[float | None]:
    """Set a deadline of *timeout* seconds from now; reset with :func:`reset_deadline`.

    Tighten-only, like :func:`bind_deadline`. Engine fast path: a raw
    ContextVar set/reset pair avoids the ``@contextmanager`` generator overhead
    on the per-operation hot path. Prefer :func:`bind_deadline` outside the engine.
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


def clear_deadline() -> Token[float | None]:
    """Drop any bound deadline for the current context; reset with :func:`reset_deadline`.

    For best-effort work that must not be aborted by the invocation deadline: after this,
    :func:`remaining_time` returns ``None`` (no budget) until the token is reset.
    """

    return _deadline_var.set(None)


# ....................... #


@contextmanager
def bind_deadline(timeout: float | None) -> Generator[None]:
    """Bind an invocation deadline of *timeout* seconds from now.

    Tighten-only: when a deadline is already bound, the effective deadline is the
    earlier of the two — a nested bind never extends the enclosing budget.
    ``timeout=None`` is a no-op passthrough, so boundaries can forward an optional
    per-request timeout without branching.
    """

    if timeout is None:
        yield
        return

    token = set_deadline(timeout)

    try:
        yield

    finally:
        reset_deadline(token)
