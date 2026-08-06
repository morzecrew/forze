"""Execution-scoped classification of a cancellation delivered to a durable body.

A durable body learns about a cancel request the ordinary Python way: a
:class:`asyncio.CancelledError` at its next await point. That is deliberate — it needs no new
API and it composes with every ``async with`` and ``finally`` a body already has.

But a ``CancelledError`` says *that* the body was stopped, never *why*. A drain-timeout, a
lost lease, a deadline, and an operator's "Stop" all arrive identically, and the durable saga
executor has to tell them apart: an operator cancel before the pivot should compensate and
land the run ``CANCELLED``, whereas the same exception from a drain is crash-shaped and must
propagate untouched. This binding is that discriminator, and nothing more.

It is **framework-internal on purpose** and is not surfaced on
:class:`~forze.application.contracts.durable.function.DurableRunContext`. Handing bodies a
pollable flag invites pre-emptive ``if cancelled: return`` checks between awaits, which is a
different feature with a different failure mode (a half-finished body that reports success);
that stays gated on a consumer that demonstrably needs it. Classifying a cancellation the
runtime already delivered is not that.
"""

from contextvars import ContextVar, Token
from typing import Self, final

# ----------------------- #


@final
class DurableCancelSignal:
    """Why the current durable body was cancelled, and what the run decided about it.

    One instance per run execution, created by the runner and shared by reference with the
    body's task (a context copy carries the same object, so the body's mutations are visible
    to the runner that awaits it). Single-task mutation only — no lock: the heartbeat sets
    :meth:`request` and the body's own task sets :meth:`refuse`, both on one event loop.
    """

    __slots__ = ("_refused", "_requested")

    def __init__(self) -> None:
        self._requested = False
        self._refused = False

    # ....................... #

    @classmethod
    def already_refused(cls) -> Self:
        """A signal for a run that recorded a refusal in an **earlier** execution.

        Re-invoking such a run (recovery after its holder died mid-forward-completion)
        must not replay the cancellation: the request is on the record, so the heartbeat
        would read it back and tear the body down again, and the saga would spend a round
        re-refusing something it already refused. Starting spent — requested *and* refused —
        makes the persisted ask inert for this attempt, which is what "the refusal stands"
        means across a restart.
        """

        signal = cls()
        signal._requested = True
        signal._refused = True

        return signal

    # ....................... #

    @property
    def requested(self) -> bool:
        """Whether this run's cancellation was asked for by an operator.

        Set by the runner's heartbeat the moment a lease renewal reports the stamp, *before*
        it cancels the body — so a body observing the resulting ``CancelledError`` always
        sees this already ``True``.
        """

        return self._requested

    # ....................... #

    def request(self) -> None:
        """Mark the cancellation about to be delivered as operator-requested."""

        self._requested = True

    # ....................... #

    @property
    def refused(self) -> bool:
        """Whether the run observed the request and declined it (a saga past its pivot)."""

        return self._refused

    # ....................... #

    def refuse(self) -> None:
        """Decline the observed request; the run completes forward on its own merits.

        A refusal also **spends** the request as a classifier: any further cancellation is
        no longer attributable to this ask and propagates raw. Without that, a body that
        absorbs its cancel and keeps running would re-absorb every subsequent one — a drain
        or a deadline would be swallowed by a stale "the operator asked" reading.
        """

        self._refused = True


# ....................... #

_current_cancel_signal: ContextVar[DurableCancelSignal | None] = ContextVar(
    "forze_durable_cancel_signal",
    default=None,
)


# ....................... #


def bind_durable_cancel_signal(
    signal: DurableCancelSignal,
) -> Token[DurableCancelSignal | None]:
    """Bind *signal* as the cancellation discriminator for the current durable run."""

    return _current_cancel_signal.set(signal)


# ....................... #


def reset_durable_cancel_signal(token: Token[DurableCancelSignal | None]) -> None:
    """Reset the binding from :func:`bind_durable_cancel_signal`."""

    _current_cancel_signal.reset(token)


# ....................... #


def current_durable_cancel_signal() -> DurableCancelSignal | None:
    """Return the active cancellation signal, or ``None`` outside a durable run."""

    return _current_cancel_signal.get()
