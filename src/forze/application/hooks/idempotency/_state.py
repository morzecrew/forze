"""Per-task flag: the idempotency result was recorded inside the business transaction.

Set by the in-transaction ``on_success`` record-write hook and read by the idempotency
middleware after the operation runs: when set, the middleware skips its
out-of-transaction ``commit`` (the record is already durable, atomically with the
business writes). When unset (a non-transactional store, or no in-transaction hook ran),
the middleware falls back to the out-of-transaction commit — so correctness never depends
on the hook being wired, only atomicity does.

The middleware brackets each guarded invocation with :func:`open_recording_scope` /
:func:`close_recording_scope` (a saved/restored ContextVar token), so a **nested**
idempotent operation's mark cannot leak into (or out of) the enclosing invocation's read:
each scope reads only the mark made by its own ``on_success`` hook.
"""

from contextvars import ContextVar, Token

# ----------------------- #

_recorded_in_tx: ContextVar[bool] = ContextVar(
    "idempotency_recorded_in_tx",
    default=False,
)


def recorded_in_tx() -> bool:
    """Whether the result was written inside the business transaction on this task."""

    return _recorded_in_tx.get()


def mark_recorded_in_tx() -> None:
    """Mark the result as recorded in-transaction (the in-tx ``on_success`` hook)."""

    _recorded_in_tx.set(True)


def open_recording_scope() -> Token[bool]:
    """Begin a fresh recording scope for one middleware invocation; return its token.

    Sets the flag to ``False`` and returns a token to restore the enclosing value, so a
    nested idempotent operation's mark is undone when that operation closes its own scope
    and cannot pollute this invocation's :func:`recorded_in_tx` read.
    """

    return _recorded_in_tx.set(False)


def close_recording_scope(token: Token[bool]) -> None:
    """Restore the flag to its value before the matching :func:`open_recording_scope`."""

    _recorded_in_tx.reset(token)
