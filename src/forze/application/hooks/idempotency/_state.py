"""Per-task flag: the idempotency result was recorded inside the business transaction.

Set by the in-transaction ``on_success`` record-write hook and read by the idempotency
middleware after the operation runs: when set, the middleware skips its
out-of-transaction ``commit`` (the record is already durable, atomically with the
business writes). When unset (a non-transactional store, or no in-transaction hook ran),
the middleware falls back to the out-of-transaction commit — so correctness never depends
on the hook being wired, only atomicity does.
"""

from contextvars import ContextVar

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


def reset_recorded_in_tx() -> None:
    """Clear the flag; the middleware calls this before each guarded invocation."""

    _recorded_in_tx.set(False)
