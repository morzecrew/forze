"""Per-task marker: a root transaction has reached its commit point.

A cross-cutting marker in its own lightweight module (like
:mod:`~forze.application.execution.context.active_operation`) so both the
transaction scope (which sets it) and the invocation boundary (which reads and
clears it) can import it without pulling in each other's heavier dependencies.
"""

from contextvars import ContextVar

# ----------------------- #

# Module-level (immortal) per-task flag: it must outlive the ``scope`` generator to
# reach the invocation boundary, so it is deliberately not one of the scope's
# reset-in-``finally`` ContextVars.
_commit_started: ContextVar[bool] = ContextVar(
    "tx_root_commit_started",
    default=False,
)
"""Set once a root transaction's body completes and its commit is imminent/in-flight.

A deadline or cancellation surfacing at or after this point means the commit may have
(or has) landed, so the invocation boundary reports a **non-retryable**
``commit_ambiguous`` error instead of a retryable deadline — an at-least-once caller
must not blindly retry into a duplicate. Left set on purpose (a body failure/cancel
before the commit never sets it, so the body stays safely retryable); the top-level
boundary clears it via :func:`reset_commit_started`.
"""


# ....................... #


def commit_started() -> bool:
    """Whether a root transaction reached its commit point on this task.

    Read by the invocation boundary to tell a deadline that tore a commit (ambiguous
    outcome, non-retryable) from one that fired during the body (safely retryable).
    """

    return _commit_started.get()


# ....................... #


def mark_commit_started() -> None:
    """Mark the current root transaction's commit as imminent/in-flight.

    Called by :meth:`~forze.application.execution.context.transaction.TransactionContext.scope`
    once the body completes cleanly, just before the driver commit runs.
    """

    _commit_started.set(True)


# ....................... #


def reset_commit_started() -> None:
    """Clear the commit-reached flag; called by the top-level invocation boundary."""

    _commit_started.set(False)
