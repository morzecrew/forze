"""Module-level marker for "an operation is currently executing on this task".

The marker is a deliberately **module-level** (immortal) ContextVar — the
CPython-blessed shape — used as a tripwire for the execution-context lifecycle
contract: :class:`~forze.application.execution.context.ExecutionContext` owns
per-instance ContextVars and per-scope caches, so it must be created once per
runtime scope, never per request. Constructing one while an operation is in
flight is the signature of per-request creation and triggers a warning.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Generator

# ----------------------- #

_active_operation: ContextVar[bool] = ContextVar(
    "forze_active_operation",
    default=False,
)

_warned_ctx_in_operation: bool = False

# ....................... #


@contextmanager
def operation_running() -> Generator[None]:
    """Mark the current task as executing an operation (token-reset on exit)."""

    token = _active_operation.set(True)

    try:
        yield

    finally:
        _active_operation.reset(token)


# ....................... #


def is_operation_running() -> bool:
    """Whether an operation is executing on the current task."""

    return _active_operation.get()


# ....................... #


def warn_if_constructed_in_operation() -> None:
    """Warn when an :class:`ExecutionContext` is constructed mid-operation.

    Creating execution contexts per request is not a supported mode: each
    context owns per-instance ContextVars (transaction scopes, outbox staging)
    and per-scope caches, so churning instances leaks entries into captured
    contexts and silently splits ambient state. The full warning is emitted
    once per process; later occurrences log at debug level.
    """

    if not _active_operation.get():
        return

    global _warned_ctx_in_operation

    from forze.application._logger import logger

    if _warned_ctx_in_operation:
        logger.debug("ExecutionContext constructed inside an active operation")
        return

    _warned_ctx_in_operation = True
    logger.warning(
        "ExecutionContext constructed inside an active operation — contexts "
        "are one-per-runtime-scope and per-request creation is unsupported "
        "(per-instance ContextVars and per-scope caches leak across churning "
        "instances). Reuse the runtime's context instead. Further occurrences "
        "are logged at debug level.",
    )
