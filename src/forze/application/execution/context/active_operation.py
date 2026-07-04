"""Module-level marker for "an operation is currently executing on this task".

The marker is a deliberately **module-level** (immortal) ContextVar — the
CPython-blessed shape — used as a tripwire for the execution-context lifecycle
contract: :class:`~forze.application.execution.context.ExecutionContext` owns
per-instance ContextVars and per-scope caches, so it must be created once per
runtime scope, never per request. Constructing one while an operation is in
flight is the signature of per-request creation and triggers a warning.

The marker holds the :class:`asyncio.Task` that entered the enclosing operation
(``False`` when none is active) rather than a bare ``True``. A ContextVar is
*copied* into every task ``asyncio.create_task`` spawns, so a task a handler
spawns (``asyncio.create_task(facade.run(...))``) inherits the marker even
though it is a distinct, detached task. Recording the owning task lets the
invoke path tell a genuine in-await nested call (same task — rides the outer
drain slot) apart from a spawned operation (a *different* task — a fresh
top-level driver that must be admitted and tracked by the drain gate); see
:mod:`forze.application.execution.operations.run.invoke`.
"""

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Generator

# ----------------------- #

active_operation_var: ContextVar["asyncio.Task[Any] | bool"] = ContextVar(
    "forze_active_operation",
    default=False,
)
"""Owning :class:`asyncio.Task` of the enclosing operation, or ``False`` when none.

Truthy exactly when an operation is active on (or inherited into) the current
context; the invoke path additionally compares the stored task to the running
one to distinguish same-task nesting from a spawned operation."""

_warned_ctx_in_operation: bool = False

# ....................... #


@contextmanager
def operation_running() -> Generator[None]:
    """Mark the current task as executing an operation (token-reset on exit)."""

    token = active_operation_var.set(True)

    try:
        yield

    finally:
        active_operation_var.reset(token)


# ....................... #


def is_operation_running() -> bool:
    """Whether an operation is executing on the current task."""

    return bool(active_operation_var.get())


# ....................... #


def warn_if_constructed_in_operation() -> None:
    """Warn when an :class:`ExecutionContext` is constructed mid-operation.

    Creating execution contexts per request is not a supported mode: each
    context owns per-instance ContextVars (transaction scopes, outbox staging)
    and per-scope caches, so churning instances leaks entries into captured
    contexts and silently splits ambient state. The full warning is emitted
    once per process; later occurrences log at debug level.
    """

    if not active_operation_var.get():
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
