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

The engine's own machinery also hops tasks while remaining *inside* one
admitted operation — a two-phase ``prepare`` task, a hedged attempt, the
post-commit callback runner, a concurrent graph wave. Those hops adopt the
enclosing operation onto the new task **explicitly** via
:func:`continue_operation_on_task` at each engine spawn site (never ambiently —
ambient inheritance is exactly what misclassifies user-spawned tasks), so a
dispatch they make is recognized as nested and rides the admitted slot.
"""

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Awaitable, Generator

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


async def continue_operation_on_task[T](awaitable: Awaitable[T]) -> T:
    """Await *awaitable* as an engine-internal continuation of the enclosing operation.

    Re-stamps an inherited active-operation marker onto the **current** task, so a
    dispatch made while awaiting is classified as nested — it rides the admitted
    operation's drain slot instead of being re-admitted (and, mid-drain, rejected
    with ``THROTTLED`` ``code="draining"``). Wrap the payload of every engine task
    spawn that stays *inside* one admitted operation: the two-phase ``prepare``
    task, each hedged attempt, the post-commit callback runner, a concurrent
    graph-wave step.

    Only for spawns the enclosing invocation structurally awaits (directly, or by
    cancelling on unwind) before releasing its slot — that is what keeps the
    drain gate's in-flight accounting exact: the slot is held until the whole
    chain settles, so drain still waits for it. A task that outlives its spawner
    must **not** adopt the operation; leave it unwrapped so the gate admits,
    counts, and can cancel it as the fresh top-level driver it is.

    No enclosing operation (or already on the owning task) is a plain
    passthrough, so the wrapper is safe on engine paths that also run outside
    operations (e.g. lifecycle waves).
    """

    inherited = active_operation_var.get()
    task = asyncio.current_task()

    if not inherited or task is None or inherited is task:
        return await awaitable

    token = active_operation_var.set(task)

    try:
        return await awaitable

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
