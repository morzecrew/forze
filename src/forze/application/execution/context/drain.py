"""Per-scope drain gate: in-flight operation accounting for graceful shutdown.

The gate is owned by the scope's :class:`ExecutionContext`; the engine admits
every **top-level** invocation through it, while nested dispatch (an operation
invoked from within an already-admitted operation) rides the outer
invocation's slot — so draining never starves in-flight work of its own
dispatch chains. :meth:`ExecutionRuntime.shutdown` flips the gate before
running lifecycle teardown: new invocations are rejected with a retryable
``THROTTLED`` error (``code="draining"``; **429** at the FastAPI edge, a
requeue-worthy nack for queue consumers) and in-flight operations get a
bounded window to finish before the clients they depend on are closed.
"""

import asyncio
from typing import Any, final

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import StrKey

# ----------------------- #


def _current_task() -> "asyncio.Task[Any] | None":
    """The running task, or ``None`` when called outside a running event loop.

    ``asyncio.current_task()`` *raises* (not returns ``None``) with no running loop, which
    the gate's synchronous counter paths must tolerate."""

    try:
        return asyncio.current_task()

    except RuntimeError:
        return None


@final
@attrs.define(slots=True)
class OperationDrainGate:
    """Tracks in-flight top-level operations; rejects new ones while draining.

    Single-event-loop discipline: :meth:`admit` and :meth:`release` run
    between awaits (no interleaving), so the plain integer update and the task-set
    membership update are both safe. The idle event is touched only during a drain —
    the per-operation hot path pays one branch, one integer update, and one set add /
    discard on each side.

    The gate also holds the :class:`asyncio.Task` of each admitted operation so a drain
    that times out can *cancel* the stragglers (:meth:`cancel_in_flight`) rather than leave
    them running against the clients lifecycle teardown is about to close.
    """

    _in_flight: int = attrs.field(default=0, init=False)
    """Top-level operations currently executing."""

    _draining: bool = attrs.field(default=False, init=False)
    """Whether the gate has stopped admitting new invocations."""

    _idle: asyncio.Event = attrs.field(
        factory=asyncio.Event,
        init=False,
        repr=False,
    )
    """Set when the count reaches zero during a drain."""

    _tasks: set["asyncio.Task[Any]"] = attrs.field(
        factory=set,
        init=False,
        repr=False,
    )
    """Tasks of the admitted top-level operations, for cancellation on a drain timeout.

    Each :meth:`admit` adds the running task and its :meth:`release` (in the operation's
    ``finally``) discards it, so the set holds exactly the operations still in flight —
    :meth:`cancel_in_flight` cancels them when the drain window expires."""

    # ....................... #

    @property
    def in_flight(self) -> int:
        """Number of top-level operations currently executing."""

        return self._in_flight

    # ....................... #

    @property
    def draining(self) -> bool:
        """Whether new invocations are being rejected."""

        return self._draining

    # ....................... #

    def admit(self, op: StrKey) -> None:
        """Admit a top-level invocation of *op*, or reject it while draining.

        :raises CoreException: ``THROTTLED`` (``code="draining"``) once the
            scope is draining — retryable by contract: the request can be
            replayed against another instance or after a restart.
        """

        if self._draining:
            raise exc.throttled(
                f"Runtime scope is draining; operation {str(op)!r} rejected",
                code="draining",
                details={"op": str(op)},
            )

        self._in_flight += 1

        # Track the running operation's task so a timed-out drain can cancel it. admit and
        # its paired release both run on this task (release is in the operation's finally,
        # no task switch between), so the same handle is added here and discarded there.
        task = _current_task()

        if task is not None:
            self._tasks.add(task)

    # ....................... #

    def release(self) -> None:
        """Mark a previously admitted invocation finished."""

        self._in_flight -= 1

        task = _current_task()

        if task is not None:
            self._tasks.discard(task)

        if self._draining and self._in_flight <= 0:
            self._idle.set()

    # ....................... #

    async def drain(self, timeout: float) -> bool:
        """Stop admitting and wait up to *timeout* seconds for in-flight work.

        Idempotent; the gate stays draining afterwards either way (the scope
        is going down). Returns immediately when nothing is in flight.

        :returns: ``True`` when the gate drained fully, ``False`` when the
            timeout expired with operations still in flight (the caller
            decides whether to proceed; :attr:`in_flight` has the count).
        """

        self._draining = True

        if self._in_flight <= 0:
            return True

        try:
            async with asyncio.timeout(timeout):
                await self._idle.wait()

            return True

        except TimeoutError:
            return False

    # ....................... #

    async def cancel_in_flight(self, *, grace: float) -> int:
        """Cancel operations still in flight and await their unwind, bounded by *grace*.

        Called after :meth:`drain` times out, **before** lifecycle teardown closes the
        clients the abandoned operations still hold: cancelling lets each unwind its own way
        out — roll back its transaction, release its connection — instead of running on
        against a client that is about to close under it. A task inside its shielded commit
        critical section finishes that commit first (cancellation is deferred past the
        shield), so a committed effect is never torn; the invocation boundary then surfaces
        the cancellation as it already does.

        The await is bounded by *grace* so one uncooperative task (ignoring
        :class:`asyncio.CancelledError`) cannot wedge shutdown — teardown proceeds once it
        elapses. Returns the number of operations cancelled.
        """

        pending = [task for task in self._tasks if not task.done()]

        for task in pending:
            task.cancel()

        if pending and grace > 0:
            # Wait for the cancelled tasks to unwind; ignore their outcomes (a cancelled
            # task raises CancelledError — expected). ``asyncio.wait`` never raises here.
            await asyncio.wait(pending, timeout=grace)

        return len(pending)
