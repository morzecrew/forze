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
from typing import final

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import StrKey

# ----------------------- #


@final
@attrs.define(slots=True)
class OperationDrainGate:
    """Counts in-flight top-level operations; rejects new ones while draining.

    Single-event-loop discipline: :meth:`admit` and :meth:`release` run
    between awaits (no interleaving), so plain integer updates are safe. The
    idle event is touched only during a drain — the per-operation hot path
    pays one branch and one integer update on each side.
    """

    _in_flight: int = attrs.field(default=0, init=False)
    """Top-level operations currently executing."""

    _draining: bool = attrs.field(default=False, init=False)
    """Whether the gate has stopped admitting new invocations."""

    _idle: asyncio.Event = attrs.field(factory=asyncio.Event, init=False, repr=False)
    """Set when the count reaches zero during a drain."""

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

    # ....................... #

    def release(self) -> None:
        """Mark a previously admitted invocation finished."""

        self._in_flight -= 1

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
