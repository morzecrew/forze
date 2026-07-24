"""Per-scope registry of background loops, brought to a clean stop before teardown.

A background lifecycle step — an outbox relay, a queue or stream consumer, a durable recovery
poller or scheduler — owns a task that runs for the life of the process. Nothing outside the
step can reach that task: it lives in the step's own hook object, which the lifecycle plan
holds and the runtime never sees. So shutdown could only ever do the blunt thing, and every one
of these steps did exactly that — ``task.cancel()``, mid-work, whatever the work was.

Cancelling a poll loop between ticks is harmless. Cancelling one *mid-unit* is not: a queue
consumer loses the ack for the message it was handling, a commit-stream consumer never commits
the offset for the batch it just processed (so the whole batch is redelivered), an outbox relay
strands claimed rows in ``processing`` until a lease expires. The work is at-least-once, so
nothing is *lost* — but every shutdown pays for it in duplicates and delay.

A loop registers itself here at startup, and the runtime asks each one to stop — between units
of work, bounded — *before* lifecycle teardown begins. Which also means the pool is still open
while they stop, so a loop whose graceful stop needs its database (the relay's drain) can have it.
"""

import asyncio
from typing import Protocol, final, runtime_checkable

import attrs

from forze.application._logger import logger

# ----------------------- #


@runtime_checkable
class DrainableLoop(Protocol):
    """A background loop that can be brought to a stop instead of cancelled."""

    @property
    def loop_name(self) -> str:
        """Identifies the loop in shutdown logs — ``outbox_relay:events``."""
        ...  # pragma: no cover

    async def stop(self, *, deadline: float) -> bool:
        """Stop taking new work, finish what is in hand, and return.

        Must be **idempotent** (the runtime stops loops before teardown, and the step's own
        shutdown hook may ask again) and must not raise on an already-stopped loop.

        :param deadline: An ``asyncio`` loop-clock instant. Past it, the loop should give up
            gracefully rather than keep working — the runtime cancels whatever is still going.
        :returns: ``True`` when the loop stopped cleanly within the deadline.
        """
        ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class StoppedLoops:
    """Per-loop outcome of :meth:`Drainables.stop_all`.

    The aggregate count alone cannot say **which** loops came to rest — and a caller
    about to act on a stopped loop's resources (quiesce flushing a relay's backlog)
    must not race one that is still unwinding or wedged mid-unit.
    """

    clean: tuple[DrainableLoop, ...]
    """The loops that stopped cleanly within the grace — the only ones whose owned
    work is provably no longer moving."""

    total: int
    """How many loops were asked to stop."""

    # ....................... #

    @property
    def count(self) -> int:
        """How many loops stopped cleanly."""

        return len(self.clean)


# ....................... #


@final
@attrs.define(slots=True)
class Drainables:
    """Registry of this scope's background loops, stopped once at shutdown."""

    _loops: list[DrainableLoop] = attrs.field(factory=list, init=False, repr=False)

    # ....................... #

    def register(self, loop: DrainableLoop) -> None:
        """Register *loop* to be stopped before lifecycle teardown.

        Called by a background step at startup. Held strongly, unlike ``BackgroundOwners`` —
        the loop's task keeps it alive anyway, and the registry dies with the scope.
        """

        self._loops.append(loop)

    # ....................... #

    @property
    def loops(self) -> tuple[DrainableLoop, ...]:
        """Every registered loop, in registration order."""

        return tuple(self._loops)

    # ....................... #

    async def stop_all(self, *, grace: float) -> StoppedLoops:
        """Stop every registered loop concurrently, bounded overall by *grace* seconds.

        Concurrent because these are independent workers on independent backends; a relay
        draining its outbox has no reason to wait for a consumer to finish its message. Each
        loop bounds itself against the shared deadline, and failures are isolated — one wedged
        loop must not keep the others from stopping cleanly.

        :returns: which loops stopped cleanly (see :class:`StoppedLoops` — a caller acting
            on a stopped loop's resources must know *which*, not merely *how many*).
        """

        loops = list(self._loops)

        if not loops:
            return StoppedLoops(clean=(), total=0)

        clock = asyncio.get_running_loop()
        deadline = clock.time() + grace
        tasks = [asyncio.ensure_future(one.stop(deadline=deadline)) for one in loops]

        try:
            async with asyncio.timeout_at(deadline):
                await asyncio.gather(*tasks, return_exceptions=True)

        except TimeoutError:
            # The grace elapsed with a loop still stopping. Cancel it and let it unwind within
            # whatever is left of the *same* deadline — never a second window — then move on:
            # teardown must not hang on a wedged loop.
            for task in tasks:
                task.cancel()

            await asyncio.wait(tasks, timeout=max(0.0, deadline - clock.time()))

            logger.warning(
                "Background loops did not all stop within %.1fs; cancelled the stragglers",
                grace,
            )

        clean: list[DrainableLoop] = []

        for one, task in zip(loops, tasks, strict=True):
            if not task.done() or task.cancelled():
                continue

            error = task.exception()

            if error is not None:
                # Isolated, but never silent: a loop that cannot stop is diagnosable.
                logger.error(
                    "Background loop %s failed to stop at shutdown",
                    one.loop_name,
                    exc_info=error,
                )
                continue

            if task.result():
                clean.append(one)

        logger.info("Stopped %d of %d background loop(s)", len(clean), len(loops))

        return StoppedLoops(clean=tuple(clean), total=len(loops))
