"""A forced-interleaving driver for deterministic concurrency tests.

A :class:`Gate` is one coroutine's lock-step checkpoint; a :class:`Conductor` releases the
participants one step at a time in a prescribed ``schedule``, so only one ever runs at a time. That
turns a normally-racy multi-transaction scenario (a write skew, a lost update, a non-repeatable
read) into a **deterministic** one — the determinism real-time concurrency can't give — that runs
the same way every time, on an in-memory mock or a real database. A general primitive (``asyncio`` +
``attrs`` only) for any test that needs an exact interleaving; its first use is adapter conformance.

The default handoff assumes each step either proceeds or aborts (an abort-based engine, like the
mock). A lock-based engine (real Postgres) can instead BLOCK a participant inside a step until a peer
commits; :meth:`Gate.arrive_blocking` (+ :meth:`Gate.wait_blocking` / :meth:`Gate.resume`) lets a
driver advance the lock-holder rather than wait for a park that cannot come — converting the block
into the same explicit signal the mock produces by aborting.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import attrs

# ----------------------- #

Session = Callable[["Gate"], Awaitable[None]]
"""One participant's script — a coroutine that calls ``gate.checkpoint()`` at each step boundary."""

# ....................... #


@attrs.define
class Gate:
    """One participant's lock-step checkpoint.

    The coroutine parks at :meth:`checkpoint`; the :class:`Conductor` resumes it with :meth:`release`
    when the schedule reaches its turn. Participants never touch each other — every handoff goes
    through the conductor, so the interleaving is exactly the schedule.
    """

    # ....................... #

    _turn: asyncio.Event = attrs.field(factory=asyncio.Event, init=False)
    _parked: asyncio.Event = attrs.field(factory=asyncio.Event, init=False)
    _blocking: asyncio.Event = attrs.field(factory=asyncio.Event, init=False)

    done: bool = attrs.field(default=False, init=False)

    # ....................... #

    async def checkpoint(self) -> None:
        """Participant side: park here until the conductor gives this session its turn."""

        self._parked.set()
        await self._turn.wait()
        self._turn.clear()

    # ....................... #

    async def arrive_blocking(self) -> None:
        """Participant side: announce the next step may BLOCK on a resource a peer holds, then run
        into it *without parking*.

        Unlike :meth:`checkpoint`, this does not wait for a turn — a lock-based engine would suspend
        the participant inside the step (a duplicate-key insert waiting on the unique index, a
        ``FOR UPDATE`` read waiting on the row lock) until the peer commits, so a park that never
        comes would wedge the conductor. On seeing this arrival the conductor advances the peer
        instead. On an abort-based engine the step does not block: the participant buffers and reaches
        its own commit / next checkpoint normally — the same script drives both.
        """

        self._blocking.set()

    # ....................... #

    async def wait_parked(self) -> None:
        """Conductor side: wait until the participant reaches its next checkpoint (or finishes)."""

        await self._parked.wait()

    # ....................... #

    async def wait_blocking(self) -> None:
        """Conductor side: wait until the participant announces it is entering a blocking step
        (or finishes without one)."""

        await self._blocking.wait()

    # ....................... #

    async def release(self) -> None:
        """Conductor side: resume the participant and wait until it parks again (or finishes)."""

        self._parked.clear()
        self._turn.set()
        await self._parked.wait()

    # ....................... #

    def resume(self) -> None:
        """Conductor side: give the participant its turn but do NOT wait for it to re-park — for a
        step that may block on a peer (the peer must be advanced to release it, so this participant
        cannot re-park on its own)."""

        self._parked.clear()
        self._turn.set()

    # ....................... #

    def mark_done(self) -> None:
        """Driver side: the participant's coroutine ended — unblock any pending :meth:`release` or
        :meth:`wait_blocking` (a participant that finished, or errored, without ever blocking)."""

        self.done = True
        self._parked.set()
        self._blocking.set()


# ....................... #


@attrs.define
class Conductor:
    """Run named participants through an explicit interleaving — ``schedule`` is the global step order.

    Each participant calls ``gate.checkpoint()`` at each step boundary; list its id in ``schedule``
    once per checkpoint it should pass (a participant with *k* checkpoints appears *k* times). Only
    one runs at a time, so the interleaving is exact and reproducible on a mock or a real database —
    the determinism a real-concurrency race can't provide.
    """

    schedule: tuple[str, ...]
    """The global step order."""

    # ....................... #

    async def run(self, sessions: dict[str, Session]) -> None:
        gates = {sid: Gate() for sid in sessions}

        async def driven(sid: str) -> None:
            try:
                await sessions[sid](gates[sid])
            finally:
                gates[sid].mark_done()

        tasks = [asyncio.create_task(driven(sid)) for sid in sessions]

        # let every participant reach its first checkpoint (or finish)
        for gate in gates.values():
            await gate.wait_parked()

        for sid in self.schedule:  # release one step at a time, in the prescribed order
            if not gates[sid].done:
                await gates[sid].release()

        await asyncio.gather(*tasks)
