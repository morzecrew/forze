"""Test utilities for Forze apps.

Currently: a **forced-interleaving driver** for deterministic concurrency tests. A :class:`Gate`
is one coroutine's lock-step checkpoint; a :class:`Conductor` releases the participants one step at
a time in a prescribed ``schedule``, so only one ever runs at a time. That turns a normally-racy
multi-transaction scenario (a write skew, a lost update, a non-repeatable read) into a
**deterministic** one — the determinism real-time concurrency can't give — that runs the same way
every time, on an in-memory mock or a real database.

Its first use is **adapter conformance**: drive the same isolation anomaly against every adapter
that claims an isolation level and assert they reach the same verdict — turning "trust the mock"
into "verify the mock". But nothing here is conformance- or DST-specific: it is a general primitive
(``asyncio`` + ``attrs`` only) for any test that needs an exact interleaving.
"""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

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

    done: bool = attrs.field(default=False, init=False)

    # ....................... #

    async def checkpoint(self) -> None:
        """Participant side: park here until the conductor gives this session its turn."""

        self._parked.set()
        await self._turn.wait()
        self._turn.clear()

    # ....................... #

    async def wait_parked(self) -> None:
        """Conductor side: wait until the participant reaches its next checkpoint (or finishes)."""

        await self._parked.wait()

    # ....................... #

    async def release(self) -> None:
        """Conductor side: resume the participant and wait until it parks again (or finishes)."""

        self._parked.clear()
        self._turn.set()
        await self._parked.wait()

    # ....................... #

    def mark_done(self) -> None:
        """Driver side: the participant's coroutine ended — unblock any pending :meth:`release`."""

        self.done = True
        self._parked.set()


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
