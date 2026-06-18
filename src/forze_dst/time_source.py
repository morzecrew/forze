"""A :class:`~forze.base.primitives.TimeSource` backed by the simulation loop's clock.

So that application reads of wall time, monotonic time, and time-ordered ids all
advance off the *one* virtual clock the :class:`SimulationEventLoop` controls — and
are therefore a deterministic function of how the scenario sleeps, nothing else.
"""

from datetime import UTC, datetime, timedelta
from typing import final
from uuid import UUID

import attrs

from .loop import SimulationEventLoop

# ----------------------- #

DEFAULT_EPOCH = datetime(2020, 1, 1, tzinfo=UTC)
"""Wall instant mapped to virtual ``loop.time() == 0.0`` (arbitrary but fixed)."""

# ....................... #


@final
@attrs.define(slots=True)
class SimulationTimeSource:
    """Wall + monotonic + ids derived from a :class:`SimulationEventLoop`'s virtual time.

    ``monotonic()`` is the loop clock directly; ``now()`` is ``epoch + loop.time()``;
    ids are UUIDv7 stamped from ``now()`` (so they sort by virtual time) with a
    sub-instant counter for strict ordering when several are minted at the same tick.
    Random bits still come from the ambient entropy seam, so the full id is
    deterministic under a bound :class:`SeededEntropySource`.
    """

    loop: SimulationEventLoop
    """The simulation loop to use for the time source."""

    epoch: datetime = DEFAULT_EPOCH
    """The epoch to use for the time source."""

    # ....................... #

    _counter: int = attrs.field(default=0, init=False)

    # ....................... #

    def now(self) -> datetime:
        return self.epoch + timedelta(seconds=self.loop.time())

    # ....................... #

    def monotonic(self) -> float:
        return self.loop.time()

    # ....................... #

    def uuid(self) -> UUID:
        from forze.base.primitives import uuid7

        base_ns = int(self.now().timestamp() * 1_000_000_000)
        result = uuid7(timestamp_ns=base_ns + self._counter)
        self._counter += 1

        return result
