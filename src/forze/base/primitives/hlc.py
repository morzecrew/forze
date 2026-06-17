"""Hybrid Logical Clock (HLC, Kulkarni et al. 2014; as used by CockroachDB).

A wall clock orders events by physical time but misorders causally-related
events across nodes whenever clocks skew; a Lamport clock captures causality but
drifts arbitrarily far from real time. An HLC takes both: each timestamp is a
``(physical_ms, logical)`` pair that **stays close to wall time** yet **never
goes backwards and always exceeds any timestamp it has observed** — so a causal
successor sorts after its cause even when the producing nodes' clocks disagree.

The logical component is a bounded counter that only advances when the physical
component does not (same millisecond, or a received timestamp at/ahead of the
local clock); it resets to zero whenever wall time moves the pair forward. That
keeps the pair monotonic and within one counter-rollover of real time.

This module is the **pure primitive** only: a clock and its timestamp value
object, with the physical component read through the ambient
:class:`~forze.base.primitives.time_source.TimeSource` (so it is deterministic
under a bound source). Total ordering *across* nodes for two equal pairs is left
to the storage layer (e.g. an outbox's time-ordered ``uuid7`` row id as the
final tiebreaker); the HLC itself models causality, not node identity.
"""

from __future__ import annotations

from datetime import timedelta

import attrs

from forze.base.exceptions import exc

from .time_source import current_time_source

# ----------------------- #

_LOGICAL_BITS = 16
"""Width of the logical counter when packed into an integer."""

_MAX_LOGICAL = (1 << _LOGICAL_BITS) - 1
"""Largest logical counter value (65535); overflow bumps the physical component."""

_ENCODE_PHYSICAL_DIGITS = 15
"""Fixed width for the physical field in :meth:`HlcTimestamp.encode` — enough for
millisecond timestamps well past year 10000, so the string stays lexsortable."""

_ENCODE_LOGICAL_DIGITS = 5
"""Fixed width for the logical field in :meth:`HlcTimestamp.encode` (covers 65535)."""

# ....................... #


@attrs.define(slots=True, frozen=True, order=True)
class HlcTimestamp:
    """A single HLC reading: ``(physical_ms, logical)``, ordered as that tuple.

    ``physical_ms`` is milliseconds since the Unix epoch; ``logical`` is the
    bounded counter. Instances sort by physical then logical, and pack into a
    single monotonic integer (:meth:`pack`) or a fixed-width lexsortable string
    (:meth:`encode`) for storage and wire transport.
    """

    physical_ms: int
    """Physical component: milliseconds since the Unix epoch."""

    logical: int
    """Logical component: a counter in ``[0, 65535]``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.physical_ms < 0:
            raise exc.validation("HlcTimestamp physical_ms must be non-negative")

        if not 0 <= self.logical <= _MAX_LOGICAL:
            raise exc.validation(f"HlcTimestamp logical must be in [0, {_MAX_LOGICAL}]")

    # ....................... #

    def pack(self) -> int:
        """Pack into a single monotonic integer (``physical_ms << 16 | logical``)."""

        return (self.physical_ms << _LOGICAL_BITS) | self.logical

    # ....................... #

    @classmethod
    def unpack(cls, packed: int) -> HlcTimestamp:
        """Inverse of :meth:`pack`."""

        if packed < 0:
            raise exc.validation("packed HLC value must be non-negative")

        return cls(
            physical_ms=packed >> _LOGICAL_BITS,
            logical=packed & _MAX_LOGICAL,
        )

    # ....................... #

    def encode(self) -> str:
        """A fixed-width, lexicographically sortable string form (for headers)."""

        return (
            f"{self.physical_ms:0{_ENCODE_PHYSICAL_DIGITS}d}"
            f".{self.logical:0{_ENCODE_LOGICAL_DIGITS}d}"
        )

    # ....................... #

    @classmethod
    def parse(cls, encoded: str) -> HlcTimestamp:
        """Inverse of :meth:`encode`."""

        physical, _, logical = encoded.partition(".")

        if not logical:
            raise exc.validation("malformed HLC string (expected 'physical.logical')")

        try:
            return cls(physical_ms=int(physical), logical=int(logical))

        except ValueError as e:
            raise exc.validation("malformed HLC string (non-integer field)") from e


# ....................... #


@attrs.define(slots=True)
class HybridLogicalClock:
    """A monotonic HLC: ``now`` to stamp a local event, ``update`` to receive one.

    Single-event-loop discipline (mutations happen between awaits), so it needs
    no lock — the same model as the resilience state objects. The physical
    component is read from the ambient :class:`TimeSource`, so a bound source
    makes the clock fully deterministic.
    """

    max_drift: timedelta | None = None
    """Optional skew guard: :meth:`update` rejects a remote timestamp whose
    physical component is more than this far ahead of the local wall clock,
    bounding how far a faulty peer can drag this clock forward. ``None`` accepts
    any remote timestamp (plain HLC)."""

    # ....................... #

    _last: HlcTimestamp = attrs.field(default=HlcTimestamp(0, 0), init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_drift is not None and self.max_drift < timedelta(0):
            raise exc.configuration("HybridLogicalClock max_drift must be non-negative")

    # ....................... #

    @property
    def last(self) -> HlcTimestamp:
        """The most recent timestamp this clock issued (read-only)."""

        return self._last

    # ....................... #

    @staticmethod
    def _wall_ms() -> int:
        """Get the current wall clock time in milliseconds."""

        return int(current_time_source().now().timestamp() * 1000)

    # ....................... #

    @staticmethod
    def _bounded(physical_ms: int, logical: int) -> HlcTimestamp:
        """Apply counter-overflow: a logical past the cap carries into physical."""

        if logical > _MAX_LOGICAL:
            return HlcTimestamp(physical_ms + 1, 0)

        return HlcTimestamp(physical_ms, logical)

    # ....................... #

    def now(self) -> HlcTimestamp:
        """Issue the next timestamp for a locally produced event."""

        wall = self._wall_ms()
        last = self._last

        if wall > last.physical_ms:
            issued = HlcTimestamp(wall, 0)

        else:
            issued = self._bounded(last.physical_ms, last.logical + 1)

        self._last = issued

        return issued

    # ....................... #

    def update(self, remote: HlcTimestamp) -> HlcTimestamp:
        """Merge a received timestamp and issue one that exceeds it (and our last)."""

        wall = self._wall_ms()

        if self.max_drift is not None:
            drift_ms = int(self.max_drift.total_seconds() * 1000)

            if remote.physical_ms - wall > drift_ms:
                raise exc.precondition(
                    "remote HLC timestamp exceeds max_drift ahead of the local clock"
                )

        last = self._last
        physical = max(last.physical_ms, remote.physical_ms, wall)

        if physical == last.physical_ms and physical == remote.physical_ms:
            logical = max(last.logical, remote.logical) + 1

        elif physical == last.physical_ms:
            logical = last.logical + 1

        elif physical == remote.physical_ms:
            logical = remote.logical + 1

        else:
            logical = 0

        issued = self._bounded(physical, logical)
        self._last = issued

        return issued
