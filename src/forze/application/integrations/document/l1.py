"""In-process L1 store for document read-through (LRU + TTL, sync, per-process).

The :class:`L1Store` protocol is the eviction-policy seam: the default
:class:`LruTtlStore` is a plain LRU with per-entry TTL, and an alternative
implementation (e.g. a W-TinyLFU-backed store for scan-heavy workloads) can be
injected through ``DocumentCache(l1_store=...)`` without touching the
coordinator.
"""

import time
import weakref
import zlib
from collections import OrderedDict
from typing import Any, Callable, Iterator, Protocol, cast, final, runtime_checkable

import attrs

from forze.base.exceptions import exc

# ----------------------- #


@runtime_checkable
class L1Store(Protocol):
    """Synchronous in-process cache seam for the document L1."""

    def get(self, key: str) -> Any | None:
        """Return a live entry or ``None`` (missing or expired)."""
        ...

    def set(self, key: str, value: Any) -> None:
        """Insert or refresh an entry."""
        ...

    def invalidate(self, key: str) -> None:
        """Drop one entry."""
        ...

    def clear(self) -> None:
        """Drop every entry."""
        ...


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class L1Stats:
    """Snapshot of an :class:`LruTtlStore`'s counters."""

    size: int
    capacity: int
    hits: int
    misses: int
    evictions: int


# ....................... #


@attrs.define(slots=True)
class LruTtlStore:
    """Default :class:`L1Store`: LRU-ordered entries with a uniform TTL.

    Single-event-loop discipline (mutations happen between awaits), monotonic
    clock, no background task — expiry is checked lazily on access.
    """

    capacity: int
    ttl: float
    clock: Callable[[], float] = time.monotonic

    # ....................... #

    _entries: OrderedDict[str, tuple[Any, float]] = attrs.field(
        factory=OrderedDict,
        init=False,
        repr=False,
    )
    _hits: int = attrs.field(default=0, init=False, repr=False)
    _misses: int = attrs.field(default=0, init=False, repr=False)
    _evictions: int = attrs.field(default=0, init=False, repr=False)

    # ....................... #

    def get(self, key: str) -> Any | None:
        entry = self._entries.get(key)

        if entry is None:
            self._misses += 1
            return None

        value, expires_at = entry

        if self.clock() >= expires_at:
            del self._entries[key]
            self._misses += 1
            return None

        self._entries.move_to_end(key)
        self._hits += 1

        return value

    # ....................... #

    def set(self, key: str, value: Any) -> None:
        self._entries[key] = (value, self.clock() + self.ttl)
        self._entries.move_to_end(key)

        while len(self._entries) > self.capacity:
            self._entries.popitem(last=False)
            self._evictions += 1

    # ....................... #

    def invalidate(self, key: str) -> None:
        self._entries.pop(key, None)

    # ....................... #

    def clear(self) -> None:
        self._entries.clear()

    # ....................... #

    def stats(self) -> L1Stats:
        """Best-effort snapshot of the store's counters."""

        return L1Stats(
            size=len(self._entries),
            capacity=self.capacity,
            hits=self._hits,
            misses=self._misses,
            evictions=self._evictions,
        )


# ....................... #


@attrs.define(slots=True)
class _FrequencySketch:
    """4-bit count-min sketch with periodic aging (the TinyLFU frequency filter).

    Counters saturate at 15; after ``sample_size`` increments every counter is
    halved — recent popularity outweighs ancient popularity, which is what
    lets the admission policy follow regime changes (seasonality) instead of
    fossilizing last month's hot set. Counters are stored one-per-byte for
    simplicity (a packed-nibble layout saves memory the L1's scale doesn't
    need).
    """

    _DEPTH = 4

    # ....................... #

    capacity: int

    # ....................... #

    _width: int = attrs.field(
        default=attrs.Factory(
            # Smallest power of two >= 4x capacity (a pow2 width keeps the
            # index computation a mask instead of a modulo).
            lambda self: 1 << (self.capacity * 4 - 1).bit_length(),
            takes_self=True,
        ),
        init=False,
        repr=False,
        eq=False,
    )
    _rows: list[bytearray] = attrs.field(
        default=attrs.Factory(
            lambda self: [bytearray(self._width) for _ in range(self._DEPTH)],
            takes_self=True,
        ),
        init=False,
        repr=False,
        eq=False,
    )
    _ops: int = attrs.field(default=0, init=False, repr=False, eq=False)
    _sample_size: int = attrs.field(
        default=attrs.Factory(
            lambda self: max(64, self.capacity * 10),
            takes_self=True,
        ),
        init=False,
        repr=False,
        eq=False,
    )

    # ....................... #

    def _indexes(self, key: str) -> tuple[int, ...]:
        mask = self._width - 1
        # Deterministic, salt-free hashing: the builtin ``hash`` is salted per process
        # (``PYTHONHASHSEED``), which makes the count-min bucket placement — and so the
        # admission duel — non-reproducible across runs/machines. One fast CRC of the key
        # gives the base; enhanced double hashing (a second value from its high bits)
        # derives the per-row indices. Two keys collide in *all* rows only when both the
        # low and high CRC bits collide (≈ 1/width²), so the rows stay independent — with a
        # single CRC and no crypto digest, keeping the L1 hot path cheap.
        base = zlib.crc32(key.encode("utf-8"))
        step = (base >> 15) | 1

        return tuple((base + i * step) & mask for i in range(self._DEPTH))

    # ....................... #

    def increment(self, key: str) -> None:
        for row, idx in zip(self._rows, self._indexes(key), strict=True):
            if row[idx] < 15:
                row[idx] += 1

        self._ops += 1

        if self._ops >= self._sample_size:
            self._age()

    # ....................... #

    def estimate(self, key: str) -> int:
        return min(
            row[idx] for row, idx in zip(self._rows, self._indexes(key), strict=True)
        )

    # ....................... #

    def _age(self) -> None:
        for row in self._rows:
            for i in range(self._width):
                row[i] >>= 1

        self._ops //= 2


# ....................... #


@attrs.define(slots=True)
class TinyLfuStore:
    """W-TinyLFU :class:`L1Store`: scan-resistant frequency-based admission.

    The Caffeine design (Einziger, Friedman & Manes — "TinyLFU: A Highly
    Efficient Cache Admission Policy"): new keys land in a small **admission
    window** (LRU, ~1% of capacity); when the window overflows, its eviction
    candidate must win a frequency duel against the **main region's** eviction
    victim to be admitted — a one-pass scan's one-hit wonders lose that duel
    every time, so they can never displace the hot set. The main region is a
    segmented LRU (probation 20% / protected 80%); a probation hit promotes
    to protected.

    TTL is orthogonal (lazy expiry on access, same as :class:`LruTtlStore`);
    ``invalidate``/``clear`` touch the segments only — the frequency sketch
    deliberately survives, so a push-invalidated hot key re-admits instantly.
    """

    capacity: int
    ttl: float
    clock: Callable[[], float] = time.monotonic

    # ....................... #

    _window: "OrderedDict[str, tuple[Any, float]]" = attrs.field(
        factory=OrderedDict,
        init=False,
        repr=False,
    )
    _probation: "OrderedDict[str, tuple[Any, float]]" = attrs.field(
        factory=OrderedDict,
        init=False,
        repr=False,
    )
    _protected: "OrderedDict[str, tuple[Any, float]]" = attrs.field(
        factory=OrderedDict,
        init=False,
        repr=False,
    )
    _sketch: _FrequencySketch = attrs.field(init=False, repr=False)
    _hits: int = attrs.field(default=0, init=False, repr=False)
    _misses: int = attrs.field(default=0, init=False, repr=False)
    _evictions: int = attrs.field(default=0, init=False, repr=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.capacity < 2:
            raise exc.configuration("TinyLfuStore capacity must be >= 2")

        self._sketch = _FrequencySketch(self.capacity)

    # ....................... #

    @property
    def _window_cap(self) -> int:
        return max(1, self.capacity // 100)

    @property
    def _main_cap(self) -> int:
        return self.capacity - self._window_cap

    @property
    def _protected_cap(self) -> int:
        return max(1, (self._main_cap * 4) // 5)

    # ....................... #

    def _expired(self, entry: tuple[Any, float]) -> bool:
        return self.clock() >= entry[1]

    # ....................... #

    def get(self, key: str) -> Any | None:
        self._sketch.increment(key)

        for segment in (self._window, self._probation, self._protected):
            entry = segment.get(key)

            if entry is None:
                continue

            if self._expired(entry):
                del segment[key]
                self._misses += 1
                return None

            if segment is self._probation:
                # Reuse while on probation proves worth: promote.
                del self._probation[key]
                self._protected[key] = entry
                self._demote_protected_overflow()

            else:
                segment.move_to_end(key)

            self._hits += 1
            return entry[0]

        self._misses += 1
        return None

    # ....................... #

    def _demote_protected_overflow(self) -> None:
        while len(self._protected) > self._protected_cap:
            demoted, entry = self._protected.popitem(last=False)
            self._probation[demoted] = entry

    # ....................... #

    def set(self, key: str, value: Any) -> None:
        entry = (value, self.clock() + self.ttl)
        self._sketch.increment(key)

        for segment in (self._window, self._probation, self._protected):
            if key in segment:
                segment[key] = entry
                segment.move_to_end(key)
                return

        self._window[key] = entry

        if len(self._window) <= self._window_cap:
            return

        candidate, candidate_entry = self._window.popitem(last=False)

        if len(self._probation) + len(self._protected) < self._main_cap:
            self._probation[candidate] = candidate_entry
            return

        victims = self._probation if self._probation else self._protected
        victim = next(iter(victims))

        # The admission duel: a newcomer must be provably hotter than the
        # incumbent victim. One-hit wonders (scans) lose and are dropped —
        # the hot set is never displaced by traffic that won't return.
        if self._sketch.estimate(candidate) > self._sketch.estimate(victim):
            del victims[victim]
            self._probation[candidate] = candidate_entry

        self._evictions += 1

    # ....................... #

    def invalidate(self, key: str) -> None:
        for segment in (self._window, self._probation, self._protected):
            if segment.pop(key, None) is not None:
                return

    # ....................... #

    def clear(self) -> None:
        self._window.clear()
        self._probation.clear()
        self._protected.clear()

    # ....................... #

    def stats(self) -> L1Stats:
        """Best-effort snapshot (evictions include rejected admissions)."""

        return L1Stats(
            size=len(self._window) + len(self._probation) + len(self._protected),
            capacity=self.capacity,
            hits=self._hits,
            misses=self._misses,
            evictions=self._evictions,
        )


# ....................... #


def tiny_lfu_l1_store(spec: Any) -> TinyLfuStore:
    """``L1Spec.store_factory`` building a W-TinyLFU store from the spec.

    Usage: ``L1Spec(ttl=..., capacity=..., store_factory=tiny_lfu_l1_store)``.
    """

    return TinyLfuStore(
        capacity=spec.capacity,
        ttl=spec.ttl.total_seconds(),
    )


# ....................... #
# Live-store registry (feeds the OTel exporter)

_LIVE_STORES: list[tuple[str, weakref.ReferenceType[Any]]] = []


def register_l1_store(name: str, store: Any) -> None:
    """Register a live L1 store under its document name (weakly referenced).

    Called by the document cache coordinator at construction; the exporter
    reads the registry at metric collection time. Weak references keep
    per-scope rebuilds from leaking — a store dies with its coordinator and
    is pruned on the next iteration.
    """

    _LIVE_STORES.append((name, weakref.ref(store)))


def iter_l1_stats() -> Iterator[tuple[str, L1Stats]]:
    """Yield ``(document_name, stats)`` for every live, stats-capable store.

    Custom :class:`L1Store` implementations without a ``stats()`` method are
    skipped. Dead references are pruned in passing.
    """

    dead: list[int] = []

    for index, (name, ref) in enumerate(_LIVE_STORES):
        store = ref()

        if store is None:
            dead.append(index)
            continue

        stats_fn = getattr(store, "stats", None)

        if not callable(stats_fn):
            continue

        yield name, cast(L1Stats, stats_fn())

    for index in reversed(dead):
        del _LIVE_STORES[index]
