"""In-process L1 store for document read-through (LRU + TTL, sync, per-process).

The :class:`L1Store` protocol is the eviction-policy seam: the default
:class:`LruTtlStore` is a plain LRU with per-entry TTL, and an alternative
implementation (e.g. a W-TinyLFU-backed store for scan-heavy workloads) can be
injected through ``DocumentCache(l1_store=...)`` without touching the
coordinator.
"""

import time
from collections import OrderedDict
from typing import Any, Callable, Protocol, final, runtime_checkable

import attrs

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

    _entries: "OrderedDict[str, tuple[Any, float]]" = attrs.field(
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
