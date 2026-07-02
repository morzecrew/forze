"""A synchronous bounded LRU map for hot-path per-key state that is safe to evict."""

from collections import OrderedDict
from typing import Callable, Hashable

from forze.base.exceptions import exc

# ----------------------- #


class BoundedLruMap[K: Hashable, V](OrderedDict[K, V]):
    """An ``OrderedDict`` that evicts the least-recently-used entry past ``max_entries``.

    A drop-in for a per-key state map that must not grow without bound: ``get`` and
    ``__getitem__`` mark an entry most-recently-used, and ``__setitem__`` evicts the oldest
    once over capacity. Intended for state that is **safe to drop** — recreated fresh on the
    next access (an idle circuit breaker resets to closed, an idle token bucket refills) —
    so a high-cardinality key space only ever loses already-idle entries. Single-event-loop
    use only (no lock): mutation happens between awaits, like the state it holds.

    ``evictable`` guards state that is *not* always safe to drop (a bulkhead still holding
    permits): eviction then skips the oldest non-evictable entries and drops the oldest
    evictable one, and if none is evictable this round it keeps every entry — a transient
    overshoot bounded by live work, not by key cardinality — rather than reset live state.
    """

    def __init__(
        self,
        max_entries: int,
        *,
        evictable: Callable[[V], bool] | None = None,
    ) -> None:
        if max_entries < 1:
            raise exc.internal("BoundedLruMap max_entries must be at least 1")

        super().__init__()
        self._max_entries = max_entries
        self._evictable = evictable

    def __setitem__(self, key: K, value: V) -> None:
        super().__setitem__(key, value)
        self.move_to_end(key)

        if len(self) > self._max_entries:
            self._evict_oldest()

    def _evict_oldest(self) -> None:
        if self._evictable is None:
            self.popitem(last=False)
            return

        # Drop the oldest evictable entry, never a live one (its eviction would reset active
        # concurrency control) and never the just-inserted newest key (``__setitem__`` moved
        # it to the end) — evicting that would discard the entry we were asked to store.
        newest = next(reversed(self), None)
        key = self._oldest_evictable_key(exclude=newest)

        if key is not None:
            del self[key]

    def _oldest_evictable_key(self, *, exclude: K | None = None) -> K | None:
        """The oldest key whose value is currently evictable, skipping *exclude*.

        ``items()`` is oldest-first and does not reorder; this reads only, so the caller
        deletes after it returns. ``None`` when nothing (other than *exclude*) is evictable.
        """

        if self._evictable is None:
            return None

        for key, value in self.items():
            if key != exclude and self._evictable(value):
                return key

        return None

    def prune(self) -> None:
        """Bring the map back to ``max_entries`` by dropping the oldest evictable entries.

        A no-op at or under capacity. Call it once an entry may have become evictable (e.g. a
        bulkhead released its last permit) so a transient over-capacity overshoot — one
        ``__setitem__`` kept because every entry was live when a new key arrived — is
        reclaimed without waiting for the next insertion. Stops as soon as nothing is evictable.
        """

        while len(self) > self._max_entries:
            if self._evictable is None:
                self.popitem(last=False)
                continue

            key = self._oldest_evictable_key()

            if key is None:
                return

            del self[key]

    def __getitem__(self, key: K) -> V:
        value = super().__getitem__(key)
        self.move_to_end(key)

        return value

    def get(self, key: K, default: V | None = None) -> V | None:  # type: ignore[override]
        if key not in self:
            return default

        return self[key]  # marks most-recently-used
