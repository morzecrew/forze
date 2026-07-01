"""A synchronous bounded LRU map for hot-path per-key state that is safe to evict."""

from collections import OrderedDict
from typing import Hashable

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
    """

    def __init__(self, max_entries: int) -> None:
        if max_entries < 1:
            raise exc.internal("BoundedLruMap max_entries must be at least 1")

        super().__init__()
        self._max_entries = max_entries

    def __setitem__(self, key: K, value: V) -> None:
        super().__setitem__(key, value)
        self.move_to_end(key)

        if len(self) > self._max_entries:
            self.popitem(last=False)

    def __getitem__(self, key: K) -> V:
        value = super().__getitem__(key)
        self.move_to_end(key)

        return value

    def get(self, key: K, default: V | None = None) -> V | None:  # type: ignore[override]
        if key not in self:
            return default

        return self[key]  # marks most-recently-used
