"""In-memory TTL/FIFO cache."""

from collections.abc import Callable, Hashable
from time import monotonic
from typing import Generic, TypeVar, final

import attrs

# ----------------------- #

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")

# ....................... #


@final
@attrs.define(slots=True)
class CacheLane(Generic[K, V]):
    """FIFO-capped in-memory cache with optional TTL (monotonic clock).

    :param max_entries: When set, evict oldest inserted keys when exceeded.
    :param ttl_seconds: When set, entries expire after this many seconds.
    :param clock: Injectable time source (defaults to :func:`time.monotonic`).
    """

    max_entries: int | None = None
    ttl_seconds: float | None = None
    clock: Callable[[], float] = attrs.field(default=monotonic, repr=False, eq=False)

    _data: dict[K, V] = attrs.field(factory=dict, init=False, repr=False)
    _timestamps: dict[K, float] = attrs.field(factory=dict, init=False, repr=False)

    # ....................... #

    def lookup(self, key: K) -> V | None:
        """Return a cached value or ``None`` if missing or TTL-expired."""

        value = self._data.get(key)

        if value is None:
            return None

        if self._is_expired(key):
            self.invalidate(key)
            return None

        return value

    # ....................... #

    def store(self, key: K, value: V) -> None:
        """Insert *value* and apply TTL timestamp and FIFO cap."""

        self._data[key] = value

        if self.ttl_seconds is not None:
            self._timestamps[key] = self.clock()

        self._trim()

    # ....................... #

    def invalidate(self, key: K) -> None:
        """Remove one key from the cache."""

        self._data.pop(key, None)
        self._timestamps.pop(key, None)

    # ....................... #

    def clear(self) -> None:
        """Remove all entries."""

        self._data.clear()
        self._timestamps.clear()

    # ....................... #

    def __contains__(self, key: K) -> bool:
        return self.lookup(key) is not None

    # ....................... #

    def __len__(self) -> int:
        return len(self._data)

    # ....................... #

    def _is_expired(self, key: K) -> bool:
        ttl = self.ttl_seconds

        if ttl is None:
            return False

        t0 = self._timestamps.get(key)

        if t0 is None:
            return False

        return self.clock() - t0 >= ttl

    # ....................... #

    def _trim(self) -> None:
        mx = self.max_entries

        if mx is None:
            return

        while len(self._data) > mx:
            oldest = next(iter(self._data))
            self.invalidate(oldest)
