import asyncio
from typing import Any, Callable, Coroutine, Hashable, final

import attrs

from forze.base.exceptions import exc

from .datetime import monotonic

# ----------------------- #


@final
@attrs.define(slots=True)
class CacheLane[K: Hashable, V]:
    """FIFO-capped in-memory cache with optional TTL (monotonic clock).

    :param max_entries: When set, evict oldest inserted keys when exceeded.
    :param ttl_seconds: When set, entries expire after this many seconds.
    :param clock: Injectable time source. Defaults to the seam ``monotonic`` (the active
        :class:`TimeSource`), so a bound simulation clock controls TTL expiry; pass an
        explicit clock to override.
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


# ....................... #


@final
@attrs.define(slots=True)
class InflightLane[T]:
    """Run at most one in-flight factory per key; concurrent callers share the same task."""

    _guard: asyncio.Lock = attrs.field(
        factory=asyncio.Lock,
        init=False,
        repr=False,
    )
    _tasks: dict[tuple[Any, ...], asyncio.Task[Any]] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
    )

    # ....................... #

    async def run(
        self,
        key: tuple[Any, ...],
        factory: Callable[[], Coroutine[Any, Any, T]],
        *,
        timeout: float | None = None,
    ) -> T:
        """Await an existing task for *key* or start *factory* and share its result."""

        async with self._guard:
            existing = self._tasks.get(key)

            if existing is None:
                existing = asyncio.create_task(factory())
                self._tasks[key] = existing

            my_task = existing

        try:
            if timeout is None:
                return await my_task

            try:
                return await asyncio.wait_for(my_task, timeout=timeout)

            except asyncio.TimeoutError as e:
                async with self._guard:
                    if self._tasks.get(key) is my_task:
                        my_task.cancel()
                        self._tasks.pop(key, None)

                raise exc.internal("InflightLane timed out") from e

        finally:
            async with self._guard:
                if self._tasks.get(key) is my_task:
                    self._tasks.pop(key, None)

    # ....................... #

    def clear(self) -> None:
        """Drop tracked in-flight tasks without cancelling them."""

        self._tasks.clear()


# ....................... #


@final
@attrs.define(slots=True)
class CachedInflightLane[K: Hashable, V]:
    """Cache hit → return; miss → singleflight; optional auto-store on success.
    * ``coalesce``: factory may call ``lane.store`` itself (Postgres introspector).
    * ``get_or_load``: primitive stores ``await factory()`` on miss.
    """

    _inflight: InflightLane[V] = attrs.field(factory=InflightLane, init=False)

    # ....................... #

    async def coalesce(
        self,
        *,
        cache_key: K,
        inflight_key: tuple[Any, ...],
        lane: CacheLane[K, V],
        factory: Callable[[], Coroutine[Any, Any, V]],
        timeout: float | None = None,
    ) -> V:
        hit = lane.lookup(cache_key)

        if hit is not None:
            return hit

        return await self._inflight.run(inflight_key, factory, timeout=timeout)

    # ....................... #

    async def get_or_load(
        self,
        *,
        cache_key: K,
        inflight_key: tuple[Any, ...],
        lane: CacheLane[K, V],
        factory: Callable[[], Coroutine[Any, Any, V]],
        timeout: float | None = None,
    ) -> V:
        hit = lane.lookup(cache_key)

        if hit is not None:
            return hit

        async def _load_and_store() -> V:
            value = await factory()
            lane.store(cache_key, value)

            return value

        return await self._inflight.run(
            inflight_key,
            _load_and_store,
            timeout=timeout,
        )

    # ....................... #

    def clear_inflight(self) -> None:
        self._inflight.clear()
