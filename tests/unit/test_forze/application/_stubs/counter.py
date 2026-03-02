"""In-memory stub for CounterPort."""

from typing import Optional, final

from forze.application.contracts.counter import CounterPort

# ----------------------- #


@final
class InMemoryCounterPort(CounterPort):
    """In-memory counter for unit tests."""

    def __init__(self, initial: int = 0) -> None:
        self._counters: dict[str, int] = {}
        self._default = initial

    def _key(self, suffix: Optional[str]) -> str:
        return suffix or "__default__"

    async def incr(self, by: int = 1, *, suffix: Optional[str] = None) -> int:
        key = self._key(suffix)
        self._counters[key] = self._counters.get(key, self._default) + by
        return self._counters[key]

    async def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: Optional[str] = None,
    ) -> list[int]:
        key = self._key(suffix)
        start = self._counters.get(key, self._default)
        result = [start + i for i in range(1, size + 1)]
        self._counters[key] = start + size
        return result

    async def decr(self, by: int = 1, *, suffix: Optional[str] = None) -> int:
        key = self._key(suffix)
        self._counters[key] = self._counters.get(key, self._default) - by
        return self._counters[key]

    async def reset(self, value: int = 1, *, suffix: Optional[str] = None) -> int:
        key = self._key(suffix)
        self._counters[key] = value
        return value
