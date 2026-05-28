"""Coalesce concurrent async work per key (singleflight)."""

import asyncio
from typing import Any, Callable, Coroutine, Generic, TypeVar, final

import attrs

# ----------------------- #

T = TypeVar("T")

# ....................... #


@final
@attrs.define(slots=True)
class InflightLane(Generic[T]):
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
    ) -> T:
        """Await an existing task for *key* or start *factory* and share its result."""

        async with self._guard:
            existing = self._tasks.get(key)

            if existing is None:
                existing = asyncio.create_task(factory())
                self._tasks[key] = existing

            my_task = existing

        try:
            return await my_task

        finally:
            async with self._guard:
                if self._tasks.get(key) is my_task:
                    self._tasks.pop(key, None)

    # ....................... #

    def clear(self) -> None:
        """Drop tracked in-flight tasks without cancelling them."""

        self._tasks.clear()
