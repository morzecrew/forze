"""Idempotent initialize/close guard shared by integration kernel clients."""

import asyncio
from typing import Awaitable, Callable, final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, eq=False, repr=False)
class GuardedLifecycle:
    """Serialized, idempotent initialize/close semantics for a resource owner.

    The guard owns only the lock; the owning client keeps its resource fields
    and reports readiness through the ``ready`` predicate. ``initialize``
    skips ``setup`` when ``ready()`` is already true and otherwise runs it
    under the lock, so concurrent initializers run setup exactly once.
    Because the owner assigns its fields only after a successful setup, a
    failed setup leaves ``ready()`` false and a later ``initialize`` retries.

    ``close`` runs ``teardown`` under the same lock, so it waits for an
    in-flight ``initialize`` and never interleaves with it. Teardown hooks
    (cancelling background tasks, returning pending deliveries) therefore run
    inside the lock, in the order the owner's teardown callable defines.
    Note that ``teardown`` itself runs unconditionally — close idempotency
    comes from the owner's teardown being a no-op once its resource fields
    are cleared (the ``if resource is not None`` guard every client carries).
    """

    _lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #

    async def initialize(
        self,
        setup: Callable[[], Awaitable[None]],
        *,
        ready: Callable[[], bool],
    ) -> None:
        """Run ``setup`` under the lock unless ``ready()`` is already true."""

        async with self._lock:
            if ready():
                return

            await setup()

    # ....................... #

    async def close(self, teardown: Callable[[], Awaitable[None]]) -> None:
        """Run ``teardown`` under the lock (waits for in-flight initialize)."""

        async with self._lock:
            await teardown()
