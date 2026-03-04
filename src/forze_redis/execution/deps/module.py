"""Redis dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.cache import CacheDepKey
from forze.application.contracts.counter import CounterDepKey
from forze.application.contracts.idempotency import IdempotencyDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import RedisClient
from .deps import redis_cache, redis_counter, redis_idempotency
from .keys import RedisClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RedisDepsModule(DepsModule):
    """Dependency module that registers Redis client, cache, counter, and idempotency ports.

    Invoke to produce a :class:`Deps` container with all Redis-backed
    dependencies. The client must be initialized separately (e.g. via
    :func:`redis_lifecycle_step`) before usecases run.
    """

    client: RedisClient
    """Pre-constructed Redis client (pool not yet initialized)."""

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Redis-backed ports.

        :returns: Deps with client, cache, counter, and idempotency ports.
        """

        return Deps(
            {
                RedisClientDepKey: self.client,
                CacheDepKey: redis_cache,
                CounterDepKey: redis_counter,
                IdempotencyDepKey: redis_idempotency,
            }
        )
