from typing import final

import attrs

from forze.application.contracts.counter import CounterDepKey
from forze.application.contracts.document import DocumentCacheDepKey
from forze.application.contracts.idempotency import IdempotencyDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import RedisClient
from .deps import redis_counter, redis_document_cache, redis_idempotency
from .keys import RedisClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RedisDepsModule(DepsModule):
    client: RedisClient

    # ....................... #

    def __call__(self) -> Deps:
        return Deps(
            {
                RedisClientDepKey: self.client,
                DocumentCacheDepKey: redis_document_cache,
                CounterDepKey: redis_counter,
                IdempotencyDepKey: redis_idempotency,
            }
        )
