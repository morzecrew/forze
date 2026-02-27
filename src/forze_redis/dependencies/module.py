from datetime import timedelta
from typing import Any

from forze.application.contracts.counter import (
    CounterDepKey,
    CounterDepPort,
    CounterPort,
)
from forze.application.contracts.deps import Deps
from forze.application.contracts.document import (
    DocumentCacheDepKey,
    DocumentCacheDepPort,
    DocumentCachePort,
    DocumentSpec,
)
from forze.application.contracts.idempotency import IdempotencyDepPort, IdempotencyPort
from forze.application.execution import ExecutionContext
from forze.base.typing import conforms_to
from forze.utils.codecs import KeyCodec

from ..adapters import (
    RedisCounterAdapter,
    RedisDocumentCacheAdapter,
    RedisIdempotencyAdapter,
)
from ..kernel.platform import RedisClient
from .keys import RedisClientDepKey

# ----------------------- #


@conforms_to(IdempotencyDepPort)
def redis_idempotency(
    context: ExecutionContext,
    ttl: timedelta = timedelta(seconds=30),
) -> IdempotencyPort:
    redis_client = context.dep(RedisClientDepKey)

    return RedisIdempotencyAdapter(client=redis_client, ttl=ttl)


# ....................... #


@conforms_to(DocumentCacheDepPort)
def redis_document_cache(
    context: ExecutionContext,
    spec: DocumentSpec[Any, Any, Any, Any],
) -> DocumentCachePort:
    redis_client = context.dep(RedisClientDepKey)

    return RedisDocumentCacheAdapter(
        client=redis_client,
        key_codec=KeyCodec(namespace=spec.namespace),
    )


# ....................... #


@conforms_to(CounterDepPort)
def redis_counter(
    context: ExecutionContext,
    namespace: str,
) -> CounterPort:
    redis_client = context.dep(RedisClientDepKey)

    return RedisCounterAdapter(
        client=redis_client,
        key_codec=KeyCodec(namespace=namespace),
    )


# ....................... #


def redis_module(client: RedisClient) -> Deps:
    return Deps(
        {
            RedisClientDepKey: client,
            DocumentCacheDepKey: redis_document_cache,
            CounterDepKey: redis_counter,
        }
    )
