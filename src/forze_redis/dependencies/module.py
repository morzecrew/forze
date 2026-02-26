from datetime import timedelta
from typing import Any

from forze.application.kernel.dependencies import (
    CounterDependencyPort,
    DocumentCacheDependencyPort,
    ExecutionContext,
    IdempotencyDependencyPort,
)
from forze.application.kernel.ports import (
    CounterPort,
    DocumentCachePort,
    IdempotencyPort,
)
from forze.application.kernel.specs import DocumentSpec
from forze.base.typing import conforms_to
from forze.utils.codecs import KeyCodec

from ..adapters import (
    RedisCounterAdapter,
    RedisDocumentCacheAdapter,
    RedisIdempotencyAdapter,
)
from .keys import RedisClientDependencyKey

# ----------------------- #


@conforms_to(IdempotencyDependencyPort)
def redis_idempotency(
    context: ExecutionContext,
    ttl: timedelta = timedelta(seconds=30),
) -> IdempotencyPort:
    redis_client = context.dep(RedisClientDependencyKey)

    return RedisIdempotencyAdapter(client=redis_client, ttl=ttl)


# ....................... #


@conforms_to(DocumentCacheDependencyPort)
def redis_document_cache(
    context: ExecutionContext,
    spec: DocumentSpec[Any, Any, Any, Any],
) -> DocumentCachePort:
    redis_client = context.dep(RedisClientDependencyKey)

    return RedisDocumentCacheAdapter(
        client=redis_client,
        key_codec=KeyCodec(namespace=spec.namespace),
    )


# ....................... #


@conforms_to(CounterDependencyPort)
def redis_counter(
    context: ExecutionContext,
    namespace: str,
) -> CounterPort:
    redis_client = context.dep(RedisClientDependencyKey)

    return RedisCounterAdapter(
        client=redis_client,
        key_codec=KeyCodec(namespace=namespace),
    )
