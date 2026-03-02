"""Factory functions for Redis counter, document cache, and idempotency adapters."""

from datetime import timedelta
from typing import Any

from forze.application.contracts.counter import (
    CounterDepPort,
    CounterPort,
)
from forze.application.contracts.document import (
    DocumentCacheDepPort,
    DocumentCachePort,
    DocumentSpec,
)
from forze.application.contracts.idempotency import (
    IdempotencyDepPort,
    IdempotencyPort,
)
from forze.application.execution import ExecutionContext
from forze.base.typing import conforms_to
from forze.utils.codecs import KeyCodec

from ...adapters import (
    RedisCounterAdapter,
    RedisDocumentCacheAdapter,
    RedisIdempotencyAdapter,
)
from .keys import RedisClientDepKey

# ----------------------- #


@conforms_to(IdempotencyDepPort)
def redis_idempotency(
    context: ExecutionContext,
    ttl: timedelta = timedelta(seconds=30),
) -> IdempotencyPort:
    """Build a Redis-backed idempotency port for the execution context.

    :param context: Execution context for resolving the Redis client.
    :param ttl: Time-to-live for idempotency keys.
    :returns: Idempotency port backed by :class:`RedisIdempotencyAdapter`.
    """
    redis_client = context.dep(RedisClientDepKey)

    return RedisIdempotencyAdapter(client=redis_client, ttl=ttl)


# ....................... #


@conforms_to(DocumentCacheDepPort)
def redis_document_cache(
    context: ExecutionContext,
    spec: DocumentSpec[Any, Any, Any, Any],
) -> DocumentCachePort:
    """Build a Redis-backed document cache port for the given spec.

    :param context: Execution context for resolving the Redis client.
    :param spec: Document specification (namespace used for key prefixing).
    :returns: Document cache port backed by :class:`RedisDocumentCacheAdapter`.
    """
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
    """Build a Redis-backed counter port for the given namespace.

    :param context: Execution context for resolving the Redis client.
    :param namespace: Counter namespace (used for key prefixing).
    :returns: Counter port backed by :class:`RedisCounterAdapter`.
    """
    redis_client = context.dep(RedisClientDepKey)

    return RedisCounterAdapter(
        client=redis_client,
        key_codec=KeyCodec(namespace=namespace),
    )
