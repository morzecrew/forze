"""Factory functions for Redis cache/counter/idempotency/pubsub/stream adapters."""

from datetime import timedelta
from typing import Any

from forze.application.contracts.cache import CacheDepPort, CachePort, CacheSpec
from forze.application.contracts.counter import CounterDepPort, CounterPort
from forze.application.contracts.idempotency import IdempotencyDepPort, IdempotencyPort
from forze.application.contracts.pubsub import (
    PubSubConformity,
    PubSubDepConformity,
    PubSubSpec,
)
from forze.application.contracts.stream import (
    StreamConformity,
    StreamDepConformity,
    StreamGroupDepPort,
    StreamGroupPort,
)
from forze.application.contracts.stream.specs import StreamSpec
from forze.application.contracts.tenant.deps import TenantContextDepKey
from forze.application.execution import ExecutionContext
from forze.base.typing import conforms_to
from forze.base.codecs import KeyCodec

from ...adapters import (
    RedisCacheAdapter,
    RedisCounterAdapter,
    RedisIdempotencyAdapter,
    RedisPubSubAdapter,
    RedisPubSubCodec,
    RedisStreamAdapter,
    RedisStreamCodec,
    RedisStreamGroupAdapter,
)
from .keys import RedisClientDepKey

# ----------------------- #


@conforms_to(IdempotencyDepPort)  #! use spec ?
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


@conforms_to(CounterDepPort)  #! use spec ?
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

    tenant_context = None
    if context.deps.exists(TenantContextDepKey):
        tenant_context = context.dep(TenantContextDepKey)()

    return RedisCounterAdapter(
        client=redis_client,
        key_codec=KeyCodec(namespace=namespace),
        tenant_context=tenant_context,
    )


# ....................... #


@conforms_to(CacheDepPort)
def redis_cache(
    context: ExecutionContext,
    spec: CacheSpec,
) -> CachePort:
    """Build a Redis-backed cache port for the given spec."""
    redis_client = context.dep(RedisClientDepKey)

    return RedisCacheAdapter(
        client=redis_client, key_codec=KeyCodec(namespace=spec.namespace)
    )


# ....................... #
# PubSub


@conforms_to(PubSubDepConformity)
def redis_pubsub(
    context: ExecutionContext,
    spec: PubSubSpec[Any],
) -> PubSubConformity:
    """Build a Redis-backed pubsub port for the given spec.

    :param context: Execution context for resolving the Redis client.
    :param spec: PubSub specification with namespace and model type.
    :returns: PubSub port backed by :class:`RedisPubSubAdapter`.
    """
    redis_client = context.dep(RedisClientDepKey)
    codec = RedisPubSubCodec(model=spec.model)

    return RedisPubSubAdapter(client=redis_client, codec=codec)


# ....................... #
# Stream


@conforms_to(StreamDepConformity)
def redis_stream(
    context: ExecutionContext,
    spec: StreamSpec[Any],
) -> StreamConformity:
    """Build a Redis-backed stream port (read and write) for the given spec.

    :param context: Execution context for resolving the Redis client.
    :param spec: Stream specification with namespace and model type.
    :returns: Stream port backed by :class:`RedisStreamAdapter`.
    """
    redis_client = context.dep(RedisClientDepKey)
    codec = RedisStreamCodec(model=spec.model)

    return RedisStreamAdapter(client=redis_client, codec=codec)


# ....................... #


@conforms_to(StreamGroupDepPort)
def redis_stream_group(
    context: ExecutionContext,
    spec: StreamSpec[Any],
) -> StreamGroupPort[Any]:
    """Build a Redis-backed stream group port for the given spec.

    :param context: Execution context for resolving the Redis client.
    :param spec: Stream specification with namespace and model type.
    :returns: Stream group port backed by :class:`RedisStreamGroupAdapter`.
    """
    redis_client = context.dep(RedisClientDepKey)
    codec = RedisStreamCodec(model=spec.model)

    return RedisStreamGroupAdapter(client=redis_client, codec=codec)
