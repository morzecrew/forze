"""Factory functions for Redis counter, document cache, and idempotency adapters."""

from datetime import timedelta

from forze.application.contracts.cache import CacheDepPort, CachePort, CacheSpec
from forze.application.contracts.counter import CounterDepPort, CounterPort
from forze.application.contracts.idempotency import IdempotencyDepPort, IdempotencyPort
from forze.application.execution import ExecutionContext
from forze.base.typing import conforms_to
from forze.utils.codecs import KeyCodec

from ...adapters import RedisCacheAdapter, RedisCounterAdapter, RedisIdempotencyAdapter
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

    return RedisCounterAdapter(
        client=redis_client,
        key_codec=KeyCodec(namespace=namespace),
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
