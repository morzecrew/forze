# Redis / Valkey Integration

This guide explains how to set up Redis (or Valkey) for Forze document cache, counters, and idempotency. It covers connection configuration, cache wiring, counter namespaces, and idempotency for HTTP request deduplication.

## Prerequisites

- Redis 6+ or Valkey (Redis-compatible)
- `forze[redis]` extra installed

## Connection

The Redis client uses a DSN and optional pool configuration. Use :class:`RedisDepsModule` to register the client and ports, and :func:`redis_lifecycle_step` for startup/shutdown:

```python
from forze.application.execution import Deps, LifecyclePlan
from forze_redis import RedisClient, RedisConfig, RedisDepsModule, redis_lifecycle_step

client = RedisClient()
deps_module = RedisDepsModule(client=client)

# Build deps and lifecycle
deps = deps_module()
lifecycle = LifecyclePlan.from_steps(
    redis_lifecycle_step(
        dsn="redis://localhost:6379/0",
        config=RedisConfig(max_size=20),
    )
)
```

The client supports connection pooling, pipelining, and configurable timeouts. See :class:`forze_redis.kernel.platform.client.RedisConfig` for pool options.

## Ports Registered

:class:`RedisDepsModule` registers the following dependency keys:

| Key | Port | Use case |
|-----|------|----------|
| :data:`CacheDepKey` | :class:`CachePort` | Document cache (pointer + body) |
| :data:`CounterDepKey` | :class:`CounterPort` | Namespace-scoped counters |
| :data:`IdempotencyDepKey` | :class:`IdempotencyPort` | HTTP request idempotency |

## Document Cache

When a :class:`DocumentSpec` has `cache` enabled, the cache port is resolved automatically from the execution context via :data:`CacheDepKey` (directly or through a router) and injected into the document adapter.

### Cache Specification

Cache is configured per document aggregate via :class:`DocumentCacheSpec` in the document spec:

```python
from forze.application.contracts.document import DocumentSpec, DocumentModelSpec
from datetime import timedelta

spec = DocumentSpec(
    namespace="documents",
    sources={"read": "public.documents", "write": "public.documents"},
    models=DocumentModelSpec(...),
    cache={"enabled": True, "ttl": timedelta(seconds=300)},
)
```

The Redis cache adapter uses the document `namespace` for key prefixing. Keys follow the pattern `{namespace}/cache/{type}/{...}` (pointer, body, kv).

### CacheSpec

When resolving a cache port directly via :meth:`ExecutionContext.cache`, use :class:`CacheSpec`:

```python
from forze.application.contracts.cache import CacheSpec
from datetime import timedelta

spec = CacheSpec(
    namespace="documents",
    ttl=timedelta(seconds=300),
)
cache_port = ctx.cache(spec)
```

## Counter

Counters are namespace-scoped. Resolve a counter port via :meth:`ExecutionContext.counter`:

```python
counter = ctx.counter("documents")
value = await counter.incr()           # increment by 1
batch = await counter.incr_batch(10)   # reserve a batch of IDs
await counter.decr(by=1)
await counter.reset(value=1)
```

The Redis counter adapter uses `INCR`/`DECR` under the hood. Keys are prefixed with the namespace.

## Idempotency

Idempotency prevents duplicate processing of HTTP requests (e.g. POST retries). Register :data:`IdempotencyDepKey` with a factory (e.g. :func:`redis_idempotency`) that builds an :class:`IdempotencyPort`.

The Redis idempotency adapter stores request fingerprints with a configurable TTL (default 30 seconds). Use :class:`IdempotentRoute` or :func:`make_idempotent_route_class` in FastAPI to apply idempotency at the route level.

```python
# RedisDepsModule registers redis_idempotency with default ttl=30s
# Customize by using a router or a wrapper factory
```

## Key Structure

The Redis adapters use :class:`forze.utils.codecs.KeyCodec` for key construction:

| Adapter | Key pattern |
|---------|-------------|
| Cache | `{namespace}/cache/pointer/{key}`, `{namespace}/cache/body/{key}/{version}` |
| Counter | `{namespace}[/{suffix}]` |
| Idempotency | `idempotency/{op}/{key}` |