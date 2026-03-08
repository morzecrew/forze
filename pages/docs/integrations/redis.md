# Redis / Valkey Integration

This guide explains how to use Redis (or Valkey) with Forze for cache, counters, and idempotency.

## Prerequisites

- `forze[redis]` installed
- Redis/Valkey reachable from your app

## What this integration provides

`RedisDepsModule` registers:

| Dependency key | Capability |
|----------------|------------|
| `RedisClientDepKey` | low-level Redis client |
| `CacheDepKey` | document cache adapter |
| `CounterDepKey` | namespace counters |
| `IdempotencyDepKey` | HTTP idempotency adapter |

## Runtime wiring

Use module + lifecycle together:

    :::python
    from forze.application.execution import DepsPlan, ExecutionRuntime, LifecyclePlan
    from forze_redis import RedisClient, RedisConfig, RedisDepsModule, redis_lifecycle_step

    redis_client = RedisClient()
    redis_module = RedisDepsModule(client=redis_client)

    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(redis_module),
        lifecycle=LifecyclePlan.from_steps(
            redis_lifecycle_step(
                dsn="redis://localhost:6379/0",
                config=RedisConfig(
                    max_size=20,
                    socket_timeout=2.0,
                    connect_timeout=2.0,
                ),
            )
        ),
    )

## Document cache

If `DocumentSpec.cache.enabled` is true, `ExecutionContext.doc(spec)` resolves and injects a cache adapter automatically.

    :::python
    from datetime import timedelta
    from forze.application.contracts.document import DocumentSpec

    spec = DocumentSpec(
        namespace="projects",
        sources={"read": "public.projects", "write": "public.projects"},
        models={...},
        cache={"enabled": True, "ttl": timedelta(minutes=5)},
    )

## Resolve cache directly

Use this if you need cache outside the document adapter:

    :::python
    from datetime import timedelta
    from forze.application.contracts.cache import CacheSpec

    cache = ctx.cache(
        CacheSpec(
            namespace="projects",
            ttl=timedelta(minutes=5),
        )
    )

## Counters

Counters are namespace-scoped and useful for number IDs or sequence allocation.

    :::python
    counter = ctx.counter("projects")

    one = await counter.incr()
    batch_end = await counter.incr_batch(10)
    await counter.decr(by=1)
    await counter.reset(value=1)

## Idempotency for FastAPI

Idempotency is auto-registered by `RedisDepsModule`. When a `ForzeAPIRouter` route is declared with `idempotent=True`, the adapter stores request fingerprints and responses by idempotency key.

    :::python
    @router.post(
        "/create",
        idempotent=True,
        operation_id="projects.create",
        idempotency_config={"dto_param": "payload"},
    )
    async def create(payload: CreatePayload):
        ...

## Key patterns

| Adapter | Pattern |
|---------|---------|
| Cache | `{namespace}/cache/pointer/{key}` and `{namespace}/cache/body/{key}/{version}` |
| Counter | `{namespace}[/{suffix}]` |
| Idempotency | `idempotency/{operation}/{idempotency_key}` |
