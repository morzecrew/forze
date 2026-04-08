# Redis / Valkey Integration

`forze_redis` provides cache, counters, and idempotency adapters backed by Redis or Valkey. It implements `CachePort`, `CounterPort`, and `IdempotencyPort`.

Kernel specs use logical names (`CacheSpec.name`, document cache on `DocumentSpec`). **`RedisDepsModule` maps each name to a `RedisCacheConfig`** (namespace prefix, optional `tenant_aware`). See [Specs and infrastructure wiring](../core-concepts/specs-and-wiring.md).

## Installation

    :::bash
    uv add 'forze[redis]'

Works with both Redis and Valkey (API-compatible).

## Runtime wiring

    :::python
    from forze.application.execution import DepsPlan, ExecutionRuntime, LifecyclePlan
    from forze_redis import RedisClient, RedisConfig, RedisDepsModule, redis_lifecycle_step

    client = RedisClient()
    module = RedisDepsModule(
        client=client,
        caches={
            "projects": {"namespace": "app:projects"},
        },
        counters={
            "projects": {"namespace": "app:seq:projects"},
        },
        idempotency={
            "default": {"namespace": "app:idempotency"},
        },
    )

    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(module),
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

### RedisConfig options

| Option | Type | Default | Purpose |
|--------|------|---------|---------|
| `max_size` | `int` | `10` | Maximum connections in the pool |
| `socket_timeout` | `float` | `5.0` | Socket read/write timeout (seconds) |
| `connect_timeout` | `float` | `5.0` | Connection establishment timeout (seconds) |

### What gets registered

`RedisDepsModule` registers **routed** factories under:

| Key | Maps |
|-----|------|
| `RedisClientDepKey` | Shared async Redis client |
| `CacheDepKey` | `caches: dict[str, RedisCacheConfig]` → `CacheSpec.name` |
| `CounterDepKey` | `counters: dict[str, RedisCounterConfig]` → `CounterSpec.name` |
| `IdempotencyDepKey` | `idempotency: dict[str, RedisIdempotencyConfig]` → idempotency route on `IdempotencySpec` |

Each config requires a `namespace` string used as a Redis key prefix.

## Document cache

When `DocumentSpec.cache` is set, `doc_query` / `doc_command` resolve `ctx.cache(spec.cache)` and pass the port into the document adapter. Register a cache route whose **key matches `CacheSpec.name`**:

    :::python
    from datetime import timedelta
    from forze.application.contracts.cache import CacheSpec
    from forze.application.contracts.document import DocumentSpec

    project_spec = DocumentSpec(
        name="projects",
        read=ProjectReadModel,
        write={
            "domain": Project,
            "create_cmd": CreateProjectCmd,
            "update_cmd": UpdateProjectCmd,
        },
        cache=CacheSpec(name="projects", ttl=timedelta(minutes=5)),
    )

    # RedisDepsModule.caches must include the same key "projects"
    RedisDepsModule(
        client=redis_client,
        caches={"projects": {"namespace": "app:projects"}},
    )

The adapter stores versioned bodies under the configured namespace.

### Cache key patterns

| Pattern | Purpose |
|---------|---------|
| `{namespace}/cache/pointer/{key}` | Points to the current cache version |
| `{namespace}/cache/body/{key}/{version}` | Stores the serialized document body |

The two-level key design allows atomic cache invalidation: updating the pointer version makes old body entries expire naturally.

## Direct cache access

When you need cache outside of the document adapter, resolve a cache port directly:

    :::python
    from datetime import timedelta
    from forze.application.contracts.cache import CacheSpec

    cache = ctx.cache(
        CacheSpec(name="sessions", ttl=timedelta(minutes=30))
    )

    await cache.set(session_id, session_data)
    result = await cache.get(session_id)
    await cache.invalidate(session_id)

## Counters

Counters are namespace-scoped atomic incrementers. Pass a `CounterSpec` whose `name` matches `RedisDepsModule.counters`:

    :::python
    from forze.application.contracts.counter import CounterSpec

    counter = ctx.counter(CounterSpec(name="projects"))

    next_id = await counter.incr()
    batch_end = await counter.incr_batch(10)
    await counter.decr(by=1)
    await counter.reset(value=1)

| Method | Returns | Purpose |
|--------|---------|---------|
| `incr(suffix?, by?)` | `int` | Increment by amount (default 1), return new value |
| `incr_batch(count, suffix?)` | `int` | Increment by count, return final value |
| `decr(suffix?, by?)` | `int` | Decrement by amount, return new value |
| `reset(suffix?, value?)` | `None` | Reset counter to value (default 0) |

Counter keys follow the pattern `{namespace}[/{suffix}]`.

## Idempotency

The Redis idempotency adapter stores request fingerprints and response snapshots. FastAPI routes that use `IdempotencyFeature` (for example the document **create** route from `attach_document_endpoints` when idempotency is enabled) resolve `IdempotencyPort` via `IdempotencyDepKey`.

Register at least one route in `RedisDepsModule.idempotency` (for example `"default"`). The `IdempotencySpec.name` on each HTTP feature must match a configured route.

Key pattern: `{namespace}/{operation}/{idempotency_key}` (namespace comes from `RedisIdempotencyConfig`)

### How it works

1. On the first request, `begin()` returns `None` (no cached response)
2. After the handler succeeds, `commit()` stores the response as an `IdempotencySnapshot`
3. On duplicate requests (same operation + key + payload hash), `begin()` returns the stored snapshot
4. The endpoint returns the cached response without re-executing the handler

## Combining with Postgres

Redis is commonly combined with Postgres for cache, counters, and idempotency:

    :::python
    from forze.application.execution import Deps, DepsPlan, ExecutionRuntime, LifecyclePlan
    from forze_postgres import PostgresDepsModule, postgres_lifecycle_step, PostgresConfig
    from forze_redis import RedisDepsModule, redis_lifecycle_step, RedisConfig

    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(
            lambda: Deps.merge(
                PostgresDepsModule(client=pg, rw_documents={...})(),
                RedisDepsModule(
                    client=redis,
                    caches={"projects": {"namespace": "app:projects"}},
                    idempotency={"default": {"namespace": "app:idempotency"}},
                )(),
            ),
        ),
        lifecycle=LifecyclePlan.from_steps(
            postgres_lifecycle_step(dsn="postgresql://...", config=PostgresConfig()),
            redis_lifecycle_step(dsn="redis://...", config=RedisConfig()),
        ),
    )

With both modules registered:

- `DocumentSpec.cache` pulls in Redis when `CacheSpec.name` exists in `caches`
- `CounterSpec` routes to `counters`
- HTTP idempotency uses `idempotency` routes
