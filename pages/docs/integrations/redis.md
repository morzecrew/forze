# Redis / Valkey Integration

`forze_redis` provides cache, counters, idempotency, pub/sub, and stream adapters backed by Redis or Valkey. It implements `CachePort`, `CounterPort`, `IdempotencyPort`, `PubSubPublishPort`, `PubSubSubscribePort`, `StreamReadPort`, `StreamWritePort`, and `StreamGroupPort`.

## Installation

    :::bash
    uv add 'forze[redis]'

Works with both Redis and Valkey (API-compatible).

## Runtime wiring

    :::python
    from forze.application.execution import DepsPlan, ExecutionRuntime, LifecyclePlan
    from forze_redis import RedisClient, RedisConfig, RedisDepsModule, redis_lifecycle_step

    client = RedisClient()
    module = RedisDepsModule(client=client)

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

`RedisDepsModule` registers these dependency keys:

| Key | Capability |
|-----|-----------|
| `RedisClientDepKey` | Raw Redis client for direct commands |
| `CacheDepKey` | Document cache adapter factory |
| `CounterDepKey` | Namespace-scoped counter adapter factory |
| `IdempotencyDepKey` | HTTP idempotency adapter |
| `PubSubPublishDepKey` | Pub/Sub publish adapter factory |
| `PubSubSubscribeDepKey` | Pub/Sub subscribe adapter factory |
| `StreamReadDepKey` | Stream read adapter factory |
| `StreamWriteDepKey` | Stream write adapter factory |
| `StreamGroupDepKey` | Stream consumer group adapter factory |

## Document cache

When a `DocumentSpec` has `cache.enabled = True`, the execution context automatically resolves and injects a Redis-backed cache adapter into the document port. No explicit wiring is needed beyond having `RedisDepsModule` in the deps plan.

    :::python
    from datetime import timedelta
    from forze.application.contracts.document import DocumentSpec

    spec = DocumentSpec(
        namespace="projects",
        read={"source": "public.projects", "model": ProjectReadModel},
        write={
            "source": "public.projects",
            "models": {
                "domain": Project,
                "create_cmd": CreateProjectCmd,
                "update_cmd": UpdateProjectCmd,
            },
        },
        cache={"enabled": True, "ttl": timedelta(minutes=5)},
    )

The cache adapter stores and retrieves serialized documents keyed by namespace and document ID. It handles cache invalidation on writes automatically.

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
        CacheSpec(namespace="sessions", ttl=timedelta(minutes=30))
    )

    await cache.set(session_id, session_data)
    result = await cache.get(session_id)
    await cache.invalidate(session_id)

## Counters

Counters are namespace-scoped atomic incrementers. They are typically used for generating human-readable sequence numbers (`number_id`).

    :::python
    counter = ctx.counter("projects")

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

## Pub/Sub

Redis Pub/Sub provides fire-and-forget message broadcasting. Messages are delivered to all connected subscribers but are not persisted. If no subscriber is listening, the message is lost.

### Publishing

    :::python
    from pydantic import BaseModel
    from forze.application.contracts.pubsub import PubSubPublishDepKey, PubSubSpec


    class OrderEvent(BaseModel):
        order_id: str
        status: str


    spec = PubSubSpec(namespace="orders", model=OrderEvent)

    publish = ctx.dep(PubSubPublishDepKey)(ctx, spec)
    await publish.publish(
        "orders.status_changed",
        OrderEvent(order_id="abc", status="shipped"),
    )

### Subscribing

    :::python
    from forze.application.contracts.pubsub import PubSubSubscribeDepKey

    subscribe = ctx.dep(PubSubSubscribeDepKey)(ctx, spec)

    async for message in subscribe.subscribe(["orders.status_changed"]):
        print(f"Order {message['payload'].order_id} -> {message['payload'].status}")

Each message is a `PubSubMessage[M]` TypedDict with `topic`, `payload`, and optional `type`, `published_at`, `key` fields.

## Streams

Redis Streams provide persistent, append-only log semantics with consumer group support. Unlike Pub/Sub, messages are stored and can be replayed.

### Writing to a stream

    :::python
    from forze.application.contracts.stream import StreamWriteDepKey, StreamSpec


    class AuditEntry(BaseModel):
        action: str
        resource_id: str


    spec = StreamSpec(namespace="audit", model=AuditEntry)

    writer = ctx.dep(StreamWriteDepKey)(ctx, spec)
    entry_id = await writer.append(
        "audit.actions",
        AuditEntry(action="create", resource_id="proj-42"),
    )

### Reading from a stream

    :::python
    from forze.application.contracts.stream import StreamReadDepKey

    reader = ctx.dep(StreamReadDepKey)(ctx, spec)

    # Read entries starting from a position
    entries = await reader.read(
        {"audit.actions": "0"},
        limit=100,
    )

    # Tail new entries as they arrive
    async for entry in reader.tail({"audit.actions": "$"}):
        print(entry["payload"].action)

### Consumer groups

Consumer groups allow multiple workers to process stream entries cooperatively. Each entry is delivered to exactly one consumer in the group.

    :::python
    from forze.application.contracts.stream import StreamGroupDepKey

    group = ctx.dep(StreamGroupDepKey)(ctx, spec)

    # Read pending entries for this consumer
    entries = await group.read(
        group="workers",
        consumer="worker-1",
        stream_mapping={"audit.actions": ">"},
        limit=10,
    )

    # Process and acknowledge
    for entry in entries:
        await process(entry["payload"])

    await group.ack(
        "workers",
        "audit.actions",
        [e["id"] for e in entries],
    )

Each stream message is a `StreamMessage[M]` TypedDict with `stream`, `id`, `payload`, and optional `type`, `timestamp`, `key` fields.

## Idempotency

The Redis idempotency adapter stores request fingerprints and response snapshots. It is used automatically by `ForzeAPIRouter` routes marked with `idempotent=True`.

The adapter is registered by `RedisDepsModule` under `IdempotencyDepKey`. No additional configuration is needed.

Key pattern: `idempotency/{operation}/{idempotency_key}`

### How it works

1. On the first request, `begin()` returns `None` (no cached response)
2. After the handler succeeds, `commit()` stores the response as an `IdempotencySnapshot`
3. On duplicate requests (same operation + key + payload hash), `begin()` returns the stored snapshot
4. The router returns the cached response without re-executing the handler

## Combining with Postgres

Redis is commonly combined with Postgres for a full stack:

    :::python
    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(
            lambda: Deps.merge(
                PostgresDepsModule(client=pg, rev_bump_strategy="database", history_write_strategy="database")(),
                RedisDepsModule(client=redis)(),
            ),
        ),
        lifecycle=LifecyclePlan.from_steps(
            postgres_lifecycle_step(dsn="postgresql://...", config=PostgresConfig()),
            redis_lifecycle_step(dsn="redis://...", config=RedisConfig()),
        ),
    )

With both modules registered:

- Document reads are cached in Redis when `cache.enabled = True`
- Counters for `number_id` use Redis atomic increments
- Idempotency uses Redis for deduplication
- Pub/Sub and Streams are available for event-driven patterns
