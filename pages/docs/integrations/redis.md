---
title: Redis / Valkey
icon: lucide/zap
summary: Cache, counters, idempotency, search snapshots, distributed locks, streams, and pub-sub
---

`forze[redis]` implements fast, ephemeral, and coordination-oriented state on
Redis or Valkey: caching, atomic counters, idempotency records, search-result
snapshots, distributed locks, and the messaging transports (Redis Streams and
pub-sub) — all behind Forze contracts.

## Install

```bash
uv add 'forze[redis]'
```

Needs a reachable Redis or Valkey server.

## The client

```python
from forze_redis import RedisClient

redis = RedisClient()
```

Use `RoutedRedisClient` when the tenant or route selects the endpoint.

## Wire it

Each resource takes a **namespace** (a logical key prefix); set
`tenant_aware=True` when keys must include the current tenant. Register the
resources on the deps module, open the pool from the lifecycle plan:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_redis import (
    RedisCacheConfig,
    RedisConfig,
    RedisDepsModule,
    RedisIdempotencyConfig,
    redis_lifecycle_step,
)

deps = DepsRegistry.from_modules(
    RedisDepsModule(
        client=redis,
        caches={"orders": RedisCacheConfig(namespace="app:orders", tenant_aware=True)},
        idempotency=RedisIdempotencyConfig(namespace="app:idempotency"),
    ),
)
lifecycle = LifecyclePlan.from_steps(
    redis_lifecycle_step(dsn="redis://localhost:6379/0", config=RedisConfig(max_size=20)),
)
```

## What it provides

| Contract | Keyed by |
|----------|----------|
| Cache | `CacheSpec.name` (`caches`) |
| Counter | `CounterSpec.name` (`counters`) |
| Idempotency | `IdempotencySpec.name` (`idempotency`, plain or routed) |
| Search-result snapshots | `SearchResultSnapshotSpec.name` (`search_snapshots`) |
| Distributed locks | `DistributedLockSpec.name` (`dlocks`) |
| Stream (read/append) | `StreamSpec.name` (`streams`) |
| Stream consumer group (read/ack/claim + provisioning) | `StreamSpec.name` (`stream_groups`) |
| Pub-sub (publish/subscribe) | `PubSubSpec.name` (`pubsub`) |

## Streams & pub-sub

Redis Streams back the `StreamSpec` transport (append/read + consumer groups, the
outbox relay's `to_stream`, and the realtime gateway); Redis Pub/Sub backs the
`PubSubSpec` transport (publish/subscribe, the relay's `to_pubsub`). Register routes
with `RedisStreamConfig`, `RedisStreamGroupConfig`, and `RedisPubSubConfig`:

```python
from forze_redis import RedisPubSubConfig, RedisStreamConfig, RedisStreamGroupConfig

RedisDepsModule(
    client=redis,
    streams={"events": RedisStreamConfig(tenant_aware=True)},
    stream_groups={"events": RedisStreamGroupConfig(tenant_aware=True)},
    pubsub={"presence": RedisPubSubConfig()},
)
```

Unlike the keyed resources above, these carry **no namespace** — the stream/topic
name is supplied per call, and `tenant_aware=True` isolates it with a
`tenant:{id}:` key prefix. Set `encryption="end_to_end"` on the `StreamSpec` /
`PubSubSpec` to seal payloads through the broker (the consumer decrypts); a wired
`CryptoDepsModule` is required, and a deployment `required_reach` floor is
enforced here. Pub-sub is at-most-once past the broker.

## L1 push invalidation

`RedisCacheConfig(invalidation_push=True)` enables Redis 6+ **client-side
caching** (`CLIENT TRACKING`, RESP3 push) for caches backing the
[document L1](../data-events/caching.md#an-in-process-l1-for-hot-documents):
one pinned connection per client receives an invalidation push for every
write, expiration, or eviction under the cache's key prefix — by any replica —
and the in-process L1 drops the entry immediately, demoting the L1 TTL to a
backstop. Fails open (stream loss flushes the L1 and falls back to TTL
semantics while reconnecting); tenant-routed clients and dynamic namespaces
stay TTL-only by design.

## Fleet-wide resilience state

Two builders turn the process-local [resilience](../running-in-prod/resilience.md)
state into shared, fleet-wide state:

```python
from forze.application.execution import ResilienceDepsModule
from forze_redis import redis_circuit_breaker_store, redis_rate_limit_store

ResilienceDepsModule(
    breaker_store=redis_circuit_breaker_store(redis),
    rate_limit_store=redis_rate_limit_store(redis),
)
```

The breaker store makes an open circuit on one replica protect them all; the
rate-limit store keeps the token bucket in a Redis hash mutated atomically by
Lua on the **server clock**, so the declared `permits/per` is the fleet's rate
(not per-replica). Both fail open to process-local state on a Redis error —
see [Fleet-wide state](../running-in-prod/resilience.md#fleet-wide-state).

## Distributed locks and fencing

A lock alone is best-effort exclusion: a holder paused by GC or a network
partition can resume after its lease expired while a new holder runs. To close
that gap, `acquire` returns an `AcquiredLock` whose `token` is a **fencing
token** — monotonically increasing per key across lock generations. The Redis
adapter issues it atomically with the `SET NX PX` acquire (a Lua script `INCR`s
a per-key `<lock key>:fence` counter; the counter has no TTL and is never
deleted on release, so tokens stay monotonic even after expiry — at the cost of
one small permanent key per lock key).

Protect downstream writes by sending the token with the write and rejecting,
storage-side, any token lower than the highest one observed for that resource:

```python
async with dlock_scope.scope("invoice:42") as lock:
    # e.g. UPDATE ... SET fence = :token WHERE id = 42 AND fence < :token
    await repo.update_invoice(invoice, fence_token=lock.token)
```

Extending a live lease (`reset`, the scope's heartbeat) keeps the same token —
only a fresh acquisition starts a new generation. Without the consumer-side
token check the lock remains best-effort exclusion; the check is what upgrades
it to fenced exclusion.

## Notes

- **Namespaces are deliberate.** Give each contract its own namespace; don't
  share one across unrelated resources.
- **TTLs are logical.** `CacheSpec` / `IdempotencySpec` TTLs are expiry intents —
  Redis memory eviction can drop keys earlier, so size and policy the server
  accordingly.
- **Idempotency** needs stable keys and a TTL longer than client retries; every
  worker that can handle an operation must share the same namespace.
- **Routed clients** use `routed_redis_lifecycle_step` — don't mix routed and
  non-routed lifecycle steps for one client.
