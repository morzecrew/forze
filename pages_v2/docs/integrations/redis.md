---
title: Redis / Valkey
icon: lucide/zap
summary: Cache, counters, idempotency, search snapshots, and distributed locks
---

`forze[redis]` implements fast, ephemeral, and coordination-oriented state on
Redis or Valkey: caching, atomic counters, idempotency records, search-result
snapshots, and distributed locks — all behind Forze contracts.

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
</content>
