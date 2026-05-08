# Redis / Valkey Integration

## Page opening

`forze_redis` provides Redis/Valkey-backed infrastructure adapters for caches, counters, idempotency records, search result snapshots, and distributed locks. It is designed for fast, ephemeral or coordination-oriented state behind Forze contracts.

| Topic | Details |
|------|---------|
| What it provides | A Redis client, lifecycle hooks, dependency module, codecs, and adapters for cache, counter, idempotency, search snapshot, and distributed-lock contracts. |
| Supported Forze contracts | `CacheDepKey`, `CounterDepKey`, `IdempotencyDepKey`, `SearchResultSnapshotDepKey`, `DistributedLockQueryDepKey`, and `DistributedLockCommandDepKey`. |
| When to use it | Use this integration for low-latency caching, duplicate request protection, counters, search pagination snapshots, or distributed locking with Redis-compatible infrastructure. |

## Installation

```bash
uv add 'forze[redis]'
```

| Requirement | Notes |
|-------------|-------|
| Package extra | `redis` installs the async Redis client dependency. |
| Required service | Redis or Valkey. |
| Local development dependency | A local Redis/Valkey server or container. Integration tests normally use testcontainers. |

## Minimal setup

### Client

```python
from forze_redis import RedisClient, RedisConfig

redis = RedisClient()
```

Use `RoutedRedisClient` when tenant or route identity selects the Redis endpoint.

### Config

```python
from forze_redis import RedisCacheConfig, RedisIdempotencyConfig

cache_config = RedisCacheConfig(namespace="projects", tenant_aware=True)
idempotency_config = RedisIdempotencyConfig(namespace="idempotency")
```

Each Redis resource needs a namespace. Set `tenant_aware=True` when keys must include the current tenancy identity.

### Deps module

```python
from forze.application.execution import DepsPlan
from forze_redis import RedisDepsModule

redis_module = RedisDepsModule(
    client=redis,
    caches={"projects": cache_config},
    idempotency=idempotency_config,
    search_snapshots={"projects": {"namespace": "project-search"}},
    dlocks={"project-locks": {"namespace": "locks"}},
)

deps_plan = DepsPlan.from_modules(redis_module)
```

Use routed mappings when the contract is named, and a plain `idempotency` config when all idempotency specs share one Redis namespace.

### Lifecycle step

```python
from forze.application.execution import LifecyclePlan
from forze_redis import redis_lifecycle_step

lifecycle = LifecyclePlan.from_steps(
    redis_lifecycle_step(
        dsn="redis://localhost:6379/0",
        config=RedisConfig(max_size=20),
    )
)
```

Use `routed_redis_lifecycle_step(client=routed_redis)` with `RoutedRedisClient` and do not combine routed and non-routed lifecycle steps for the same client.

## Contract coverage table

| Forze contract | Adapter implementation | Dependency key/spec name | Limitations |
|----------------|------------------------|--------------------------|-------------|
| Cache | `ConfigurableRedisCache` / `RedisCacheAdapter` | `CacheDepKey`, route usually equal to `CacheSpec.name`. | TTL behavior comes from `CacheSpec`; Redis memory eviction policy still applies. |
| Counter | `ConfigurableRedisCounter` / `RedisCounterAdapter` | `CounterDepKey`, route usually equal to `CounterSpec.name`. | Counters are Redis-backed and should not be treated as relational source-of-truth rows. |
| Idempotency | `ConfigurableRedisIdempotency` / `RedisIdempotencyAdapter` | `IdempotencyDepKey`, plain or routed by `IdempotencySpec.name`. | Correctness depends on stable idempotency keys and a TTL long enough for client retries. |
| Search result snapshots | `ConfigurableRedisSearchResultSnapshot` / `RedisSearchResultSnapshotAdapter` | `SearchResultSnapshotDepKey`, route usually equal to `SearchResultSnapshotSpec.name`. | Snapshot IDs expire; clients must handle expired pagination snapshots. |
| Distributed locks | `ConfigurableRedisDistributedLock` / `RedisDistributedLockAdapter` | `DistributedLockQueryDepKey` and `DistributedLockCommandDepKey`, route usually equal to `DistributedLockSpec.name`. | Lock safety depends on Redis availability, expiry settings, and clock/timeout choices. |
| Raw client | `RedisClient` | `RedisClientDepKey`. | Prefer contract adapters in usecases unless raw Redis commands are truly infrastructure-specific. |

## Idempotency

Redis idempotency stores in-flight or completed operation records for the TTL defined by the `IdempotencySpec`. Use one namespace for shared idempotency state or route named idempotency specs through `RedisDepsModule.idempotency`. Clients and handlers still need stable operation keys; Redis provides the storage and expiry semantics.

## Complete recipe link

See [Add Caching](../recipes/add-caching.md) and [Add Idempotency](../recipes/add-idempotency.md) for focused recipes, or [CRUD with FastAPI, Postgres, and Redis](../recipes/crud-fastapi-postgres-redis.md) for a combined stack.

## Configuration reference

### Connection settings

`RedisClient` connects with a Redis URL. Use a routed client for per-tenant or per-region endpoints. Configure TLS, authentication, database number, and Sentinel/cluster strategy in the URL or client construction supported by the underlying Redis client.

### Pool settings

`RedisConfig` exposes `max_size`, `socket_timeout`, and `connect_timeout`. Keep `max_size` aligned with application concurrency and Redis server limits.

### Serialization settings

Adapters use Redis key codecs with the configured namespace and optional tenant identity. Payload serialization is handled by the adapter for the relevant contract; use Pydantic models at contract boundaries.

### Retry/timeout behavior

Set `socket_timeout` and `connect_timeout` for bounded Redis calls. Add higher-level retries only for operations that are safe to repeat; idempotency and lock operations should be retried carefully because timing matters.

## Operational notes

| Concern | Notes |
|---------|-------|
| Migrations/schema requirements | None. Namespaces are logical key prefixes, but production deployments should document key patterns and retention expectations. |
| Cleanup/shutdown | Register `redis_lifecycle_step` or `routed_redis_lifecycle_step` so connection pools close cleanly. |
| Idempotency/caching behavior | Cache and idempotency records expire according to their specs. Redis eviction can remove keys earlier if memory policies allow it. |
| Production caveats | Enable persistence/replication only when the stored data requires it, monitor memory and key cardinality, and avoid using one namespace for unrelated contracts. |

## Troubleshooting

| Common error | Likely cause | Fix |
|--------------|--------------|-----|
| Cache misses immediately after writes | TTL is too short or Redis evicted keys. | Review `CacheSpec` TTL values and Redis eviction/memory settings. |
| Idempotency does not deduplicate requests | Different idempotency keys are sent or the idempotency adapter is not registered. | Send a stable key and register `IdempotencyDepKey` through `RedisDepsModule`. |
| Tenant data appears in shared keys | `tenant_aware` is disabled or tenancy identity is missing from the context. | Set `tenant_aware=True` and ensure the execution context has tenancy information. |
| Lock acquisition never succeeds | A previous holder has not expired or lock timeout settings are mismatched. | Check the lock spec lease/timeout values and Redis key TTLs. |
