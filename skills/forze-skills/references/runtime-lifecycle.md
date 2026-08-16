# Runtime and lifecycle

## Runtime setup

Logical **specs** (`DocumentSpec`, `SearchSpec`, `CacheSpec`, …) declare model types and `name` only—no DSNs, table names, collection paths, or index DDL. **Deps modules** (`PostgresDepsModule`, `MongoDepsModule`, `RedisDepsModule`, …) map that same `name` to physical configs (read/write relations, Redis namespaces, `PostgresSearchConfig`, …). **`DepsRegistry.from_modules(...)`** merges those modules so `ExecutionContext` resolves factories by route `spec.name` (for example `DocumentQueryDepKey` / `DocumentCommandDepKey`). See [Specs and wiring](https://morzecrew.github.io/forze/latest/writing-operation/wiring/).

### Dependency registry

Pass **`DepsModule` instances** to `DepsRegistry.from_modules`. Each module’s `__call__` returns a `Deps` container; the plan merges them (conflicting keys raise `CoreException`).

```python
from enum import StrEnum

from forze.application.execution import DepsRegistry, ExecutionRuntime, LifecyclePlan
from forze_postgres import (
    PostgresClient,
    PostgresConfig,
    PostgresDepsModule,
    PostgresDocumentConfig,
    postgres_lifecycle_step,
)
from forze_redis import RedisCacheConfig, RedisClient, RedisConfig, RedisDepsModule, redis_lifecycle_step


class ResourceName(StrEnum):
    PROJECTS = "projects"


class TxRoute(StrEnum):
    DEFAULT = "default"


postgres_client = PostgresClient()
redis_client = RedisClient()

deps_registry = DepsRegistry.from_modules(
    PostgresDepsModule(
        client=postgres_client,
        rw_documents={
            ResourceName.PROJECTS: PostgresDocumentConfig(
                read=("public", "projects"),
                write=("public", "projects"),
                bookkeeping_strategy="database",
            ),
        },
        tx={TxRoute.DEFAULT},
    ),
    RedisDepsModule(
        client=redis_client,
        caches={ResourceName.PROJECTS: RedisCacheConfig(namespace="app:projects")},
    ),
)
```

Alternatively, a single callable module may return `Deps.merge(...)` — see [Getting started](https://morzecrew.github.io/forze/latest/get-started/quickstart/).

Merge optional integration modules the same way — for example `TenancyDepsModule` from `forze_identity.tenancy.execution` registers `TenantResolverDepKey` / `TenantManagementDepKey` routes for document-backed tenant resolution (see [Multi-tenancy](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/multi-tenancy/)):

```python
from forze_identity.tenancy.execution import TenancyDepsModule

deps_registry = DepsRegistry.from_modules(
    PostgresDepsModule(...),
    TenancyDepsModule(tenant_resolver={"main"}),
)
```

### Lifecycle plan

Manages startup/shutdown of connection pools. Use `LifecyclePlan.from_modules(...)` for integration modules (for example `PostgresLifecycleModule`) or `from_steps(...)` for individual factories. Call `freeze()` to build topological waves using `requires` / `provides` / `depends_on` on each `LifecycleStep`, then pass the frozen plan to `ExecutionRuntime`. Use `with_concurrent()` when independent steps in the same wave may start in parallel.

This block continues the dependency-registry snippet above — it reuses its imports (`PostgresConfig`, `RedisConfig`, `redis_lifecycle_step`) and the `postgres_client` / `redis_client` instances.

```python
from forze_postgres import PostgresLifecycleModule

lifecycle_plan = LifecyclePlan.from_modules(
    PostgresLifecycleModule(
        client=postgres_client,
        dsn="postgresql://app:app@localhost:5432/app",
        config=PostgresConfig(min_size=2, max_size=15),
    ),
).with_steps(
    redis_lifecycle_step(
        dsn="redis://localhost:6379/0",
        config=RedisConfig(max_size=20),
    ),
)
```

### Execution runtime

```python
runtime = ExecutionRuntime(
    deps=deps_registry.freeze(),
    lifecycle=lifecycle_plan.freeze(),
)
```

Run work inside `runtime.scope()`:

```python
async with runtime.scope():
    ctx = runtime.get_context()
```

`build_runtime(deps_modules, *, lifecycle_modules=, lifecycle_steps=, ...)` (from `forze.application.execution`) — one positional argument, a module or an iterable of them; everything else is keyword-only. It assembles the same thing in one call, freezing both plans for you. Production knobs live there too: `drain_timeout=` (graceful drain window on shutdown, default 10s) and `deployment=DeploymentProfile.FLEET` (fails assembly for unguarded shared-state-mutating lifecycle steps when running N replicas; guard them with `forze_kits.lifecycle.singleton_lifecycle_step`). See [shutdown and fleet posture](shutdown-fleet.md).

## Declare the spec inventory

Pass `specs=` and the runtime knows what your application consists of — the dependency registry knows every `(key, route)` it binds but not a single spec, so without an inventory nothing can enumerate your planes:

```python
from forze.application.contracts.inventory import SpecRegistry

specs = SpecRegistry().register(order_spec, order_search_spec, invoice_blob_spec)

runtime = build_runtime(modules, specs=specs, lifecycle_steps=steps)
```

Kits and the identity plane contribute their own entries, including the routes nobody hand-wrote (a kit's search-sync outbox, queue and inbox). At construction `build_runtime` reconciles the inventory against the wiring and **logs** a bound route the inventory does not know — a drift signal, not a gate.

Declare it whenever you may need to export, migrate, or `quiesce()` this application: all three refuse to run without an inventory, because a plane nobody catalogued is a plane they cannot vouch for.

## Anti-patterns

- **Hand-building `Deps` for production** — prefer `DepsRegistry.from_modules` and integration modules.
- **Skipping lifecycle** — real adapters need pools started/stopped.
- **`get_context()` outside `runtime.scope()`** — raises `RuntimeError`.

## Reference

- [Getting started](https://morzecrew.github.io/forze/latest/get-started/quickstart/)
