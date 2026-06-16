---
title: Wiring
icon: lucide/cable
summary: Assembling a real runtime from several integration packages
---

[Core Concepts](../core-concepts/runtime.md) showed that an `ExecutionRuntime`
takes a frozen **dependency registry** and a frozen **lifecycle plan**. A real
service builds both from several integration packages at once. This chapter is
how you get from `forze[postgres,redis]` to a running runtime.

![Several integration modules register into one dependency container](../_diagrams/light/dependency-plan.svg#only-light){ data-src="../_diagrams/light/dependency-plan.svg" }
![Several integration modules register into one dependency container](../_diagrams/dark/dependency-plan.svg#only-dark){ data-src="../_diagrams/dark/dependency-plan.svg" }

## Integration modules

Every integration package ships a **deps module** — a small object that
registers its adapters, keyed by **specification name**. You never construct
adapters yourself; you hand the module its client and the routes it should
serve.

| Module | Registers (examples) |
|--------|----------------------|
| `PostgresDepsModule` | document query/command, search, transactions |
| `RedisDepsModule` | cache, counters, idempotency |
| `S3DepsModule` | object storage |
| `MockDepsModule` | in-memory adapters for every contract |

## Build the dependency registry

`DepsRegistry.from_modules(...)` merges every module into one registry. The same
logical name — `"orders"` — appears in the spec, in each module's route map, and
nowhere does a handler learn which backend answered.

```python
from forze.application.execution import DepsRegistry
from forze_postgres import PostgresClient, PostgresDepsModule
from forze_redis import RedisClient, RedisDepsModule

pg = PostgresClient()
redis = RedisClient()

deps = DepsRegistry.from_modules(
    PostgresDepsModule(client=pg, rw_documents={"orders": orders_pg}, tx={"orders"}),
    RedisDepsModule(client=redis, caches={"orders": orders_cache}),
)
```

!!! warning "One name, everywhere"

    A module route key (`rw_documents={"orders": ...}`) must match the
    `DocumentSpec.name` it serves. Merging two modules that register the same
    contract under the same route raises a `CoreException` at build time — a
    misconfiguration caught before the app starts, not at the first request.

Build incrementally with `registry.with_modules(...)` when modules come from
different parts of your codebase.

## The lifecycle plan

Deps describe **what to build**; the **lifecycle plan** decides **when clients
connect and disconnect**. They're kept separate on purpose — registration is
pure and cheap, while opening a pool is an ordered, fallible startup step.

```python
from forze.application.execution import LifecyclePlan
from forze_postgres import PostgresConfig, PostgresLifecycleModule
from forze_redis import redis_lifecycle_step

lifecycle = (
    LifecyclePlan.from_modules(
        PostgresLifecycleModule(
            client=pg,
            dsn="postgresql://forze:forze@localhost:5432/forze",
            config=PostgresConfig(min_size=1, max_size=10),
        ),
    )
    .with_steps(redis_lifecycle_step(dsn="redis://localhost:6379/0"))
)
```

Each step declares what it `requires` and `provides`, so the plan orders startup
by dependency (and runs shutdown in reverse). If a startup step fails, the steps
that already ran are torn down before the error propagates.

## Freeze and construct

The runtime takes **frozen** inputs and does not coerce them — freezing is what
validates the plan and makes it safe to share across every request.

```python
from forze.application.execution import ExecutionRuntime

runtime = ExecutionRuntime(
    deps=deps.freeze(),
    lifecycle=lifecycle.freeze(),
)
```

From here, `async with runtime.scope():` opens the pools and serves requests, as
covered in [Runtime](../core-concepts/runtime.md#the-scope-lifecycle).

## Routed clients

When the current tenant decides *which* database or cache to talk to, swap the
plain client for a routed one — `RoutedPostgresClient`, `RoutedRedisClient` — and
use that integration's routed lifecycle step. The deps and specs don't change;
only the client and its lifecycle step do.

```python
from forze_postgres import RoutedPostgresClient

# resolves a per-tenant DSN from secrets — see Multi-tenancy for the resolver wiring
pg = RoutedPostgresClient(...)
# PostgresDepsModule(client=pg, ..., introspector_cache_partition_key=current_tenant)
```

How routing resolves a tenant to a connection — and the schema-per-tenant
alternative — is the subject of [Multi-tenancy](multi-tenancy.md).
