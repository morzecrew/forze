---
name: forze-specs-infrastructure
description: >-
  Maps kernel specs (DocumentSpec, SearchSpec, CacheSpec, CounterSpec) to
  integration configs on PostgresDepsModule, MongoDepsModule, RedisDepsModule,
  and transaction routes. Use when wiring tables, collections, Redis namespaces,
  or aligning spec.name with deps module keys.
---

# Forze specs and infrastructure wiring

Kernel **specs** declare model types and logical `name`. Integration **configs** on dependency modules map each `name` to physical storage (relations, Redis prefixes, tx routes). Use with [`forze-domain-aggregates`](forze-domain-aggregates/SKILL.md) for spec fields and [`forze-wiring`](forze-wiring/SKILL.md) for runtime setup.

## Same `name` everywhere

Use **identical** strings for:

- `DocumentSpec(name=...)`, `SearchSpec(name=...)`, `CacheSpec(name=...)`, `CounterSpec(name=...)`, …
- Keys in `PostgresDepsModule.rw_documents` / `ro_documents` / `searches`, `MongoDepsModule` maps, `RedisDepsModule.caches` / `counters`, etc.

`ExecutionContext` resolves routed factories using `spec.name` as the route.

## DocumentSpec vs Postgres / Mongo

`DocumentSpec` has **no** SQL table or Mongo collection fields. Supply tuples like `("schema", "table")` or `(database, collection)` in `PostgresDocumentConfig` / `MongoDocumentConfig` under the aggregate’s `name`:

```python
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

pg_module = PostgresDepsModule(
    client=pg_client,
    rw_documents={
        "projects": {
            "read": ("public", "projects"),
            "write": ("public", "projects"),
            "bookkeeping_strategy": "database",
            "history": ("public", "projects_history"),
        },
    },
    tx={"default"},
)
```

## Redis cache namespace

`CacheSpec(name="projects", ...)` must match the key in `RedisDepsModule.caches`:

```python
redis_module = RedisDepsModule(
    client=redis_client,
    caches={"projects": {"namespace": "app:projects"}},
)
```

## Transaction routes

Register routes on the backend module (e.g. `PostgresDepsModule(tx={"default"})`). Application code uses `async with ctx.transaction("default"):` and `UsecasePlan().tx(..., route="default")`.

## Gotchas

- Mismatch between `spec.name` and infra dict keys is a frequent wiring bug — grep both sides when debugging “dependency not registered”.
- Enable `history_enabled` on the **spec** when you want history semantics; the **relation** still comes from infra (`history` on Postgres/Mongo config).

## Reference

- [`pages/docs/core-concepts/specs-and-wiring.md`](../../pages/docs/core-concepts/specs-and-wiring.md)
- [`pages/docs/integrations/postgres.md`](../../pages/docs/integrations/postgres.md)
- [`pages/docs/integrations/mongo.md`](../../pages/docs/integrations/mongo.md)
- [`pages/docs/integrations/redis.md`](../../pages/docs/integrations/redis.md)
