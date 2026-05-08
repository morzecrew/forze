---
name: forze-specs-infrastructure
description: >-
  Maps kernel specs (DocumentSpec, SearchSpec, CacheSpec, CounterSpec) to
  integration configs on PostgresDepsModule, MongoDepsModule, RedisDepsModule,
  S3DepsModule, queue/workflow modules, and transaction routes. Use when wiring
  logical StrEnum spec names to tables, collections, namespaces, buckets, queues,
  or deps-module routes.
---

# Forze specs and infrastructure wiring

Kernel **specs** declare model types and logical `name`. Integration **configs** on dependency modules map each `name` to physical infrastructure (relations, collections, Redis namespaces, buckets, queues, task queues, transaction routes). Use with [`forze-domain-aggregates`](../forze-domain-aggregates/SKILL.md) for spec fields, [`forze-deps-modules`](../forze-deps-modules/SKILL.md) for custom modules, and [`forze-wiring`](../forze-wiring/SKILL.md) for runtime setup.

## Prefer `StrEnum` names

Use a shared `StrEnum` for spec names and dependency routes. `BaseSpec.name` and built-in deps modules accept `str | StrEnum`, so enum values keep application specs, deps-module maps, and transaction routes aligned during refactors.

```python
from enum import StrEnum


class ResourceName(StrEnum):
    PROJECTS = "projects"
    PROJECT_ATTACHMENTS = "project-attachments"
    ORDERS = "orders"


class TxRoute(StrEnum):
    DEFAULT = "default"
```

Use enum members consistently for:

- `DocumentSpec(name=...)`, `SearchSpec(name=...)`, `CacheSpec(name=...)`, `CounterSpec(name=...)`, …
- keys in `PostgresDepsModule.rw_documents` / `ro_documents` / `searches`, `MongoDepsModule` maps, `RedisDepsModule.caches` / `counters` / `dlocks`, `S3DepsModule.storages`, `SQSDepsModule.queue_readers` / `queue_writers`, `RabbitMQDepsModule.queue_readers` / `queue_writers`, `TemporalDepsModule.workflows`, etc.
- transaction route sets such as `PostgresDepsModule(tx={TxRoute.DEFAULT})`

`ExecutionContext` resolves routed factories using `spec.name` as the route.

## DocumentSpec vs Postgres / Mongo

`DocumentSpec` has **no** SQL table or Mongo collection fields. Supply tuples like `("schema", "table")` or `(database, collection)` in `PostgresDocumentConfig` / `MongoDocumentConfig` under the aggregate’s `name`:

```python
from datetime import timedelta

from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.document import DocumentSpec

project_spec = DocumentSpec(
    name=ResourceName.PROJECTS,
    read=ProjectReadModel,
    write={
        "domain": Project,
        "create_cmd": CreateProjectCmd,
        "update_cmd": UpdateProjectCmd,
    },
    cache=CacheSpec(name=ResourceName.PROJECTS, ttl=timedelta(minutes=5)),
)

pg_module = PostgresDepsModule(
    client=pg_client,
    rw_documents={
        ResourceName.PROJECTS: {
            "read": ("public", "projects"),
            "write": ("public", "projects"),
            "bookkeeping_strategy": "database",
            "history": ("public", "projects_history"),
        },
    },
    tx={TxRoute.DEFAULT},
)
```

## Redis cache, counters, locks, and idempotency

`CacheSpec(name=ResourceName.PROJECTS, ...)` must match the key in `RedisDepsModule.caches`. Use the same naming style for counters, distributed locks, idempotency routes, and search result snapshots:

```python
redis_module = RedisDepsModule(
    client=redis_client,
    caches={ResourceName.PROJECTS: {"namespace": "app:projects"}},
    counters={ResourceName.PROJECTS: {"namespace": "app:projects:counter"}},
    dlocks={ResourceName.PROJECTS: {"namespace": "app:projects:locks"}},
)
```

For FastAPI idempotency, either register a plain `idempotency` config or use a routed map whose key matches the `IdempotencySpec.name` / endpoint feature route.

## Storage, queue, and workflow routes

```python
from forze.application.contracts.queue import QueueSpec
from forze.application.contracts.storage import StorageSpec
from forze.application.contracts.workflow import WorkflowSpec

attachments = StorageSpec(name=ResourceName.PROJECT_ATTACHMENTS)
orders = QueueSpec(name=ResourceName.ORDERS, model=OrderPayload)
workflow_spec: WorkflowSpec[StartOrderIn, OrderResult] = ...

s3_module = S3DepsModule(
    client=s3_client,
    storages={ResourceName.PROJECT_ATTACHMENTS: {"bucket": "project-files"}},
)
sqs_module = SQSDepsModule(
    client=sqs_client,
    queue_readers={ResourceName.ORDERS: {"namespace": "app"}},
    queue_writers={ResourceName.ORDERS: {"namespace": "app"}},
)
temporal_module = TemporalDepsModule(
    client=temporal_client,
    workflows={workflow_spec.name: {"queue": "orders"}},
)
```

## Transaction routes

Register routes on the backend module (e.g. `PostgresDepsModule(tx={TxRoute.DEFAULT})`). Application code uses `async with ctx.transaction(TxRoute.DEFAULT):` and `UsecasePlan().tx(..., route=TxRoute.DEFAULT)`.

## Gotchas

- Mismatch between `spec.name` and infra dict keys is a frequent wiring bug — check the spec enum and deps-module map when debugging “dependency not registered”.
- Do not mix plain strings and enum members casually in new code. Equality works by value, but shared enums make missing routes easier to catch in review.
- Enable `history_enabled` on the **spec** when you want history semantics; the **relation** still comes from infra (`history` on Postgres/Mongo config).
- `S3DepsModule(client=...)`, `SQSDepsModule(client=...)`, and `TemporalDepsModule(client=...)` register only client keys unless their routed maps are populated.

## Reference

- [`pages/docs/core-concepts/specs-and-wiring.md`](../../pages/docs/core-concepts/specs-and-wiring.md)
- [`pages/docs/integrations/postgres.md`](../../pages/docs/integrations/postgres.md)
- [`pages/docs/integrations/mongo.md`](../../pages/docs/integrations/mongo.md)
- [`pages/docs/integrations/redis.md`](../../pages/docs/integrations/redis.md)
- [`pages/docs/integrations/s3.md`](../../pages/docs/integrations/s3.md)
- [`pages/docs/integrations/sqs.md`](../../pages/docs/integrations/sqs.md)
- [`pages/docs/integrations/rabbitmq.md`](../../pages/docs/integrations/rabbitmq.md)
- [`pages/docs/integrations/temporal.md`](../../pages/docs/integrations/temporal.md)
