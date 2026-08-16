# Spec to backend config

Mapping a logical spec name onto a real backend — Postgres and Mongo relations, Redis caches, counters and locks, and storage, queue and workflow routes. The specs themselves stay logical: [document and search specs](document-spec.md).

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

from forze_postgres import PostgresDepsModule, PostgresDocumentConfig

pg_module = PostgresDepsModule(
    client=pg_client,
    rw_documents={
        ResourceName.PROJECTS: PostgresDocumentConfig(
            read=("public", "projects"),
            write=("public", "projects"),
            bookkeeping_strategy="database",
            history=("public", "projects_history"),
        ),
    },
    tx={TxRoute.DEFAULT},
)
```

The same spec routes to Mongo or Firestore by swapping the deps module — the spec above does not change, which is the whole point of it naming nothing physical. Relations are `(database, collection)` pairs rather than `(schema, table)`:

```python
from forze_firestore import (
    FirestoreDepsModule,
    FirestoreDocumentConfig,
    FirestoreReadOnlyDocumentConfig,
)
from forze_mongo import MongoDepsModule, MongoDocumentConfig

mongo_module = MongoDepsModule(
    client=mongo_client,
    rw_documents={
        ResourceName.PROJECTS: MongoDocumentConfig(
            read=("app", "projects"),
            write=("app", "projects"),
            history=("app", "projects_history"),
        ),
    },
    tx={TxRoute.DEFAULT},
)

firestore_module = FirestoreDepsModule(
    client=firestore_client,
    # Firestore splits the two directions: a read-only route is its own config type rather
    # than a flag, so a spec wired read-only cannot acquire a write port by accident.
    rw_documents={ResourceName.PROJECTS: FirestoreDocumentConfig(read="projects", write="projects")},
    ro_documents={ResourceName.AUDIT: FirestoreReadOnlyDocumentConfig(read="audit_events")},
)
```

Two differences that are not inferable from the Postgres example. Firestore splits read-only from read-write at the **type** level, so a read-only route cannot acquire a write port by wiring alone. And Mongo sorts nulls smallest natively: a sort that asks for the other placement is **refused** with `query_feature_unsupported` unless you set `computed_null_ordering=True`, which sorts through an aggregation pipeline instead — a computed key no index can serve, so Mongo sorts in memory. The same spec and the same query DSL, one backend answering and the other refusing.

## Redis cache, counters, locks, and idempotency

`CacheSpec(name=ResourceName.PROJECTS, ...)` must match the key in `RedisDepsModule.caches`. Use the same naming style for counters, distributed locks, idempotency routes, and search result snapshots:

```python
from forze_redis import (
    RedisCacheConfig,
    RedisCounterConfig,
    RedisDepsModule,
    RedisDistributedLockConfig,
)

redis_module = RedisDepsModule(
    client=redis_client,
    caches={ResourceName.PROJECTS: RedisCacheConfig(namespace="app:projects")},
    counters={ResourceName.PROJECTS: RedisCounterConfig(namespace="app:projects:counter")},
    dlocks={ResourceName.PROJECTS: RedisDistributedLockConfig(namespace="app:projects:locks")},
)
```

For FastAPI idempotency, either register a plain `idempotency` config or use a routed map whose key matches the `IdempotencySpec.name` / endpoint feature route.

**Counters are not Redis-only.** Postgres, Mongo and Firestore also implement `CounterPort` — map the same `CounterSpec.name` through that module's `counters={...}` instead, which keeps sequences next to the data rather than adding Redis for one plane. Postgres needs an app-migrated counter table. Whichever backend you pick, an allocation runs on its own connection and **never joins the caller's transaction**: a rollback does not hand the number back.

## Storage, queue, and workflow routes

```python
from forze.application.contracts.queue import QueueSpec
from forze.application.contracts.storage import StorageSpec
from forze.application.contracts.durable.workflow import DurableWorkflowSpec
from forze.base.serialization import PydanticModelCodec
from forze_s3 import S3DepsModule, S3StorageConfig
from forze_sqs import SQSDepsModule, SQSQueueConfig
from forze_temporal import TemporalDepsModule, TemporalWorkflowConfig

attachments = StorageSpec(name=ResourceName.PROJECT_ATTACHMENTS)
orders = QueueSpec(
    name=ResourceName.ORDERS,
    codec=PydanticModelCodec(OrderPayload),
)
workflow_spec: DurableWorkflowSpec[StartOrderIn, OrderResult] = ...

s3_module = S3DepsModule(
    client=s3_client,
    storages={ResourceName.PROJECT_ATTACHMENTS: S3StorageConfig(bucket="project-files")},
)
sqs_module = SQSDepsModule(
    client=sqs_client,
    queue_readers={ResourceName.ORDERS: SQSQueueConfig(namespace="app")},
    queue_writers={ResourceName.ORDERS: SQSQueueConfig(namespace="app")},
)
temporal_module = TemporalDepsModule(
    client=temporal_client,
    workflows={workflow_spec.name: TemporalWorkflowConfig(queue="orders")},
)
```

`GCSDepsModule` takes the same `storages={...}` map as `S3DepsModule` shown here — for either backend, `<Module>(client=...)` alone registers no storage route (see [object storage](object-storage.md)).

## Anti-patterns

- **Constructing a deps module with `client=` and nothing else.** `S3DepsModule(client=...)`, `SQSDepsModule(client=...)` and `TemporalDepsModule(client=...)` register the client key only. Without the routed map (`storages=`, `queue_readers=`/`queue_writers=`, `workflows=`) the spec resolves to nothing and the failure surfaces at call time as an unregistered dependency, far from the wiring that caused it.
- **Allocating a counter inside a transaction you expect to roll back.** Allocation runs on its own connection on every backend, so a rollback does not return the number. Code that treats a counter as transactional silently produces gaps — or worse, reuses a value it believed was released.

## Reference

- [Postgres integration](https://morzecrew.github.io/forze/latest/integrations/postgres/)
- [Mongo integration](https://morzecrew.github.io/forze/latest/integrations/mongo/)
- [Redis integration](https://morzecrew.github.io/forze/latest/integrations/redis/)
