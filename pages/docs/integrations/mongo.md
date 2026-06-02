# MongoDB Integration

## What this integration provides

Persist documents and transactions in MongoDB behind Forze document contracts.

## When to use it

Use this when MongoDB is your document store or when you need collection-oriented persistence for aggregates.

## Standard setup checklist

1. Install the matching optional extra.
2. Create the integration client or module configuration.
3. Register the module in `DepsPlan` with routes that match your specs.
4. Add lifecycle steps when the integration opens network connections.
5. Resolve ports from `ExecutionContext`; do not import adapters in handlers.


`forze_mongo` provides document storage and transaction management backed by MongoDB. It implements `DocumentQueryPort`, `DocumentCommandPort`, and `TxManagerPort` using async `pymongo`.

Kernel `DocumentSpec` names must match keys in `MongoDepsModule.rw_documents` / `ro_documents`. See [Specs and infrastructure wiring](../concepts/specs-and-wiring.md).

## Installation

    :::bash
    uv add 'forze[mongo]'

## Runtime wiring

    :::python
    from forze.application.execution import DepsPlan, ExecutionRuntime, LifecyclePlan
    from forze_mongo import (
        MongoClient,
        MongoConfig,
        MongoDepsModule,
        MongoDocumentConfig,
        mongo_lifecycle_step,
    )

    client = MongoClient()
    module = MongoDepsModule(
        client=client,
        rw_documents={
            "projects": MongoDocumentConfig(
                read=("app", "projects"),
                write=("app", "projects"),
                history=("app", "projects_history"),
            ),
        },
        tx={"default"},
    )

    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(module),
        lifecycle=LifecyclePlan.from_steps(
            mongo_lifecycle_step(
                uri="mongodb://localhost:27017",
                db_name="app",
                config=MongoConfig(max_pool_size=100, min_pool_size=5),
            )
        ),
    )

### MongoConfig options

| Option | Type | Default | Purpose |
|--------|------|---------|---------|
| `max_pool_size` | `int` | `100` | Maximum connections in the pool |
| `min_pool_size` | `int` | `0` | Minimum connections in the pool |

#### `read_validation` (read throughput)

`MongoReadOnlyDocumentConfig` and `MongoDocumentConfig` accept `read_validation`:

| Value | Behavior |
|-------|----------|
| `"strict"` (default) | Full Pydantic validation on every document returned from reads. |
| `"trusted"` | Build read models with `model_construct` when stored fields match the read model (no validator run). |

Use `"trusted"` only when collection fields match `DocumentSpec.read` and the driver already returns correct Python types. Extra fields not on the read model raise a precondition error. History blobs, cache payloads, and write paths stay strict.

### What gets registered

| Key | Capability |
|-----|------------|
| `MongoClientDepKey` | Motor / async Mongo client |
| `DocumentQueryDepKey` | Routed document query factories |
| `DocumentCommandDepKey` | Routed document command factories |
| `TxManagerDepKey` | Transaction managers per route in `tx` |
| `OutboxCommandDepKey` / `OutboxQueryDepKey` | Transactional outbox per route in `outboxes` |

Example outbox route:

    :::python
    from forze_mongo.execution.deps.configs import MongoOutboxConfig

    MongoDepsModule(
        client=mongo_client,
        tx={"default"},
        outboxes={
            "events": MongoOutboxConfig(collection=("app", "outbox")),
        },
    )

See [Outbox contracts](../core-package/contracts/outbox.md) and [Transactional outbox recipe](../recipes/transactional-outbox.md). Flush requires a **replica set** (same as other Mongo transactions).

For framework tests or advanced wiring, prefer `from forze_mongo.execution.deps import ConfigurableMongoDocument` and `ConfigurableMongoSearch` rather than removed `forze_mongo.execution.deps.deps` paths.

## DocumentSpec and Mongo config

`DocumentSpec` carries model types, `history_enabled`, and optional `CacheSpec`. Per-database mapping uses `MongoDocumentConfig`:

| Field | Purpose |
|-------|---------|
| `read` | `(database, collection)` for reads |
| `write` | `(database, collection)` for writes |
| `history` | Optional `(database, collection)` for snapshots |
| `batch_size` | Optional write batch size |
| `tenant_aware` | Optional tenant field handling |

### Ensure / upsert and unique indexes

`ensure`, `ensure_many`, `upsert`, and `upsert_many` are **idempotent by document `id`** (stored as MongoDB `_id`). The adapter uses `update` + `$setOnInsert` + `upsert` filtered on `_id` (and tenant when `tenant_aware=True`).

- When a document with the same `id` already exists, the insert half of the upsert is skipped and the existing row is returned (or updated for `upsert`).
- When the `id` is new but a **secondary unique index** (for example on `email`) would be violated, MongoDB returns a duplicate-key error mapped to `CoreException.conflict`.
- There is no Postgres-style `conflict_target` on `MongoDocumentConfig`; logical identity is always `_id`.

Optional startup validation (warn only on secondary unique indexes):

    :::python
    from forze_mongo import (
        mongo_document_index_spec_for_binding,
        mongo_document_index_validation_lifecycle_step,
    )

    index_specs = [
        mongo_document_index_spec_for_binding("projects", spec=project_spec, config=project_config),
    ]
    # Add mongo_document_index_validation_lifecycle_step(specs=index_specs) to LifecyclePlan
    # after mongo_lifecycle_step.

DocumentSpec example:

    :::python
    from forze.application.contracts.document import DocumentSpec
    from forze_kits.domain.soft_deletion import SoftDeletionMixin
    from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument


    class Project(SoftDeletionMixin, Document):
        title: str
        is_deleted: bool = False


    class CreateProjectCmd(CreateDocumentCmd):
        title: str


    class UpdateProjectCmd(BaseDTO):
        title: str | None = None


    class ProjectReadModel(ReadDocument):
        title: str
        is_deleted: bool = False


    project_spec = DocumentSpec(
        name="projects",
        read=ProjectReadModel,
        write={
            "domain": Project,
            "create_cmd": CreateProjectCmd,
            "update_cmd": UpdateProjectCmd,
        },
        history_enabled=True,
    )

The `"projects"` key must match `rw_documents["projects"]` in `MongoDepsModule`.

## Document operations

    :::python
    doc_q = ctx.document.query(project_spec)
    doc_c = ctx.document.command(project_spec)

    created = await doc_c.create(CreateProjectCmd(title="Alpha"))
    fetched = await doc_q.get(created.id)
    updated = await doc_c.update(
        created.id,
        created.rev,
        UpdateProjectCmd(title="Beta"),
    )
    touched = await doc_c.touch(updated.id)

    deleted = await doc_c.delete(touched.id, touched.rev)
    restored = await doc_c.restore(deleted.id, deleted.rev)
    await doc_c.kill(restored.id)

### Batch operations

    :::python
    created_many = await doc_c.create_many([
        CreateProjectCmd(title="Project A"),
        CreateProjectCmd(title="Project B"),
    ])

## Query and filter behavior

The Mongo adapter uses the shared query DSL:

    :::python
    page = await doc_q.find_many(
        filters={
            "$and": [
                {"$values": {"is_deleted": False}},
                {"$values": {"title": {"$neq": ""}}},
            ]
        },
        sorts={"created_at": "desc"},
        pagination={"limit": 20, "offset": 0},
        return_count=True,
    )
    projects = page.hits
    total = page.count

    count = await doc_q.count({"$values": {"is_deleted": False}})

See [Query Syntax](../reference/query-syntax.md).

### Mongo-specific behavior

- `$null: true` matches explicit `null` and missing fields
- Array operators map to MongoDB operators
- Use `id` in sort expressions; the adapter maps it to MongoDB `_id`. If
  `sorts` is omitted, this layer does not pass an explicit sort to MongoDB.

## Transactions

MongoDB transactions require a replica set or sharded cluster. Within `ctx.tx_ctx.scope("default")`, operations share a session when using the registered tx route.

    :::python
    async with ctx.tx_ctx.scope("default"):
        await doc_c.create(CreateProjectCmd(title="In transaction"))
        existing = await doc_q.get(existing_id)
        await doc_c.update(existing.id, existing.rev, UpdateProjectCmd(title="Also in tx"))

## Revision and history

The adapter manages `rev` in application space: fetch, validate patch, increment `rev`, and write. When `history_enabled` and a `history` collection are configured, snapshots are stored after updates.

## Combining with Redis

    :::python
    deps_plan = DepsPlan.from_modules(
        lambda: Deps.merge(
            MongoDepsModule(client=mongo, rw_documents={...})(),
            RedisDepsModule(
                client=redis,
                caches={"projects": {"namespace": "app:projects"}},
            )(),
        ),
    )

Enable caching on the kernel side:

    :::python
    from datetime import timedelta
    from forze.application.contracts.cache import CacheSpec

    project_spec = DocumentSpec(
        name="projects",
        read=ProjectReadModel,
        write={...},
        cache=CacheSpec(name="projects", ttl=timedelta(minutes=5)),
    )

The `CacheSpec.name` must match a key in `RedisDepsModule.caches`.

## Search

Register `SearchSpec.name` in `MongoDepsModule.searches` with a `MongoSearchConfig` (`engine`: `text`, `atlas`, or `vector`). Resolve at runtime with `ctx.search_query(spec)` — same port surface as Postgres (`search`, `search_page`, `*_cursor`, projections, snapshots).

    :::python
    from forze.application.contracts.search import SearchSpec
    from forze_mongo import MongoDepsModule, MongoSearchConfig

    project_search = SearchSpec(
        name="projects",
        model_type=ProjectRead,
        fields=("title", "description"),
    )

    module = MongoDepsModule(
        client=mongo,
        rw_documents={...},
        searches={
            "projects": MongoSearchConfig(
                read=("app", "projects"),
                engine="atlas",
                index_name="default",  # required for atlas and vector
            ),
        },
    )

`index_name` is the physical Atlas Search or Vector Search index name when using those engines; it is optional for `text` (queries use the collection text index via `$text`, not a named index in the aggregation).

| Engine | MongoDB feature | `index_name` |
|--------|-----------------|--------------|
| `text` | Compound text index + `$text` | Optional (not passed to `$text`) |
| `atlas` | Atlas Search `$search` stage | Required |
| `vector` | `$vectorSearch` | Required (plus `vector_path`, embeddings deps) |

For local Atlas Search / vector development and CI, use the [`mongodb/mongodb-atlas-local`](https://hub.docker.com/r/mongodb/mongodb-atlas-local) image (`mongod` + `mongot`). Integration tests marked `mongo_atlas_search` exercise `atlas` and `vector` engines.

## Differences from Postgres

| Aspect | Postgres | MongoDB |
|--------|----------|---------|
| Config | `(schema, table)` tuples | `(database, collection)` tuples |
| Search in box | `SearchSpec` + `PostgresSearchConfig` | `SearchSpec` + `MongoSearchConfig` (`text` / `atlas` / `vector`) |
| Hub / federated search | Supported | Not bundled in `forze_mongo` yet |
| Transactions | Always available on server | Requires replica set for multi-doc tx |
| Rev / history | `bookkeeping_strategy` + optional triggers | Application-managed |
