# MongoDB Integration

`forze_mongo` provides document storage and transaction management backed by MongoDB. It implements `DocumentQueryPort`, `DocumentCommandPort`, and `TxManagerPort` using async `pymongo`.

Kernel `DocumentSpec` names must match keys in `MongoDepsModule.rw_documents` / `ro_documents`. See [Specs and infrastructure wiring](../core-concepts/specs-and-wiring.md).

## Installation

    :::bash
    uv add 'forze[mongo]'

## Runtime wiring

    :::python
    from forze.application.execution import DepsPlan, ExecutionRuntime, LifecyclePlan
    from forze_mongo import MongoClient, MongoConfig, MongoDepsModule, mongo_lifecycle_step

    client = MongoClient()
    module = MongoDepsModule(
        client=client,
        rw_documents={
            "projects": {
                "read": ("app", "projects"),
                "write": ("app", "projects"),
                "history": ("app", "projects_history"),
            },
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

### What gets registered

| Key | Capability |
|-----|------------|
| `MongoClientDepKey` | Motor / async Mongo client |
| `DocumentQueryDepKey` | Routed document query factories |
| `DocumentCommandDepKey` | Routed document command factories |
| `TxManagerDepKey` | Transaction managers per route in `tx` |

## DocumentSpec and Mongo config

`DocumentSpec` carries model types, `history_enabled`, and optional `CacheSpec`. Per-database mapping uses `MongoDocumentConfig`:

| Field | Purpose |
|-------|---------|
| `read` | `(database, collection)` for reads |
| `write` | `(database, collection)` for writes |
| `history` | Optional `(database, collection)` for snapshots |
| `batch_size` | Optional write batch size |
| `tenant_aware` | Optional tenant field handling |

    :::python
    from forze.application.contracts.document import DocumentSpec
    from forze.domain.mixins import SoftDeletionMixin
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
    doc_q = ctx.doc_query(project_spec)
    doc_c = ctx.doc_command(project_spec)

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
                {"$fields": {"is_deleted": False}},
                {"$fields": {"title": {"$neq": ""}}},
            ]
        },
        sorts={"created_at": "desc"},
        pagination={"limit": 20, "offset": 0},
        return_count=True,
    )
    projects = page.hits
    total = page.count

    count = await doc_q.count({"$fields": {"is_deleted": False}})

See [Query Syntax](../core-package/query-syntax.md).

### Mongo-specific behavior

- `$null: true` matches explicit `null` and missing fields
- Array operators map to MongoDB operators
- Use `id` in sort expressions; the adapter maps it to MongoDB `_id`. If
  `sorts` is omitted, this layer does not pass an explicit sort to MongoDB.

## Transactions

MongoDB transactions require a replica set or sharded cluster. Within `ctx.transaction("default")`, operations share a session when using the registered tx route.

    :::python
    async with ctx.transaction("default"):
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

## Differences from Postgres

| Aspect | Postgres | MongoDB |
|--------|----------|---------|
| Config | `(schema, table)` tuples | `(database, collection)` tuples |
| Search in box | `SearchSpec` + `PostgresSearchConfig` | Not bundled — use Atlas Search or external search |
| Transactions | Always available on server | Requires replica set for multi-doc tx |
| Rev / history | `bookkeeping_strategy` + optional triggers | Application-managed |
