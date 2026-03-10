# MongoDB Integration

`forze_mongo` provides document storage and transaction management backed by MongoDB. It implements `DocumentReadPort`, `DocumentWritePort`, and `TxManagerPort` using the async `pymongo` (Motor) driver.

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
        rev_bump_strategy="application",
        history_write_strategy="application",
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

### DepsModule options

| Option | Type | Values | Purpose |
|--------|------|--------|---------|
| `rev_bump_strategy` | `str` | `"application"` | Who increments `rev` on update |
| `history_write_strategy` | `str` | `"application"` | Who writes to the history collection |

Both strategies are currently application-managed in the Mongo integration. The adapter handles revision bumping and history writes in application code.

### What gets registered

| Key | Capability |
|-----|------------|
| `MongoClientDepKey` | Raw Mongo client for direct queries |
| `DocumentReadDepKey` | Document read adapter factory |
| `DocumentWriteDepKey` | Document write adapter factory |
| `TxManagerDepKey` | Transaction manager adapter |

## Document models and specification

Define your models the same way as for Postgres. The only difference is the source naming: MongoDB uses collection names without a schema prefix.

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
        namespace="projects",
        read={"source": "projects", "model": ProjectReadModel},
        write={
            "source": "projects",
            "models": {
                "domain": Project,
                "create_cmd": CreateProjectCmd,
                "update_cmd": UpdateProjectCmd,
            },
        },
        history={"source": "projects_history"},
    )

Note that MongoDB sources use plain collection names (e.g. `"projects"`) rather than `schema.table` format.

## Document operations

Resolve document ports from the execution context identically to the Postgres integration:

    :::python
    doc_read = ctx.doc_read(project_spec)
    doc_write = ctx.doc_write(project_spec)

    # Create
    created = await doc_write.create(CreateProjectCmd(title="Alpha"))

    # Read
    fetched = await doc_read.get(created.id)

    # Update with optimistic concurrency
    updated = await doc_write.update(
        created.id,
        UpdateProjectCmd(title="Beta"),
        rev=created.rev,
    )

    # Touch (bump last_update_at)
    touched = await doc_write.touch(created.id)

    # Soft delete / restore
    await doc_write.delete(created.id)
    await doc_write.restore(created.id)

    # Hard delete
    await doc_write.kill(created.id)

### Batch operations

All write operations have batch variants:

    :::python
    created_many = await doc_write.create_many([
        CreateProjectCmd(title="Project A"),
        CreateProjectCmd(title="Project B"),
    ])

## Query and filter behavior

The Mongo adapter uses the same shared query DSL as the Postgres adapter. Filters and sorts use the same expression syntax:

    :::python
    projects, total = await doc_read.find_many(
        filters={
            "$and": [
                {"$fields": {"is_deleted": False}},
                {"$fields": {"title": {"$neq": ""}}},
            ]
        },
        sorts={"created_at": "desc"},
        limit=20,
        offset=0,
    )

    count = await doc_read.count({"$fields": {"is_deleted": False}})

The query DSL is rendered into MongoDB query syntax by the adapter's query renderer. See [Query Syntax](../core-package/query-syntax.md) for the full filter and sort reference.

### Mongo-specific behavior

- `$null: true` matches both explicit `null` values and missing fields
- Array operators (`$superset`, `$subset`, `$overlaps`, `$disjoint`) map to MongoDB array query operators
- Sorting defaults to `_id` descending when no sorts are specified

## Transactions

MongoDB transactions require a replica set or sharded cluster. The Mongo adapter uses `pymongo` sessions for transaction management.

    :::python
    async with ctx.transaction():
        await doc_write.create(CreateProjectCmd(title="In transaction"))
        await doc_write.update(existing_id, UpdateProjectCmd(title="Also in tx"))

Within a transaction scope, all document operations share the same MongoDB session. Nested `transaction()` calls are tracked by the execution context for consistent scope management.

## Revision and history

### Revision strategy

The Mongo adapter increments `rev` in application code before writing the update. On each update:

1. The current document is fetched
2. Domain `update()` validates the patch and computes the diff
3. `rev` is incremented and included in the MongoDB `$set` operation
4. If `rev` was provided by the caller, the update query includes a `rev` filter for optimistic concurrency

### History strategy

When history is configured, the adapter inserts a snapshot into the history collection after each update:

    :::json
    {
        "source": "projects",
        "id": "uuid-value",
        "rev": 1,
        "data": { /* full document snapshot */ }
    }

## Combining with Redis

Add Redis for caching, counters, and idempotency:

    :::python
    deps_plan = DepsPlan.from_modules(
        lambda: Deps.merge(
            MongoDepsModule(client=mongo, rev_bump_strategy="application", history_write_strategy="application")(),
            RedisDepsModule(client=redis)(),
        ),
    )

    lifecycle = LifecyclePlan.from_steps(
        mongo_lifecycle_step(uri="mongodb://localhost:27017", db_name="app", config=MongoConfig()),
        redis_lifecycle_step(dsn="redis://localhost:6379/0", config=RedisConfig()),
    )

With both modules, enable caching in your document spec:

    :::python
    project_spec = DocumentSpec(
        namespace="projects",
        read={"source": "projects", "model": ProjectReadModel},
        write={
            "source": "projects",
            "models": {"domain": Project, "create_cmd": CreateProjectCmd, "update_cmd": UpdateProjectCmd},
        },
        cache={"enabled": True},
    )

## Differences from Postgres integration

| Aspect | Postgres | MongoDB |
|--------|----------|---------|
| Source format | `schema.table` | Collection name |
| Rev bump | Database trigger or application | Application only |
| History write | Database trigger or application | Application only |
| Full-text search | PGroonga or native FTS adapter | Not provided (use MongoDB Atlas Search externally) |
| Transactions | Always available | Requires replica set |
