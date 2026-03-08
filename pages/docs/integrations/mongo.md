# MongoDB Integration

This guide explains how to use `forze_mongo` for document storage and transactions.

## Prerequisites

- `forze[mongo]` installed
- MongoDB running and reachable

## What this integration provides

`MongoDepsModule` registers:

| Dependency key | Capability |
|----------------|------------|
| `MongoClientDepKey` | low-level Mongo client |
| `TxManagerDepKey` | Mongo transaction manager |
| `DocumentDepKey` | Mongo document adapter factory |

## Runtime wiring

    :::python
    from forze.application.execution import DepsPlan, ExecutionRuntime, LifecyclePlan
    from forze_mongo import MongoClient, MongoConfig, MongoDepsModule, mongo_lifecycle_step

    mongo_client = MongoClient()
    mongo_module = MongoDepsModule(
        client=mongo_client,
        rev_bump_strategy="application",
        history_write_strategy="application",
    )

    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(mongo_module),
        lifecycle=LifecyclePlan.from_steps(
            mongo_lifecycle_step(
                uri="mongodb://localhost:27017",
                db_name="app",
                config=MongoConfig(max_pool_size=100, min_pool_size=5),
            )
        ),
    )

## Define document models and spec

    :::python
    from forze.application.contracts.document import DocumentSpec
    from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

    class Project(Document):
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
        sources={
            "read": "projects",
            "write": "projects",
            "history": "projects_history",
        },
        models={
            "domain": Project,
            "read": ProjectReadModel,
            "create_cmd": CreateProjectCmd,
            "update_cmd": UpdateProjectCmd,
        },
    )

## Use document port from context

    :::python
    adapter = ctx.doc(project_spec)

    created = await adapter.create(CreateProjectCmd(title="alpha"))
    fetched = await adapter.get(created.id)
    updated = await adapter.update(created.id, UpdateProjectCmd(title="beta"), rev=created.rev)
    touched = await adapter.touch(created.id)
    deleted = await adapter.delete(created.id)
    restored = await adapter.restore(created.id)
    await adapter.kill(created.id)

## Query/filter behavior

Mongo adapter uses the shared query DSL contracts, so filtering/sorting semantics are aligned with other adapters where possible.

    :::python
    docs, total = await adapter.find_many(limit=20, offset=0)
    assert total >= 0

## Revision and history strategies

`MongoDepsModule` exposes:

- `rev_bump_strategy` (default `"application"`)
- `history_write_strategy` (default `"application"`)

Both are currently application-managed in Mongo integration.
