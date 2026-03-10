# Core Package

This section explains what the `forze` core package provides and how to use it in day-to-day application code, before choosing specific infrastructure integrations.

## What core gives you

- **Contracts**: protocol interfaces for documents, search, storage, cache, transactions, queues, pub/sub, streams, idempotency, and workflows
- **Specifications**: `DocumentSpec`, `SearchSpec`, `QueueSpec`, `PubSubSpec`, `StreamSpec`, and `CacheSpec` that describe your aggregates and messaging
- **Execution model**: `ExecutionContext`, `ExecutionRuntime` for dependency resolution and lifecycle management
- **Usecase composition**: registries, plans, facades, and middleware for building and wiring operations
- **Domain primitives**: `Document`, `BaseDTO`, mixins, validation, and shared constants
- **Shared query DSL**: filters, sorts, and operators that work across all adapters

## Practical workflow

1. Define your aggregate domain model and DTOs
2. Declare a `DocumentSpec` (and optional `SearchSpec`) as the contract for adapters
3. Build runtime and dependency plans using integration modules
4. Resolve ports from `ExecutionContext` in usecases
5. Pass filters and sorts using the shared query syntax

    :::python
    from forze.application.contracts.document import DocumentSpec
    from forze.application.contracts.search import SearchSpec

    project_spec = DocumentSpec(
        namespace="projects",
        read={"source": "public.projects", "model": ProjectReadModel},
        write={
            "source": "public.projects",
            "models": {
                "domain": Project,
                "create_cmd": CreateProjectCmd,
                "update_cmd": UpdateProjectCmd,
            },
        },
        history={"source": "public.projects_history"},
        cache={"enabled": True},
    )

    project_search_spec = SearchSpec(
        namespace="projects",
        model=ProjectReadModel,
        indexes={
            "public.idx_projects_title": {
                "source": "public.projects",
                "fields": [{"path": "title"}],
            },
        },
        default_index="public.idx_projects_title",
    )

## In-use examples

Once you have an execution context, all operations stay contract-driven:

    :::python
    doc = ctx.doc_read(project_spec)
    rows, total = await doc.find_many(
        filters={"$fields": {"is_deleted": False}},
        sorts={"created_at": "desc"},
        limit=20,
        offset=0,
    )

    search = ctx.search(project_search_spec)
    hits, total = await search.search(
        query="roadmap",
        filters={"$fields": {"is_deleted": False}},
        limit=20,
        offset=0,
    )

    doc_w = ctx.doc_write(project_spec)
    created = await doc_w.create(CreateProjectCmd(title="New", description="..."))

## Read next

- [Query Syntax](query-syntax.md): full filter and sort DSL reference
- [Core Concepts](../core-concepts/index.md): architecture, layers, and execution model
- Integration guides: [PostgreSQL](../integrations/postgres.md) | [MongoDB](../integrations/mongo.md) | [Redis](../integrations/redis.md)
