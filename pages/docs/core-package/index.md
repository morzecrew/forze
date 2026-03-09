# Core Package

This section explains what the `forze` core package provides and how to use it in day-to-day application code, before choosing specific infrastructure integrations.

## What core gives you

Core package responsibilities:

- **Contracts** for documents, search, storage, cache, transactions, and more
- **Specifications** (`DocumentSpec`, `SearchSpec`) that describe your aggregate and search model
- **Execution model** (`ExecutionContext`, `ExecutionRuntime`) for dependency resolution and lifecycle
- **Usecase composition** primitives for building document/search facades
- **Shared query DSL** for filters and sorting across adapters

## Practical workflow

Use core in this order:

1. Define your aggregate and read/command DTOs.
2. Declare `DocumentSpec` (and optional `SearchSpec`) as the contract for adapters.
3. Build runtime/dependency plans using integration modules.
4. Resolve ports from `ExecutionContext` in usecases (`ctx.doc_read(...)`, `ctx.doc_write(...)`, `ctx.search(...)`).
5. Pass filters/sorts with the shared query syntax.

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
            }
        },
        default_index="public.idx_projects_title",
    )

## In-use examples

Once you have an execution context, operations stay contract-driven:

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
        options={"mode": "fulltext"},
        limit=20,
        offset=0,
    )

## Read next

- [Query Syntax](query-syntax.md) for full filter/sort DSL reference
- [PostgreSQL Integration](../integrations/postgres.md) for schema/index requirements
- [MongoDB Integration](../integrations/mongo.md) for Mongo-specific behavior
