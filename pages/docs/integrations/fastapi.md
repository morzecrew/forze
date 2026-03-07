# FastAPI Integration

This guide explains how to use Forze with FastAPI. It covers prebuilt routers for documents and search, execution context injection, idempotent routes, exception handlers, and OpenAPI documentation.

## Prerequisites

- FastAPI
- `forze[fastapi]` extra installed (includes `fastapi`, `scalar-fastapi`, `starlette`)

## Overview

The `forze_fastapi` package provides:

- **Prebuilt routers** — Document CRUD and search endpoints wired to Forze usecases
- **ForzeAPIRouter** — FastAPI router with execution context injection and idempotency support
- **Exception handlers** — Mapping of Forze errors (`NotFoundError`, `ConflictError`, `ValidationError`) to HTTP status codes
- **OpenAPI** — Scalar API reference UI via `register_scalar_docs`

## Execution Context Dependency

All Forze-backed routes need an `ExecutionContext` to resolve ports (document, search, cache, etc.). Provide a dependency that returns the context:

    :::python
    from fastapi import FastAPI, Depends
    from forze.application.execution import Deps, ExecutionContext

    # Build your deps (e.g. from PostgresDepsModule, RedisDepsModule)
    deps = Deps({...})

    def context_dependency() -> ExecutionContext:
        return ExecutionContext(deps=deps)

    app = FastAPI()

Pass `context_dependency` to `ForzeAPIRouter` as `context_dependency`.

## Prebuilt Routers

### Document Router

`build_document_router` creates a router with CRUD and optional soft-delete endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/medatada` | GET | Metadata for a single document by ID |
| `/create` | POST | Create document (idempotent when `IdempotencyDepKey` registered) |
| `/update` | PATCH | Partial update (requires `id`, `rev`, body) |
| `/delete` | PATCH | Soft-delete (when spec supports it) |
| `/restore` | PATCH | Restore soft-deleted document |
| `/kill` | DELETE | Hard-delete |

    :::python
    from forze_fastapi.routers import (
        build_document_router, 
        document_facade_dependency,
    )
    from forze.application.composition.document import (
        build_document_registry,
        build_document_plan,
        DocumentUsecasesFacadeProvider,
    )

    # Build provider from spec, registry, plan
    provider = DocumentUsecasesFacadeProvider(
        spec=doc_spec,
        reg=build_document_registry(doc_spec),
        plan=build_document_plan(),
        dtos={
            "read": ReadDocument, 
            "create": CreateDocumentCmd, 
            "update": UpdateDocumentCmd
        },
    )

    router = build_document_router(
        prefix="/documents",
        tags=["documents"],
        provider=provider,
        context=context_dependency,
    )

    app.include_router(router)

### Search Router

`build_search_router` creates a router with typed and raw search endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/search` | POST | Typed search with pagination |
| `/raw-search` | POST | Raw search (field projection) |

    :::python
    from forze_fastapi.routers import (
        build_search_router, 
        search_facade_dependency,
    )
    from forze.application.composition.search import (
        build_search_registry,
        build_search_plan,
        SearchUsecasesFacadeProvider,
    )

    provider = SearchUsecasesFacadeProvider(
        spec=search_spec,
        reg=build_search_registry(search_spec),
        plan=build_search_plan(),
        read_dto=DocumentReadModel,
    )

    router = build_search_router(
        prefix="/search",
        tags=["search"],
        provider=provider,
        context=context_dependency,
    )

    app.include_router(router)

You can also attach search routes to an existing router by passing it as the first argument.

## Forze API Router

`ForzeAPIRouter` extends `fastapi.APIRouter` with:

- **Context dependency** — Required; injects `ExecutionContext` for route handlers
- **Idempotency** — Per-route or router-level config for POST endpoints
- **Operation IDs** — Stable OpenAPI operation IDs (required for idempotent routes)

Use it when building custom routers that need Forze ports or idempotency.

## Idempotency

Idempotent POST routes deduplicate requests by key. Register `IdempotencyDepKey` (e.g. via Redis) and mark routes with `idempotent=True`:

    :::python
    @router.post("/create", idempotent=True, operation_id="documents.create")
    async def create(dto: CreateDTO = Body(...), ...):
        ...

The client sends an `Idempotency-Key` header. Duplicate requests with the same key return the cached response. Configure via `idempotency_config`:

- `dto_param` — DTO parameter name for payload hashing (default: inferred)
- `ttl` — Time-to-live for idempotency entries (default: 30 seconds)
- `header_key` — Header name (default: `Idempotency-Key`)

## Exception Handlers

Register Forze exception handlers to map domain errors to HTTP responses:

    :::python
    from forze_fastapi.handlers import register_exception_handlers

    register_exception_handlers(app)

| Error | Status |
|-------|--------|
| `NotFoundError` | 404 |
| `ConflictError` | 409 |
| `ValidationError` | 422 |
| `CoreError` | 500 |

Responses include `X-Error-Code` header and `detail` body.

## OpenAPI Documentation

Register Scalar API reference UI:

    :::python
    from forze_fastapi.openapi import register_scalar_docs

    register_scalar_docs(app, path="/docs", scalar_version="1.41.0")

Uses `app.title` for the docs page title. Serves at `/docs` by default.
