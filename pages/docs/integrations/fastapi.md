# FastAPI Integration

`forze_fastapi` connects Forze usecases to HTTP routes. It attaches typed endpoints to a standard FastAPI `APIRouter`: document CRUD (`attach_document_endpoints`), full-text search (`attach_search_endpoints`), and lower-level helpers in `forze_fastapi.endpoints.http` for custom operations. Optional middleware binds call context to requests; `register_exception_handlers` and `register_scalar_docs` integrate errors and API docs.

## Package layout

| Area | Import path | Role |
|------|-------------|------|
| Document HTTP | `forze_fastapi.endpoints.document` | `attach_document_endpoints` — CRUD and list routes from `DocumentSpec` + `DocumentDTOs` |
| Search HTTP | `forze_fastapi.endpoints.search` | `attach_search_endpoints` — typed and raw search routes |
| HTTP primitives | `forze_fastapi.endpoints.http` | `attach_http_endpoint`, `build_http_endpoint_spec`, idempotency and ETag features |
| Middleware | `forze_fastapi.middlewares` | `ContextBindingMiddleware`, `LoggingMiddleware` |
| OpenAPI | `forze_fastapi.openapi` | `register_scalar_docs` |
| Errors | `forze_fastapi.exceptions` | `register_exception_handlers` |

Integration packages are separate wheels (`forze_fastapi`, `forze_postgres`, …) but ship in the same repository; optional extras in `pyproject.toml` pull them in (see [Installation](../installation.md)).

## Installation

    :::bash
    uv add 'forze[fastapi]'

## Execution context dependency

All Forze routes resolve ports through `ExecutionContext`. Provide a callable dependency that returns the current context:

    :::python
    from fastapi import FastAPI
    from forze.application.execution import ExecutionRuntime

    runtime = ExecutionRuntime(...)
    app = FastAPI()


    def context_dependency():
        return runtime.get_context()

Pass this as `ctx_dep=` to `attach_document_endpoints`, `attach_search_endpoints`, and `attach_http_endpoint`.

## Document endpoints

`attach_document_endpoints` registers routes on an existing `APIRouter`. It builds FastAPI handlers from a `UsecaseRegistry`, `DocumentSpec`, and `DocumentDTOs`, resolving `DocumentUsecasesFacade` per request.

### Generated routes

Default paths (each can be overridden or disabled via the `endpoints` argument; see `DocumentEndpointsSpec` in the source):

| Path | Method | Description |
|------|--------|-------------|
| `/get` | GET | Fetch one document by ID (optional ETag / 304) |
| `/list` | POST | Typed list with pagination body |
| `/raw-list` | POST | Raw JSON list |
| `/create` | POST | Create (optional idempotency via `Idempotency-Key`) |
| `/update` | PATCH | Partial update (`id`, `rev`, body DTO) |
| `/delete` | PATCH | Soft delete when supported |
| `/restore` | PATCH | Restore when soft delete is enabled |
| `/kill` | DELETE | Hard delete (204) |

### Setup

    :::python
    from fastapi import APIRouter, FastAPI

    from forze.application.composition.document import (
        DocumentDTOs,
        build_document_registry,
        tx_document_plan,
    )
    from forze_fastapi.endpoints.document import attach_document_endpoints

    app = FastAPI(title="Projects API")
    projects_router = APIRouter(prefix="/projects", tags=["projects"])

    project_dtos = DocumentDTOs(
        read=ProjectReadModel,
        create=CreateProjectCmd,
        update=UpdateProjectCmd,
    )

    registry = build_document_registry(project_spec, project_dtos)
    registry.extend_plan(tx_document_plan, inplace=True)

    attach_document_endpoints(
        projects_router,
        document=project_spec,
        dtos=project_dtos,
        registry=registry,
        ctx_dep=context_dependency,
    )

    app.include_router(projects_router)

Endpoints are attached only when the spec and DTOs support them (for example, list routes are skipped if disabled in `endpoints`, soft-delete routes require `SoftDeletionMixin`, and so on).

## Search endpoints

`attach_search_endpoints` adds typed and raw full-text search routes:

| Path | Method | Description |
|------|--------|-------------|
| `/search` | POST | Typed search with paginated `read` model |
| `/raw-search` | POST | Raw search rows |

### Setup

    :::python
    from fastapi import APIRouter

    from forze.application.composition.search import (
        SearchDTOs,
        build_search_registry,
    )
    from forze_fastapi.endpoints.search import attach_search_endpoints

    search_router = APIRouter(prefix="/projects", tags=["projects-search"])

    search_dtos = SearchDTOs(read=ProjectReadModel)
    search_registry = build_search_registry(project_search_spec, search_dtos)

    attach_search_endpoints(
        search_router,
        dtos=search_dtos,
        registry=search_registry,
        ctx_dep=context_dependency,
    )

    app.include_router(search_router)

Use separate routers or prefixes when combining document and search routes on the same URL prefix so paths do not collide.

## Custom HTTP endpoints

For routes that are not covered by the document or search attach helpers, use `attach_http_endpoint` with a spec from `build_http_endpoint_spec` (`forze_fastapi.endpoints.http`). Document and search attach functions are implemented on top of these primitives.

Idempotency and ETag are implemented as endpoint features (`IdempotencyFeature`, `ETagFeature`); POST routes with idempotency require the `Idempotency-Key` header and a registered idempotency adapter (for example via `RedisDepsModule`).

## Idempotency

The document **create** route can attach idempotency (enabled by default in `attach_document_endpoints` when `document.write` and `dtos.create` are set). Requirements:

1. `IdempotencyPort` registered in the dependency container (for example `RedisDepsModule`)
2. Client sends `Idempotency-Key` with a unique value per logical operation
3. The feature hashes the request body and replays stored responses for duplicate keys

Tune TTL and toggles via `endpoints["config"]` on `attach_document_endpoints` (`enable_idempotency`, `idempotency_ttl`, etc.).

### How it works

1. Before the handler runs, `IdempotencyPort.begin()` checks for a stored snapshot for the operation ID, key, and payload hash
2. If found, the cached response is returned
3. Otherwise the usecase runs; on success `commit()` stores the response snapshot

## Exception handlers

Register built-in handlers to map Forze errors to HTTP status codes:

    :::python
    from forze_fastapi.exceptions import register_exception_handlers

    register_exception_handlers(app)

| Forze error | HTTP status | When |
|-------------|-------------|------|
| `NotFoundError` | 404 | Document or resource not found |
| `ConflictError` | 409 | Revision conflict, duplicate key |
| `ValidationError` | 422 | Domain validation failure |
| `CoreError` | 500 | Unexpected framework error |

The response body includes the error message and, when available, a machine-readable `code` in the `X-Error-Code` header.

## Scalar API reference

Register Scalar docs for interactive API exploration:

    :::python
    from forze_fastapi.openapi import register_scalar_docs

    register_scalar_docs(app, path="/docs", scalar_version="1.41.0")

The page title is derived from `app.title`. The Scalar docs page replaces the default Swagger UI with a more modern interface.

## Request shapes

Prebuilt routes take parameters from typed DTOs in `forze.application.dto` (for example `DocumentIdDTO` on the query string for `/get`, `DocumentIdRevDTO` for `/update`, list bodies for `/list`). Prefer these DTOs when calling facades from custom code so behavior matches HTTP.

## Call context middleware

`ContextBindingMiddleware` (`forze_fastapi.middlewares`) binds call and optional principal context to each request and can echo call context in response headers. Use it when you need request-scoped context propagation beyond `ExecutionContext` alone.

## Runtime scope with FastAPI lifespan

Use the runtime scope as a FastAPI lifespan context manager:

    :::python
    from contextlib import asynccontextmanager
    from fastapi import FastAPI


    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with runtime.scope():
            yield


    app = FastAPI(title="My API", lifespan=lifespan)

This ensures infrastructure clients are connected during the application lifetime and properly shut down when the application stops.

## Complete example

/// details | Complete example
    type: note

    :::python
    import asyncio
    from contextlib import asynccontextmanager

    import uvicorn
    from fastapi import APIRouter, FastAPI

    from forze.application.composition.document import (
        DocumentDTOs,
        build_document_registry,
        tx_document_plan,
    )
    from forze.application.composition.search import (
        SearchDTOs,
        build_search_registry,
    )
    from forze.application.execution import Deps, DepsPlan, ExecutionRuntime, LifecyclePlan
    from forze_fastapi.endpoints.document import attach_document_endpoints
    from forze_fastapi.endpoints.search import attach_search_endpoints
    from forze_fastapi.exceptions import register_exception_handlers
    from forze_fastapi.openapi import register_scalar_docs
    from forze_postgres import (
        PostgresClient,
        PostgresConfig,
        PostgresDepsModule,
        postgres_lifecycle_step,
    )
    from forze_redis import RedisClient, RedisConfig, RedisDepsModule, redis_lifecycle_step

    # Runtime setup
    pg = PostgresClient()
    redis = RedisClient()

    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(
            lambda: Deps.merge(
                PostgresDepsModule(
                    client=pg,
                    rw_documents={
                        "projects": {
                            "read": ("public", "projects"),
                            "write": ("public", "projects"),
                            "bookkeeping_strategy": "database",
                        },
                    },
                    tx={"default"},
                )(),
                RedisDepsModule(
                    client=redis,
                    caches={"projects": {"namespace": "app:projects"}},
                    idempotency={"default": {"namespace": "app:idempotency"}},
                )(),
            ),
        ),
        lifecycle=LifecyclePlan.from_steps(
            postgres_lifecycle_step(dsn="postgresql://app:app@localhost:5432/app", config=PostgresConfig()),
            redis_lifecycle_step(dsn="redis://localhost:6379/0", config=RedisConfig()),
        ),
    )


    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with runtime.scope():
            yield


    app = FastAPI(title="Projects API", lifespan=lifespan)
    register_exception_handlers(app)
    register_scalar_docs(app)

    ctx_dep = lambda: runtime.get_context()

    # Document routes
    project_dtos = DocumentDTOs(
        read=ProjectReadModel,
        create=CreateProjectCmd,
        update=UpdateProjectCmd,
    )
    doc_registry = build_document_registry(project_spec, project_dtos)
    doc_registry.extend_plan(tx_document_plan, inplace=True)

    doc_router = APIRouter(prefix="/projects", tags=["projects"])
    attach_document_endpoints(
        doc_router,
        document=project_spec,
        dtos=project_dtos,
        registry=doc_registry,
        ctx_dep=ctx_dep,
    )
    app.include_router(doc_router)

    # Search routes
    search_dtos = SearchDTOs(read=ProjectReadModel)
    search_registry = build_search_registry(project_search_spec, search_dtos)
    search_router = APIRouter(prefix="/projects", tags=["search"])
    attach_search_endpoints(
        search_router,
        dtos=search_dtos,
        registry=search_registry,
        ctx_dep=ctx_dep,
    )
    app.include_router(search_router)


    async def main():
        server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8000))
        await server.serve()


    if __name__ == "__main__":
        asyncio.run(main())
///
