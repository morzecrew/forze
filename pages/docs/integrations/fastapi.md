# FastAPI Integration

`forze_fastapi` connects Forze usecases to HTTP routes. It provides prebuilt routers for document and search operations, a custom `ForzeAPIRouter` with idempotency support, exception handlers, and OpenAPI docs integration.

## Installation

    :::bash
    uv add 'forze[fastapi]'

## Execution context dependency

All Forze routes resolve ports through `ExecutionContext`. In FastAPI, provide a callable dependency that returns the current context:

    :::python
    from fastapi import FastAPI
    from forze.application.execution import ExecutionRuntime

    runtime = ExecutionRuntime(...)
    app = FastAPI()


    def context_dependency():
        return runtime.get_context()

This function is passed as `context=` to prebuilt routers or `context_dependency=` to `ForzeAPIRouter`.

## Document router

`build_document_router` wires standard CRUD operations from a `UsecaseRegistry` and `DocumentDTOs`. It generates routes that resolve the `DocumentUsecasesFacade`, build each usecase, and execute it.

### Generated endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/metadata` | GET | Fetch document metadata by ID |
| `/create` | POST | Create a document (idempotent when configured) |
| `/update` | PATCH | Partial update with `id`, `rev`, and DTO body |
| `/delete` | PATCH | Soft-delete (when the spec supports it) |
| `/restore` | PATCH | Restore a previously soft-deleted document |
| `/kill` | DELETE | Hard-delete a document |
| `/list` | POST | List documents with typed results (opt-in) |
| `/raw-list` | POST | List documents with raw results (opt-in) |

List endpoints are disabled by default. Enable them with `include_list_endpoints=True`.

### Setup

    :::python
    from forze.application.composition.document import (
        DocumentDTOs,
        build_document_registry,
        tx_document_plan,
    )
    from forze_fastapi.routers import build_document_router

    project_dtos = DocumentDTOs(
        read=ProjectReadModel,
        create=CreateProjectCmd,
        update=UpdateProjectCmd,
    )

    registry = build_document_registry(project_spec, project_dtos)
    registry.extend_plan(tx_document_plan, inplace=True)

    app.include_router(
        build_document_router(
            prefix="/projects",
            tags=["projects"],
            registry=registry,
            spec=project_spec,
            dtos=project_dtos,
            ctx_dep=context_dependency,
        )
    )

The router automatically detects whether the spec supports soft-delete and update operations and only generates applicable endpoints.

## Search router

`build_search_router` exposes typed and raw full-text search endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/search` | POST | Typed search with Pydantic model response |
| `/raw-search` | POST | Raw search returning JSON dicts |

### Setup

    :::python
    from forze.application.composition.search import (
        SearchDTOs,
        build_search_registry,
    )
    from forze_fastapi.routers import build_search_router

    search_dtos = SearchDTOs(read=ProjectReadModel)
    search_registry = build_search_registry(project_search_spec, search_dtos)

    app.include_router(
        build_search_router(
            prefix="/projects",
            tags=["projects-search"],
            registry=search_registry,
            spec=project_search_spec,
            dtos=search_dtos,
            ctx_dep=context_dependency,
        )
    )

You can also attach search routes to an existing router using `attach_search_routes()` for a combined endpoint group.

## Custom routes with ForzeAPIRouter

When you need custom endpoints that still leverage Forze idempotency behavior, use `ForzeAPIRouter`:

    :::python
    from fastapi import Body
    from pydantic import BaseModel
    from forze_fastapi.routing.router import ForzeAPIRouter


    class CreatePayload(BaseModel):
        title: str


    router = ForzeAPIRouter(
        prefix="/custom",
        tags=["custom"],
        context_dependency=context_dependency,
    )


    @router.post(
        "/create",
        idempotent=True,
        operation_id="custom.create",
        idempotency_config={"dto_param": "payload"},
    )
    async def create(payload: CreatePayload = Body(...)):
        ctx = router.resolve_context()
        doc = ctx.doc_write(project_spec)
        return await doc.create(payload)

`ForzeAPIRouter` extends FastAPI's `APIRouter` with:

- `context_dependency` : a callable that returns the `ExecutionContext`
- `idempotent` flag on routes for automatic deduplication
- `idempotency_config` for per-route or router-level idempotency settings

## Idempotency

Idempotent POST routes prevent duplicate side effects when clients retry requests. The system requires:

1. `idempotent=True` on the route decorator
2. A stable `operation_id` for the route
3. An idempotency adapter registered in the dependency container (e.g. via `RedisDepsModule`)
4. The client sends an `Idempotency-Key` header with a unique key per request

When a duplicate request arrives (same operation, same key, same payload hash), the adapter returns the previously stored response instead of re-executing the operation.

### How it works

1. The route middleware calls `IdempotencyPort.begin()` with the operation ID, idempotency key, and a hash of the request payload
2. If a cached snapshot exists, it is returned immediately
3. If no snapshot exists, the route handler runs normally
4. After a successful response, `IdempotencyPort.commit()` stores the response for future deduplication

### Configuration

Router-level defaults:

    :::python
    router = ForzeAPIRouter(
        prefix="/api",
        context_dependency=context_dependency,
        idempotency_config={
            "key_header": "Idempotency-Key",
            "dto_param": "payload",
        },
    )

Per-route overrides:

    :::python
    @router.post(
        "/create",
        idempotent=True,
        operation_id="resource.create",
        idempotency_config={"dto_param": "body"},
    )
    async def create(body: CreatePayload = Body(...)):
        ...

## Exception handlers

Register built-in handlers to map Forze errors to appropriate HTTP status codes:

    :::python
    from forze_fastapi.handlers import register_exception_handlers

    register_exception_handlers(app)

| Forze error | HTTP status | When |
|-------------|-------------|------|
| `NotFoundError` | 404 | Document or resource not found |
| `ConflictError` | 409 | Revision conflict, duplicate key |
| `ValidationError` | 422 | Domain validation failure |
| `CoreError` | 500 | Unexpected framework error |

The response body includes the error message and, when available, a machine-readable `code` in the `X-Error-Code` header.

## Scalar API reference

Register Scalar docs page for interactive API exploration:

    :::python
    from forze_fastapi.openapi import register_scalar_docs

    register_scalar_docs(app, path="/docs", scalar_version="1.41.0")

The page title is derived from `app.title`. The Scalar docs page replaces the default Swagger UI with a more modern interface.

## Route parameters

`forze_fastapi` provides common parameter helpers used by prebuilt routers:

| Helper | Type | Purpose |
|--------|------|---------|
| `UUIDQuery` | `UUID` | Document ID query parameter |
| `RevQuery` | `int` | Revision query parameter for optimistic concurrency |
| `pagination()` | `Pagination` | Limit/offset pagination dependency |

These are also available for custom routes when building your own endpoints.

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
    from fastapi import FastAPI

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
    from forze_fastapi.handlers import register_exception_handlers
    from forze_fastapi.openapi import register_scalar_docs
    from forze_fastapi.routers import build_document_router, build_search_router
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
                PostgresDepsModule(client=pg, rev_bump_strategy="database", history_write_strategy="database")(),
                RedisDepsModule(client=redis)(),
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
    app.include_router(
        build_document_router(
            prefix="/projects",
            tags=["projects"],
            registry=doc_registry,
            spec=project_spec,
            dtos=project_dtos,
            ctx_dep=ctx_dep,
        )
    )

    # Search routes
    search_dtos = SearchDTOs(read=ProjectReadModel)
    search_registry = build_search_registry(project_search_spec, search_dtos)
    app.include_router(
        build_search_router(
            prefix="/projects",
            tags=["search"],
            registry=search_registry,
            spec=project_search_spec,
            dtos=search_dtos,
            ctx_dep=ctx_dep,
        )
    )


    async def main():
        server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8000))
        await server.serve()


    if __name__ == "__main__":
        asyncio.run(main())
///
