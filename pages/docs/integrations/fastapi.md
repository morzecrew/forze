# FastAPI Integration

This guide shows how to expose Forze usecases over HTTP with `forze_fastapi`.

## Prerequisites

- `forze[fastapi]` installed
- a prepared `ExecutionRuntime` (or at least a context dependency)

## What `forze_fastapi` gives you

- Prebuilt document router (`build_document_router`)
- Prebuilt search router (`build_search_router`)
- `ForzeAPIRouter` with idempotent POST support
- Exception handlers for common Forze errors
- Optional Scalar docs page registration

## Execution Context Dependency

Forze routes resolve ports through `ExecutionContext`. In FastAPI, provide a dependency that returns the current context:

    :::python
    from fastapi import FastAPI
    from forze.application.execution import ExecutionRuntime

    runtime = ExecutionRuntime(...)
    app = FastAPI()

    def context_dependency():
        return runtime.get_context()

Use this function as `context=` (prebuilt routers) or `context_dependency=` (`ForzeAPIRouter`).

## Document router

`build_document_router` wires CRUD operations from a `DocumentUsecasesFacadeProvider`:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/medatada` | GET | Metadata by document ID |
| `/create` | POST | Create document (idempotent when idempotency port exists) |
| `/update` | PATCH | Partial update (`id`, `rev`, DTO body) |
| `/delete` | PATCH | Soft-delete (when spec supports it) |
| `/restore` | PATCH | Restore soft-deleted document |
| `/kill` | DELETE | Hard-delete |

!!! note ""
    Endpoint `/medatada` is currently spelled this way in the package for backward compatibility.

    :::python
    from forze_fastapi.routers import build_document_router
    from forze.application.composition.document import (
        DocumentUsecasesFacadeProvider,
        build_document_plan,
        build_document_registry,
    )
    from myapp.models import CreateProjectCmd, ProjectReadModel, UpdateProjectCmd
    from myapp.specs import project_spec

    provider = DocumentUsecasesFacadeProvider(
        spec=project_spec,
        reg=build_document_registry(project_spec),
        plan=build_document_plan(),
        dtos={
            "read": ProjectReadModel,
            "create": CreateProjectCmd,
            "update": UpdateProjectCmd,
        },
    )

    app.include_router(
        build_document_router(
            prefix="/projects",
            tags=["projects"],
            provider=provider,
            context=context_dependency,
        )
    )

## Search router

`build_search_router` exposes typed and raw search routes:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/search` | POST | Typed search response |
| `/raw-search` | POST | Raw search response |

    :::python
    from forze.application.composition.search import (
        SearchUsecasesFacadeProvider,
        build_search_plan,
        build_search_registry,
    )
    from forze_fastapi.routers import build_search_router
    from myapp.models import ProjectReadModel
    from myapp.specs import project_search_spec

    provider = SearchUsecasesFacadeProvider(
        spec=project_search_spec,
        reg=build_search_registry(project_search_spec),
        plan=build_search_plan(),
        read_dto=ProjectReadModel,
    )

    app.include_router(
        build_search_router(
            prefix="/projects",
            tags=["projects-search"],
            provider=provider,
            context=context_dependency,
        )
    )

## `ForzeAPIRouter` for custom routes

Use `ForzeAPIRouter` when you need custom endpoints but still want Forze idempotency behavior.

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
        return {"ok": True}

## Idempotency

Idempotent POST routes require:

1. `idempotent=True`
2. stable `operation_id`
3. an idempotency implementation registered by dependency key (for example via `RedisDepsModule`)
4. request header `Idempotency-Key` (default name)

Router-level defaults can be configured with `idempotency_config`.

## Exception handlers

Register built-in handlers to map common Forze errors to HTTP codes:

    :::python
    from forze_fastapi.handlers import register_exception_handlers

    register_exception_handlers(app)

| Error | HTTP status |
|-------|-------------|
| `NotFoundError` | 404 |
| `ConflictError` | 409 |
| `ValidationError` | 422 |
| `CoreError` | 500 |

## Scalar API reference page

    :::python
    from forze_fastapi.openapi import register_scalar_docs

    register_scalar_docs(app, path="/docs", scalar_version="1.41.0")

The page title is derived from `app.title`.
