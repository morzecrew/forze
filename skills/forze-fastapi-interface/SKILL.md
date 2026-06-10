---
name: forze-fastapi-interface
description: >-
  Connects Forze handlers to FastAPI: runtime context dependency and lifespan,
  generated routes from an operation registry (attach_document_routes /
  attach_search_routes / attach_storage_routes), SecurityContextMiddleware
  identity binding, custom headers/logging middleware, Scalar docs, and
  CoreException error handling. Use when exposing handlers over HTTP.
---

# Forze FastAPI interface

Use when connecting Forze handlers to HTTP. Pair with [`forze-wiring`](../forze-wiring/SKILL.md) for runtime/lifecycle and [`forze-auth-tenancy-secrets`](../forze-auth-tenancy-secrets/SKILL.md) for identity binding.

> **Migration note:** the previous `forze_fastapi.endpoints.*` helpers
> (`attach_document_endpoints`, `attach_search_endpoints`, `attach_authn_endpoints`,
> `attach_http_endpoint`, `build_http_endpoint_spec`) were **removed**. Their
> replacement is `forze_fastapi.routes` (`attach_document_routes`,
> `attach_search_routes`, `attach_storage_routes`), which generates routes from a
> frozen operation registry — see **Generated routes** below. Hand-written routes
> that dispatch through the registry / facade remain fully supported.

## Context dependency and lifespan

All routes need an active `ExecutionRuntime.scope()` and a `ctx_dep` that returns `runtime.get_context()`.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with runtime.scope():
        yield


app = FastAPI(lifespan=lifespan)


def ctx_dep():
    return runtime.get_context()
```

## Generated routes

`forze_fastapi.routes` projects a frozen operation registry (built with
`forze_kits.aggregates.*` factories) onto a plain `APIRouter` you own. Schemas
come from the operation descriptors and each route's `operationId` is the
registry operation key verbatim (e.g. `notes.get`):

```python
from fastapi import APIRouter

from forze_fastapi.routes import attach_document_routes

router = APIRouter(prefix="/notes", tags=["notes"])

attach_document_routes(
    router,
    registry=registry,  # build_document_registry(spec, dtos).freeze()
    ns=spec.default_namespace,
    ctx_dep=ctx_dep,
    style="rest",  # or "rpc" — explicit, required
)

app.include_router(router)
```

- `style="rest"` gives resource paths (`POST ""` 201, `GET /{id}`,
  `PATCH /{id}?rev=`, `DELETE /{id}` 204); `style="rpc"` gives uniform
  `POST /<op>` with the input DTO as body. List operations are `POST /<op>` in
  both styles.
- Only operations the registry holds are attached (a read-only spec yields a
  read-only router); narrow with `include={"get", "list"}`.
- Merging `build_soft_deletion_registry(spec)` into the document registry adds
  `POST /{id}/delete|restore?rev=` automatically.
- `attach_search_routes` (no `style` — every search request is a filter body,
  always `POST /<op>`) and `attach_storage_routes` (`style` required; multipart
  upload, raw-bytes download) follow the same pattern.

## Hand-written routes

Define plain FastAPI routes that resolve a context with `ctx_dep`, dispatch through your operation registry / facade (see [`forze-wiring`](../forze-wiring/SKILL.md) and [`forze-documents-search`](../forze-documents-search/SKILL.md)), and return the result. For simple reads you can call the ports directly:

```python
from uuid import UUID

from fastapi import APIRouter, Depends

router = APIRouter(prefix="/projects", tags=["projects"])


@router.get("/{project_id}")
async def get_project(project_id: UUID, ctx=Depends(ctx_dep)):
    return await ctx.document.query(project_spec).get(project_id)


app.include_router(router)
```

For writes, run them inside a transaction scope (`ctx.tx_ctx.scope(route)`) or dispatch through the frozen `OperationRegistry` / facade built with `forze_kits.aggregates.*`.

## Middleware, errors, and docs

```python
from forze_fastapi.docs import register_scalar_docs
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.middlewares import (
    CustomHeadersMiddleware,
    LoggingMiddleware,
    SecurityContextMiddleware,
)

build_id = "dev"  # e.g. from os.environ at import time

# SecurityContextMiddleware binds identity/tenant from an AuthnRequirement;
# see forze-auth-tenancy-secrets for the full authn= config.
app.add_middleware(LoggingMiddleware)
app.add_middleware(
    CustomHeadersMiddleware,
    static_headers={"X-API-Version": "1"},
    dynamic_headers={"X-Build-Id": lambda: build_id},
)
register_exception_handlers(app)
register_scalar_docs(app, path="/docs")
```

`SecurityContextMiddleware` binds `InvocationMetadata`, `AuthnIdentity`, and `TenantIdentity` at the boundary from an `AuthnRequirement`; handlers only read identity from `ExecutionContext`. `CustomHeadersMiddleware` adds response headers from `static_headers` and/or `dynamic_headers` (callables may be sync or async) and raises `CoreException` if a header is already set. `register_exception_handlers(app)` maps `CoreException` to JSON responses (and unhandled exceptions to 500) — see [`forze-observability-errors`](../forze-observability-errors/SKILL.md).

## Anti-patterns

1. **Creating `ExecutionContext` per request by hand** — use `runtime.get_context()` via `ctx_dep`.
2. **Calling `runtime.get_context()` outside lifespan scope** — it raises at runtime.
3. **Importing the removed `forze_fastapi.endpoints.*` helpers** — use `forze_fastapi.routes.attach_*_routes`, or define your own routes that dispatch through the registry/facade.
4. **Binding identity inside route handlers** — let `SecurityContextMiddleware` bind at the boundary.
5. **Catching `CoreException` manually in routes** — register the built-in exception handlers.

## Reference

- [FastAPI integration](https://morzecrew.github.io/forze/integrations/fastapi/)
- [`forze-wiring`](../forze-wiring/SKILL.md)
- [`forze-auth-tenancy-secrets`](../forze-auth-tenancy-secrets/SKILL.md)
