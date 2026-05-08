---
name: forze-fastapi-interface
description: >-
  Builds Forze FastAPI interfaces: context dependencies, document/search/custom
  endpoint attachment, middleware, idempotency, ETags, multipart forms, Scalar
  docs, and exception handling. Use when exposing usecases over HTTP.
---

# Forze FastAPI interface

Use when connecting Forze usecases to HTTP. Pair with [`forze-wiring`](forze-wiring/SKILL.md) for runtime/lifecycle and [`forze-auth-tenancy-secrets`](forze-auth-tenancy-secrets/SKILL.md) for identity binding.

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

## Document and search routes

Attach routes to standard FastAPI `APIRouter` instances.

```python
from fastapi import APIRouter

from forze.application.composition.document import DocumentDTOs, build_document_registry
from forze.application.composition.search import SearchDTOs, build_search_registry
from forze_fastapi.endpoints.document import attach_document_endpoints
from forze_fastapi.endpoints.search import attach_search_endpoints

project_dtos = DocumentDTOs(read=ProjectRead, create=CreateProject, update=UpdateProject)
project_registry = build_document_registry(project_spec, project_dtos)

router = APIRouter(prefix="/projects", tags=["projects"])
attach_document_endpoints(
    router,
    document=project_spec,
    dtos=project_dtos,
    registry=project_registry,
    ctx_dep=ctx_dep,
)

search_dtos = SearchDTOs(read=ProjectRead)
search_registry = build_search_registry(project_search_spec, search_dtos)
attach_search_endpoints(router, dtos=search_dtos, registry=search_registry, ctx_dep=ctx_dep)
```

Document routes are attached only when the spec/DTOs support the operation. Use separate prefixes or explicit endpoint specs when document and search paths would collide.

## Custom HTTP operations

Use `forze_fastapi.endpoints.http` when an operation is not document/search CRUD.

```python
from forze_fastapi.endpoints.http import attach_http_endpoint, build_http_endpoint_spec

spec = build_http_endpoint_spec(
    operation="projects.archive",
    method="POST",
    path="/archive",
    request=ArchiveProjectRequest,
    response=ProjectRead,
)
attach_http_endpoint(router, spec=spec, registry=registry, ctx_dep=ctx_dep)
```

For multipart/form routes, set `body_mode="form"` and declare `UploadFile` / `list[UploadFile]` on the request model. Map files to stable bytes or object keys before using idempotency.

## Idempotency and ETags

Document create routes can use idempotency when an `IdempotencyPort` is registered, usually via `RedisDepsModule.idempotency`. The feature hashes the mapped usecase input, not the raw HTTP body.

ETag support is route-feature based. Use it for stable read responses such as document `id:rev`, not for volatile projections.

## Middleware, errors, and docs

```python
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.middlewares import ContextBindingMiddleware, LoggingMiddleware
from forze_fastapi.openapi import register_scalar_docs

app.add_middleware(ContextBindingMiddleware, ctx_dep=ctx_dep)
app.add_middleware(LoggingMiddleware)
register_exception_handlers(app)
register_scalar_docs(app, path="/docs")
```

`ContextBindingMiddleware` binds `CallContext`, `AuthnIdentity`, and `TenantIdentity` at the boundary. Use resolvers/codecs there; usecases should only read identity from `ExecutionContext`.

## Anti-patterns

1. **Creating `ExecutionContext` per request by hand** — use the runtime context.
2. **Calling `runtime.get_context()` outside lifespan scope** — it raises at runtime.
3. **Importing old router helpers** — use `attach_document_endpoints`, `attach_search_endpoints`, and `attach_http_endpoint`.
4. **Using idempotency with raw `UploadFile` in mapped input** — consume the stream first and hash stable data.
5. **Catching `CoreError` manually in routes** — register built-in exception handlers.

## Reference

- [`pages/docs/integrations/fastapi.md`](../../pages/docs/integrations/fastapi.md)
- [`src/forze_fastapi/endpoints/document`](../../src/forze_fastapi/endpoints/document)
- [`src/forze_fastapi/endpoints/http`](../../src/forze_fastapi/endpoints/http)
- [`src/forze_fastapi/middlewares`](../../src/forze_fastapi/middlewares)
