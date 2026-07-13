---
name: forze-fastapi-interface
description: >-
  Connects Forze handlers to FastAPI: runtime context dependency and lifespan,
  generated routes from an operation registry (attach_document_routes /
  attach_search_routes / attach_storage_routes / attach_aggregate_routes for a
  whole AggregateKit slice), SecurityContextMiddleware
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

- Both styles use the same REST verbs; they differ only in how the resource is
  addressed. `style="rest"` puts the id in the path (`POST ""` 201, `GET /{id}`,
  `PATCH /{id}?rev=`, `DELETE /{id}` 204); `style="rpc"` keeps one
  operation-named path per op and puts the id in a query parameter
  (`GET /get?id=`, `PATCH /update?id=&rev=` with the patch body,
  `DELETE /kill?id=` 204). List operations are `POST /<op>` with a filter body
  in both styles. `create` also posts its input DTO as a body, but mounts at the
  router root in REST (`POST ""`, 201) and at `POST /create` in RPC.
- Only operations the registry holds are attached (a read-only spec yields a
  read-only router); narrow with `include={"get", "list"}`.
- Merging `build_soft_deletion_registry(spec)` into the document registry adds
  soft delete/restore automatically — `POST /{id}/delete|restore?rev=` (REST) or
  `PATCH /delete|restore?id=&rev=` (RPC); hard delete keeps the `DELETE` verb.
- `attach_search_routes` (no `style` — every search request is a filter body,
  always `POST /<op>`) and `attach_storage_routes` (`style` required; multipart
  upload, raw-bytes download) follow the same pattern.
- An aggregate declared with `AggregateKit` (see [`forze-wiring`](../forze-wiring/SKILL.md))
  attaches its **whole slice** in one call — document, soft-delete, search, and
  (under `storage_prefix`, default `/blobs`) storage routes:

  ```python
  from forze_fastapi.routes import attach_aggregate_routes

  attach_aggregate_routes(router, kit, ctx_dep=ctx_dep, style="rest", tx_route="default")
  ```

  The routes execute through the kit's composed registry, so `tx_route` is
  load-bearing — pass the same route the deps module registers its transaction
  manager under (and the same one the kit's `facade()` uses).
- An operation with a plan-declared deadline surfaces it as an
  `x-deadline-seconds` OpenAPI extension and a "Time budget" description line —
  see [`forze-resilience-deadlines`](../forze-resilience-deadlines/SKILL.md).
- `apply_openapi_security(app, requirement)` (from `forze_fastapi.security`) makes
  the generated OpenAPI honest about auth: it derives `securitySchemes` from the
  same `AuthnRequirement` you give `SecurityContextMiddleware` (bearer for an
  `Authorization` token; `apiKey` in header/cookie otherwise) and attaches
  `security` to operations flagged `requires_authn` (derived at freeze from the
  plan's `AuthnRequired`/authz hooks). Call once after attaching routers; token-
  minting routes (`/login`, `/refresh`) stay open. Documents auth, does not enforce
  it — `exclude={op, ...}` leaves a flagged op open.

## Readiness and deadline headers

- `attach_readiness_route(router, runtime)` (from `forze_fastapi.routes`) adds
  `GET /readyz`: `200` while serving, `503` once shutdown starts draining —
  point the load balancer's readiness check here.
- `InvocationMetadataMiddleware(..., bind_deadline_from_header=True)` opts in
  to honoring an upstream `X-Forze-Deadline-Budget` header (tighten-only, so a
  forged value can only shorten the sender's own request).

## Hand-written routes

When you need a route the generators don't cover, define a plain FastAPI route that resolves a context with `ctx_dep` and dispatches through a **facade** built from your frozen registry (see [`forze-documents-search`](../forze-documents-search/SKILL.md)) — not the raw ports:

```python
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from forze_kits.aggregates.document import DocumentFacade, DocumentIdDTO

router = APIRouter(prefix="/projects", tags=["projects"])


def projects(ctx) -> DocumentFacade[ProjectRead, CreateProject, UpdateProject]:
    return DocumentFacade(ctx=ctx, registry=registry, namespace=project_spec.default_namespace)


@router.get("/{project_id}")
async def get_project(project_id: UUID, ctx=Depends(ctx_dep)):
    return await projects(ctx).get(DocumentIdDTO(id=project_id))


app.include_router(router)
```

The facade runs each operation through the pipeline (mapping, hooks, transaction). Reach a raw `ctx.<port>` only inside a custom handler that a facade operation can't express.

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

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version (e.g. `.../forze/0.3/...`) or use the version selector on the site.

- [FastAPI integration](https://morzecrew.github.io/forze/latest/integrations/fastapi/)
- [`forze-wiring`](../forze-wiring/SKILL.md)
- [`forze-auth-tenancy-secrets`](../forze-auth-tenancy-secrets/SKILL.md)
