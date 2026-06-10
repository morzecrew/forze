---
title: FastAPI
icon: lucide/globe
summary: Run an ExecutionRuntime behind FastAPI — lifespan, request context, and error mapping
---

`forze[fastapi]` connects an `ExecutionRuntime` to a FastAPI app: it runs the
runtime from the app's lifespan, binds per-request context (and identity) via
middleware, and maps `CoreException`s to HTTP responses. Routes themselves are
ordinary FastAPI handlers that resolve the context and run operations.

## Install

```bash
uv add 'forze[fastapi]'
```

No external service — FastAPI is in-process.

## Run the runtime from lifespan

The runtime's lifecycle opens and closes every backing client (Postgres, Redis,
…). Drive it from the app lifespan:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI

@asynccontextmanager
async def lifespan(app: FastAPI):
    await runtime.startup()
    try:
        yield
    finally:
        await runtime.shutdown()

app = FastAPI(title="Orders API", lifespan=lifespan)
```

## Bind request context

Two ASGI middlewares attach the per-request context, both given a factory that
returns the current `ExecutionContext`:

```python
from forze_fastapi.middlewares import (
    InvocationMetadataMiddleware,
    SecurityContextMiddleware,
)

def context() -> "ExecutionContext":
    return runtime.get_context()

app.add_middleware(InvocationMetadataMiddleware, ctx_dep=context)
app.add_middleware(SecurityContextMiddleware, ctx_dep=context)
```

`InvocationMetadataMiddleware` binds correlation/execution metadata and the
`Idempotency-Key` header; `SecurityContextMiddleware` binds the authenticated
identity and tenant.

## Map errors to HTTP

`register_exception_handlers` turns a `CoreException` into a response — the kind
decides the status, the `code` rides an error-code header, and details are
exposed only when the kind's [egress policy](../in-depth/errors.md) allows:

```python
from forze_fastapi.exceptions import register_exception_handlers

register_exception_handlers(app)
# raise exc.not_found("...") in a handler → 404 {"detail": "..."}
```

## Routes

Routes are ordinary FastAPI handlers. A route resolves the context and runs an
operation through the frozen registry (or a facade) — the domain code stays
untouched:

```python
from forze_kits.aggregates.document import DocumentFacade

@app.post("/orders")
async def create_order(cmd: CreateOrderCmd) -> ReadOrder:
    facade = DocumentFacade(ctx=runtime.get_context(), registry=registry, namespace=order_spec.default_namespace)
    return await facade.create(cmd)
```

## Generated routes

`attach_document_routes` projects the document operations of a frozen registry
(built with `build_document_registry`) onto a router you own. Request and
response schemas come from the operation descriptors, and each route's
`operationId` is the registry operation key verbatim (`notes.get`) — the HTTP
surface, MCP tool names, and the operation catalog share one identity:

```python
from fastapi import APIRouter
from forze_fastapi.routes import attach_document_routes

router = APIRouter(prefix="/notes", tags=["notes"])

attach_document_routes(
    router,
    registry=registry,  # build_document_registry(spec, dtos).freeze()
    ns=spec.default_namespace,
    ctx_dep=runtime.get_context,
    style="rest",
)

app.include_router(router)
```

Only operations the registry actually holds are attached, so a read-only spec
yields a read-only router; narrow further with `include={"get", "list"}`.
Merging a soft-deletion registry (`build_soft_deletion_registry`) into the
document registry adds its `delete`/`restore` operations to the same router
automatically.

The `style` is an explicit choice:

- `"rest"` — resource paths: `POST /notes` (201), `GET /notes/{id}`,
  `PATCH /notes/{id}?rev=` with the patch DTO as body, `DELETE /notes/{id}`
  (204, hard delete). Soft deletion surfaces as action sub-paths —
  `POST /notes/{id}/delete?rev=` and `POST /notes/{id}/restore?rev=`. List
  operations keep `POST /notes/list`-style paths since their filter bodies
  have no natural REST verb.
- `"rpc"` — one `POST /notes/<op>` per operation with the input DTO as the
  body; the routes mirror the catalog one-to-one.

`attach_search_routes` does the same for a search registry
(`build_search_registry` or its hub/federated siblings). Search requests are
filter/query bodies with no natural REST verb, so there is no style choice —
every operation is `POST /<op>`:

```python
from forze_fastapi.routes import attach_search_routes

search_router = APIRouter(prefix="/notes/search", tags=["notes"])

attach_search_routes(
    search_router,
    registry=search_registry,  # build_search_registry(search_spec).freeze()
    ns=search_spec.default_namespace,
    ctx_dep=runtime.get_context,
)
```

`attach_storage_routes` covers a storage registry (`build_storage_registry`)
and takes the same explicit `style`. Transport shapes are fixed by the
payloads — multipart upload (the file plus optional `description`/`prefix`
form fields), the listing DTO as JSON body, the object bytes back with content
type and a `Content-Disposition` filename, a void delete (204); keys may
contain slashes. The style only decides paths and verbs:

- `"rest"` — `POST /files` (201), `POST /files/list`, `GET /files/{key}`,
  `DELETE /files/{key}`.
- `"rpc"` — `POST /files/upload`, `POST /files/list`,
  `GET /files/download/{key}`, `POST /files/delete/{key}`. Download stays
  `GET` so byte responses remain linkable and cacheable.

```python
from forze_fastapi.routes import attach_storage_routes

files_router = APIRouter(prefix="/files", tags=["files"])

attach_storage_routes(
    files_router,
    registry=storage_registry,  # build_storage_registry(storage_spec).freeze()
    ns=storage_spec.default_namespace,
    ctx_dep=runtime.get_context,
    style="rest",
)
```

Identity, invocation metadata, and error mapping stay with the middlewares and
exception handlers above — generated routes only validate the input DTO and run
the operation through the normal pipeline.
