---
title: FastAPI
icon: lucide/globe
summary: Expose operations over typed HTTP — without domain code touching HTTP
---

`forze[fastapi]` serves Forze operations over typed HTTP routes: generated
CRUD / search / storage endpoints, request-context middleware, and error
handlers. The domain and application layers stay framework-independent — FastAPI
only resolves the runtime and calls registered operations.

## Install

```bash
uv add 'forze[fastapi]'
```

No external service — FastAPI is in-process. The extra brings FastAPI, Uvicorn,
and the docs tooling.

## How it plugs in

There's no client. The **`ExecutionRuntime`** is what routes resolve from. Two
seams connect FastAPI to it: the runtime's lifecycle runs from the app's
**lifespan**, and routes read the current context from a **dependency**.

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

def context_dependency():
    return runtime.get_context()
```

## Attach routes

Generated endpoints resolve operations from a **frozen** registry. Hand a router
the spec, its DTOs, the registry, and the context dependency:

```python
from fastapi import APIRouter
from forze_fastapi.endpoints.document import attach_document_endpoints

orders = APIRouter(prefix="/orders", tags=["orders"])
attach_document_endpoints(
    orders,
    document=order_spec,
    dtos=order_dtos,
    registry=order_registry,   # built with build_document_registry(...).freeze()
    ctx_dep=context_dependency,
)
app.include_router(orders)
```

## What it provides

| Contract | Helper | Notes |
|----------|--------|-------|
| `DocumentSpec` | `attach_document_endpoints` | CRUD + list routes |
| `SearchSpec` | `attach_search_endpoints` | typed and projected search routes |
| `StorageSpec` | `attach_storage_endpoints` | multipart upload, download, list, delete |
| Custom operations | `attach_http_endpoint` | you own request/response mapping |
| Idempotency / ETag | endpoint features | opt-in per mutating / read route |

## Notes

- **Lifespan owns the runtime.** Wiring `runtime.startup()` / `shutdown()` into
  the lifespan is what opens and closes every backing client (Postgres, Redis, …).
- **Register error handlers.** `register_exception_handlers(app)` maps
  `CoreException` to JSON responses and keeps tracebacks out of response bodies.
- **Bind identity at the edge.** `ContextBindingMiddleware` attaches
  `InvocationMetadata`, `AuthnIdentity`, and `TenantIdentity` to the context — see
  [Identity & access](../in-depth/identity.md).
- **Idempotency** needs an idempotency adapter (commonly Redis) and a stable
  client-sent `Idempotency-Key`.
</content>
