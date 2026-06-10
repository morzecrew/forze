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

!!! note "Generated routes are planned"

    Higher-level route builders (`attach_*` helpers that generate CRUD / search
    / storage endpoints from a spec) are a planned addition. Until they land,
    wire routes by hand as above.
