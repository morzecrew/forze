# FastAPI setup

Standing up the FastAPI application: the context dependency, lifespan, middleware order, and error handlers. Attaching routes is [FastAPI routes](fastapi-generated-routes.md).

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

`SecurityContextMiddleware` binds `InvocationMetadata`, `AuthnIdentity`, and `TenantIdentity` at the boundary from an `AuthnRequirement`; handlers only read identity from `ExecutionContext`. `CustomHeadersMiddleware` adds response headers from `static_headers` and/or `dynamic_headers` (callables may be sync or async) and raises `CoreException` if a header is already set. `register_exception_handlers(app)` maps `CoreException` to JSON responses (and unhandled exceptions to 500) — see [errors](errors.md).

Three middleware settings you will need:

- **`anonymous_paths={"/auth/login", "/auth/refresh"}`** — exact paths where an *authentication-kind* failure binds no identity instead of 401ing. Without it a stale credential (a cookie especially) is refused on the very route that would replace it. A valid credential still binds; every other failure kind still errors. Exact paths, never prefixes.
- **`bypass_paths=DEFAULT_HEALTH_PATHS`** (from `forze.base.logging`) — exact HTTP paths neither middleware runs for. Both resolve the execution context on every request, so in front of `/livez` they answer 500 while the runtime scope is not yet open — the window a liveness probe exists to observe. A bypassed path serves with no identity, tenant or envelope bound and no error shaping: probe and scrape paths only. Exact full mounted paths, never prefixes — `check_bypass_paths` fails the boot if a bypassed path serves a generated operation route, if the two middlewares disagree, or if the set matches no route at all.
- **`allowed_websocket_paths={"/realtime/ws"}`** — both middlewares **refuse raw WebSocket scopes** unless the exact mounted path (router prefixes included) is allowlisted, because identity and tenancy resolve for HTTP scopes only. The boot check fails if an allowlisted path does not serve exactly one governed route. `allow_raw_websockets=True` opts out app-wide and hands you identity, tenancy and error shaping on every socket.

For browser clients, `attach_authn_routes(cookies=AuthnCookieCarrier(...))` puts the tokens in `HttpOnly` cookies instead of the response body; pair it with `anonymous_paths` as above. The realtime SSE and WebSocket routes (`attach_realtime_sse_route`, `attach_realtime_ws_route`) and the AsyncAPI document (`attach_asyncapi_route`) are in [realtime transports](realtime-transports.md).

## FastAPI integration

### Context dependency

```python
def context_dependency():
    return runtime.get_context()
```

### Endpoints

> **Note:** the former `forze_fastapi.endpoints.*` router helpers (`attach_document_endpoints`, `attach_search_endpoints`, `attach_http_endpoint`, …) have been removed. Their replacement is `forze_fastapi.routes` (`attach_document_routes`, `attach_search_routes`, `attach_storage_routes`), which generates routes from a frozen operation registry — see [FastAPI routes](fastapi-generated-routes.md). You can also define your own FastAPI routes that resolve a context with the dependency above, dispatch through your operation registry / facade (see [Search composition](#search-composition)), and return the result. Use `SecurityContextMiddleware` for identity binding and `register_exception_handlers(app)` for error mapping (see [FastAPI identity](fastapi-identity.md) and [errors](errors.md)).

```python
from uuid import UUID

from fastapi import APIRouter, Depends

from forze_kits.aggregates.document import DocumentFacade, DocumentIdDTO

router = APIRouter(prefix="/projects", tags=["projects"])


@router.get("/{project_id}")
async def get_project(project_id: UUID, ctx=Depends(context_dependency)):
    facade = DocumentFacade(ctx=ctx, registry=registry, namespace=project_spec.default_namespace)
    return await facade.get(DocumentIdDTO(id=project_id))


app.include_router(router)
```

### Lifespan with runtime scope

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with runtime.scope():
        yield


app = FastAPI(lifespan=lifespan)
```

## Anti-patterns

- **Creating `ExecutionContext` per request by hand** — use `runtime.get_context()` via `ctx_dep`.
- **Calling `runtime.get_context()` outside lifespan scope** — it raises at runtime.
- **Catching `CoreException` manually in routes** — register the built-in exception handlers.
- Passing unfrozen registry to FastAPI attach — call `.freeze()` after plan binding.
- **Missing `ctx_dep` on FastAPI routers** — each request needs a context from the active scope.

## Reference

- [FastAPI integration](https://morzecrew.github.io/forze/latest/integrations/fastapi/)
