# FastAPI setup

Standing up the FastAPI application: the context dependency, lifespan, middleware order, and error handlers. Attaching routes is [FastAPI routes](fastapi-generated-routes.md).

## Context dependency and lifespan

All routes need an active `ExecutionRuntime.scope()` and a `ctx_dep` that returns `runtime.get_context()`.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from forze_fastapi import run_startup_checks


@asynccontextmanager
async def lifespan(app: FastAPI):
    run_startup_checks(app)  # what runtime_lifespan(runtime) does for you

    async with runtime.scope():
        yield


app = FastAPI(lifespan=lifespan)


def ctx_dep():
    return runtime.get_context()
```

An app factory that must import without the runtime's environment, or that runs one runtime per worker process, lets the lifespan build it at startup and wires routes to the lifespan:

```python
from datetime import timedelta

from fastapi import FastAPI
from forze_fastapi import runtime_lifespan


def create_app() -> FastAPI:
    lifespan = runtime_lifespan(
        build=build_my_runtime,             # called at startup; raising fails the startup
        drain_budget=timedelta(seconds=5),  # SIGTERM drains first, in every uvicorn worker
    )
    app = FastAPI(lifespan=lifespan)
    # ctx_dep=lifespan.get_context for middlewares and attach_* routes;
    # attach_readiness_route(router, lifespan.runtime) for the probe.
    return app
```

Serve it with `uvicorn pkg.app:create_app --factory --workers N`. A second entry while the lifespan runs is refused (`configuration`), never a second runtime.

A mounted app that needs its own lifespan (an MCP HTTP app's session manager: `server.http_app()`) passes it as `runtime_lifespan(..., nested=(mcp_app.lifespan,))`. Nested lifespans are entered in order inside the runtime's scope. On shutdown the runtime drains first, then they exit in reverse, then it tears down, so drain phases (or a `SIGTERM` drain) run while they are entered. Their yielded state is merged into `request.state`.

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

- **`anonymous_paths={"/auth/login", "/auth/refresh", "/auth/password-reset/request", "/auth/password-reset/confirm"}`** — exact mounted paths where a missing credential or an *authentication-kind* failure binds no identity instead of 401ing (add `/auth/logout` in cookie mode, and `/docs`, `/openapi.json` if they are public). Everywhere else a request no ingress authenticates is refused (`AuthnRequirement(required=True)`, the default; a real CORS preflight passes, for a CORS layer to answer), and a stale credential (a cookie especially) would be refused on the very route that replaces it. A valid credential still binds; every other failure kind still errors. Exact paths, never prefixes.
- **`bypass_paths=DEFAULT_HEALTH_PATHS`** (from `forze.base.logging`) — exact HTTP paths neither middleware runs for. Both resolve the execution context on every request, so in front of `/livez` they answer 500 while the runtime scope is not yet open — the window a liveness probe exists to observe. A bypassed path serves with no identity, tenant or envelope bound and no error shaping: probe and scrape paths only. Exact paths with every router prefix, never prefixes; on an app mounted under a prefix, the full path or the path relative to the mount — `check_bypass_paths` fails the boot if a bypassed path serves a generated operation route, if the two middlewares disagree, if the set matches no route at all, or if an entry could reach a mounted ASGI app the checks cannot see into (mount a metrics app on an outer app, outside the gated one; a `StaticFiles` mount, such as an SPA at `/`, is exempt).
- **`allowed_websocket_paths={"/realtime/ws"}`** — both middlewares **refuse raw WebSocket scopes** unless the exact path (router prefixes included; on a mounted app, full or relative to the mount) is allowlisted, because identity and tenancy resolve for HTTP scopes only. The boot check fails if an allowlisted path does not serve exactly one governed route. `allow_raw_websockets=True` opts out app-wide and hands you identity, tenancy and error shaping on every socket.

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
from forze_fastapi import run_startup_checks


@asynccontextmanager
async def lifespan(app: FastAPI):
    run_startup_checks(app)  # what runtime_lifespan(runtime) does for you

    async with runtime.scope():
        yield


app = FastAPI(lifespan=lifespan)
```

## Anti-patterns

- **Creating `ExecutionContext` per request by hand** — use `runtime.get_context()` via `ctx_dep`.
- **Calling `runtime.get_context()` outside lifespan scope** — it raises at runtime.
- **A lifespan on a mounted app** — Starlette never runs it, so neither the runtime nor the startup checks start. Give it to the outermost app; `run_startup_checks` walks the mounted apps from there, and `nested=` runs a mounted app's own lifespan.
- **Catching `CoreException` manually in routes** — register the built-in exception handlers.
- Passing unfrozen registry to FastAPI attach — call `.freeze()` after plan binding.
- **Missing `ctx_dep` on FastAPI routers** — each request needs a context from the active scope.

## Reference

- [FastAPI integration](https://morzecrew.github.io/forze/latest/integrations/fastapi/)
