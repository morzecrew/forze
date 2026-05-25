# FastAPI Integration

## Page opening

`forze_fastapi` exposes Forze application contracts over typed FastAPI routes without making domain code depend on HTTP. It provides route attach helpers for document CRUD, search, object storage, custom handler endpoints, request context middleware, exception handlers, and Scalar API docs integration.

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/fastapi-request-flow.svg" alt="FastAPI request flow through middleware, endpoint features, handler, port, and adapter">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/fastapi-request-flow.svg" alt="FastAPI request flow through middleware, endpoint features, handler, port, and adapter">
</div>

| Topic | Details |
|------|---------|
| What it provides | FastAPI routers and middleware that resolve Forze dependencies from `ExecutionContext` and call registered handlers. |
| Supported Forze contracts | `DocumentSpec` and `SearchSpec` through generated endpoints; `StorageSpec` via `attach_storage_endpoints` (multipart upload, list, binary download, delete); arbitrary handlers through HTTP endpoint specs; optional ETag and idempotency behaviors through endpoint features. |
| When to use it | Use this integration when FastAPI is the delivery layer for a Forze service, when you want generated CRUD/search routes, or when custom HTTP operations should still run through `ExecutionRuntime`. |
| Authn / authz / tenancy | [Recipe: boundary vs feature guards, resolver policy, default features on generated routes](../recipes/authn-authz-tenancy-fastapi.md). |

## Installation

```bash
uv add 'forze[fastapi]'
```

| Requirement | Notes |
|-------------|-------|
| Package extra | `fastapi` installs FastAPI, Starlette, Uvicorn, python-multipart, httpx, and Scalar docs dependencies. |
| Required service | None. FastAPI is an in-process web framework. |
| Local development dependency | Uvicorn is included by the extra for local ASGI serving. Use your normal test client for route tests. |

## Minimal setup

### Client

FastAPI does not require a network client. The Forze runtime is the object that routes requests to dependency ports and handlers.

```python
from fastapi import FastAPI
from forze.application.execution import ExecutionRuntime

runtime = ExecutionRuntime(...)
app = FastAPI(title="Projects API")
```

### Config

Configure routers from `DocumentSpec`, `DocumentDTOs`, `SearchDTOs`, `build_storage_registry`, and endpoint options. Keep HTTP DTOs at the edge and keep domain/application code framework-independent.

```python
from fastapi import APIRouter
from forze.application.composition.document import DocumentDTOs, build_document_registry
from forze.application.composition.storage import build_storage_registry
from forze.application.contracts.storage import StorageSpec
from forze.base.primitives import str_key_selector
from forze_fastapi.endpoints.document import attach_document_endpoints
from forze_fastapi.endpoints.storage import attach_storage_endpoints

projects = APIRouter(prefix="/projects", tags=["projects"])
files = APIRouter(prefix="/files", tags=["files"])
project_dtos = DocumentDTOs(read=ProjectReadModel, create=CreateProjectCmd, update=UpdateProjectCmd)

project_reg = build_document_registry(project_spec, project_dtos)
registry = (
    project_reg.patch(str_key_selector.all_keys())
    .bind_tx()
    .set_route("postgres")
    .finish(deep=True)
    .freeze()
)
files_spec = StorageSpec(name="files")
file_reg = build_storage_registry(files_spec)
file_registry = (
    file_reg.patch(str_key_selector.all_keys())
    .bind_tx()
    .set_route("postgres")
    .finish(deep=True)
    .freeze()
)
```

### Deps module

FastAPI routes do not register storage dependencies themselves. Register the adapters needed by the handlers in your normal `DepsPlan` and expose the current context as a FastAPI dependency.

```python
def context_dependency():
    return runtime.get_context()

attach_document_endpoints(
    projects,
    document=project_spec,
    dtos=project_dtos,
    registry=registry,
    ctx_dep=context_dependency,
)
attach_storage_endpoints(
    files,
    registry=file_registry,
    ctx_dep=context_dependency,
    storage=files_spec,
)
app.include_router(projects)
app.include_router(files)
```

### Lifecycle step

There is no FastAPI-specific lifecycle step. Start and stop the `ExecutionRuntime` lifecycle from FastAPI lifespan so database, cache, queue, and workflow clients open before requests are served and close on shutdown.

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

app = FastAPI(lifespan=lifespan)
```

## Transport attach (experimental)

`forze_fastapi.transport.http` provides function-first route factories on a plain `APIRouter`.

### Transport layers

| Layer | Module | Role |
|-------|--------|------|
| Catalog | `forze.application.composition.*.catalog` | Which operations exist (`*_OPERATIONS`), presets (`DocumentPreset`, …), capability checks |
| Bindings | `forze_fastapi.transport.http.bindings` | HTTP method, default path, response model, handler builder per operation |
| Options | `forze_fastapi.transport.http.options` | Per-route `RouteOpts` and attach `config` (ETag, idempotency, token transport) |
| Attach | `forze_fastapi.transport.http.attach` | `attach_*_routes`: `enable` loop, policies, `register_route` on `APIRouter` |

Real-time Socket.IO commands use [`forze_socketio`](../integrations/socketio.md) (`SocketIONamespaceRouter.command`) — not nested under `transport.http`. A future catalog-driven `attach_*_commands` for Socket.IO would consume the same composition catalogs without importing FastAPI transport.

Legacy [`attach_*_endpoints`](../../src/forze_fastapi/endpoints/) (`endpoints={...}` dicts) is unchanged; removal is planned in a follow-up PR. Prefer transport attach for new code.

**Planned follow-ups:** catalog-driven `attach_*_commands` in `forze_socketio`; legacy `endpoints/` removal; native FastAPI WebSocket only if a concrete use case appears.

```python
from fastapi import APIRouter
from forze.application.composition.document import DocumentFacade, DocumentPreset, build_document_registry
from forze_fastapi.transport.http import attach_document_routes, make_facade_dep

registry = build_document_registry(project_spec, project_dtos).freeze()
facade_dep = make_facade_dep(
    DocumentFacade,
    registry=registry,
    namespace=project_spec.default_namespace,
    ctx_dep=context_dependency,
)

attach_document_routes(
    projects,
    document=project_spec,
    dtos=project_dtos,
    facade_dep=facade_dep,
    ctx_dep=context_dependency,
    registry=registry,
    enable=DocumentPreset.CRUD,
    per_route={"get": {"path_override": "/metadata"}},
    config={"enable_etag": True},
)
```

| Parameter | Role |
|-----------|------|
| `enable` | Tuple of operation names or a preset (`DocumentPreset.READ`, `CRUD`, `FULL`, …) from the composition catalog |
| `paths` | Bulk path overrides keyed by enable name |
| `per_route` | Per-operation `RouteOpts`: `path_override`, `authn`, `policies`, `include_in_schema` |
| `config` | HTTP features such as `enable_etag` / `enable_idempotency` on document routes |

Legacy `attach_*_endpoints` with `endpoints={...}` dicts remains available; prefer transport attach for new code.

## Contract coverage table

| Forze contract | Adapter implementation | Dependency key/spec name | Limitations |
|----------------|------------------------|--------------------------|-------------|
| `DocumentSpec` | `attach_document_endpoints` creates CRUD/list HTTP handlers that resolve operations from a **frozen** `OperationRegistry` via absolute `StrKey` values on each `HttpEndpointSpec`. | Pass a registry built with `build_document_registry`, bind kernel operations, set the transaction route, then `.finish(deep=True).freeze()` before attach. Uses `DocumentSpec.name` and runtime document dependency keys (`DocumentQueryDepKey`, `DocumentCommandDepKey`). | Routes are generated only for supported DTO/spec features; soft-delete routes require a soft-deletion-capable domain model (`supports_soft_delete()`). |
| `SearchSpec` | `attach_search_endpoints` creates typed and raw search routes; pass ``search=`` with the same spec used to build the registry. | Freeze the search registry the same way as document routes. Uses `SearchSpec.name` and runtime search dependencies, commonly `SearchQueryDepKey`. | Search execution still depends on a search adapter such as Postgres; FastAPI only exposes the route. |
| `StorageSpec` | `attach_storage_endpoints` creates list, multipart upload, binary download, and delete routes resolved from a frozen registry. | Uses `StorageSpec.name` for `StorageDepKey` routing, optional upload idempotency naming, and `namespace=` (defaults to the spec namespace) for operation keys. | Download returns raw bytes (`application/octet-stream`); register `register_exception_handlers` (or equivalent) so `NotFoundError` maps to HTTP 404. |
| Custom operations | `attach_http_endpoint` with `build_http_endpoint_spec(operation=...)`. | The spec carries an absolute operation key; the handler calls `registry.resolve(operation, ctx)`. | You own request/response mapping and status-code choices for custom endpoints. |
| Idempotency feature | HTTP idempotency feature for mutating endpoints. | Requires an idempotency dependency such as `IdempotencyDepKey` when enabled. | Requires clients to send stable idempotency keys; storage and TTL behavior come from the configured idempotency adapter. |
| ETag feature | HTTP ETag handling for reads. | Uses document revision/version data exposed by the document handler. | Only useful when the read model exposes stable revision metadata. |

## Idempotency

FastAPI endpoint idempotency is an HTTP feature for mutating routes. Enable it on the endpoint spec or generated document create endpoint, require clients to send a stable `Idempotency-Key`, and register an idempotency dependency such as Redis under `IdempotencyDepKey`. The stored response is keyed by the operation, idempotency key, and mapped handler input, so avoid using one key for different payloads.

## ASGI middleware

Most services stack `ContextBindingMiddleware` (bind `InvocationMetadata`, `AuthnIdentity`, `TenantIdentity`), `LoggingMiddleware`, `register_exception_handlers`, and Scalar docs. See [Authn, authz, and tenancy with FastAPI](../recipes/authn-authz-tenancy-fastapi.md) for boundary wiring.

`CustomHeadersMiddleware` (`from forze_fastapi.middlewares import CustomHeadersMiddleware`) injects **response** headers before the response is sent. Pass `static_headers` as a string-to-string map and/or `dynamic_headers` as a map of header names to zero-argument callables that return `str` or `Awaitable[str]`. If the outgoing response already includes any of the injected header names, the middleware raises `CoreError` with a duplicate-headers message.

## Complete recipe link

See [CRUD with FastAPI, Postgres, and Redis](../recipes/crud-fastapi-postgres-redis.md) for a complete application-shaped recipe. Keep this page as the integration reference and put long end-to-end examples in recipe pages.

## Configuration reference

### Connection settings

FastAPI itself has no Forze connection settings. Configure host, port, workers, TLS, and proxy headers in your ASGI server and deployment platform.

### Pool settings

FastAPI does not own adapter pools. Configure pools on backing integrations such as Postgres, Redis, SQS, RabbitMQ, or Temporal.

### Serialization settings

Use Pydantic DTOs for request and response bodies. `DocumentDTOs`, `SearchDTOs`, and HTTP endpoint specs control which models are accepted and returned. Avoid exposing domain entities directly when a public API shape should be stable.

### Retry/timeout behavior

HTTP server timeouts are owned by the ASGI server or ingress. Use Forze adapter retry/timeout settings on backing services; avoid adding route-level retries around non-idempotent operations unless idempotency is enabled.

## Operational notes

| Concern | Notes |
|---------|-------|
| Migrations/schema requirements | None for FastAPI. Migrations belong to persistence integrations such as Postgres. |
| Cleanup/shutdown | Wire runtime startup/shutdown into FastAPI lifespan so integration clients close cleanly. |
| Idempotency/caching behavior | FastAPI can expose idempotency and ETag features, but storage is provided by configured Forze idempotency/cache adapters. |
| Production caveats | Run behind a production ASGI server setup, configure trusted proxy headers carefully, and keep error handlers registered so domain/application errors map to consistent HTTP responses. |

## Troubleshooting

| Symptom | Likely cause | Fix | See also |
|---------|--------------|-----|----------|
| Mutating requests return a validation/error response because `Idempotency-Key` is missing. | Idempotency is enabled for the endpoint, but the client did not send the required stable header. | Send the same `Idempotency-Key` for retries of the same operation and register an idempotency adapter such as Redis. | [Idempotency](#idempotency) |
| Domain or application exceptions return generic 500 responses instead of Forze error JSON. | `register_exception_handlers(app)` was not called on the FastAPI app. | Register Forze exception handlers during app setup before serving requests. | [Operational notes](#operational-notes) |
| Document and search endpoints shadow each other or OpenAPI shows unexpected paths. | Generated document/search routes share the same router prefix and default or overridden paths. | Use separate router prefixes or set explicit `path_override` values so document and search paths are unique. | [Contract coverage table](#contract-coverage-table) |
| `DepKey` resolution fails inside a route. | The FastAPI dependency returned a context whose `DepsPlan` does not contain the needed adapter route. | Register the backing integration module and ensure the route/spec name matches. | [Deps module](#deps-module) |
| Endpoint returns 422 for a valid business command. | The request DTO does not match the endpoint body mode or Pydantic model. | Check `DocumentDTOs`, `SearchDTOs`, or the custom endpoint spec and align the client payload. | [Minimal setup](#minimal-setup) |
| Generated soft-delete or restore routes are missing. | The document spec does not advertise soft-deletion support or the endpoint was disabled. | Enable the feature in the document spec/endpoint spec or remove the route from docs and clients. | [Contract coverage table](#contract-coverage-table) |
