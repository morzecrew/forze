# FastAPI Integration

## Page opening

`forze_fastapi` exposes Forze application contracts over typed FastAPI routes without making domain code depend on HTTP. It provides route attach helpers for document CRUD, search, custom usecase endpoints, request context middleware, exception handlers, and Scalar API docs integration.

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/fastapi-request-flow.svg" alt="FastAPI request flow through middleware, endpoint features, usecase, port, and adapter">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/fastapi-request-flow.svg" alt="FastAPI request flow through middleware, endpoint features, usecase, port, and adapter">
</div>

| Topic | Details |
|------|---------|
| What it provides | FastAPI routers and middleware that resolve Forze dependencies from `ExecutionContext` and call registered usecases. |
| Supported Forze contracts | `DocumentSpec` and `SearchSpec` through generated endpoints; arbitrary `Usecase` classes through HTTP endpoint specs; optional ETag and idempotency behaviors through endpoint features. |
| When to use it | Use this integration when FastAPI is the delivery layer for a Forze service, when you want generated CRUD/search routes, or when custom HTTP operations should still run through `ExecutionRuntime`. |

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

FastAPI does not require a network client. The Forze runtime is the object that routes requests to dependency ports and usecases.

```python
from fastapi import FastAPI
from forze.application.execution import ExecutionRuntime

runtime = ExecutionRuntime(...)
app = FastAPI(title="Projects API")
```

### Config

Configure routers from `DocumentSpec`, `DocumentDTOs`, `SearchDTOs`, and endpoint options. Keep HTTP DTOs at the edge and keep domain/application code framework-independent.

```python
from fastapi import APIRouter
from forze.application.composition.document import DocumentDTOs, build_document_registry
from forze_fastapi.endpoints.document import attach_document_endpoints

projects = APIRouter(prefix="/projects", tags=["projects"])
project_dtos = DocumentDTOs(read=ProjectReadModel, create=CreateProjectCmd, update=UpdateProjectCmd)
registry = build_document_registry(project_spec, project_dtos)
```

### Deps module

FastAPI routes do not register storage dependencies themselves. Register the adapters needed by the usecases in your normal `DepsPlan` and expose the current context as a FastAPI dependency.

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
app.include_router(projects)
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

## Contract coverage table

| Forze contract | Adapter implementation | Dependency key/spec name | Limitations |
|----------------|------------------------|--------------------------|-------------|
| `DocumentSpec` | `attach_document_endpoints` creates CRUD/list HTTP handlers backed by a `DocumentUsecasesFacade`. | Uses the `DocumentSpec.name` and the storage dependency keys configured by the runtime, commonly `DocumentQueryDepKey` and `DocumentCommandDepKey`. | Routes are generated only for supported DTO/spec features; soft-delete routes require a soft-deletion-capable document spec. |
| `SearchSpec` | `attach_search_endpoints` creates typed and raw search routes. | Uses the `SearchSpec.name` and runtime search dependencies, commonly `SearchQueryDepKey`. | Search execution still depends on a search adapter such as Postgres; FastAPI only exposes the route. |
| `Usecase` | `attach_http_endpoint` with `build_http_endpoint_spec`. | The endpoint spec identifies the usecase, request/response DTOs, and context dependency. | You own request/response mapping and status-code choices for custom endpoints. |
| Idempotency feature | HTTP idempotency feature for mutating endpoints. | Requires an idempotency dependency such as `IdempotencyDepKey` when enabled. | Requires clients to send stable idempotency keys; storage and TTL behavior come from the configured idempotency adapter. |
| ETag feature | HTTP ETag handling for reads. | Uses document revision/version data exposed by the document usecase. | Only useful when the read model exposes stable revision metadata. |

## Idempotency

FastAPI endpoint idempotency is an HTTP feature for mutating routes. Enable it on the endpoint spec or generated document create endpoint, require clients to send a stable `Idempotency-Key`, and register an idempotency dependency such as Redis under `IdempotencyDepKey`. The stored response is keyed by the operation, idempotency key, and mapped usecase input, so avoid using one key for different payloads.

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
