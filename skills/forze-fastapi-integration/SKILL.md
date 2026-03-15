---
name: forze-fastapi-integration
description: Build HTTP APIs with Forze's FastAPI integration including routers, document routes, route features, error handling, and dependency injection. Use when creating API endpoints, configuring routers, or integrating Forze with FastAPI.
---

# Forze FastAPI Integration

The `forze_fastapi` package integrates Forze with FastAPI. Requires the `forze[fastapi]` extra.

## ForzeAPIRouter

Extended `APIRouter` with context injection, idempotency, ETag, and composable route features:

```python
from forze_fastapi.routing import ForzeAPIRouter

router = ForzeAPIRouter(
    prefix="/api/v1",
    tags=["my-entity"],
    context_dependency=get_execution_context,  # required
    idempotency_config={"header_key": "X-Idempotency-Key"},
    etag_config={"auto_304": True},
)
```

**Required:** `context_dependency` — a callable returning `ExecutionContext` (used as a FastAPI `Depends`).

## Document Routes

### build_document_router

Creates a router with full document CRUD:

```python
from forze_fastapi.routers.document import build_document_router

router = build_document_router(
    prefix="/my-entities",
    tags=["my-entity"],
    registry=my_usecase_registry,
    spec=my_document_spec,
    dtos=my_document_dtos,
    ctx_dep=get_execution_context,
    include_list_endpoint=True,
)
```

### attach_document_routes

Attach document CRUD to an existing router:

```python
from forze_fastapi.routers.document import attach_document_routes

attach_document_routes(
    router,
    registry=registry,
    spec=spec,
    dtos=dtos,
    ctx_dep=ctx_dep,
    include_metadata_endpoint=True,
    include_create_endpoint=True,
    include_update_endpoint=True,
    include_soft_delete_endpoints=True,
    include_hard_delete_endpoint=True,
    include_list_endpoint=False,
    include_raw_list_endpoint=False,
)
```

### Generated Endpoints

| Endpoint | Method | Feature | Flag |
|----------|--------|---------|------|
| `/metadata` | GET | ETag | `include_metadata_endpoint` |
| `/create` | POST | Idempotency | `include_create_endpoint` |
| `/update` | PATCH | — | `include_update_endpoint` |
| `/delete` | DELETE | — | `include_soft_delete_endpoints` |
| `/restore` | POST | — | `include_soft_delete_endpoints` |
| `/kill` | DELETE | — | `include_hard_delete_endpoint` |
| `/list` | POST | — | `include_list_endpoint` |
| `/raw-list` | POST | — | `include_raw_list_endpoint` |

### Path Overrides

```python
from forze_fastapi.routers.document import OverrideDocumentEndpointPaths

attach_document_routes(
    router,
    ...,
    path_overrides={"metadata": "details", "create": "new"},
)
```

## Custom Routes

### GET with ETag

```python
@router.get(
    "/items/{id}",
    response_model=MyReadDocument,
    etag=True,
    etag_config={"provider": MyETagProvider()},
)
async def get_item(id: UUID, ctx: ExecutionContext = Depends(ctx_dep)):
    return await ctx.doc_read(my_spec).get(id)
```

### POST with Idempotency

```python
@router.post(
    "/items",
    response_model=MyReadDocument,
    idempotent=True,
    idempotency_config={"dto_param": "dto"},
)
async def create_item(dto: CreateMyDTO = Body(...), ctx: ExecutionContext = Depends(ctx_dep)):
    return await ctx.doc_write(my_spec).create(dto)
```

## Route Features

### RouteFeature Protocol

```python
from forze_fastapi.routing.routes import RouteFeature

class MyFeature(RouteFeature):
    def wrap(self, handler):
        async def wrapped(request):
            # pre-processing
            response = await handler(request)
            # post-processing
            return response
        return wrapped

    @property
    def extra_dependencies(self):
        return ()
```

### Composing Features

Apply custom features to individual routes:

```python
@router.get(
    "/items",
    route_features=[MyFeature(), AnotherFeature()],
)
async def get_items():
    ...
```

Features compose via `compose_route_class`: first feature is outermost wrapper.

### Built-in Features

| Feature | Purpose | Activation |
|---------|---------|------------|
| `IdempotencyFeature` | Deduplicate POST requests | `idempotent=True` |
| `ETagFeature` | ETag headers + conditional 304 | `etag=True` |

## Error Handling

### Exception Handler

Register Forze's exception handler to convert `CoreError` to JSON responses:

```python
from forze_fastapi.handlers import register_exception_handlers

app = FastAPI()
register_exception_handlers(app)
```

### Error Mapping

| Error Type | HTTP Status | Code |
|------------|-------------|------|
| `NotFoundError` | 404 | `not_found` |
| `ConflictError` | 409 | `conflict` |
| `ValidationError` | 422 | `validation_error` |
| Other `CoreError` | 500 | `internal_error` |

Response format:

```json
{
    "detail": "Error message",
    "context": { "field": "details" }
}
```

Error code is also returned in the `X-Error-Code` header.

## Facade Dependency

Create a FastAPI dependency that resolves a `UsecasesFacade`:

```python
from forze_fastapi.routers._utils import facade_dependency

ucs_dep = facade_dependency(
    facade=DocumentUsecasesFacade,
    reg=my_registry,
    ctx_dep=get_execution_context,
)

@router.get("/items/{id}")
async def get_item(id: UUID, ucs=Depends(ucs_dep)):
    return await ucs.get(id)
```

## Logging Middleware

```python
from forze_fastapi.middlewares import LoggingMiddleware

app.add_middleware(LoggingMiddleware)
```

Logs request method, path, status code, and duration.

## Uvicorn Logging Integration

```python
from forze_fastapi.logging import UVICORN_LOG_CONFIG_TEMPLATE

uvicorn.run(app, log_config=UVICORN_LOG_CONFIG_TEMPLATE)
```

## Form Data

Convert a Pydantic model to accept form data:

```python
from forze_fastapi.routing.forms import as_form

@as_form
class MyFormDTO(BaseDTO):
    name: str
    value: int
```

## Query Parameters

```python
from forze_fastapi.routing.params import UUIDQuery, RevQuery

@router.get("/items/{id}")
async def get_item(id: UUIDQuery, rev: RevQuery | None = None):
    ...
```

## Scalar API Docs

```python
from forze_fastapi.openapi import register_scalar_docs

register_scalar_docs(app)
```

## Application Setup Example

```python
from fastapi import FastAPI
from forze_fastapi.handlers import register_exception_handlers
from forze_fastapi.middlewares import LoggingMiddleware

app = FastAPI(title="My Service")

register_exception_handlers(app)
app.add_middleware(LoggingMiddleware)

app.include_router(my_document_router)
app.include_router(my_search_router)
```

## Checklist

When creating a new API endpoint:

1. Create a `ForzeAPIRouter` with `context_dependency`
2. For document CRUD, use `build_document_router` or `attach_document_routes`
3. For custom endpoints, use `@router.get/post/patch/delete` with `etag`/`idempotent` flags
4. Register `forze_exception_handler` on the app
5. Include the router in the FastAPI app
6. Place router code in `src/forze_fastapi/routers/`
7. Place route features in `src/forze_fastapi/routing/routes/`
