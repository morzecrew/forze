# HTTP service contracts

Outbound HTTP is modeled like other configurable ports: a **spec** (what to call) and infrastructure **config** (where to call).

## `HttpOperationSpec`

Per-operation metadata:

| Field | Purpose |
|-------|---------|
| `name` | Logical operation id (matches facade attribute name) |
| `method` | `GET`, `POST`, `PUT`, `PATCH`, `DELETE` |
| `path` | Path template relative to base URL (`/v1/items/{id}`) |
| `args_type` | Request Pydantic model (`None` when no inputs) |
| `return_type` | Response Pydantic model |
| `query_from` | Fields sent as query parameters on GET |
| `allows_empty_body` | Allow empty HTTP body when decoding response |
| `site` | Optional exception/tracing site override |

## `HttpServiceSpec`

Extends `BaseSpec` with `operations: dict[str, HttpOperationSpec]`. The spec `name` must match `HttpxDepsModule.services` keys.

## `HttpServicePort`

- `spec: HttpServiceSpec`
- `async def invoke(op, args=None) -> BaseModel`

Resolve via `ctx.http.service(spec)` or a `BaseHttpIntegration` facade.

## Toolkit

`forze.application.integrations.http` provides `async_http_op`, `BaseHttpIntegration`, and `build_http_service_spec` to declare operations without httpx imports in application modules.

## Infrastructure

See [HTTP integration](../../integrations/http.md) for `HttpxDepsModule`, `HttpxHttpServiceConfig`, and clients.
