---
name: forze-http-outbound
description: >-
  Calls external HTTP APIs from Forze handlers with declarative
  BaseHttpIntegration / async_http_op operations, HttpServiceSpec, HttpDepsModule,
  static and tenant-routed clients, auth, and mock tests. Use when a handler needs
  to call a remote service over HTTP.
---

# Forze outbound HTTP

Use when a handler calls an external HTTP API (a third-party service, an internal microservice). You declare operations once as typed request/response models, wire a client, and invoke through `ExecutionContext`. For general handler patterns see [`forze-framework-usage`](../forze-framework-usage/SKILL.md); for wiring see [`forze-wiring`](../forze-wiring/SKILL.md).

## Declare a service and its operations

Subclass `BaseHttpIntegration` and declare each remote call with `async_http_op`. Request/response are Pydantic models; `query_from` lists request fields serialized as query params.

```python
from pydantic import BaseModel

from forze.application.integrations.http import (
    BaseHttpIntegration,
    async_http_op,
    build_http_service_spec,
)


class GetOrdersQuery(BaseModel):
    status: str | None = None


class OrdersListResponse(BaseModel):
    items: list[str]


class OrdersClient(BaseHttpIntegration):
    get_orders = async_http_op(
        request=GetOrdersQuery,
        response=OrdersListResponse,
        method="GET",
        path="/v1/orders",
        query_from=("status",),
        idempotent=True,
    )


orders_spec = build_http_service_spec(OrdersClient, name="orders")
```

`async_http_op` also accepts `allows_empty_body=True` (an empty response body yields `response.model_construct()`) and `site=...` (override the tracing/exception label). `path` is a template relative to the service base URL and may contain `{placeholders}` filled from request fields.

## Wire the client and service routes

`HttpDepsModule` registers the shared client plus one route per service. `HttpServiceSpec.name` is the route; it must match a key in `services`.

```python
from datetime import timedelta

from forze_http import (
    HttpAuthConfig,
    HttpClient,
    HttpDepsModule,
    HttpServiceConfig,
    http_lifecycle_step,
)

http_module = HttpDepsModule(
    client=HttpClient(),
    services={
        "orders": HttpServiceConfig(
            base_url="https://api.example.com",
            timeout=timedelta(seconds=30),
            default_headers={"Accept": "application/json"},
            auth=HttpAuthConfig(kind="bearer", token="...from-secrets..."),
        ),
    },
)
```

`HttpAuthConfig.kind` is `"bearer"` | `"api_key"` | `"header"` (with `header_name` / `prefix` knobs). Resolve `token` from secrets — never hard-code it (see [`forze-auth-tenancy-secrets`](../forze-auth-tenancy-secrets/SKILL.md)). `HttpDepsModule(client=...)` alone registers only the client; `ctx.http.service(spec)` needs a matching `services` route.

## Lifecycle

The bare `HttpClient()` opens its connection pool in a lifecycle step:

```python
from forze.application.execution import LifecyclePlan
from forze_http import http_lifecycle_step

lifecycle = LifecyclePlan.from_steps(
    http_lifecycle_step(),  # or routed_http_lifecycle_step() for tenant-routed clients
)
```

## Handler pattern

Resolve the service port by spec with `ctx.http.service(spec)`. Either call `port.invoke(op, args)` directly, or wrap it in the typed facade for IDE-friendly calls:

```python
from forze.application.contracts.execution import Handler


class ListOrders(Handler[ListOrdersCmd, OrdersListResponse]):
    async def __call__(self, args: ListOrdersCmd) -> OrdersListResponse:
        port = self.ctx.http.service(orders_spec)

        # Typed facade:
        client = OrdersClient(port=port, spec=orders_spec)
        return await client.get_orders(GetOrdersQuery(status=args.status))

        # Equivalent untyped call:
        # return await port.invoke("get_orders", GetOrdersQuery(status=args.status))
```

## Tenant-routed services

For per-tenant base URLs / credentials, use `RoutedHttpClient` with `routed_http_lifecycle_step()` and set `tenant_aware=True` on the service config. The client resolves each tenant's `HttpRoutingCredentials` (base URL, headers, bearer token) from a `SecretRef` per tenant, so the adapter never needs a `tenant_provider`. Bind `TenantIdentity` at the boundary before the handler runs.

## Deadline propagation

When the caller has an invocation deadline bound, the adapter automatically forwards the remaining budget as an `X-Forze-Deadline-Budget` header (a duration in seconds), so a downstream Forze service can inherit it. Opt out per service with `HttpServiceConfig(propagate_deadline=False)`. See [`forze-resilience-deadlines`](../forze-resilience-deadlines/SKILL.md).

## Testing

Inject a stub `HttpServicePort` (any object with a `spec` attribute and an async `invoke`) in unit tests, or construct the facade with that port — no real network calls. Keep request/response model assertions in the test rather than asserting on raw HTTP.

## Logging

HTTP client/adapter/execution loggers are named under `FORZE_HTTP_LOGGER_NAMES`; route them through your Forze logging configuration rather than the root logger.

## Anti-patterns

1. **Building `httpx` calls inside a handler** — declare an `async_http_op` and resolve via `ctx.http.service(spec)`; keep transport details out of domain logic.
2. **Hard-coding tokens/URLs in `HttpServiceConfig`** — resolve credentials from secrets; only base routing belongs in config.
3. **Mismatched route names** — `HttpServiceSpec.name` must equal the `services` key, or resolution fails.
4. **Passing tenant ids through DTOs for routing** — use `tenant_aware=True` + `RoutedHttpClient` and bind `TenantIdentity` at the boundary.
5. **Marking non-idempotent operations `idempotent=True`** — only safe-to-retry calls; it affects retry behavior.

## Reference

- [HTTP integration](https://morzecrew.github.io/forze/integrations/http/)
- [`forze-framework-usage`](../forze-framework-usage/SKILL.md)
- [`forze-auth-tenancy-secrets`](../forze-auth-tenancy-secrets/SKILL.md)
