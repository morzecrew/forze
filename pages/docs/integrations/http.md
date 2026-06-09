---
title: HTTP outbound integration
summary: Declarative outbound HTTP services with httpx
---

## What problem this solves

Call remote HTTP APIs from handlers with typed request/response models, dependency wiring, and optional per-tenant base URLs—without ad hoc `httpx` usage in application code.

## When you need this

Use this when a service integrates with one or more external HTTP APIs (microservices, SaaS REST endpoints) and you want the same spec + deps pattern as documents or durable workflows.

## Install

```bash
uv add 'forze[http]'
```

## Logging

Register HTTP loggers with the rest of your app:

```python
from forze_http import FORZE_HTTP_LOGGER_NAMES
from forze.base.logging import configure_logging

configure_logging(logger_names=[*FORZE_HTTP_LOGGER_NAMES, ...])
```

Logger names: `http.kernel`, `http.adapters`, `http.execution`.

## Architecture

| Layer | Type | Example |
|-------|------|---------|
| Application contract | `HttpServiceSpec` + `HttpOperationSpec` | Operation catalog, Pydantic types, paths |
| Infrastructure config | `HttpxHttpServiceConfig` | `base_url`, `secret_ref_for_tenant`, `timeout`, auth |
| Port | `HttpServicePort` | `invoke(op, args)` |
| Optional facade | `BaseHttpIntegration` + `async_http_op` | `await client.get_orders(...)` |

**Same `name` everywhere:** `HttpServiceSpec(name="orders")` must match the key in `HttpxDepsModule.services`.

Kernel specs do **not** contain `base_url`; that belongs in `HttpxHttpServiceConfig` or per-tenant secrets (see [Specs and infrastructure wiring](../concepts/specs-and-wiring.md)).

## Declarative operations

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
    )

OrdersApiSpec = build_http_service_spec(OrdersClient, name="orders")
```

## Wiring (static base URL)

```python
from datetime import timedelta

from forze_http import (
    HttpxClient,
    HttpxDepsModule,
    HttpxHttpServiceConfig,
    http_lifecycle_step,
)

client = HttpxClient()
module = HttpxDepsModule(
    client=client,
    services={
        "orders": HttpxHttpServiceConfig(
            base_url="https://orders.example.com",
            timeout=timedelta(seconds=15),
        ),
    },
)
# Register module on your Deps plan; add http_lifecycle_step when using lifecycle startup.
```

In handlers:

```python
port = ctx.http.service(OrdersApiSpec)
orders = OrdersClient(port=port, spec=OrdersApiSpec)
result = await orders.get_orders(GetOrdersQuery(status="open"))
```

## Tenant-aware services

Set `tenant_aware=True` on `HttpxHttpServiceConfig` (do **not** set `base_url` on the config). Tenancy is resolved in a **routed httpx client** (same pattern as Postgres/Redis/GCS): the adapter sends relative paths only; the client resolves tenant base URL and credential headers from secrets.

Either:

1. Register one `RoutedHttpxClient` at `HttpxClientDepKey` with `routed_http_lifecycle_step` when all tenant-aware services share the same per-tenant endpoint, or
2. Set `secret_ref_for_tenant` on each service config — the HTTP dep factory builds a service-scoped `RoutedHttpxClient` (use when services need different secret paths per tenant).

Secrets resolve to `HttpRoutingCredentials`:

```json
{"base_url": "https://tenant.example.com", "headers": {"X-Custom": "1"}, "bearer_token": "..."}
```

```python
from uuid import UUID

from forze.application.contracts.secrets import SecretRef
from forze_http import HttpxClient, HttpxDepsModule, HttpxHttpServiceConfig

module = HttpxDepsModule(
    client=HttpxClient(),
    services={
        "orders": HttpxHttpServiceConfig(
            tenant_aware=True,
            secret_ref_for_tenant={tenant_id: SecretRef(path="tenants/orders")},
        ),
    },
)
```

Use a shared `HttpxClient` without a global base URL when multiple services resolve different tenant endpoints.

## Routed client (single endpoint per tenant)

When every HTTP service for a tenant shares one base URL and you want a pooled `httpx` client per tenant, use `RoutedHttpxClient` at `HttpxClientDepKey` with client-level `secret_ref_for_tenant`, plus `routed_http_lifecycle_step`. Service configs can set `tenant_aware=True` without `secret_ref_for_tenant` on the config when the routed client supplies the base URL.

## Related

- [HTTP contracts](../core-package/contracts/http.md)
- [Specs and infrastructure wiring](../concepts/specs-and-wiring.md)
- [Contracts and adapters](../concepts/contracts-adapters.md)
- [Execution reference](../reference/execution.md#dependencies)
