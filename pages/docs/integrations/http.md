---
title: HTTP (outbound)
icon: lucide/arrow-up-right
summary: Call external HTTP services as typed, declarative operations
---

`forze[http]` is the outbound HTTP transport — an httpx-backed client behind the
`HttpServicePort`. You describe an external API as a service of typed operations
and call it from handlers through the context, never touching httpx directly.

## Install

```bash
uv add 'forze[http]'
```

## The client

```python
from forze_http import HttpClient

http = HttpClient()
```

`RoutedHttpClient` resolves per-tenant base URLs and credentials from secrets.

## Wire it

Register a service config per `HttpServiceSpec.name`:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_http import HttpAuthConfig, HttpClient, HttpDepsModule, HttpServiceConfig, http_lifecycle_step

payments = HttpServiceConfig(
    base_url="https://api.payments.example.com",
    auth=HttpAuthConfig(kind="bearer", token="…"),
)

deps = DepsRegistry.from_modules(HttpDepsModule(client=HttpClient(), services={"payments": payments}))
lifecycle = LifecyclePlan.from_steps(http_lifecycle_step())
```

The service's operations are declared with `async_http_op` on a
`BaseHttpIntegration` subclass (from `forze.application.integrations.http`) and
resolved via `ctx.http.service(spec)`.

## What it provides

| Contract | Keyed by |
|----------|----------|
| Outbound HTTP service (typed operations) | `HttpServiceSpec.name` (`services`) |

## Notes

- A service is either static (`base_url`) or per-tenant — `tenant_aware=True`
  forbids a static `base_url` (it comes from secrets) and requires
  `secret_ref_for_tenant`.
- `HttpAuthConfig` covers `bearer`, `api_key`, and custom-`header` auth.
- When the caller has a [deadline](../in-depth/deadlines.md) bound, the adapter
  forwards the remaining budget as an `X-Forze-Deadline-Budget` header so a
  downstream Forze service can inherit it; opt out per service with
  `HttpServiceConfig(propagate_deadline=False)`.
- The `HttpServiceSpec` / `HttpOperationSpec` / `HttpServicePort` contracts live
  in core; `forze_http` provides the httpx transport and wiring.
