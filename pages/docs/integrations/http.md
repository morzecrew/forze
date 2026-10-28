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

### Settings

`HttpSettings` holds the base URL, bearer token, default headers and the response cap —
see [connection settings](index.md#connection-settings). The cap (`max_response_bytes`)
counts the decoded body as it decodes, so a small compressed body cannot expand past it. A
client with a cap asks only for `gzip` and `deflate`, and refuses a response in any other
content coding.

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

## Stream a response

`invoke` reads the whole body and validates it. For a body you read as it arrives — a
streamed completion, a large export — `stream` takes the same operation and yields the
body in chunks:

```python
async with aclosing(ctx.http.service(spec).stream(op, args)) as chunks:
    async for chunk in chunks:
        ...
```

It is a call through the same service, so the request is the one `invoke` sends: the
wiring gate, the credential, the deadline budget, the trace context and the egress tag all
apply. What changes is when things happen:

- A rejected response is read — held to `max_response_bytes` when one is configured —
  and raised before the first chunk, as the exception `invoke` raises. An accepted body is
  never buffered, so the cap does not apply to it and nothing validates it against
  `return_type`.
- The timeout bounds the connect and each wait for the next chunk, not the whole stream.
  The [deadline](../running-in-prod/deadlines.md) budget travels as a header when the
  stream opens and does not bound the body.
- Under a [resilience](../running-in-prod/resilience.md) port policy that covers `stream`,
  the breaker gates the open and records the outcome. Nothing retries: a body you have
  started reading cannot be replayed. No per-port client span is opened over a stream, so
  the egress tag lands on the enclosing span.
- Closing the generator closes the connection. A bare `break` leaves that to Python's
  finalizer; `contextlib.aclosing` closes it where you leave.
- On a tenant-routed service, evicting a tenant's client — a rotation, or overflow past
  `max_cached_tenants` — fails a stream still open on it. A `RoutedHttpClient` you build
  with `guarded=True` and register as the client defers the eviction until open streams
  end; the one a service config builds from `secret_ref_for_tenant` is unguarded. See
  [routed clients](../identity-tenancy-enc/multi-tenancy.md#routed-clients-dedicated).

## Hand a vendor SDK the service

A vendor SDK, such as a model provider's client or an agent framework built on one, takes an
`httpx.AsyncClient` and builds its own requests, so `invoke` never sees them. Give that
client a transport over the declared service:

```python
import httpx

from forze_http import http_service_transport

transport = http_service_transport(ctx, llm_spec)
sdk_http = httpx.AsyncClient(base_url="https://api.llm.example.com/v1", transport=transport)
```

Pass `sdk_http` wherever the SDK accepts an `httpx.AsyncClient`. Every request it sends then
leaves the way a declared operation does:

- **It is held to the service's route.** The URL must be the service's `base_url`, or a path
  below it, on the same scheme, host and port. On a tenant-routed service it must be under
  the current tenant's route instead. Anything else is refused with `http_request_off_route`
  before it is sent. That includes another host, a path outside the prefix, a dot segment
  (percent-encoded or not), a `;` path parameter, an escaped `%` (`%25`) and userinfo in the
  URL. Point the SDK at the same base URL. The
  pooled client never follows a redirect: the SDK's client follows it if configured to, and
  the next hop is held to the route again.
- **The service's headers replace the SDK's headers of the same names.** The service's
  `default_headers` and credential override them, and so do the pooled client's own default
  headers, which on a routed service carry the tenant's credential. You can therefore build
  the SDK with a placeholder key and keep the real one in the service config or the tenant's
  secret. Replacement goes by name only: a credential the SDK sends under another header
  name, such as `x-api-key` beside the service's `Authorization`, still goes out to the
  pinned host. Give the SDK a placeholder under every header it fills in.
- **The deadline budget, the trace context and the egress tag apply** as they do for
  `invoke`. A deadline that is already spent is refused with `deadline_exceeded`, and nothing
  is sent. As with `stream`, the budget travels when the request opens and does not bound
  the body. The invocation deadline itself cancels a call still running.
- **It goes out on the service's pooled client, and the response comes back as sent:**
  status, headers and the body undecoded. Status handling, retries and error types stay the
  SDK's, and forze's error mapping does not apply. The SDK's timeouts apply, not the
  service's `timeout`. When the client sets `max_response_bytes`, the cap bounds every
  body's *decoded* size, counted chunk by chunk before the SDK sees the chunk, so it also
  cuts off a long stream and a small compressed body that would expand past it. A capped
  client asks only for `gzip` and `deflate` and refuses a response in any other content
  coding. On a tenant-routed service, evicting a tenant's client fails a response still
  streaming, unless the routed client is built with `guarded=True`, as for
  [`stream`](#stream-a-response).
- **Only a declared service yields one.** The transport is resolved through the service's
  dep key, so an undeclared service is refused. An `egress_sensitive` service exists only
  with `acknowledge_data_egress`, so the wiring gate has already run.

A refusal raises inside the SDK's call. An SDK may retry it and then wrap it in its own
connection error, with forze's exception as the cause. Closing the SDK's client leaves the
pooled client open, because the lifecycle owns it. `HttpServiceTransport` is the class, if
you build one yourself from a client and a config.

## Errors

A failed call raises a `CoreException` whose kind follows the counterparty's answer:

| Answer | Kind | Retryable |
|--------|------|-----------|
| 404 | `not_found` | no |
| 429 | `throttled`, with a parsed `Retry-After` as `retry_after` seconds (at most 3600) in `details` | yes |
| 401, 403 | `infrastructure`: the counterparty refused this service's credential | yes |
| 408, 5xx, timeout, connection failure | `infrastructure` | yes |
| 3xx | `internal`: a redirect is not followed | no |
| any other 4xx | `internal`: it rejected the request this service built | no |
| 2xx whose body fails validation, or is empty when the operation needs one | `internal` | no |

A refused credential is `infrastructure` rather than `configuration` because a supervised
loop stops for good on `configuration`, so a rotated credential would need a restart.

The details stay on the exception for your code; a client sees none of them.

## Notes

- A service is either static (`base_url`) or per-tenant — `tenant_aware=True`
  forbids a static `base_url` (it comes from secrets) and requires
  `secret_ref_for_tenant`.
- `HttpAuthConfig` covers `bearer`, `api_key`, custom-`header`, and `basic` auth. The
  `basic` kind takes `username` and `password` and sends the RFC 7617 header; declare one
  half without the other and wiring is refused, because sending no header at all would
  surface at the counterparty as a rejected request rather than here as the mistake it is.
  The other kinds keep their existing behaviour — an absent token means no auth.
- **A request body is JSON unless an operation says otherwise.**
  `async_http_op(..., body_encoding="form")` sends
  `application/x-www-form-urlencoded`, which is what an OAuth token endpoint requires.
  It is per operation on purpose: one provider mixes a form-encoded token endpoint with a
  JSON API behind one base URL and one credential. A form body carries scalars only —
  a nested object, a list, or a non-finite number is refused by field name rather than
  stringified into something no server parses; `None` is omitted and booleans are sent
  lowercase.
- **An operation can declare what its errors look like.**
  `async_http_op(..., error_type=ProviderError)` validates the body of a rejected
  response against that model and attaches it to the raised exception's `details` under
  `response_error`, so a caller can read the provider's own code — `invalid_grant`, say —
  and decide whether the failure is worth retrying. Only fields the model declares travel,
  on the same scrubbed channel as every other error context, and a body that does not
  match the model changes nothing: you get exactly the exception you would have got
  without a declaration. A model configured `extra="allow"` still contributes only its
  declared fields.
- **A credential or declared-sensitive payload over a plaintext `base_url` is warned
  about at wiring** — for every auth kind, and for an `egress_sensitive` route even when it
  carries no `HttpAuthConfig` at all, which is the shape of an OAuth token request posting
  its client secret as a form field. A **tenant-routed** service has no `base_url` at
  wiring, so the same check runs once per tenant client against the URL that tenant's
  secret resolved to — on the same two triggers, since a declared-sensitive body needs no
  credential headers to be worth protecting — naming the tenant rather than the credential — a bearer token on the wire is as readable as a base64 user-and-password. Loopback
  is exempt. It warns rather than refuses because a service mesh legitimately terminates
  TLS in a sidecar, so `http://service.namespace.svc` with a credential is plaintext at
  this hop and encrypted on the network; refusing that would need an opt-out to stay
  usable.
- When the caller has a [deadline](../running-in-prod/deadlines.md) bound, the adapter
  forwards the remaining budget as an `X-Forze-Deadline-Budget` header so a
  downstream Forze service can inherit it; opt out per service with
  `HttpServiceConfig(propagate_deadline=False)`.
- A route that sends business data, personal data or prompts outside your trust
  boundary can say so, and saying so requires accepting it:
  `HttpServiceConfig(egress_sensitive=True, acknowledge_data_egress=True)`. Declare
  the first without the second and wiring fails closed
  (`http_egress_unacknowledged`). The acknowledgement must be a real `True` — wiring
  from environment text hands you the string `"false"`, which is perfectly truthy, and
  a gate that reads that as agreement is not a gate. This is a governance marker and a conscious-choice
  gate, not a DLP filter — nothing inspects a payload. Calls through the service port
  also tag their span `forze.egress.sensitive`, so sensitive egress is queryable in
  [observability](../running-in-prod/observability.md) rather than only visible in
  review. Both halves cover the services you declare: a bare `HttpClient` you point at
  a provider yourself has no service config and therefore no declaration to gate,
  because a shared client carries no destination of its own. Declare the hop as a
  service if you want it governed — a streamed answer included, through
  [`stream`](#stream-a-response), and a vendor SDK's own requests through
  [a transport over the service](#hand-a-vendor-sdk-the-service). Routes that leave
  both fields alone are unaffected, and an inference route is always sensitive and
  always requires the acknowledgement, which is the same gate. [Build a question-answering
  agent](../recipes/query-agent.md) declares such a route for a model call.
- The `HttpServiceSpec` / `HttpOperationSpec` / `HttpServicePort` contracts live
  in core; `forze_http` provides the httpx transport and wiring.
