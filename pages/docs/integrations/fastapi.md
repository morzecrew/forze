---
title: FastAPI
icon: lucide/globe
summary: Run an ExecutionRuntime behind FastAPI — lifespan, request context, error mapping, and generated routes
---

`forze[fastapi]` connects an `ExecutionRuntime` to a FastAPI app: it runs the
runtime from the app's lifespan, binds per-request context (and identity) via
middleware, and maps `CoreException`s to HTTP responses. Routes are ordinary
FastAPI handlers that resolve the context and run operations — written by hand,
or [generated from an operation registry](#generated-routes).

## Install

```bash
uv add 'forze[fastapi]'
```

No external service — FastAPI is in-process.

## Run the runtime from lifespan

The runtime's lifecycle opens and closes every backing client (Postgres, Redis,
…). `runtime_lifespan` holds `runtime.scope()` open for the app's lifetime —
context created and startup run on app startup, shutdown run (and the context
reset) on app shutdown, even if the app's lifetime ends with an error:

```python
from fastapi import FastAPI
from forze.application.execution import build_runtime
from forze_fastapi import runtime_lifespan

runtime = build_runtime(...)  # deps modules + lifecycle modules/steps

app = FastAPI(title="Orders API", lifespan=runtime_lifespan(runtime))
```

`build_runtime` assembles the runtime in one call — it builds and freezes the
deps registry and the lifecycle plan and returns the
[`ExecutionRuntime`](../core-concepts/runtime.md).

## Bind request context

Two ASGI middlewares attach the per-request context, both given a factory that
returns the current `ExecutionContext` — `runtime.get_context` is that factory:

```python
from forze_fastapi.middlewares import (
    InvocationMetadataMiddleware,
    SecurityContextMiddleware,
)

app.add_middleware(InvocationMetadataMiddleware, ctx_dep=runtime.get_context)
app.add_middleware(SecurityContextMiddleware, ctx_dep=runtime.get_context)
```

`InvocationMetadataMiddleware` binds correlation/execution metadata and the
`Idempotency-Key` header; `SecurityContextMiddleware` binds the authenticated
identity and tenant.

When an upstream Forze service forwards its remaining [time
budget](../running-in-prod/deadlines.md) as `X-Forze-Deadline-Budget`, opt in to
honoring it with `InvocationMetadataMiddleware(...,
bind_deadline_from_header=True)` — binding is tighten-only, so a forged value
can only shorten the sender's own request.

Both middlewares **refuse raw websocket scopes** (the upgrade handshake is closed
with a policy violation): identity, tenancy, and the envelope are resolved for HTTP
only, so a raw `@app.websocket` route would otherwise run with none of them —
silently. If you deliberately self-manage websocket routes, opt out per middleware
with `allow_raw_websockets=True`; you then own identity, tenancy, and error shaping
on every websocket route yourself. For governed duplex realtime, use the
[Socket.IO integration](socketio.md); for server-push, the SSE route below.

## Map errors to HTTP

`register_exception_handlers` turns a `CoreException` into a response — the kind
decides the status, the `code` rides an error-code header, and details are
exposed only when the kind's [egress policy](../writing-operation/errors.md) allows:

```python
from forze_fastapi.exceptions import register_exception_handlers

register_exception_handlers(app)
# raise exc.not_found("...") in a handler → 404 {"detail": "..."}
```

## Readiness probe

`attach_readiness_route(router, runtime)` adds a `GET /readyz` that reflects
the runtime's scope state: `200` while serving, `503 draining` once shutdown
flips the [drain gate](../running-in-prod/shutdown-and-fleets.md), `503 unavailable`
before the scope exists. Point your load balancer's readiness check at it so
routing stops before the drain window starts.

## Routes

Routes are ordinary FastAPI handlers. A route resolves the context and runs an
operation through the frozen registry (or a facade) — the domain code stays
untouched:

```python
from forze_kits.aggregates.document import DocumentFacade

@app.post("/orders")
async def create_order(cmd: CreateOrderCmd) -> ReadOrder:
    facade = DocumentFacade(ctx=runtime.get_context(), registry=registry, namespace=order_spec.default_namespace)
    return await facade.create(cmd)
```

## Generated routes

Instead of hand-writing each route, project a frozen operation registry onto a
router with `attach_document_routes`. Request and response schemas come from the
operation descriptors, and each route's `operationId` is the registry operation
key verbatim (`notes.get`) — so the HTTP surface, MCP tool names, and the
operation catalog share one identity:

```python
from fastapi import APIRouter
from forze_fastapi.routes import attach_document_routes

router = APIRouter(prefix="/notes", tags=["notes"])

attach_document_routes(
    router,
    registry=registry,  # build_document_registry(spec, dtos).freeze()
    ns=spec.default_namespace,
    ctx_dep=runtime.get_context,
    style="rest",
)

app.include_router(router)
```

Only operations the registry holds are attached, so a read-only spec yields a
read-only router. Sibling helpers project search, storage (including direct and
resumable uploads), and authn registries the same way, and `apply_openapi_security`
declares the auth scheme in the generated OpenAPI. The full catalog — `rest` vs
`rpc` styles, every endpoint each generator produces, the `include`/`path_overrides`
knobs, and the upload flow — is in
[FastAPI route generators](../reference/fastapi-routes.md).

## Realtime egress over SSE

`attach_realtime_sse_route` serves the [realtime egress
plane](../data-events/realtime.md) as an authenticated `text/event-stream`
endpoint — the browser-native transport when a duplex socket is more than you
need. On connect it replays the offline mailbox past the device's cursor (a
browser-supplied `Last-Event-ID` beats the stored cursor), then tails live
signals from a per-node hub; a `POST …/ack` endpoint alongside carries the
cumulative ack. Frames use the same versioned `{id, data}` envelope as the
Socket.IO gateway — one [wire protocol](../reference/realtime-protocol.md),
two transports:

```python
from forze_fastapi.realtime import (
    RealtimeSseHub,
    attach_realtime_sse_route,
    realtime_sse_tail_lifecycle_step,
)

hub = RealtimeSseHub()
attach_realtime_sse_route(
    router,
    ctx_dep=runtime.get_context,
    mailbox_factory=build_realtime_mailbox,  # the same stores the gateway fills
    cursors_factory=build_realtime_cursors,
    hub=hub,
)
# register alongside the app's lifecycle steps: one supervised tail loop per node
step = realtime_sse_tail_lifecycle_step(hub, stream_spec=realtime_stream_spec())
```

The live leg reads the realtime stream with a plain (non-group) tail — broadcast
semantics, so every node sees every signal, with zero consumer-group lifecycle —
and is at-most-once by contract: the mailbox carries the durable guarantee, and the
Socket.IO gateway remains its sole writer. Without a hub the endpoint is
catch-up-only (the browser's auto-reconnect gives long-poll-style delivery). Topics
are subscribed per connection with `?topics=a,b` (live-only, like Socket.IO rooms).

## What it provides

Unlike a backend, FastAPI doesn't implement Forze contracts — it's the edge that
runs them. The surface, at a glance:

| Piece | What it does |
|-------|--------------|
| `runtime_lifespan` | run the runtime's lifecycle from the app lifespan |
| `InvocationMetadataMiddleware` / `SecurityContextMiddleware` | bind per-request context, identity, and tenant |
| `CustomHeadersMiddleware` / `LoggingMiddleware` | inject response headers; sampled, probe-excluded access logs |
| `register_exception_handlers` | map a `CoreException` to an HTTP response by kind |
| `attach_readiness_route` | a drain-aware `GET /readyz` probe |
| `attach_document_routes` / `attach_search_routes` / `attach_storage_routes` / `attach_authn_routes` | project a frozen registry's operations onto a router |
| `attach_realtime_sse_route` / `realtime_sse_tail_lifecycle_step` | realtime egress over SSE: mailbox replay + per-node live tail |
| `apply_openapi_security` | declare the auth scheme in the generated OpenAPI |

## Notes

- **No external service** — FastAPI runs in-process; the runtime's lifecycle owns
  the backing clients.
- **You write or generate routes.** Handlers resolve the context and run
  operations; the `attach_*_routes` helpers project a frozen registry, but you
  still mount the router.
- **Identity is extracted, not enforced.** Middleware binds the principal;
  enforcement lives in the engine's authn/authz hooks, and `apply_openapi_security`
  only documents it.
- **Guard write-granting routes.** `deactivate`, presigned-upload, and
  multipart-session endpoints ship unguarded or grant write — bind authn/authz
  before exposing them.
