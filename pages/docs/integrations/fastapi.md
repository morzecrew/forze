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

### Build the runtime at startup

Pass `build` instead of a runtime, and the lifespan builds it at startup and drops
it at shutdown. Use it when an app factory must import without the environment its
runtime needs, such as secrets to validate or clients to connect. Use it too when
every worker process needs its own runtime. The routes are wired before that
runtime exists, so wire them to the lifespan, which hands them the runtime once it
is built:

```python
from fastapi import APIRouter, FastAPI
from forze.application.execution import build_runtime
from forze_fastapi import runtime_lifespan
from forze_fastapi.middlewares import InvocationMetadataMiddleware
from forze_fastapi.routes import attach_readiness_route


def create_app() -> FastAPI:
    lifespan = runtime_lifespan(build=lambda: build_runtime(...))
    app = FastAPI(title="Orders API", lifespan=lifespan)
    app.add_middleware(InvocationMetadataMiddleware, ctx_dep=lifespan.get_context)

    router = APIRouter()
    attach_readiness_route(router, lifespan.runtime)
    app.include_router(router)

    return app
```

`lifespan.get_context` is the `ctx_dep` for middlewares and generated routes, and
`lifespan.runtime` is the runtime provider that `attach_readiness_route` takes. A
`build` that raises fails the startup, so the server never serves. The lifespan
holds one runtime at a time. Entered again while it runs, it refuses with
`configuration` rather than build a second one. Entered after its runtime was torn
down, it builds a new one.

## Bind request context

Two ASGI middlewares attach the per-request context, both given a factory that
returns the current `ExecutionContext` — `runtime.get_context` is that factory
(`lifespan.get_context` when the lifespan [builds the runtime](#build-the-runtime-at-startup)):

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

Outside a short list of exact paths, a request whose credential fails to verify is
refused before routing, and so is one that no ingress authenticates at all
(`AuthnRequirement(required=True)`, the default; a CORS preflight passes, for a CORS
layer to answer). That list is the handful of paths that exist *because* the caller has
no working credential — login, refresh, password reset, cookie-mode logout, and any
public page such as FastAPI's `/docs` and `/openapi.json`. Name them exactly, as
mounted:

```python
app.add_middleware(
    SecurityContextMiddleware,
    ctx_dep=runtime.get_context,
    anonymous_paths={"/auth/login", "/auth/refresh", "/docs", "/openapi.json"},
)
```

On those paths a missing credential, or an **authentication-kind** failure — an
expired or invalid credential, ambiguous credentials, a tenant mismatch — binds no
identity instead of 401ing, so the route authenticates from its body or serves
anonymously. A valid credential still binds normally, and every
other failure kind still returns the error response: a secrets-store outage is a
server fault, not a missing credential. This matters most in
[cookie mode](../reference/fastapi-routes.md#cookie-mode), where a stale access
cookie rides every request including the one that would replace it. Paths are
exact, never prefixes.

On an application mounted under a prefix, with the middleware added to the mounted
application, an entry may be the full path or the path relative to the mount:

```python
api = FastAPI()
api.add_middleware(
    SecurityContextMiddleware,
    ctx_dep=runtime.get_context,
    anonymous_paths={"/auth/login", "/auth/refresh"},  # relative to the mount
)
app.mount("/v1", api)  # /v1/auth/login is anonymous; "/v1/auth/login" works too
```

A server `root_path` counts as a mount the same way. An entry whose leading path segments
are the mount prefix (`/v1` or `/v1/...` under `/v1`, not `/v1x`) is only ever read as a
full path, so no entry names two routes. A middleware added to the parent application
sees no mount: list full paths there. `bypass_paths` and `allowed_websocket_paths`, below,
read their entries the same way.

The relative reading trusts the ASGI `root_path`, so it must come from server
configuration: a `Mount`, or the server's `--root-path`. Never derive it from a request
header. A middleware that sets it from `X-Forwarded-Prefix` lets the client choose the
prefix, and a relative entry would then admit paths the startup checks never verified.
The startup checks see only the app's own `root_path` and its mounts. A root path the
server sets (`--root-path`, typically behind a proxy that strips the prefix) is invisible
to them: under one, list entries relative to it, which match with or without it.

Probe paths want the opposite treatment. Both middlewares resolve the execution
context on every HTTP request, so in front of `/livez` they answer 500 while the
runtime scope is not yet open — precisely the window a liveness probe exists to
observe. Name the paths neither middleware should run for at all:

```python
from forze.base.logging import DEFAULT_HEALTH_PATHS

app.add_middleware(
    InvocationMetadataMiddleware,
    ctx_dep=runtime.get_context,
    bypass_paths=DEFAULT_HEALTH_PATHS,
)
```

A bypassed path serves with no identity, no tenant and no invocation envelope
bound, and no error shaping — list probe and scrape paths, never anything that
reads or writes tenant data. `anonymous_paths` is the softer tool: it still runs
the middleware, and still binds a valid credential. Paths are exact here too, so
a route *under* a bypassed path stays governed — and they include every router
prefix, since middleware runs before routing: list `/api/livez`, not `/livez`, for a
router included under `/api`. On an application mounted under a prefix, an entry may be
the full path or the path relative to the mount, as for `anonymous_paths`.

`check_bypass_paths` fails the boot on
the three ways that goes wrong: a bypassed path serving a **generated operation
route** (it would read and write tenant data with nothing bound), the two
middlewares carrying **different sets** (the one still resolving the context fails
the request anyway, so the bypass reads as configured and does nothing), and a
non-empty set matching **no route at all** — the prefix mistake above. A superset
is fine and expected: `DEFAULT_HEALTH_PATHS` names ten paths and most apps serve
three. In [cookie mode](../reference/fastapi-routes.md#cookie-mode),
`check_authn_cookie_paths` fails the boot when
the refresh cookie's path does not cover the logout route, or when
`anonymous_paths` leaves out the login, refresh or logout route. Under a root path,
list those paths relative to it: behind a proxy that strips the prefix the middleware
never sees it, so a full-path entry is refused, and so is a `root_path` declared with a
trailing slash. The checks know only the app's own `root_path`. They find the
middlewares by the fields they declare, so one wrapped in your own middleware or factory
is not checked.

`runtime_lifespan` runs all three checks through `run_startup_checks(app)`. An app that
supplies its own lifespan calls that function there; none of the checks runs on its own:

```python
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from forze_fastapi import run_startup_checks


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    run_startup_checks(app)

    async with build_runtime(...).scope():
        yield
```

Give the lifespan to the outermost app. Starlette never sends lifespan events to a
mounted application, so a mounted app's own lifespan never runs: neither its runtime
nor its checks would start. Run from the outermost app, the checks walk every Starlette
or FastAPI app mounted beneath it, and hold each one's middlewares against the routes
they reach, at the path they see. A bypass on the outer app that reaches a generated
route of a mounted one fails the boot too.

Routing takes the first match in declaration order, and a mount matches every path
under its prefix. The checks treat a route declared after a mount that matches its path
as unreachable, so an entry naming only that route fails the boot.

The checks cannot see into any other mounted ASGI app, such as a Prometheus
`make_asgi_app()`. A bypass or websocket allowlist entry that could admit a request into
one fails the boot, even when a route declared before that app would serve it. A mount
at `/` or a `Host` route is reached by every entry, and a mount with path parameters
(`/api/{tenant}`) by every path under its static prefix. A `StaticFiles` mount is the exception: it serves files and no
governed operation, and closes every websocket, so a single-page app mounted at `/` with
`StaticFiles(html=True)` keeps its `/livez` bypass. Mount it after the probe routes. For
any other app, two layouts pass:

- serve the probe or scrape endpoint as a route of the gated app (`attach_metrics_route`
  for Prometheus), and bypass that route;
- mount the other app on an outer app beside the gated one, outside its middlewares:

```python
root = FastAPI(lifespan=runtime_lifespan(runtime))
root.mount("/metrics", make_asgi_app())  # no gate in front of it
root.mount("/api", api)  # api carries the middlewares and their bypass_paths
```

A mounted FastAPI app must not declare its own `root_path`. FastAPI writes it over the
one the mount passes down, so its routes are matched against another prefix and the
checks cannot tell what its middlewares see. The checks refuse such a mount unless the
value equals the mount's own prefix.

When an upstream Forze service forwards its remaining [time
budget](../running-in-prod/deadlines.md) as `X-Forze-Deadline-Budget`, opt in to
honoring it with `InvocationMetadataMiddleware(...,
bind_deadline_from_header=True)` — binding is tighten-only, so a forged value
can only shorten the sender's own request.

Both middlewares **refuse raw websocket scopes** (the upgrade handshake is closed
with a policy violation): identity, tenancy, and the envelope are resolved for HTTP
only, so a raw `@app.websocket` route would otherwise run with none of them —
silently. Framework-attached websocket routes (`attach_realtime_ws_route`) are
allowlisted by **exact path** (router prefixes included; on a mounted application, the
full path or the path relative to the mount) with
`allowed_websocket_paths={"/realtime/ws"}` — they resolve identity at connect
themselves, and `check_websocket_allowlist` (run by `run_startup_checks`) fails the boot
if an allowlisted path doesn't serve exactly one governed route. Only if you deliberately self-manage
your own websocket routes, opt out app-wide with `allow_raw_websockets=True`; you
then own identity, tenancy, and error shaping on every websocket route yourself.
For governed duplex realtime, use the [Socket.IO integration](socketio.md) or the
WebSocket route below; for server-push, the SSE route below.

A browser front end on another origin needs CORS, and it reads forze's own response
headers — the `X-Error-Code`, the request and correlation ids, a download's filename,
validator and range headers, a throttled error's `Retry-After` —
only once they are exposed. `AllowlistCORSMiddleware` is Starlette's `CORSMiddleware`
answering the origins an allowlist names (a port range or `*` on a loopback host, for a dev
server) and exposing `EXPOSED_RESPONSE_HEADERS`:

```python
from forze_fastapi.middlewares import AllowlistCORSMiddleware

app.add_middleware(
    AllowlistCORSMiddleware,
    allowed_origins=["https://app.example.com", "http://localhost:5173-5199"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

Add it last, so it is outermost: a refusal from the security middleware then carries the
CORS headers too, and the browser can read its error code. An app that renames the invocation headers, or sets its own, passes
`expose_headers=(*EXPOSED_RESPONSE_HEADERS, "X-Mine")`.

## Map errors to HTTP

`register_exception_handlers` turns a `CoreException` into a response — the kind
decides the status, the `code` rides both the body and the `X-Error-Code` header, and
details are exposed only when the kind's [egress policy](../writing-operation/errors.md) allows:

```python
from forze_fastapi.exceptions import register_exception_handlers

register_exception_handlers(app)
# raise exc.not_found("...") in a handler → 404 {"detail": "...", "code": "core.not_found",
#                                                "kind": "not_found"}
```

The body is `{"detail", "code", "kind", "context"}`, the shape the Socket.IO and
WebSocket error frames carry; `context` appears only when the policy exposes details.
Branch on `code`, never on `detail`: a stale `?rev=` on an update answers
`400 {"code": "revision_mismatch", ...}`, which no other 400 shares. A server-side
error keeps its code and gets a generic `detail`; an exception that is not a
`CoreException` answers `core.internal`.

A `throttled` or `unavailable` error withholds its details, but it can still tell the client
when to come back: the response carries a `Retry-After` header read from two `details` keys.

| Key | Value | `Retry-After` |
|-----|-------|---------------|
| `retry_after` | non-negative seconds, `int` or `float` | the seconds, rounded up |
| `retry_at` | an aware `datetime` | that instant, rounded up to the second, as an HTTP-date |

A usable `retry_after` wins when both are set; an unusable one — a negative or non-finite
number, a string — falls through to `retry_at`. When neither is usable (a naive `datetime`
is not, nor one whose zone raises), no header is sent, never an error. The [retry layers](../reference/resilience-tuning.md#retry-backoff)
read the same keys the same way, through `retry_hint`. `retry_after` is also what the
[HTTP client](http.md) puts on an upstream `429`, so a throttle passed through keeps its
hint — but the client caps it at 3600 seconds, so a longer upstream wait arrives as 3600.

## Readiness probe

`attach_readiness_route(router, runtime)` adds a `GET /readyz` that reflects
the runtime's scope state: `200` once lifecycle startup finished, `503 draining`
from the moment a [drain](../running-in-prod/shutdown-and-fleets.md#graceful-deploys)
is announced or shutdown closes the gate, `503 unavailable` before that. Point
your load balancer's readiness check at it so routing stops before any work is
refused.

That answers whether the process is *willing* to take traffic. Pass `probes` to
also ask whether it can *reach* what it needs — see
[Readiness](../running-in-prod/shutdown-and-fleets.md#readiness).

## Graceful shutdown under uvicorn

When the orchestrator sends `SIGTERM`, the replica should withdraw readiness and
run its drain phases while it still serves, and stop accepting only after that.
Uvicorn's own signal handling stops the server at once, so serve the app with
`serve_uvicorn` instead:

```python
import asyncio
from datetime import timedelta

import uvicorn
from forze.application.execution import build_runtime
from forze_fastapi import serve_uvicorn

runtime = build_runtime(
    ...,
    drain_timeout=timedelta(seconds=5),
    shutdown_step_timeout=timedelta(seconds=2),
)
config = uvicorn.Config(app, host="0.0.0.0", port=8000)

asyncio.run(
    serve_uvicorn(
        config,
        runtime,
        drain_budget=timedelta(seconds=5),
        announce_delay=timedelta(seconds=3),
        graceful_timeout=timedelta(seconds=5),
    )
)
```

Once startup has finished, the worst case of that exit is 25 seconds plus 2 per
lifecycle step — the
[sum](../running-in-prod/shutdown-and-fleets.md#fit-the-budget-to-the-grace-period)
`serve_uvicorn` logs at startup. A signal that lands during startup waits for it
first, and startup hooks have no timeout. Past two steps it no longer fits Kubernetes'
default 30-second grace period: set `terminationGracePeriodSeconds` from the
logged figure, with headroom.

On the first `SIGTERM` or `SIGINT` it runs `runtime.drain()` — readiness fails at
once, `announce_delay` passes, the drain phases run — and only then lets uvicorn
exit: stop accepting, finish in-flight requests, run the lifespan shutdown where
the runtime closes its gate and tears down. A drain that fails still lets the
server exit. A `SIGINT` during the drain cancels it and exits without waiting for
open connections; a repeated `SIGTERM` is logged and ignored, so a signal delivered
twice — to the process group and again by a wrapper — cannot cut a deploy short.
A forced exit gives the drain phases up, even those that had not started yet.
Uvicorn then skips the lifespan shutdown, so the runtime tears down only when the
event loop closes and cancels the lifespan — at the end of `asyncio.run`.

- **Graceful timeout.** Unless `config` sets `timeout_graceful_shutdown`,
  `serve_uvicorn` sets it to `graceful_timeout` (10 seconds), so an open SSE
  stream or long poll cannot hold the exit forever. The figures of the
  [exit budget](../running-in-prod/shutdown-and-fleets.md#fit-the-budget-to-the-grace-period)
  are logged when the server starts.
- **Off the main thread, pass `stop`.** Signal handlers can be installed only on
  the main thread, so elsewhere `serve_uvicorn` installs none. Pass
  `stop=asyncio.Event()` and set it on the server's loop — from another thread,
  `loop.call_soon_threadsafe(stop.set)` — to drain and stop as a first signal
  would. Setting it works on the main thread too.
- **A signal during startup drains later.** Until the lifespan startup finishes
  there is nothing to drain: the server exits once it does, and the drain phases
  run in the lifespan shutdown, after the server stopped accepting. A later
  signal cannot cut them short there.
- **No re-raised signal.** Uvicorn raises the signal again once its server
  returns; `serve_uvicorn` does not, so your process decides how it exits.

### Several workers, or a runtime built at startup

`serve_uvicorn` serves one process, with a runtime built before the app. Under
`uvicorn --workers N`, or gunicorn with uvicorn's worker class, every worker runs
uvicorn's own server. And a runtime that `build` makes does not exist until the
app starts. In both cases, give the lifespan a `drain_budget` instead:

```python
from datetime import timedelta

from forze.application.execution import build_runtime
from forze_fastapi import runtime_lifespan

lifespan = runtime_lifespan(
    build=lambda: build_runtime(...),
    drain_budget=timedelta(seconds=5),
    announce_delay=timedelta(seconds=3),
)
```

```bash
uvicorn orders.app:create_app --factory --workers 4 --timeout-graceful-shutdown 5
```

At startup the lifespan takes over the signal handler of the uvicorn server that
runs it, in every worker. The supervisor passes `SIGTERM` on to each worker. Each
worker then drains its own runtime as `serve_uvicorn` would: readiness fails and
the drain phases run while the worker still serves. Only then does it let its
server exit: stop accepting, finish in-flight requests within the graceful timeout,
and tear down. A connection that arrives after that is not served. It is refused,
or closed unanswered when the supervisor exits.

The signals behave as under `serve_uvicorn`, and so does a signal during startup.
Each worker logs the figures of its worst-case exit at startup. The workers drain
side by side, so that figure is the whole deployment's too. Unless the server sets a
graceful timeout, the lifespan sets it to 10 seconds.

- **Gunicorn.** Its uvicorn worker runs the same server, so the same lifespan drains
  there. Set gunicorn's `--graceful-timeout` above the logged worst case: past it,
  the master kills the worker.
- **Under `serve_uvicorn`,** the lifespan leaves the signals to `serve_uvicorn`.
- **Under another server,** or a uvicorn off the main thread that installs no
  handler, the lifespan logs a warning. The drain phases then run only in the
  lifespan shutdown, after the server stopped accepting.
- **A lifespan that does not use `runtime_lifespan`** wraps its runtime's scope in
  `with drain_ahead_of_uvicorn(runtime, drain_budget=..., announce_delay=...):`.

An app run as `uvicorn app:app` without a `drain_budget` keeps uvicorn's handling,
and pays for it:

- **WebSockets close at once** with code 1012 when the server stops, before the
  lifespan shutdown runs the drain phases that would have closed them gently.
- **The wait for open connections is unbounded** without
  `--timeout-graceful-shutdown`: one SSE client turns every restart into a kill
  at the grace period. Set it at the very least.

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
    # the same stores the gateway fills; retention has no default, so the factory
    # names the window (and a sweeper must be registered to back it)
    mailbox_factory=lambda ctx: build_realtime_mailbox(
        ctx, retention=MailboxRetention(max_age=timedelta(days=7)),
    ),
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
are subscribed per connection with `?topics=a,b` (live-only, like Socket.IO rooms)
and are **fail-closed**: they require an `authorize_topics` resolver — `(ctx,
principal, tenant, requested) -> granted` — and the connection is refused
(`realtime_topics_unauthorized`) unless every requested topic is granted. Topic
membership is the app's authorization decision, exactly as Socket.IO topic rooms
are joined by app code; the requested set is also bounded (`max_topics`, default
32). Once the deployment registers a tenant resolver, a stream with no tenant bound is
refused with `tenant_required` before it subscribes; the client selects a tenant, as a
token refresh does not clear it.

With the realtime stream route wired `tenant_aware` — a per-tenant stream key, the
[tenancy ladder](../identity-tenancy-enc/multi-tenancy.md)'s `tagged` tier — use
`realtime_sse_sharded_tail_lifecycle_step(hub, shard=shard)` instead: one supervised
tail loop per shard tenant, each bound to its tenant, so signals fan out under the
**stream's** trusted identity rather than an untrusted header (the SSE analog of
`TenantShardedSignalSource`). Hand it the same `RealtimeShard` the publish-side
steps use. The plain tail reads with no tenant bound, so over a `tenant_aware` route it
refuses to start (`tenant_global_required`).

Pass the **same** presence store the Socket.IO side uses
(`attach_realtime_sse_route(..., presence=...)`) and open SSE streams join their
principal/topic rooms for the connection's lifetime — so "is this user online"
counts an SSE stream and a socket identically. With a TTL-backed store (e.g.
`RedisRealtimePresence`) also register
`realtime_sse_presence_heartbeat_lifecycle_step(hub, presence)` so live streams
re-assert within the TTL. The heartbeat runs with no tenant bound, so it refuses a store
on a client routed per tenant; rooms already carry their tenant. A closing stream leaves its rooms even when it closes cancelled —
its client gone, or cut off by a drain — within 2 s; past that, a warning is logged and the
rest expire with the store's TTL.

## Realtime over raw WebSocket

`attach_realtime_ws_route` is the duplex sibling for clients that cannot run
Socket.IO (strict-protocol peers, non-JS embedded clients): the same replay + live
egress as SSE — sharing the hub, presence store, and topic authorization — plus a
typed ingress. Identity is resolved by an app-supplied resolver from the upgrade
request (add the path to the middlewares' `allowed_websocket_paths`), which must return
the identity with its `actor` chain intact: a delegated identity is refused; the ack rides
inline (`{"type": "realtime.ack", "up_to"}`), a rotating token refreshes in place
(`realtime.reauth`, same principal and tenant only), and — given a frozen registry
plus `RealtimeCommandRoute` declarations (the same ones a Socket.IO namespace
router registers) — `{"type": "cmd", …}` frames dispatch through the identical
governed operation path as HTTP, error-acked with the shared envelope and bounded
by in-flight and frame-size limits. The full framing is normative in the
[realtime wire protocol](../reference/realtime-protocol.md); `asyncapi_document`
documents WS commands via its `commands=` parameter.

### Authenticate the connection

`build_ws_connection_resolver` is the shipped resolver, so the ~30 lines every app
writes by hand — pick a credential, verify it, carry the expiry, handle the reauth —
exist once. The ladder is fixed: a `realtime.reauth` payload, then the cookie, then
`Authorization: Bearer`, then the query parameter, each source enabled by naming it.
The first source that *presents* a credential is the one used; an invalid one refuses
the connection rather than falling through to the next.

```python
from forze_fastapi.realtime import attach_realtime_ws_route, build_ws_connection_resolver

attach_realtime_ws_route(
    router,
    ctx_dep=ctx_dep,
    resolve=build_ws_connection_resolver(
        ctx_dep=ctx_dep,
        authn_spec=spec,                     # verified through the wired authn plane
        cookie_name="forze_access",          # the browser path — pair it with the allowlist below
        origin_allowlist_attested=True,      # your statement that the pairing is there
    ),
    allowed_origins=["https://app.example.com"],  # the entire cross-site perimeter
    mailbox_factory=..., cursors_factory=...,
)
```

Four things to know before you wire it:

- **Cookie mode requires `allowed_origins`.** A browser attaches the cookie to a
  cross-site upgrade by itself and the handshake has no CORS preflight, so the
  server-side Origin check is the whole defense. The factory cannot see the attach
  call's argument, so it asks you to attest it — building cookie mode without
  `origin_allowlist_attested=True` is a configuration error. Entries are exact
  origins, or a port range on a loopback host for a dev server
  (`http://localhost:5173-5199`, `http://localhost:*`).
- **The query-parameter source is off by default.** Query strings land in access logs,
  proxy logs and anything that reads a URL. Enable `query_param="token"` only for
  clients that can set neither cookie nor header, with short-lived tokens.
- **`expires_at` comes from the verified credential**, and the route enforces it
  continuously — the socket closes once past it, and a `realtime.reauth` frame swaps
  in a fresh one without reconnecting.
- **The tenant is the resolver's binding**, looked up on the authn spec's own route
  (the shipped tenancy module registers it there), falling back to the issuer's own
  claim; the `X-Tenant-Id` header is *not* honored here, because an upgrade's headers
  are set by the client being authenticated. Once the deployment registers a tenant
  resolver on any route, not only this one, a connection that resolves no tenant is
  refused with `tenant_required`, which asks the client to select a tenant (a token
  refresh does not clear it).

`forze_socketio.build_socketio_connection_resolver` is the same ladder over a
Socket.IO handshake, so a deployment serving both transports authenticates by one set
of rules.

## Drain the realtime tails

A replica that leaves with open SSE streams and WebSocket tails should end them while it
still serves, a few at a time, so the clients reconnect to replicas that are still ready
instead of all at once when the server stops. Both live legs subscribe to the same hub, so
one drain step covers them. Register it beside the tail step:

```python
from datetime import timedelta

from forze_fastapi.realtime import RealtimeSseHub, realtime_sse_drain_lifecycle_step

hub = RealtimeSseHub()
step = realtime_sse_drain_lifecycle_step(
    hub,
    window=timedelta(seconds=10),  # spread the ends over this
    tail=timedelta(seconds=2),
)
```

When the runtime drains — on `SIGTERM` under [`serve_uvicorn`](#graceful-shutdown-under-uvicorn)
— its phase ends every subscription the hub holds, end *i* of *n* at `i × window / n`,
with the window fitted to the phase's allowance like the
[Socket.IO drain](socketio.md#drain-a-replica):

- **An SSE stream** finishes its response, and the browser reconnects with its
  `Last-Event-ID`.
- **A WebSocket tail** closes with 1012 (service restart), and the client reconnects with
  `last_event_id`.

An SSE stream still replaying the mailbox when the phase reaches it ends once its replay
has. One not finished 5 s after its end — blocked writing to a client that stopped reading,
say — is cut off: its connection closes, the server logs the response as incomplete, and the
browser resumes from its `Last-Event-ID` all the same. A WebSocket
closes at once, mid-replay or mid-write to a slow client — the close is bounded — and
resumes from `last_event_id` elsewhere. A WebSocket route attached without a
hub has no live tail, and the drain does not reach it.

While the replica drains, from the drain's announcement on, the routes turn new connections
away. The drain step tells them through the hub, so it does not matter what `ctx_dep`
returns; a route without a hub reads the state from that context instead:

- **A new SSE request** is answered 200 with a stream that carries only a `retry:` of
  1–3 seconds, jittered, and ends at once, with `X-Error-Code: realtime_draining`. A
  browser `EventSource` reconnects after it, through the balancer; any status but 200 would
  end it for good.
- **A new WebSocket upgrade** is accepted and then closed with 1012, as the route's other
  refusals are, because a close before accept is an opaque 403.

## What it provides

Unlike a backend, FastAPI doesn't implement Forze contracts — it's the edge that
runs them. The surface, at a glance:

| Piece | What it does |
|-------|--------------|
| `runtime_lifespan` | run the runtime's lifecycle from the app lifespan; build the runtime at startup; drain it on `SIGTERM` in every uvicorn worker |
| `serve_uvicorn` | an embedded uvicorn whose `SIGTERM` drains the runtime before the server stops |
| `drain_ahead_of_uvicorn` | the `SIGTERM` drain `runtime_lifespan` installs, for a lifespan that does not use `runtime_lifespan` |
| `InvocationMetadataMiddleware` / `SecurityContextMiddleware` | bind per-request context, identity, and tenant |
| `CustomHeadersMiddleware` / `LoggingMiddleware` | inject response headers; sampled, probe-excluded access logs |
| `register_exception_handlers` | map a `CoreException` to an HTTP response by kind |
| `attach_readiness_route` | a drain-aware `GET /readyz` probe, optionally sweeping dependency `health()` |
| `attach_document_routes` / `attach_search_routes` / `attach_storage_routes` / `attach_authn_routes` | project a frozen registry's operations onto a router |
| `attach_realtime_sse_route` / `realtime_sse_tail_lifecycle_step` | realtime egress over SSE: mailbox replay + per-node live tail |
| `realtime_sse_sharded_tail_lifecycle_step` | tenant-aware SSE: per-tenant tail loops, tenant trusted from the stream |
| `realtime_sse_presence_heartbeat_lifecycle_step` | SSE streams report into the shared presence store (TTL heartbeat) |
| `realtime_sse_drain_lifecycle_step` | end the SSE streams and WebSocket tails spread over a window when the replica drains |
| `attach_realtime_ws_route` | duplex realtime over raw WebSocket: replay + live egress, inline ack/reauth, governed `cmd` dispatch |
| `build_ws_connection_resolver` | the shipped connection resolver: reauth payload, then cookie, header and opt-in query; credential expiry |
| `attach_asyncapi_route` | serve the app-built AsyncAPI document, `/openapi.json`-style |
| `apply_openapi_security` | declare the auth scheme in the generated OpenAPI |
| `require_permission` | a dependency gating a route that runs no operation, with the authz hook's own decision |

## Notes

- **No external service** — FastAPI runs in-process; the runtime's lifecycle owns
  the backing clients.
- **You write or generate routes.** Handlers resolve the context and run
  operations; the `attach_*_routes` helpers project a frozen registry, but you
  still mount the router.
- **Identity is required at the boundary, and authorized in the engine.** The
  middleware refuses a request no ingress authenticates outside `anonymous_paths`
  (`AuthnRequirement(required=False)` turns that off); authorization lives in the
  engine's authz hooks, and `apply_openapi_security` only documents it. A hand-written route that runs no operation gates itself with
  [`require_permission`](../identity-tenancy-enc/identity.md#gating-a-route-that-runs-no-operation).
- **Guard write-granting routes.** `deactivate`, presigned-upload, and
  multipart-session endpoints ship unguarded or grant write — bind authn/authz
  before exposing them.
