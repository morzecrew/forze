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
budget](../in-depth/deadlines.md) as `X-Forze-Deadline-Budget`, opt in to
honoring it with `InvocationMetadataMiddleware(...,
bind_deadline_from_header=True)` — binding is tighten-only, so a forged value
can only shorten the sender's own request.

## Map errors to HTTP

`register_exception_handlers` turns a `CoreException` into a response — the kind
decides the status, the `code` rides an error-code header, and details are
exposed only when the kind's [egress policy](../in-depth/errors.md) allows:

```python
from forze_fastapi.exceptions import register_exception_handlers

register_exception_handlers(app)
# raise exc.not_found("...") in a handler → 404 {"detail": "..."}
```

## Readiness probe

`attach_readiness_route(router, runtime)` adds a `GET /readyz` that reflects
the runtime's scope state: `200` while serving, `503 draining` once shutdown
flips the [drain gate](../in-depth/shutdown-and-fleets.md), `503 unavailable`
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

`attach_document_routes` projects the document operations of a frozen registry
(built with `build_document_registry`) onto a router you own. Request and
response schemas come from the operation descriptors, and each route's
`operationId` is the registry operation key verbatim (`notes.get`) — the HTTP
surface, MCP tool names, and the operation catalog share one identity:

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

Only operations the registry actually holds are attached, so a read-only spec
yields a read-only router; narrow further with `include={"get", "list"}`. A
plan-declared [deadline](../in-depth/deadlines.md) surfaces on each generated
route as an `x-deadline-seconds` OpenAPI extension and a "Time budget" line in
its description, so API clients can set their own timeouts.
Merging a soft-deletion registry (`build_soft_deletion_registry`) into the
document registry adds its `delete`/`restore` operations to the same router
automatically.

Both styles use the same REST verbs; they differ only in how a resource is
addressed — REST puts the id in the path, RPC keeps one operation-named path per
operation (mirroring the catalog one-to-one) and puts the id in a query
parameter:

- `"rest"` — resource paths: `POST /notes` (201), `GET /notes/{id}`,
  `PATCH /notes/{id}?rev=` with the patch DTO as body, `DELETE /notes/{id}`
  (204, hard delete). Soft deletion surfaces as action sub-paths —
  `POST /notes/{id}/delete?rev=` and `POST /notes/{id}/restore?rev=`. List
  operations keep `POST /notes/list`-style paths since their filter bodies
  have no natural REST verb.
- `"rpc"` — `GET /notes/get?id=`, `PATCH /notes/update?id=&rev=` with the patch
  DTO as body, `DELETE /notes/kill?id=` (204), and `PATCH /notes/delete?id=&rev=`
  / `PATCH /notes/restore?id=&rev=` for soft deletion. `create` and the list
  operations keep `POST /notes/<op>` with the input DTO as body (a new entity or
  a filter payload has no id to address). So an RPC read is a plain linkable,
  cacheable `GET`, not an opaque `POST`.

`attach_search_routes` does the same for a search registry
(`build_search_registry` or its hub/federated siblings). Search requests are
filter/query bodies with no natural REST verb, so there is no style choice —
every operation is `POST /<op>`:

```python
from forze_fastapi.routes import attach_search_routes

search_router = APIRouter(prefix="/notes/search", tags=["notes"])

attach_search_routes(
    search_router,
    registry=search_registry,  # build_search_registry(search_spec).freeze()
    ns=search_spec.default_namespace,
    ctx_dep=runtime.get_context,
)
```

`attach_storage_routes` covers a storage registry (`build_storage_registry`)
and takes the same explicit `style`. Transport shapes are fixed by the
payloads — multipart upload (the file plus optional `description`/`prefix`
form fields), the listing DTO as JSON body, the object bytes back with content
type and a `Content-Disposition` filename, a void delete (204); keys may
contain slashes. The style only decides paths and verbs:

- `"rest"` — `POST /files` (201), `POST /files/list`, `GET /files/{key}`,
  `DELETE /files/{key}`.
- `"rpc"` — `POST /files/upload`, `POST /files/list`,
  `GET /files/download/{key}`, `DELETE /files/delete/{key}`. The key rides the
  path tail in both styles since it is slash-bearing, not a JSON field.

```python
from forze_fastapi.routes import attach_storage_routes

files_router = APIRouter(prefix="/files", tags=["files"])

attach_storage_routes(
    files_router,
    registry=storage_registry,  # build_storage_registry(storage_spec).freeze()
    ns=storage_spec.default_namespace,
    ctx_dep=runtime.get_context,
    style="rest",
)
```

`attach_authn_routes` projects an authn registry (`build_authn_registry`) the
same way. Authn flows are RPC-shaped with one natural surface each, so there is
no style choice — fixed action paths: `POST /login` and `POST /refresh` (200,
token response), `POST /logout` (204, no body), `POST /change-password` (204),
`POST /password-reset/request` (202, uniform ack — never the token) and
`POST /password-reset/confirm` (204), and `POST /deactivate` (204). Login,
refresh, and the password-reset pair are meant to be reachable without a bearer
token (the operations authenticate via their bodies, or deliberately not at all
for the reset request); logout and change-password declare `AuthnRequired` (so
they 401 without a bound identity and show up protected under
`apply_openapi_security`), while `deactivate_principal` ships unguarded — bind
`AuthnRequired` + authz hooks on it or exclude it via `include=`. The full
wiring (including how the reset token reaches the user via the outbox) is in the
[Authn, authz & tenancy recipe](../recipes/authn-authz-tenancy-fastapi.md#http-login-endpoints).

It also generates **self-service API-key management** as a resource collection
(all `AuthnRequired`): `POST /api-keys` issues a key for the caller — the raw
secret is in the response **once**, optionally a user→agent delegation key
(`actor_principal_id`) with a human `label` — `GET /api-keys` lists the caller's
keys as non-secret descriptors (a `hint` like `ab12…wxyz`, never the secret), and
`DELETE /api-keys/{id}` revokes one. This is the minting surface for the
[MCP API-key flow](mcp.md#protect-it-with-api-key-auth): the user issues a key
here and pastes it into the agent host.

Identity, invocation metadata, and error mapping stay with the middlewares and
exception handlers above — generated routes only validate the input DTO and run
the operation through the normal pipeline.

## Document auth in OpenAPI

`SecurityContextMiddleware` extracts identity; it doesn't tell the schema. By
default the generated OpenAPI (and the Scalar/Swagger UI) shows every endpoint as
open — no Authorize button. `apply_openapi_security` closes that gap from the
**same** `AuthnRequirement` you handed the middleware, so the scheme is declared
once:

```python
from forze.application.contracts.authn import AuthnSpec
from forze_fastapi.security import (
    AuthnRequirement,
    HeaderTokenAuthn,
    apply_openapi_security,
)

# Your authn aggregate's spec — the same one your routes and the engine resolve.
API = AuthnSpec(name="api", enabled_methods=frozenset({"token"}))

requirement = AuthnRequirement(
    ingress=(HeaderTokenAuthn(authn_spec=API, header_name="Authorization"),),
)
app.add_middleware(SecurityContextMiddleware, ctx_dep=runtime.get_context, authn=requirement, ...)

# After every router is attached:
apply_openapi_security(app, requirement)
```

It registers one `securityScheme` per ingress (bearer for a token on
`Authorization`; `apiKey` in header or cookie otherwise) and attaches a `security`
requirement — the ingress methods as alternatives — to exactly the operations the
catalog flagged as needing a bound principal. That flag (`requires_authn`) is
derived at freeze from the plan's `AuthnRequired` or authz hooks, so protected
routes advertise the scheme while token-minting routes (`/login`, `/refresh`) stay
open. Use `exclude={"orders.deactivate", ...}` to leave a flagged operation open.

This **documents** auth; it doesn't enforce it — enforcement stays in the engine
(the `AuthnRequired`/authz hooks) and identity extraction in the middleware.
