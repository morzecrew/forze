---
title: FastAPI route generators
icon: lucide/route
summary: The attach_*_routes helpers — what each projects, the rest/rpc styles, and how auth surfaces in OpenAPI
---

The `attach_*_routes` helpers project a frozen operation registry onto a FastAPI
router: request and response schemas come from the operation descriptors, and
each route's `operationId` is the registry operation key verbatim (`notes.get`),
so the HTTP surface, MCP tool names, and the operation catalog share one
identity. Setup (lifespan, middleware, error mapping) lives on the
[FastAPI integration](../integrations/fastapi.md) page; this is the catalog of
what each generator produces.

## Document routes

`attach_document_routes` projects the document operations of a frozen registry
(built with `build_document_registry`) onto a router you own:

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
plan-declared [deadline](../running-in-prod/deadlines.md) surfaces on each generated
route as an `x-deadline-seconds` OpenAPI extension and a "Time budget" line in
its description, so API clients can set their own timeouts. Merging a
soft-deletion registry (`build_soft_deletion_registry`) into the document
registry adds its `delete`/`restore` operations to the same router automatically.

Instead of `ns=spec.default_namespace` you can pass `resource="notes"` — the
attacher builds the namespace for you (`StrKeyNamespace(prefix="notes")`). It
must equal the prefix the operations were registered under (the kit builders
default to `spec.default_namespace`); `ns=` and `resource=` are mutually
exclusive, so provide exactly one. To move a single operation off its default
path, pass `path_overrides={DocumentKernelOp.GET: "/by-id/{id}"}` (keyed like
`include=`). Only the path changes — the `operationId` stays the verbatim catalog
key, so the HTTP/MCP/catalog identity is preserved; an override that drops a
placeholder the default path binds (e.g. `{id}`) is a configuration error rather
than a silent demotion to a query parameter. Both knobs are available on every
`attach_*_routes` helper.

### Styles: rest vs rpc

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

## Search routes

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

## Storage routes

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

### Direct & resumable uploads

For large or direct uploads the application stays **out of the data path**: the
browser (or an Uppy client) transfers bytes straight to/from the object store
through short-lived presigned URLs, and the app only mints URLs and orchestrates
the session. `build_storage_registry` already wires these ops; the same
`attach_storage_routes` call projects them when they are registered (JSON-body
`POST`s; `rest` paths shown):

- `POST /files/presign/download` → a presigned `GET` url (`{key, expires_in}`).
- `POST /files/presign/upload` → a presigned `PUT` url + headers
  (`{key, expires_in, content_type?}`).
- `POST /files/uploads` (201) → a multipart session `{key, upload_id, ...}`.
- `POST /files/uploads/parts/url` → a presigned `PUT` url for one part
  (`{session, part_number, expires_in}`).
- `POST /files/uploads/parts` → the parts already uploaded (`{session}`) — the
  resume primitive.
- `POST /files/uploads/complete` → the assembled object's head
  (`{session, parts}`).
- `POST /files/uploads/abort` (204) → discard an unfinished session.

The browser flow for a resumable multipart upload:

1. **begin** — `POST /files/uploads` with the target key; keep the returned
   `upload_id` (it is the resume/complete handle — round-trip it on every later
   call).
2. **request part URLs** — for each part, `POST /files/uploads/parts/url` with
   the session and a 1-indexed `part_number`.
3. **PUT parts** — the browser `PUT`s each part's bytes **directly and in
   parallel** to its presigned URL (the app never sees the bytes). To resume an
   interrupted upload, `POST /files/uploads/parts` to learn which parts landed,
   then presign only the missing ones.
4. **complete** — `POST /files/uploads/complete` with the session and the part
   list (each part carries the `etag` the client got back from its `PUT`); the
   response is the assembled object's head.

!!! warning "Guard these endpoints — and never log the URL"

    Minting an upload URL (or beginning a multipart session) **grants write to a
    key**, so these endpoints should sit behind authn/authz — treat them like the
    `deactivate` route. `presign_upload` and every multipart-session op are
    *command* ops, so you can bind `AuthnRequired` + authz hooks on them in your
    registry (they then show up protected under `apply_openapi_security`). The
    minted URL is a **bearer credential**: it appears in the response body the
    client needs but is never logged (the access-log middleware logs only request
    path/status/duration, never the response body) — prefer short `expires_in`
    windows.

    A presign/multipart op on a **client-side-encrypting** route is refused by the
    adapter (the app never sees the bytes, so it cannot encrypt them) and the
    error propagates to a normal error status. **Server-side** (SSE/CMEK)
    encryption is transparent and does not refuse.

## Authn routes

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

### API-key management

`attach_authn_routes` also generates **self-service API-key management** as a
resource collection (all `AuthnRequired`): `POST /api-keys` issues a key for the
caller — the raw secret is in the response **once**, optionally a user→agent
delegation key (`actor_principal_id`) with a human `label` — `GET /api-keys`
lists the caller's keys as non-secret descriptors (a `hint` like `ab12…wxyz`,
never the secret), and `DELETE /api-keys/{id}` revokes one. This is the minting
surface for the [MCP API-key flow](../integrations/mcp.md#protect-it-with-api-key-auth):
the user issues a key here and pastes it into the agent host.

Identity, invocation metadata, and error mapping stay with the middlewares and
exception handlers from the [integration setup](../integrations/fastapi.md) —
generated routes only validate the input DTO and run the operation through the
normal pipeline.

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
