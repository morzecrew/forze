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
form fields), the listing DTO as JSON body, a streamed download with content
type and a `Content-Disposition` filename, a void delete (204); keys may
contain slashes. The style only decides paths and verbs:

- `"rest"` — `POST /files` (201), `POST /files/list`, `GET /files/{key}` (with a
  `HEAD` sibling on the same path), `DELETE /files/{key}`.
- `"rpc"` — `POST /files/upload`, `POST /files/list`,
  `GET /files/download/{key}` (+ `HEAD`), `DELETE /files/delete/{key}`. The key
  rides the path tail in both styles since it is slash-bearing, not a JSON field.

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

### Downloads: streaming, ranges & caching

The `GET` download route **streams by default** (`stream=True`): the body is a
`StreamingResponse` consumed chunk by chunk, so the process never buffers a
whole object and a large download can't OOM it. It is backed by three read-only
query operations the registry carries (`build_storage_registry` registers them;
they acquire only the storage query port): `head`, `download_stream`, and
`download_range`. A plain unconditional `GET` runs a single `download_stream`
operation — the cache validators ride along on its result, no separate `head`
round-trip; a conditional or `Range` request runs `head` first.

The route is a full HTTP range/cache citizen:

| Request | Response |
|---------|----------|
| plain `GET` | **200**, streamed body, `Accept-Ranges: bytes` |
| `If-None-Match` / `If-Modified-Since` match | **304**, no body, validators echoed (`If-None-Match` takes precedence per RFC 7232) |
| `Range: bytes=…` within the object | **206** from a real backend-ranged fetch, with `Content-Range` |
| well-formed `Range` beyond the object | **416** with `Content-Range: bytes */total` |
| malformed / non-`bytes` / multi-range | header ignored per RFC 7233 — full **200** stream |

- **Range cap.** A single `Range` window is buffered, so it is capped —
  `max_range_bytes`, default **16 MiB**. A wider request is served *truncated*
  to the cap with a `206` whose `Content-Range` reports the bytes actually
  returned — a valid partial the client simply re-requests from. Only explicit
  range windows are bounded; a plain download always streams.
- **ETag is the backend's.** `ETag` / `Last-Modified` come from the backend
  head, not from hashing the body. For a client-side-encrypted object the ETag
  is over the *stored ciphertext* — an opaque but stable validator, exactly what
  conditional requests need.
- **Encrypted objects stream too.** A client-side-encrypted object is decrypted
  chunk by chunk on the way out; its plaintext size isn't known up front, so the
  response carries **no `Content-Length`** (chunked transfer). Ranged reads
  still work over chunked-AEAD objects — only the chunks the window covers are
  fetched and decrypted; a legacy *whole-payload* envelope can't be sliced, so a
  `Range` request against one falls back to the full streamed body rather than
  erroring.
- **`HEAD` mirrors the `GET`** on the same path: `Content-Type`,
  `Content-Length`, `ETag`, `Last-Modified`, `Accept-Ranges`, no body. Its
  `Content-Length` is the **stored** size (for an encrypted object, the
  ciphertext length — the streamed `GET` decrypts and uses chunked transfer
  instead).

`stream=False` opts back into the legacy fully-buffered download: the whole
object in memory, an ETag hashed from the body bytes, and `Range` served by
slicing the buffer instead of a backend-ranged fetch. Prefer the default; if you
must keep large objects out of the app entirely, expose `presign_download` (see
below) so the client fetches straight from the backend. When a registry predates
the three streaming operations, the attacher quietly keeps the buffered
endpoint, so enabling `stream` is safe on any registry.

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

## Tenancy routes

`attach_tenancy_routes` projects the self-service tenant-selector registry — fixed
paths, no style choice: `GET /tenants` (the caller's tenants), `POST
/tenants/{id}/activate` (switch the active tenant), `DELETE /tenants/{id}` (204,
leave). `attach_tenancy_admin_routes` projects the admin plane: `POST /tenants`
(201, create), `GET /tenants/{id}/members`, `POST /tenants/{id}/deactivate` (204),
`POST /memberships` (204, invite) and `DELETE /memberships` (204, remove). Admin
operations ship without hooks bound — guard them with `AuthnRequired` + authz hooks
in the registry, like `deactivate`.

## Aggregate routes

`attach_aggregate_routes(router, kit, *, ctx_dep, style="rest", tx_route="default",
storage_prefix="/blobs")` is the composite: given an `AggregateKit` it attaches the
kit's document (+ soft-deletion), search, and — when the kit declares storage —
blob routes (under `storage_prefix`) onto one router with a single call, each
sub-surface exactly as its dedicated attacher would. `tx_route` must match the
transaction route the deps module registers.

## Infrastructure routes

- `attach_jwks_route(router, jwks_provider, *, path="/.well-known/jwks.json",
  cache_max_age=300)` — the JWKS document for token verification; excluded from the
  OpenAPI schema.
- `attach_readiness_route(router, runtime, *, path="/readyz")` — 200 while the
  runtime is active and not draining, 503 otherwise; excluded from the schema.

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
