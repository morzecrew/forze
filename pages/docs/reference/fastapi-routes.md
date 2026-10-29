---
title: FastAPI route generators
icon: lucide/route
summary: The attach_*_routes helpers — what each projects, the rest/rpc styles, and how auth surfaces in OpenAPI
---

The `attach_*_routes` helpers project a frozen operation registry onto a FastAPI
router: request and response schemas come from the operation descriptors, and
each route's `operationId` is the registry operation key verbatim (`notes.get`),
so the HTTP surface, MCP tool names, and the operation catalog share one
identity. Each path or query parameter a generated route declares, such as a
document's `id` and `rev` or a storage `key`, carries its own description in the
schema. Setup (lifespan, middleware, error mapping) lives on the
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
yields a read-only router, and a spec declaring `hard_delete=False` gets no
`DELETE` route; narrow further with `include={"get", "list"}`. A
plan-declared [deadline](../running-in-prod/deadlines.md) surfaces on each generated
route as an `x-deadline-seconds` OpenAPI extension and a "Time budget" line in
its description, so API clients can set their own timeouts. Merging a
soft-deletion registry (`build_soft_deletion_registry`) into the document
registry adds its `delete`/`restore` operations to the same router automatically.

Instead of `ns=spec.default_namespace` you can pass `resource="notes"` — the
attacher builds the namespace for you (`StrKeyNamespace(prefix="notes")`). It
must equal the prefix the operations were registered under (the kit builders
default to `spec.default_namespace`); `ns=` and `resource=` are mutually
exclusive, so provide exactly one (only `attach_authn_routes` can infer it, see
[Authn routes](#authn-routes)). To move a single operation off its default
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

### Uploads: the size cap

The upload route refuses a file over `max_upload_size` (default **64 MiB**,
`DEFAULT_MAX_UPLOAD_SIZE`; `None` disables it) with a 422 coded
`upload_too_large`, before the operation runs. A `Content-Length` over the cap is
rejected early, as a shortcut; the guard is the reader, which counts bytes as it
reads the file, 1 MiB at a time, so at most one chunk past the cap is held in
memory. That bounds memory, not disk: before the route runs, the multipart
parser keeps the first 1 MiB of the file in memory and rolls everything past
1 MiB to a temporary file on disk, with no cap — so cap the request body at your
proxy too.

When an upload takes parameters the generated route doesn't — which record the
file attaches to, say — write the route by hand and read the file with
`read_upload_capped`, the generated route's reader. It refuses with the same 422
status and `upload_too_large` code:

```python
from typing import Any

from fastapi import APIRouter, Depends, UploadFile

from forze.application.execution.operations import run_operation
from forze_fastapi.routes import DEFAULT_MAX_UPLOAD_SIZE, read_upload_capped

records_router = APIRouter(prefix="/records", tags=["records"])


@records_router.post("/{record_id}/attachments", status_code=201)
async def attach_file(record_id: str, file: UploadFile, ctx=Depends(ctx_dep)) -> Any:
    data = await read_upload_capped(file, DEFAULT_MAX_UPLOAD_SIZE)
    args = AttachFile(record_id=record_id, filename=file.filename or "upload", data=data)

    return await run_operation(registry, "records.attach_file", args, ctx)
```

The refusal answers 422 only once the app has called
`register_exception_handlers(app)` (from `forze_fastapi.exceptions`); without it
the error surfaces as a 500.

A stored-file kit's `max_bytes` is a backstop, not a memory guard: it is checked
once the whole upload is in memory, so keep the read capped at the route.

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
must keep large objects out of the app entirely, serve `presign_download` from a
route of your own (see below) so the client fetches straight from the backend. When a registry predates
the three streaming operations, the attacher quietly keeps the buffered
endpoint, so enabling `stream` is safe on any registry.

### Direct & resumable uploads

For large or direct uploads the application stays **out of the data path**: the
browser (or an Uppy client) transfers bytes straight to/from the object store
through short-lived presigned URLs, and the app only mints URLs and orchestrates
the session. `build_storage_registry` already wires these ops; the same
`attach_storage_routes` call projects them when they are registered (JSON-body
`POST`s; `rest` paths shown):

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

`presign_download` gets no generated route. Its result is a bearer credential, so
it is marked sensitive, and generated routes, MCP, agent tools and `forze ops`
refuse it; naming it in `include` is a configuration error. Serve it from your
own route, and guard the operation in the registry as you guard `download`:

```python
from fastapi import Depends

from forze.application.execution.operations import run_operation
from forze.application.hooks.authn import AuthnRequired
from forze.application.hooks.authz import AuthzBeforeAuthorize
from forze_kits.aggregates.storage import (
    PresignDownloadRequestDTO,
    PresignedUrlDTO,
    StorageKernelOp,
    build_storage_registry,
)

ns = storage_spec.default_namespace
storage_registry = (
    build_storage_registry(storage_spec)
    .bind(ns.key(StorageKernelOp.PRESIGN_DOWNLOAD))
    .bind_outer()
    .before(
        AuthnRequired().to_step(),
        AuthzBeforeAuthorize(spec=AUTHZ, action="files:presign_download").to_step(),
    )
    .finish(deep=True)
    .freeze()
)


@files_router.post("/presign/download")
async def presign_download(
    body: PresignDownloadRequestDTO, ctx=Depends(runtime.get_context)
) -> PresignedUrlDTO:
    return await run_operation(storage_registry, "files.presign_download", body, ctx)
```

`presign_upload` and `presign_part` keep their generated routes. They are command
operations, so read-only defaults never expose them: MCP and `forze ops` need
`include_writes=True`, and a route exists only where you attach one. The direct-upload
flow above needs them.

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

### Stored-file downloads

The stored-file kit ships no generated JSON routes. It has four read-only
download operations, and each one loads the row first: a row the caller can't
read (another tenant's, or a missing one) answers not-found, and a `pending` or
`failed` file is refused before storage is touched. `download` is always
registered. The other three are opt-in, because an operation runs only the
guards bound on its own key: enable each with
`StoredFileKitSpec(stream_downloads=True, range_downloads=True, presign_downloads=True)`,
then bind your authn and authz on it as you do on `download`. A
`StoredFileFacade` method for an operation left off raises a configuration
error.

| Operation | Input | Result |
|-----------|-------|--------|
| `download` | `StoredFileIdDTO` | `StoredFileDownloadDTO`: the whole object in memory, for small files and tests |
| `download_stream` | `StoredFileIdDTO` | `StoredFileStream`: the row plus a bounded-memory `StreamedDownload` named after the stored filename |
| `download_range` | `DownloadStoredFileRangeRequestDTO` (`id`, inclusive `start`, `end` or `None` for the end) | `StoredFileRangeDTO`: the row plus the window's bytes, `content_range` and `total_size`; a start past the end is a precondition error |
| `presign_download` | `PresignStoredFileDownloadRequestDTO` (`id`, `expires_in` up to 7 days) | `StoredFilePresignedDownloadDTO`: the row plus a presigned `GET` URL |

A hand-written route hands the stream to `streamed_download_response`, which
builds the generated download route's `200`: `Content-Disposition`, `ETag`,
`Last-Modified`, and `Content-Length` when the size is known. It doesn't answer
`Range` or conditional requests.

```python
from uuid import UUID

from fastapi import APIRouter, Depends

from forze.application.execution.operations import run_operation
from forze_fastapi.routes import streamed_download_response
from forze_kits.aggregates.stored_file import StoredFileIdDTO

files_router = APIRouter(prefix="/files", tags=["files"])


@files_router.get("/{file_id}/content")
async def file_content(file_id: UUID, ctx=Depends(ctx_dep)):
    args = StoredFileIdDTO(id=file_id)
    result = await run_operation(registry, "files.download_stream", args, ctx)

    return streamed_download_response(result.stream)
```

`attach_stored_file_download_route` attaches the full download route instead.
It runs `download_stream` and `download_range`, so it needs both enabled, and it
answers like the storage kit's streaming download: `206` for a `Range`, each
window capped at `max_range_bytes` (16 MiB by default), `416` past the end,
`304` for a matching `If-None-Match` or `If-Modified-Since`, and the streamed
`200` otherwise. Each request opens the stream first, which reads the object's
head but not its body, so a `304` transfers nothing.

```python
from forze_fastapi.routes import attach_stored_file_download_route

files_router = APIRouter(prefix="/files", tags=["files"])

# GET /files/{id}/content
attach_stored_file_download_route(
    files_router,
    registry=registry,
    ns=kit.document.default_namespace,
    ctx_dep=ctx_dep,
)
```

On a client-side-encrypting route the kit's `upload` seals each file as one
AEAD payload, which a range can't slice. `download_range` refuses such a file
with `core.storage.range_whole_payload_unsupported`, and the route answers a
`Range` with the whole file and a `200`.

The stream reads the object in parts while the response is sent. If the file is
deleted mid-download, the next read fails with `not_found`. The status is already
sent, so the response is cut short: the client gets fewer bytes than
`Content-Length`, or a chunked body with no final chunk. A stored file's key is
never reused, so a stream can't mix two files.

`presign_download` follows the storage kit's presign rules: the URL is a bearer
credential and the storage key's tenant check applies. The URL signs in the
stored filename as the response's `Content-Disposition`, so the download saves
under that name rather than the object key's.

Because its result is a credential, `presign_download` is marked sensitive.
MCP tools, agent tools, generated routes and `forze ops` refuse it, so serve it
from a hand-written route like the one above. An MCP server over a registry that
enables it needs an `operations=` allowlist that leaves it out, because the
default exposes every read-only operation and the refusal covers the whole
registration. `download_stream` is not marked sensitive: it returns the same
bytes as `download`, not a credential.

`download_stream` returns a byte stream, and `download` and `download_range`
return raw bytes, not JSON, so their descriptors set `streams=True`. JSON can't
carry a binary file: its bytes aren't UTF-8, so encoding them fails. MCP tools
and resource templates, agent tools, `forze ops` and `attach_operation_routes`
can't carry these operations. The MCP and `forze ops` defaults leave them out,
and naming one anywhere is refused. Serve them from a route that calls
`streamed_download_response`, like the one above.

!!! note "Known limit: encrypting routes refuse per request"

    A client-side-encrypting storage route refuses `presign_download`, but only
    when it is called. The storage adapter refuses before anything is signed. The
    kit spec doesn't know a route's encryption config, so setting
    `presign_downloads=True` for such a route can't fail at build time.

### Stored-file direct uploads

`upload` takes the whole file in the request body. For large files,
`StoredFileKitSpec(direct_uploads=True)` registers three commands that keep the
bytes out of the app: the client sends them straight to the bucket through
presigned URLs. Bind authn and authz on each one, as you do on `upload`.

| Operation | Input | Result |
|-----------|-------|--------|
| `initiate_upload` | `InitiateStoredFileUploadRequestDTO` (`filename`, exact `size`, `content_type`, `multipart`, and `expires_in` for a single `PUT`: 15 minutes by default, at most 1 hour) | `StoredFileUploadSessionDTO`: the `pending` row plus a presigned `PUT` (`presigned`) or a multipart `upload_id` |
| `presign_upload_part` | `PresignStoredFileUploadPartRequestDTO` (`id`, `upload_id`, `part_number`, `expires_in` with the same bounds) | `PresignedUrlDTO` for one part |
| `complete_upload` | `CompleteStoredFileUploadRequestDTO` (`id`, plus `upload_id` and `parts` for a multipart upload) | the `ready` row |

1. `initiate_upload` mints the storage key, `{tenant}/{file_id}`, and writes a
   `pending` row that holds it. The client never names a key. A declared size
   over `max_bytes` is refused before anything is minted.
2. The client `PUT`s the bytes to `presigned.url` with every header in
   `presigned.headers`: the signed content type, and the create-only
   precondition (`If-None-Match: *` on S3, `x-goog-if-generation-match: 0` on
   GCS). For a multipart upload it asks `presign_upload_part` for each part
   instead, and keeps each part's `ETag`. S3 needs the ETags, and every part but
   the last must be at least 5 MiB there.
3. `complete_upload` checks a multipart upload before assembling it: the stored
   sizes of the named parts must fit `max_bytes` and the declared size, or the
   row is marked `failed` and the session aborted. It assembles create-only,
   then reads the object's head. The object's size must equal the declared size
   and stay within `max_bytes`, and its content type must equal the declared
   one. A match marks the row `ready`. A mismatch marks the row `failed`, then
   deletes the object, and raises `upload_mismatch` or `upload_too_large`. If
   nothing has been uploaded yet, it raises `upload_incomplete` and the row
   stays `pending`.

An uploaded object is create-only: the single `PUT` URL creates it once and can
never replace it, for as long as the URL stays valid, and a multipart completion
refuses to assemble over an existing object. So the bytes `complete_upload`
checked are the bytes the file keeps. Two `complete_upload` calls that race are
settled by the row's revision: the loser raises `revision_mismatch`, and a loser
that refused the object marks nothing and deletes nothing.

`initiate_upload` and `presign_upload_part` return credentials: a bearer write
URL, or a session id that lets its holder add parts. Both are marked sensitive,
so generated surfaces refuse them; serve them from your own routes.
`bind_stored_file_writes` runs `initiate_upload` in a transaction, as it does
`upload`, and leaves `complete_upload` outside one, so the `failed` mark
survives the refusal raised after it.

A client-side-encrypting route refuses `initiate_upload` before any row is
written: the app never sees the bytes, so it can't encrypt them.

!!! note "Known limit: abandoned uploads are not swept"

    A client that initiates and never completes leaves a `pending` row, and
    possibly an object or an unfinished multipart upload. Nothing reaps them
    yet. Deleting the row purges the object at its key, but not multipart
    parts. A refused upload leaves its key free, so the single `PUT` URL can
    still create an object there until it expires; the row stays `failed`.

    - **S3.** `initiate_upload` opens the multipart upload inside its
      transaction, so a rollback after that leaves an empty upload behind, and
      an abandoned one keeps its parts. Add a bucket lifecycle rule that aborts
      incomplete multipart uploads (`AbortIncompleteMultipartUpload`).
    - **GCS.** There is no native session: an abandoned upload's parts are
      objects named `{key}.__forze_mpu__/{upload_id}/{n}`, next to each file's
      own key. A lifecycle rule matches only a name prefix or suffix, so none
      can single them out from the files; they stay until the session is
      aborted. A rollback leaves nothing on GCS, because opening a session
      writes nothing.

## Authn routes

`attach_authn_routes` projects an authn registry (`build_authn_registry`) the
same way. Authn flows are RPC-shaped with one natural surface each, so there is
no style choice — fixed action paths: `POST /login` and `POST /refresh` (200,
token response), `POST /logout` (204, no body), `POST /change-password` (204),
`POST /password-reset/request` (202, uniform ack — never the token) and
`POST /password-reset/confirm` (204), and, behind `admin_guards` only,
`POST /deactivate` (204). Login,
refresh, and the password-reset pair are meant to be reachable without a bearer
token (the operations authenticate via their bodies, or deliberately not at all
for the reset request); logout and change-password declare `AuthnRequired` (so
they 401 without a bound identity and show up protected under
`apply_openapi_security`). The operations that act on another principal,
`deactivate_principal` and the admin API-key routes below, are registered only when
`build_authn_registry` gets `admin_guards`, and run behind them. The full
wiring (including how the reset token reaches the user via the outbox) is in the
[Authn, authz & tenancy recipe](../recipes/authn-authz-tenancy-fastapi.md#http-login-endpoints).

Unlike the other helpers, `attach_authn_routes` needs neither `ns=` nor `resource=`:
without them it uses the one namespace in the registry that holds `password_login` or
`refresh_tokens` under the default `.` separator, which is `spec.default_namespace` for
`build_authn_registry(spec)`. It never guesses — several such namespaces, or none, is a
configuration error that asks for `ns=`.

### Cookie mode

Browser clients that must not hold a token in JavaScript pass an
`AuthnCookieCarrier` to `attach_authn_routes(cookies=…)`. Login and refresh then
set and rotate two `HttpOnly` cookies instead of returning token strings; the
body keeps the scheme and the `*_expires_in` lifetimes, so a cookie client still
learns how long its credential is valid. Refresh reads the refresh cookie when
the request has no body or an empty JSON object (`{}`, as fetch wrappers and
generated clients send); a token in the body takes precedence. Logout revokes
the session and expires both cookies idempotently.

```python
from fastapi import APIRouter
from forze_fastapi.security import AuthnCookieCarrier

router = APIRouter(prefix="/api/auth")
cookies = AuthnCookieCarrier(
    access_path="/api",          # API calls carry the access cookie
    refresh_path="/api/auth",    # the long-lived one: every authn route, no other API call
)

attach_authn_routes(router, registry=registry, ctx_dep=ctx_dep, cookies=cookies)
```

Logout revokes what a logout by access token revokes (every session of the
principal). With no identity bound, which is the case once the access cookie has
expired, it runs `logout_by_refresh_token` with the refresh cookie instead. That
operation has no route of its own. So scope `refresh_path` to cover both the refresh
and the logout route, as above. No single cookie path covers only `/refresh` and
`/logout`: with `refresh_path="/api/auth"` the browser sends the refresh cookie to
every authn route under that prefix, and only refresh and logout read it.
`runtime_lifespan` refuses to start an app whose cookie-mode logout route the
refresh cookie's path does not cover (`check_authn_cookie_paths`, run by
`run_startup_checks` on the app and every app mounted beneath it).

Every tab and window shares the refresh cookie, so two of them can refresh with the
same token at once. With a [reuse grace](../integrations/authn.md#notes) the losing
refresh answers 401 `refresh_superseded` instead of revoking the session as reuse. The
client refreshes again once the winner's response has set the new cookie: a retry sent
before then gets `refresh_superseded` again, so wait for the other refresh or retry after
a short delay.

Point the inbound `CookieTokenAuthn`'s `cookie_name` at the same
`access_cookie`, and list the login, refresh and logout paths in the security
middleware's [`anonymous_paths`](../integrations/fastapi.md#bind-request-context) —
otherwise a stale access cookie 401s the very route that would replace it. A 401'd
logout never runs, so the session stays live and the cookies stay set.
`check_authn_cookie_paths` also refuses to start an app whose security middleware
leaves one of the three cookie-mode routes out of `anonymous_paths`.
Cookies are always `HttpOnly` and `Secure` by default, with `samesite="lax"` as
the outbound browser-side default. Inbound, `CookieTokenAuthn` ships a
server-side CSRF gate **on by default** (`CookieCsrf`): a request using either
cookie on an unsafe method must prove a same-host origin via `Origin` (or
`Referer`), so the defense holds even when a proxy strips `SameSite` or a
`samesite="none"` deployment relies on it. The middleware applies it to the
access cookie. Login, logout and refresh apply the same ingress's gate to every
request, with or without a cookie (the default gate if no `CookieTokenAuthn` reads
the carrier's `access_cookie`), so the settings below cover all three. A cross-origin frontend (a SPA on
another host) lists itself in `CookieCsrf(allowed_origins={...})` — a dev server
that picks a free port may be named as a range on a loopback host
(`http://localhost:5173-5199`, or `http://localhost:*`); non-browser
cookie clients that send neither header opt in with `allow_missing_origin=True`
or authenticate via a header ingress. `csrf=None` disables the gate — a declared
decision to bring your own CSRF layer. When several `CookieTokenAuthn` read one cookie,
every gate they declare applies to it, so it is ungated only when all of them opt out.

### API-key management

`attach_authn_routes` also generates **self-service API-key management** as a
resource collection (all `AuthnRequired`): `POST /api-keys` issues a key for the
caller — the raw secret is in the response **once**, optionally a user→agent
delegation key (`actor_principal_id`: another principal authentication accepts, and a
registered `service` principal when `AuthnDepsModule(authz_route=...)` is set; otherwise
`422 delegate_invalid`) with a human `label` — `GET /api-keys`
lists the caller's keys as non-secret descriptors (a `hint` like `ab12…wxyz`,
never the secret), and `DELETE /api-keys/{id}` revokes one (204, and 204 again for a key
already revoked; a key the caller does not hold is a 404, as an unknown one is). This is the minting
surface for the [MCP API-key flow](../integrations/mcp.md#protect-it-with-api-key-auth):
the user issues a key here and pastes it into the agent host.

An administrator finds a principal's keys with `GET /admin/principals/{id}/api-keys`
(`list_principal_api_keys`: the same non-secret descriptors, revoked keys included, and
an empty list for a principal with none) and revokes one with `DELETE /admin/api-keys/{id}`
(`revoke_principal_api_key`, 204, and 204 again for a key already revoked; an unknown key
is a 404). They exist only when the registry is built with `admin_guards`, the steps that
decide who may act on another principal; they run behind them, and so does
`deactivate_principal`, which without them is not registered either. One guard set gates
all three. At least one guard must declare the permission keys it enforces
(`permission_keys()`), as `AuthzBeforeAuthorize` does, or the registry is refused when
built (`admin_guards_unauthorized`): `AuthnRequired`, `TenantRequired` or a logging step
let every signed-in principal through. A step of your own counts once it implements
`permission_keys()`, and its keys then show in the catalog and in MCP tool descriptions; a
`functools.partial` or `__wrapped__` wrapper counts as what it wraps. If one of your guards
authorizes without declaring it, pass `trust_admin_guards=True` and answer for it. If you
bound your own guards on `deactivate_principal`, pass them as `admin_guards` instead: the
same step bound twice fails at `freeze()` with `Step ID … is not unique`.

The guards also run on the list, which is a read: there they run read-only, so a guard
that writes (an audit row, say) fails the list with `Cannot use command (write) port`.
Write such a record from an after-hook or through the outbox instead.

The admin API-key routes are **global**, like `deactivate_principal`: API-key accounts are
not tenant-scoped, so they find a principal's keys or a key by id in every tenant, and the
revoke's 404 tells a caller whether an id exists anywhere. An app whose administrators
are scoped to a tenant must not expose these routes, or must guard them with a policy
only platform administrators pass.

A delegated caller (an agent acting for the user) may list the keys but not issue or revoke
them, and may not log out or change the password: only the user manages their own account.
Nor may it act as an administrator, to list or revoke anyone's keys or deactivate a
principal: an agent acting for an administrator is not one. Those routes answer `403` (`delegate_denied`), and so do switching and leaving a
tenant below.

Identity, invocation metadata, and error mapping stay with the middlewares and
exception handlers from the [integration setup](../integrations/fastapi.md) —
generated routes only validate the input DTO and run the operation through the
normal pipeline.

## Tenancy routes

`attach_tenancy_routes` projects the self-service tenant-selector registry — fixed
paths, no style choice: `GET /tenants` (the caller's tenants), `POST
/tenants/{id}/activate` (switch the active tenant), `DELETE /tenants/{id}` (204,
leave). In [cookie mode](#cookie-mode), pass the same carrier as `cookies=`: `/activate`
then sets and rotates both cookies and strips the token strings from its body, behind the
gate login applies. It authenticates from the access cookie, so leave it out of
`anonymous_paths`. The access cookie's `access_path` must cover the tenancy routes' mount,
or the switch answers 401. The refresh cookie's path need not cover it: the response sets
that cookie at its own path, and the route never reads it. The gate follows the `csrf` of
the `CookieTokenAuthn` reading the access cookie: `csrf=None` on every such ingress turns it
off for the switch too, as for login.
`attach_tenancy_admin_routes` projects the admin plane: `POST /tenants`
(201, create), `GET /tenants/{id}/members`, `POST /tenants/{id}/deactivate` (204),
`POST /memberships` (204, invite) and `DELETE /memberships` (204, remove). Admin
operations ship without hooks bound — guard them with `AuthnRequired` + authz hooks
in the registry.

## Aggregate routes

`attach_aggregate_routes(router, kit, *, ctx_dep, style="rest", tx_route="default",
storage_prefix="/blobs")` is the composite: given an `AggregateKit` it attaches the
kit's document (+ soft-deletion), search, and — when the kit declares storage —
blob routes (under `storage_prefix`) onto one router with a single call, each
sub-surface exactly as its dedicated attacher would. `tx_route` must match the
transaction route the deps module registers.

## Your own operation routes

An operation no attacher ships gets its route from a binding table, not a hand-written
endpoint. `attach_operation_routes` takes the table keyed by operation suffix under a
namespace; the schema, `operation_id` (the namespaced key, e.g. `stock.add`) and dispatch
come from the catalog, as for the generated routes.

```python
from forze.base.primitives import StrKeyNamespace
from forze_fastapi.routes import (
    RouteBinding,
    attach_operation_routes,
    body_endpoint,
    id_endpoint,
    query_endpoint,
)

attach_operation_routes(
    router,
    registry=registry,
    ns=StrKeyNamespace(prefix="stock"),
    ctx_dep=ctx_dep,
    bindings={
        "levels": RouteBinding(method="GET", path="/levels", build=query_endpoint),
        "add": RouteBinding(method="POST", path="/add", build=body_endpoint, status_code=201),
        "get": RouteBinding(
            method="GET",
            path="/{id}",
            build=id_endpoint,
            param_descriptions={"id": "The stock item's ID."},
        ),
    },
)
```

A binding whose operation is not registered is refused, so a typo fails at startup rather
than answering 404; pass `skip_unregistered=True` to skip it instead.

Every routed operation's key lives under the namespace, as `<prefix>.<suffix>`. A key
registered without one, such as a bare `add`, cannot be bound: register it as `stock.add`
instead. The refusal names a bare key it finds, so the cause is not mistaken for a typo.

`query_endpoint` reads the whole input DTO from query parameters, one per field; a list
field repeats (`?tag=a&tag=b`). A field a query string cannot carry, such as a nested
model or `bytes`, is refused when the route is attached. The operation receives the DTO a
body with the same keys would build: only the parameters sent count as set, so a patch built
from it leaves the rest alone, and validators and extras behave as for a body. A parameter
the DTO does not declare is ignored unless the DTO forbids or allows extra fields. A literal
`+` in a value, such as a timezone offset, must be sent as `%2B`: a query string reads `+` as
a space.

`body_endpoint`, `id_endpoint`, `id_rev_endpoint` and `id_rev_body_endpoint` are the
builders the document routes use. The id builders take `id` and `rev` from the path where it
has their placeholder and from the query otherwise, typed and defaulted as the input DTO
declares them. Each
shipped builder names the placeholders it fills, and a path with any other placeholder is
refused. An `EndpointBuilder` of your own takes `(runner, input_type, op)`; give it a
`path_params` frozenset to have its paths checked the same way.

A binding's `param_descriptions` describe its endpoint's parameters by name. Each becomes
FastAPI's own `Path` where the path has the placeholder and `Query` otherwise, so `id`
reads the same in both route styles. Only a plain path or query parameter takes one. When
the route is attached, an empty description is refused, and so is a name the endpoint does
not take. A body, the request, or a parameter with its own `Query`, `Depends`, … is refused
too: describe that one in its own metadata. A `query_endpoint` parameter takes its
description from the DTO field.

The operation's output type is the route's response model. It need not be a bare model: an
operation that may find nothing declares `output_type=Model | None` and answers `200` with
`null`, and MCP returns it as `{"result": null}`. An operation without one, such as
a federated search, still has a model result written as a typed route's is: fields that are
`None` are left out unless the attacher passes `exclude_none=False`, and the OpenAPI schema
documents an untyped body. Any other result, such as a dict or a list, is written as FastAPI
writes a route without a response model. Either way an infinite or NaN float is written as
`null`.

## Infrastructure routes

- `attach_jwks_route(router, jwks_provider, *, path="/.well-known/jwks.json",
  cache_max_age=300)` — the JWKS document for token verification; excluded from the
  OpenAPI schema.
- `attach_readiness_route(router, runtime, *, path="/readyz", probes=None,
  probe_timeout=timedelta(seconds=2))` — 200 while the runtime is active and not
  draining, 503 otherwise; excluded from the schema. `runtime` may also be a
  zero-argument callable returning the runtime or `None`, asked per request, for an
  app that builds its runtime in the lifespan. `probes` maps a name to the
  deps key of a client to ask `health()`, adding a `checks` breakdown and a
  `degraded` 503 when a dependency is unreachable — see [Readiness](../running-in-prod/shutdown-and-fleets.md#readiness).
- `attach_liveness_route(router, *, path="/livez")` — 200 unconditionally; reaching
  the handler is the check. Deliberately blind to drain, so a draining pod reads as
  alive-but-not-ready instead of dead. Excluded from the schema.
- `attach_metrics_route(router, reader, *, path="/metrics")` — a Prometheus pull
  endpoint over an already-registered `PrometheusMetricReader` (needs
  `opentelemetry-exporter-prometheus`; any other reader is refused). Renders off
  the event loop, since collection runs every observable callback in the process.
  **Single-process deployments only** — see [The Grafana
  stack](../running-in-prod/grafana-stack.md#pull-metrics-and-why-not).

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
# ...plus whatever else your middleware takes.
app.add_middleware(
    SecurityContextMiddleware,
    ctx_dep=runtime.get_context,
    authn=requirement,
)

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

This **documents** auth; it doesn't enforce it. The middleware refuses a request no
ingress authenticates outside its `anonymous_paths` (list the mounted `/auth/login` and `/auth/refresh`
there), and per-operation enforcement stays in the engine's `AuthnRequired`/authz
hooks.

## Match OpenAPI to what the app serves

FastAPI documents its own 422 body (`HTTPValidationError`) on every route that
takes input, but the Forze exception handlers answer errors — a failed request
parse included — with the Forze envelope. Descriptions come from docstrings, so
reST such as ``:class:`~shop.Order` `` reaches the rendered docs as written. One
call fixes both:

```python
from forze_fastapi import apply_openapi_conventions

# After every router is attached, and after apply_openapi_security:
apply_openapi_conventions(app)
```

Each FastAPI 422 becomes the `ForzeErrorResponse` envelope (`detail`, `code`, `kind`,
optional `context`), and every operation without its own `default` response gains one in
the same shape. Only `detail` is listed as required: every error Forze raises carries `code`
and `kind`, but an `HTTPException` from your code or a FastAPI dependency answers with
`detail` alone.
The `X-Error-Code` header accompanies the errors that carry a code, so it is
documented as optional. FastAPI's validation schemas are dropped once nothing
references them. A 422 your route declares with its own model is left alone. A
different model of yours already named `ForzeErrorResponse` is not overwritten:
the schema request fails with a configuration error, since routers can still be
attached after the call. Responses a route sends
with no body at all, such as a storage download's `304` and `416`, are not
envelopes; the `default` entry does not describe them.

Every `description` and `summary` then has its reST rendered as Markdown without
losing prose: roles and ``` ``literals`` ``` become code spans, field lists
(`:param:`, `:returns:`, …) are dropped, `code-block` and `::` literal blocks
become fenced blocks, and any other directive (`note`, `deprecated`,
`versionadded`, …) becomes a labelled blockquote. Fenced blocks already in
Markdown pass through as written. Call it last: text a later wrapper adds, such as a security scheme's
description, stays as written.

A generated client types a response field optional when its schema does not list it required,
and pydantic leaves every field with a default optional, though the app sends it every time.
`apply_openapi_conventions(app, require_sent_fields=True)` lists what every route returning a
schema sends. Where only routes that keep nulls (`exclude_none=False`) return it, every field is
required. Where a route drops nulls, the default for the routes Forze attaches, the fields that
cannot be null are required, and a nullable one, left out when it is null, is not, even
without a default. A schema is one component shared by all the routes that return it, so where
a route that keeps nulls and one that drops them return the same model, its nullable fields
are optional for both. A schema a request also uses is left as it is, and so is one a route
trims some other way (`response_model_exclude_unset`, `include`, `exclude`).

`tag_groups` groups the sidebar that Scalar and Redoc render, through the top-level
`x-tagGroups` extension. Groups and their tags keep the order you give:

```python
from forze_fastapi import apply_openapi_conventions

apply_openapi_conventions(
    app,
    tag_groups={"Content": ["notes", "files"], "Access": ["auth"]},
)
```

A group without a name, tags given as one string (`"notes"` for `["notes"]`), or a tag
listed twice is a configuration error raised by the call itself. A listed tag that no
operation is tagged with, a typo or a renamed router, is left out of its group with a
warning, and a group left with no tags is dropped. Neither renderer shows a tag that no
group lists, nor the operations under it. The tags you leave out therefore join a group
named `Other`, in the order operations first use them: last, or where you list `Other`
yourself. An operation with no tags is in no group: Scalar still lists it, Redoc does not,
so tag every route. A later call with `tag_groups` replaces the groups; one without leaves
them as they are. `register_scalar_docs` needs no setting, since Scalar reads the groups
from the schema.

`register_scalar_docs` points Scalar at the URL the app serves its document at
(`app.openapi_url`, prefixed with the request's `root_path`), so a versioned
`FastAPI(openapi_url="/v1/openapi.json")` needs nothing extra. Pass `openapi_url=` for a
document another route serves; an app with `openapi_url=None` must pass one.
