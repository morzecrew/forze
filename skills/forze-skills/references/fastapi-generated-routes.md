# FastAPI routes

Generating routes from a frozen registry with `attach_*_routes`, writing your own beside them, the deadline and readiness headers they emit, and projecting the same registry to agents over MCP. Application setup comes first: [FastAPI setup](fastapi-setup.md).

## Generated routes

`forze_fastapi.routes` projects a frozen operation registry (built with
`forze_kits.aggregates.*` factories) onto a plain `APIRouter` you own. Schemas
come from the operation descriptors and each route's `operationId` is the
registry operation key verbatim (e.g. `notes.get`):

```python
from fastapi import APIRouter

from forze_fastapi.routes import attach_document_routes

router = APIRouter(prefix="/notes", tags=["notes"])

attach_document_routes(
    router,
    registry=registry,  # build_document_registry(spec, dtos).freeze()
    ns=spec.default_namespace,
    ctx_dep=ctx_dep,
    style="rest",  # or "rpc" — explicit, required
)

app.include_router(router)
```

- Both styles use the same REST verbs; they differ only in how the resource is
  addressed. `style="rest"` puts the id in the path (`POST ""` 201, `GET /{id}`,
  `PATCH /{id}?rev=`, `DELETE /{id}` 204); `style="rpc"` keeps one
  operation-named path per op and puts the id in a query parameter
  (`GET /get?id=`, `PATCH /update?id=&rev=` with the patch body,
  `DELETE /kill?id=` 204). List operations are `POST /<op>` with a filter body
  in both styles. `create` also posts its input DTO as a body, but mounts at the
  router root in REST (`POST ""`, 201) and at `POST /create` in RPC.
- Only operations the registry holds are attached (a read-only spec yields a
  read-only router, and a spec with `hard_delete=False` gets no `DELETE` route); narrow with
  `include={"get", "list"}`.
- Merging `build_soft_deletion_registry(spec)` into the document registry adds
  soft delete/restore automatically — `POST /{id}/delete|restore?rev=` (REST) or
  `PATCH /delete|restore?id=&rev=` (RPC); hard delete keeps the `DELETE` verb.
- `attach_search_routes` (no `style` — every search request is a filter body,
  always `POST /<op>`) and `attach_storage_routes` (`style` required; multipart
  upload, raw-bytes download) follow the same pattern.
- An aggregate declared with `AggregateKit` (see [AggregateKit](aggregate-kit.md))
  attaches its **whole slice** in one call — document, soft-delete, search, and
  (under `storage_prefix`, default `/blobs`) storage routes:

  ```python
  from forze_fastapi.routes import attach_aggregate_routes

  attach_aggregate_routes(router, kit, ctx_dep=ctx_dep, style="rest", tx_route="default")
  ```

  The routes execute through the kit's composed registry, so `tx_route` is
  load-bearing — pass the same route the deps module registers its transaction
  manager under (and the same one the kit's `facade()` uses).
- An operation with a plan-declared deadline surfaces it as an
  `x-deadline-seconds` OpenAPI extension and a "Time budget" description line —
  see [resilience and deadlines](resilience.md).
- `apply_openapi_security(app, requirement)` (from `forze_fastapi.security`) makes
  the generated OpenAPI honest about auth: it derives `securitySchemes` from the
  same `AuthnRequirement` you give `SecurityContextMiddleware` (bearer for an
  `Authorization` token; `apiKey` in header/cookie otherwise) and attaches
  `security` to operations flagged `requires_authn` (derived at freeze from the
  plan's `AuthnRequired`/authz hooks). Call once after attaching routers; token-
  minting routes (`/login`, `/refresh`) stay open. Documents auth, does not enforce
  it — `exclude={op, ...}` leaves a flagged op open.
- Generated routes describe their own parameters (`id`, `rev`, a storage `key`); a
  `RouteBinding` of yours takes `param_descriptions={"id": "..."}`. For sidebar groups in
  Scalar or Redoc, call `apply_openapi_conventions(app, tag_groups={"Content": ["notes"]})`
  (from `forze_fastapi`) last: a listed tag no operation uses is left out with a warning,
  tags you leave out join a trailing `Other` group, and an untagged route shows in Scalar
  but not in Redoc.

## Hand-written routes

When you need a route the generators don't cover, define a plain FastAPI route that resolves a context with `ctx_dep` and dispatches through a **facade** built from your frozen registry (see [document access](document-facade.md)) — not the raw ports:

```python
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from forze_kits.aggregates.document import DocumentFacade, DocumentIdDTO

router = APIRouter(prefix="/projects", tags=["projects"])


def projects(ctx) -> DocumentFacade[ProjectRead, CreateProject, UpdateProject]:
    return DocumentFacade(ctx=ctx, registry=registry, namespace=project_spec.default_namespace)


@router.get("/{project_id}")
async def get_project(project_id: UUID, ctx=Depends(ctx_dep)):
    return await projects(ctx).get(DocumentIdDTO(id=project_id))


app.include_router(router)
```

The facade runs each operation through the pipeline (mapping, hooks, transaction). Reach a raw `ctx.<port>` only inside a custom handler that a facade operation can't express.

A hand-written upload route reads the file with `await read_upload_capped(file, max_size)` (from `forze_fastapi.routes`), never a bare `await file.read()`: it is the generated upload route's reader, refusing a file over the cap, without buffering it all, with the same 422 status and `upload_too_large` code. A stored-file kit's `max_bytes` checks only after the whole body is in memory.

A stored-file download route runs the kit's `download_stream` operation (`StoredFileIdDTO`) and returns `streamed_download_response(result.stream)` (from `forze_fastapi.routes`), not the buffered `download`. For a direct fetch, `presign_download` (`id`, `expires_in` up to 7 days) returns the row and a presigned URL; a client-side-encrypting route refuses to resolve it, so `check_wiring` reports it. Both load the row and refuse a file that is not ready. Both are opt-in: enable them with `StoredFileKitSpec(stream_downloads=True, presign_downloads=True)` and bind authn and authz on each one, since a guard on `download` does not cover them. `presign_download` is marked sensitive, so MCP, agent tools and generated routes refuse it: serve it from a hand-written route, and give an MCP server over such a registry an `operations=` allowlist without it. `download_stream` and the buffered `download` set `streams=True` (a byte stream and raw bytes, not JSON): the MCP and `forze ops` defaults leave them out, and naming one on MCP, agent tools, `forze ops` or `attach_operation_routes` is refused.

For `Range` and conditional requests, enable `range_downloads=True` as well and call `attach_stored_file_download_route(router, registry=..., ns=kit.document.default_namespace, ctx_dep=...)` (from `forze_fastapi.routes`): `GET {id}/content` runs `download_stream` and `download_range` and answers `206`, `416` and `304` like the storage download route. On a client-side-encrypting route a range can't slice the kit's buffered uploads, so the route serves the whole file.

For a large upload, enable `direct_uploads=True`: `initiate_upload` (`filename`, exact `size`, `content_type`, `multipart`, `expires_in` up to 1 hour) writes a pending row under a key the kit mints and returns a create-only presigned PUT or a multipart `upload_id`; the client sends every header in `presigned.headers`; `presign_upload_part` presigns each part; `complete_upload` checks the parts' sizes before assembling, then the object's size and content type against the declared ones and `max_bytes`, and marks the row ready, or fails the row and deletes the object. The first two are sensitive, so serve them from hand-written routes. A client-side-encrypting route refuses to resolve all three. Add `stored_file_pending_sweep_lifecycle_step(kit)` (from `forze_kits.aggregates.stored_file`) to your lifecycle steps: it marks rows left `pending` past `older_than` (24 hours by default) `failed`, aborts their multipart uploads and deletes any object that landed; pass `tenants=` for tenant-aware routes.

## Readiness and deadline headers

- `attach_readiness_route(router, runtime)` (from `forze_fastapi.routes`) adds
  `GET /readyz`: `200` while serving, `503` once shutdown starts draining —
  point the load balancer's readiness check here. Pass
  `probes={"postgres": PostgresClientDepKey, ...}` to also sweep each named
  client's `health()` and answer `503 degraded` with a per-dependency `checks`
  breakdown; see the shutdown/readiness reference.
- `InvocationMetadataMiddleware(..., bind_deadline_from_header=True)` opts in
  to honoring an upstream `X-Forze-Deadline-Budget` header (tighten-only, so a
  forged value can only shorten the sender's own request).

## Exposing operations over MCP

The same frozen registry can be projected to AI agents as MCP tools via `forze_mcp` (extra `mcp`, FastMCP-based):

```python
from forze_mcp import AccessTokenIdentityResolver, ForzeApiKeyVerifier, build_mcp_server

server = build_mcp_server(
    registry,                       # the same frozen registry the HTTP routes use
    ctx_factory,
    name="projects",
    # Read-only by default. Every write in the registry stays unexposed until you say
    # otherwise — an agent that can call `projects.kill` is a decision, not a default.
    include_writes=False,
    auth=ForzeApiKeyVerifier(ctx_factory=ctx_factory, authn_spec=authn_spec),
    identity=AccessTokenIdentityResolver(agent=AGENT_PRINCIPAL),
)
```

For a FastMCP server you build yourself, `register_tools(server, registry, ctx_factory, ...)` attaches the same tools to it.

`register_tools(...)` + `exposed_operations(...)` project operation keys as tool names, `runtime_lifespan` scopes the runtime, and auth is **API-key-as-bearer** — `ForzeApiKeyVerifier` validates the inbound bearer through the same `AuthnSpec`/authn brain as your HTTP routes (pass both `auth=` and the identity binder). See [MCP integration](https://morzecrew.github.io/forze/latest/integrations/mcp/) and [Expose an aggregate over MCP](https://morzecrew.github.io/forze/latest/recipes/expose-an-aggregate-over-mcp/).

## Running operations from the command line

The same registry runs from a terminal, a cron job or a Kubernetes Job through `forze ops` (extra `cli`), governed exactly as over HTTP: input validated, API key verified, tenant resolved, authz and audit applied. Declare an `OpsApp` in a module the web process can import too, since `forze_cli.ops` needs no Typer:

```python
from forze.application.contracts.authn import AuthnSpec
from forze.application.execution import DeploymentProfile, ExecutionRuntime, build_runtime
from forze_cli.ops import ApiKeyIdentity, OpsApp


def build_ops_runtime() -> ExecutionRuntime:
    # One call per process: no long-running steps, no drain window. Wire only what the
    # exposed operations need; a poller nobody declared long-running would still start.
    return build_runtime(deps_modules, deployment=DeploymentProfile.SERVERLESS)


ops = OpsApp(
    registry=registry,              # the same frozen registry the routes use
    runtime=build_ops_runtime,      # built once per `call`, never by `list` / `describe`
    identity=ApiKeyIdentity(AuthnSpec(name="main", enabled_methods=frozenset({"api_key"}))),
    include_writes=True,
    operations=["projects.create", "projects.get"],  # an allowlist, reviewed like a grant
)
```

Run it with `FORZE_OPS_APP=myapp.ops:ops forze ops call projects.get --input '{"id": "..."}'`, the key in `FORZE_API_KEY` (never argv). `call` prints exactly one JSON document — the result or a masked error object with `execution_id` / `correlation_id` — and exits with a stable code: 0 ok, 2 usage, 3 not found, 4 rejected, 5 denied, 6 timeout, 75 safe to retry, 69 retryable but a write may have landed, 78 configuration. An input of flat scalars usually also takes typed flags (`--id ...`) instead of `--input` (`describe` says when it does not), `--batch @rows.jsonl` runs one call per line in one runtime and stops at the first failure, and `forze --install-completion` completes operation names. `build_ops_cli(ops)` mounts the same group in your own Typer CLI; set `pretty_exceptions_show_locals=False` on its root.

## Anti-patterns

- **Importing the removed `forze_fastapi.endpoints.*` helpers** — use `forze_fastapi.routes.attach_*_routes`, or define your own routes that dispatch through the registry/facade.
- **Reaching for `StaticIdentity` because it is easy** — it binds an identity nothing verifies, so every operator shares one audit actor and one idempotency namespace, and nothing can be revoked. Issue a key per operator or job and use `ApiKeyIdentity`; keep `StaticIdentity` for break-glass inside the cluster.
- **Patching `app.openapi` to describe generated parameters or add `x-tagGroups`** — it breaks when a parameter is renamed and must run before `apply_openapi_conventions`. The generated routes describe their parameters already, and `tag_groups` writes the groups.
- **`include_writes=True` with no allowlist on `OpsApp`** — every command in the registry becomes callable from a shell, including ones whose only guard was their route. Name the operations.

## Reference

- [Generated routes reference](https://morzecrew.github.io/forze/latest/reference/fastapi-routes/)
- [MCP integration](https://morzecrew.github.io/forze/latest/integrations/mcp/)
- [Operations CLI](https://morzecrew.github.io/forze/latest/running-in-prod/operations-cli/) and its [reference](https://morzecrew.github.io/forze/latest/reference/ops-cli/)
