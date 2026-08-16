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
  read-only router); narrow with `include={"get", "list"}`.
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

## Readiness and deadline headers

- `attach_readiness_route(router, runtime)` (from `forze_fastapi.routes`) adds
  `GET /readyz`: `200` while serving, `503` once shutdown starts draining —
  point the load balancer's readiness check here.
- `InvocationMetadataMiddleware(..., bind_deadline_from_header=True)` opts in
  to honoring an upstream `X-Forze-Deadline-Budget` header (tighten-only, so a
  forged value can only shorten the sender's own request).

## Exposing operations over MCP

The same frozen registry can be projected to AI agents as MCP tools via `forze_mcp` (extra `mcp`, FastMCP-based): `register_tools(...)` + `exposed_operations(...)` project operation keys as tool names, `runtime_lifespan` scopes the runtime, and auth is **API-key-as-bearer** — `ForzeApiKeyVerifier` validates the inbound bearer through the same `AuthnSpec`/authn brain as your HTTP routes (pass both `auth=` and the identity binder). See [MCP integration](https://morzecrew.github.io/forze/latest/integrations/mcp/) and [Expose an aggregate over MCP](https://morzecrew.github.io/forze/latest/recipes/expose-an-aggregate-over-mcp/).

## Anti-patterns

- **Importing the removed `forze_fastapi.endpoints.*` helpers** — use `forze_fastapi.routes.attach_*_routes`, or define your own routes that dispatch through the registry/facade.

## Reference

- [Generated routes reference](https://morzecrew.github.io/forze/latest/reference/fastapi-routes/)
- [MCP integration](https://morzecrew.github.io/forze/latest/integrations/mcp/)
