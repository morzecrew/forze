# Document access

Reading and writing documents from application code — the facade for standard CRUD, the raw ports for everything else, and cache-aware reads. Building the filter itself is [query DSL](query-dsl.md).

## Document access with `DocumentFacade`

In ordinary application code (routes, services, scripts) drive documents through a **typed `DocumentFacade`**, not raw ports. Build a frozen registry from the spec and your boundary DTOs once, then construct a facade per execution context:

```python
from forze_kits.aggregates.document import (
    DocumentDTOs,
    DocumentFacade,
    DocumentIdDTO,
    DocumentUpdateDTO,
    ListRequestDTO,
    build_document_registry,
)

registry = build_document_registry(
    project_spec,
    DocumentDTOs(read=ProjectRead, create=CreateProject, update=UpdateProject),
).freeze()


def projects(ctx) -> DocumentFacade[ProjectRead, CreateProject, UpdateProject]:
    return DocumentFacade(ctx=ctx, registry=registry, namespace=project_spec.default_namespace)
```

The facade exposes the document operations as typed methods — each runs through the normal operation pipeline (mapping, hooks, transaction):

```python
project = await projects(ctx).get(DocumentIdDTO(id=project_id))

page = await projects(ctx).list(
    ListRequestDTO(
        page=1,
        size=20,
        filters={"$values": {"status": "active"}},
        sorts={"created_at": "desc", "id": "asc"},
    )
)
rows, total = page.hits, page.count  # `list` returns a Paginated[ProjectRead]

created = await projects(ctx).create(CreateProject(title="Roadmap"))

result = await projects(ctx).update(
    DocumentUpdateDTO(id=project_id, rev=project.rev, dto=UpdateProject(title="Done"))
)
updated, diff = result.data, result.diff   # carries the old→new field diff

await projects(ctx).kill(DocumentIdDTO(id=project_id))
```

`update` carries the document's **`rev`** — a stale revision raises `exc.conflict`, the optimistic-concurrency guarantee. Other methods: `raw_list` / `raw_list_cursor` (projected dict rows), `list_cursor` (keyset pagination), `agg_list` (group-by / metrics).

A read-only spec (`write=None`) builds a read-only registry — pass `DocumentDTOs(read=...)` alone and the full **read** surface (`get`, `list`, `raw_list`, `list_cursor`, `raw_list_cursor`, `agg_list`) is attached; only the write operations (`create` / `update` / `kill`) are gated out.

## Custom operations and raw ports

The facade covers the standard document/search surface. When you need behaviour the facade doesn't model — a multi-step domain operation, a saga step, a one-off projection — write a handler (or reach the port directly) via the namespaced context: `ctx.document.query(spec)` → `DocumentQueryPort[read]`, `ctx.document.command(spec)` → `DocumentCommandPort`, `ctx.search.query(spec)` → `SearchQueryPort`. This is the escape hatch, not the default for CRUD.

For bounded-memory **exports**, the query ports stream keyset chunks: documents expose `find_stream` / `project_stream` / `select_stream`, search exposes `search_stream` / `project_search_stream` / `select_search_stream` (`chunk_size=500` default, no total count; backends without keyset support refuse with `query_feature_unsupported`).

## Adapter boundaries

- Postgres, Mongo, and Firestore implement the document gateways (and history where configured). Firestore wires them via `FirestoreDepsModule(ro_documents=..., rw_documents=...)` with `FirestoreReadOnlyDocumentConfig` / `FirestoreDocumentConfig`.
- Postgres implements search (FTS/PGroonga/vector); Mongo when `MongoDepsModule.searches` is wired (`text`, `atlas`, `vector`); Meilisearch via `MeilisearchDepsModule(searches={...})` with `MeilisearchSearchConfig` (plus `MeilisearchFederatedSearchConfig`). All resolve through the same facade/port surface.
- Mock implements document/search behaviour for unit tests.
- Use adapters in deps modules and integration tests, never in handlers.

## Cache-aware documents

Attach `CacheSpec` to `DocumentSpec.cache` and register a matching cache route, usually in `RedisDepsModule.caches`. Reads then serve from the cache on a hit and populate it on a miss; writes invalidate. The facade code is unchanged — caching is pure wiring.

```python
from datetime import timedelta

from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes

project_spec = DocumentSpec(
    name=ResourceName.PROJECTS,
    read=ProjectRead,
    write=DocumentWriteTypes(domain=Project, create_cmd=CreateProject, update_cmd=UpdateProject),
    cache=CacheSpec(name=ResourceName.PROJECTS, ttl=timedelta(minutes=5)),
)
```

`ResourceName` (your spec-name enum) and `Project` / `ProjectRead` / `CreateProject` / `UpdateProject` (your domain model and DTOs) are app-defined symbols.

Stampede protection, an opt-in in-process L1 (`CacheSpec(l1=L1Spec(...))`, a cross-replica staleness budget), early refresh, and adaptive lifetimes are all spec-level opt-ins — see [Caching reads](https://morzecrew.github.io/forze/latest/data-events/caching/) for the full set and their consistency trade-offs.

## Anti-patterns

- **Reaching raw `ctx.document.query/command` for standard CRUD** — use a `DocumentFacade` (likewise `SearchFacade` for search); raw ports are for custom handlers and orchestration only.
- **Importing Postgres/Mongo adapters in handlers** — go through the facade/ports.
- **Using removed flat accessors (`ctx.search_query`, `ctx.doc_read`, `ctx.doc_write`)** — use the namespaced `ctx.document.query` / `ctx.document.command` / `ctx.search.query`.
- **Bypassing the revision on updates** — always pass the read `rev` through `DocumentUpdateDTO` to preserve optimistic concurrency.

## Reference

- [Reading data](https://morzecrew.github.io/forze/latest/data-events/reading-data/)
- [Caching reads](https://morzecrew.github.io/forze/latest/data-events/caching/)
- [Document contracts](https://morzecrew.github.io/forze/latest/reference/contracts/document/)
