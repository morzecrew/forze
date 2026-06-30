---
name: forze-documents-search
description: >-
  Implements Forze document and search access the kit-first way: DocumentFacade
  and SearchFacade over build_document_registry / build_search_registry, kit
  DTOs (DocumentIdDTO, ListRequestDTO, DocumentUpdateDTO), the query DSL,
  pagination, cache-aware DocumentSpec / SearchSpec, and the Postgres / Mongo /
  Firestore / Meilisearch backends behind them. Use when building data access
  features.
---

# Forze documents and search

Use when implementing document persistence, filtered listings, cursor pagination, text search, hub search, or federated search. Pair with [`forze-domain-aggregates`](../forze-domain-aggregates/SKILL.md) for aggregate models and [`forze-specs-infrastructure`](../forze-specs-infrastructure/SKILL.md) for mapping `spec.name` to tables and indexes in deps modules.

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

## Query DSL

`filters` and `sorts` on `ListRequestDTO` (and on search requests) use the shared JSON DSL — never adapter-specific SQL/Mongo syntax in application code:

```python
filters = {
    "$and": [
        {"$values": {"status": {"$in": ["active", "paused"]}}},
        {"$values": {"created_at": {"$gte": since}}},
    ]
}
```

Common operators: `$eq`, `$neq`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$null`, `$empty`, `$superset`, `$subset`, `$overlaps`, `$disjoint`.

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

## Search with `SearchFacade`

Drive search through a **`SearchFacade`** built from a `SearchSpec`, the same way:

```python
from forze.application.contracts.search import SearchSpec
from forze_kits.aggregates.search import (
    SearchFacade,
    SearchRequestDTO,
    build_search_registry,
)

project_search = SearchSpec(
    name=ResourceName.PROJECTS,
    model_type=ProjectRead,
    fields=("title", "description"),
    default_weights={"title": 0.7, "description": 0.3},
)
search_registry = build_search_registry(project_search).freeze()


def project_search_facade(ctx) -> SearchFacade[ProjectRead]:
    return SearchFacade(ctx=ctx, registry=search_registry, namespace=project_search.default_namespace)


page = await project_search_facade(ctx).search(
    SearchRequestDTO(query="roadmap", page=1, size=20, filters={"$values": {"status": "active"}})
)
hits, total = page.hits, page.count
```

`ResourceName.PROJECTS` (your spec-name enum) and `ProjectRead` (your read model) are app-defined symbols.

Methods: `search` (typed, offset), `cursor_search` (typed, keyset), `projected_search` / `projected_cursor_search` (raw dict rows). The physical FTS/PGroonga/vector layout belongs in **`PostgresDepsModule.searches`** (or hub/federated maps), never on the spec.

For faceted navigation and result highlighting, declare `facetable_fields` / `highlightable_fields` on the `SearchSpec` and request them per query through search options (`facets=[…]`, `highlight=True`); the page carries `page.facets` and per-hit `page.highlights`, failing closed when a field or backend can't serve them. Per-request options are backend-agnostic — single-index search takes `SearchOptions`, while hub and federated search take `MultiSourceSearchOptions` (adds `member_weights` / `members`).

## Hub and federated search

Use `HubSearchSpec` with `build_hub_search_registry` when one hub entity searches through weighted member legs — it yields the full `SearchFacade` surface. Use `FederatedSearchSpec` with `build_federated_search_registry` to merge independent specs; it registers only the typed `search` and `cursor_search` (no `projected_search` / `projected_cursor_search`). Keep snapshot storage and cursor/keyset behaviour in infrastructure config.

## Custom operations and raw ports

The facade covers the standard document/search surface. When you need behaviour the facade doesn't model — a multi-step domain operation, a saga step, a one-off projection — write a handler (or reach the port directly) via the namespaced context: `ctx.document.query(spec)` → `DocumentQueryPort[read]`, `ctx.document.command(spec)` → `DocumentCommandPort`, `ctx.search.query(spec)` → `SearchQueryPort`. This is the escape hatch, not the default for CRUD.

## Adapter boundaries

- Postgres, Mongo, and Firestore implement the document gateways (and history where configured). Firestore wires them via `FirestoreDepsModule(ro_documents=..., rw_documents=...)` with `FirestoreReadOnlyDocumentConfig` / `FirestoreDocumentConfig`.
- Postgres implements search (FTS/PGroonga/vector); Mongo when `MongoDepsModule.searches` is wired (`text`, `atlas`, `vector`); Meilisearch via `MeilisearchDepsModule(searches={...})` with `MeilisearchSearchConfig` (plus `MeilisearchFederatedSearchConfig`). All resolve through the same facade/port surface.
- Mock implements document/search behaviour for unit tests.
- Use adapters in deps modules and integration tests, never in handlers.

## Anti-patterns

1. **Reaching raw `ctx.document.query/command` for standard CRUD** — use a `DocumentFacade` (likewise `SearchFacade` for search); raw ports are for custom handlers and orchestration only.
2. **Putting table/collection/index names in `DocumentSpec` or `SearchSpec`** — use deps-module configs.
3. **Importing Postgres/Mongo adapters in handlers** — go through the facade/ports.
4. **Using removed flat accessors (`ctx.search_query`, `ctx.doc_read`, `ctx.doc_write`)** — use the namespaced `ctx.document.query` / `ctx.document.command` / `ctx.search.query`.
5. **Sorting cursor pages without stable key fields** — include a deterministic sort key, usually `id`.
6. **Bypassing the revision on updates** — always pass the read `rev` through `DocumentUpdateDTO` to preserve optimistic concurrency.

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version (e.g. `.../forze/0.3/...`) or use the version selector on the site.

- [Reading data](https://morzecrew.github.io/forze/latest/data-events/reading-data/)
- [Caching reads](https://morzecrew.github.io/forze/latest/data-events/caching/)
- [Specs and wiring](https://morzecrew.github.io/forze/latest/writing-operation/wiring/)
- [Document contracts](https://morzecrew.github.io/forze/latest/reference/contracts/document/)
- [Query syntax](https://morzecrew.github.io/forze/latest/reference/query-syntax/)
- [Postgres integration](https://morzecrew.github.io/forze/latest/integrations/postgres/)
- [Mongo integration](https://morzecrew.github.io/forze/latest/integrations/mongo/)
- Sibling skills: [`forze-domain-aggregates`](../forze-domain-aggregates/SKILL.md), [`forze-specs-infrastructure`](../forze-specs-infrastructure/SKILL.md)
