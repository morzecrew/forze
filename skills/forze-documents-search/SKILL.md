---
name: forze-documents-search
description: >-
  Implements Forze document and search access: DocumentQueryPort,
  DocumentCommandPort, SearchQueryPort, query DSL, pagination, cache-aware
  DocumentSpec / SearchSpec usage, Postgres/Mongo/mock adapters, and Postgres
  simple/hub/federated search. Use when building data access features.
---

# Forze documents and search

Use when implementing document persistence, filtered listings, cursor pagination, text search, hub search, or federated search. Pair with [`forze-domain-aggregates`](../forze-domain-aggregates/SKILL.md) for aggregate models and [`forze-specs-infrastructure`](../forze-specs-infrastructure/SKILL.md) for mapping `spec.name` to tables and indexes in deps modules.

## Document ports (`DocumentQueryPort` / `DocumentCommandPort`)

Logical **`DocumentSpec`** carries model types and `name` only; **`PostgresDepsModule`** / **`MongoDepsModule`** (and related maps) supply tables, history relations, and bookkeeping. At runtime, `ctx.document.query(spec)` resolves the factory registered under **`DocumentQueryDepKey`** for route `spec.name` and returns **`DocumentQueryPort[read]`**; `ctx.document.command(spec)` does the same for **`DocumentCommandDepKey`** → **`DocumentCommandPort`**.

```python
doc_q = self.ctx.document.query(project_spec)
doc_c = self.ctx.document.command(project_spec)

project = await doc_q.get(project_id)
page = await doc_q.find_page(
    filters={"$values": {"status": "active"}},
    pagination={"limit": 20, "offset": 0},
    sorts={"created_at": "desc", "id": "asc"},
)
rows, total = page.hits, page.count

updated = await doc_c.update(project_id, project.rev, UpdateProjectCmd(title="Done"))
```

Revision-bearing writes enforce optimistic concurrency. Use **`DocumentQueryPort`** methods such as ``project`` / ``project_many`` for partial reads; use `return_new=False` on command updates when the updated model is not needed.

## Query DSL

Use the shared JSON DSL, not adapter-specific SQL/Mongo syntax, in application code.

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

Attach `CacheSpec` to `DocumentSpec.cache` and register a matching cache route, usually in `RedisDepsModule.caches`. Document deps factories resolve `ctx.cache(spec.cache)` while building query/command ports.

```python
project_spec = DocumentSpec(
    name=ResourceName.PROJECTS,
    read=ProjectRead,
    write={...},
    cache=CacheSpec(name=ResourceName.PROJECTS, ttl=timedelta(minutes=5)),
)
```

When `AfterCommitPort` is wired, document cache warm/invalidation happens after a successful commit.

## Search (`SearchQueryPort`)

Use **`SearchSpec`** for logical searchable models. `ctx.search_query(spec)` resolves **`SearchQueryDepKey`** for route `spec.name` and returns **`SearchQueryPort`**; physical FTS/PGroonga layout belongs in **`PostgresDepsModule.searches`** (or hub/federated maps), not on the spec.

```python
project_search = SearchSpec(
    name=ResourceName.PROJECTS,
    model_type=ProjectRead,
    fields=("title", "description"),
    default_weights={"title": 0.7, "description": 0.3},
)

hits, total = await self.ctx.search_query(project_search).search(
    query="roadmap",
    filters={"$values": {"status": "active"}},
    limit=20,
)
```

## Hub and federated search

Use `HubSearchSpec` when one hub entity searches through weighted member legs. Use `FederatedSearchSpec` when merging independent search specs. Resolve with `ctx.hub_search_query(spec)` or `ctx.federated_search_query(spec)`.

Keep snapshot storage and cursor/keyset behavior in infrastructure config; use the application search options helpers rather than duplicating merge or cursor logic.

## Adapter boundaries

- Postgres and Mongo implement document query/command gateways and history where configured.
- Mongo implements `SearchQueryPort` when `MongoDepsModule.searches` is wired (`text`, `atlas`, `vector` engines).
- Mock implements document/search behavior for unit tests.
- Use adapters in integration tests or deps modules, not handlers.

## Anti-patterns

1. **Putting table/collection/index names in `DocumentSpec` or `SearchSpec`** — use deps-module configs.
2. **Importing Postgres/Mongo adapters in handlers** — use ports.
3. **Using old `ctx.doc_read` / `ctx.doc_write` / `ctx.search` helpers** — use current helpers.
4. **Sorting cursor pages without stable key fields** — include deterministic sort keys, usually `id`.
5. **Bypassing revision fields on writes** — preserve optimistic concurrency semantics.

## Reference

- [Specs and wiring](https://morzecrew.github.io/forze/docs/concepts/specs-and-wiring/)
- [Document contracts](https://morzecrew.github.io/forze/docs/core-package/contracts/document/)
- [Query syntax](https://morzecrew.github.io/forze/docs/core-package/query-syntax/)
- [Contracts overview](https://morzecrew.github.io/forze/docs/core-package/contracts/)
- [Postgres integration](https://morzecrew.github.io/forze/docs/integrations/postgres/)
- [Mongo integration](https://morzecrew.github.io/forze/docs/integrations/mongo/)
