---
name: forze-documents-search
description: >-
  Implements Forze document reads/writes, query DSL, pagination, cache-aware
  DocumentSpec wiring, SearchSpec, Postgres/Mongo/mock document adapters, and
  Postgres simple/hub/federated search. Use when building data access features.
---

# Forze documents and search

Use when implementing document persistence, filtered listings, cursor pagination, text search, hub search, or federated search. Pair with [`forze-domain-aggregates`](../forze-domain-aggregates/SKILL.md) for aggregate models.

## Document ports

Use `ctx.doc_query(spec)` for reads and `ctx.doc_command(spec)` for writes.

```python
doc_q = self.ctx.doc_query(project_spec)
doc_c = self.ctx.doc_command(project_spec)

project = await doc_q.get(project_id)
rows, total = await doc_q.find_many(
    filters={"$fields": {"status": "active"}},
    limit=20,
    sorts={"created_at": "desc", "id": "asc"},
)

updated = await doc_c.update(project_id, project.rev, UpdateProjectCmd(title="Done"))
```

Revision-bearing writes enforce optimistic concurrency. Use `return_fields` for projections and `return_new=False` when the updated model is not needed.

## Query DSL

Use the shared JSON DSL, not adapter-specific SQL/Mongo syntax, in application code.

```python
filters = {
    "$and": [
        {"$fields": {"status": {"$in": ["active", "paused"]}}},
        {"$fields": {"created_at": {"$gte": since}}},
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

## Search

Use `SearchSpec` for logical searchable models and `ctx.search_query(spec)` for full-text search.

```python
project_search = SearchSpec(
    name=ResourceName.PROJECTS,
    model_type=ProjectRead,
    fields=("title", "description"),
    default_weights={"title": 0.7, "description": 0.3},
)

hits, total = await self.ctx.search_query(project_search).search(
    query="roadmap",
    filters={"$fields": {"status": "active"}},
    limit=20,
)
```

Postgres physical search layout belongs in `PostgresDepsModule.searches`, `hub_searches`, or `federated_searches`, not in `SearchSpec`.

## Hub and federated search

Use `HubSearchSpec` when one hub entity searches through weighted member legs. Use `FederatedSearchSpec` when merging independent search specs. Resolve with `ctx.hub_search_query(spec)` or `ctx.federated_search_query(spec)`.

Keep snapshot storage and cursor/keyset behavior in infrastructure config; use the application search options helpers rather than duplicating merge or cursor logic.

## Adapter boundaries

- Postgres and Mongo implement document query/command gateways and history where configured.
- Mock implements document/search behavior for unit tests.
- Use adapters in integration tests or deps modules, not usecases.

## Anti-patterns

1. **Putting table/collection/index names in `DocumentSpec` or `SearchSpec`** — use deps-module configs.
2. **Importing Postgres/Mongo adapters in usecases** — use ports.
3. **Using old `ctx.doc_read` / `ctx.doc_write` / `ctx.search` helpers** — use current helpers.
4. **Sorting cursor pages without stable key fields** — include deterministic sort keys, usually `id`.
5. **Bypassing revision fields on writes** — preserve optimistic concurrency semantics.

## Reference

- [`pages/docs/core-package/query-syntax.md`](../../pages/docs/core-package/query-syntax.md)
- [`pages/docs/core-package/contracts.md`](../../pages/docs/core-package/contracts.md)
- [`pages/docs/integrations/postgres.md`](../../pages/docs/integrations/postgres.md)
- [`pages/docs/integrations/mongo.md`](../../pages/docs/integrations/mongo.md)
- [`src/forze/application/contracts/search`](../../src/forze/application/contracts/search)
