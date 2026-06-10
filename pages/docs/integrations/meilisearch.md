---
title: Meilisearch
icon: lucide/search
summary: Full-text and federated search on Meilisearch
---

`forze[meilisearch]` implements the search contracts on Meilisearch — querying an
external index plus the command side that maintains it. The index holds a **read
model**, maintained explicitly (not auto-synced from your document store).

## Install

```bash
uv add 'forze[meilisearch]'
```

Needs a Meilisearch server.

## The client

```python
from forze_meilisearch import MeilisearchClient

meili = MeilisearchClient()
```

`RoutedMeilisearchClient` resolves a per-tenant instance/key.

## Wire it

Each search route names an index, keyed by `SearchSpec.name`:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_meilisearch import MeilisearchClient, MeilisearchDepsModule, MeilisearchSearchConfig, meilisearch_lifecycle_step

orders_search = MeilisearchSearchConfig(
    index_uid="orders",
    filterable_attributes=("status",),
    sortable_attributes=("created_at",),
)

deps = DepsRegistry.from_modules(MeilisearchDepsModule(client=meili, searches={"orders": orders_search}))
lifecycle = LifecyclePlan.from_steps(meilisearch_lifecycle_step(url="http://localhost:7700", api_key="…"))
```

## What it provides

| Contract | Keyed by |
|----------|----------|
| Search query | `SearchSpec.name` (`searches`) |
| Search command (index maintenance: `ensure_index`, `upsert`, `delete`) | `SearchSpec.name` |
| Federated search | federated route (`federated_searches`) |

## Notes

- **The index is yours to maintain.** There's no auto-sync from
  `DocumentCommandPort` — call `ctx.search.command(spec).upsert(...)` (e.g. via
  the outbox) when documents change. `ensure_index` applies the
  searchable/filterable/sortable attributes.
- Cursor pagination and hub search aren't supported here; the filter language is
  a subset of the [Query DSL](../reference/query-syntax.md).
- Federated routes merge ≥2 member indexes (`federation` or in-process RRF).
