---
title: Search ports
icon: lucide/search
summary: Methods on the search query and command (index-maintenance) ports
---

Search splits into a **query** port (run searches) and a **command** port
(maintain the index):

```python
q = ctx.search.query(spec)    # search
c = ctx.search.command(spec)  # index maintenance
```

`ctx.search` also exposes `.hub(spec)` and `.federated(spec)` (both return a
query port over composed indexes) and `.snapshot(spec)` for result-set snapshots.

## Query port

Same `search` / `project_search` / `select_search` flavors and `_page` / `_cursor`
containers as the [document query port](document.md). The query text is the first
argument; everything else mirrors the document side:

| Method | Result |
|--------|--------|
| `search(query, filters=None, pagination=None, sorts=None, *, options=None, snapshot=None)` | `CountlessPage[R]` |
| `search_page(...)` | `Page[R]` (with `.count`) |
| `search_cursor(query, filters=None, cursor=None, sorts=None, *, options=None)` | `CursorPage[R]` |
| `project_search` / `project_search_page` / `project_search_cursor` `(fields, query, …)` | pages of `JsonDict` |
| `select_search` / `select_search_page` / `select_search_cursor` `(return_type, query, …)` | pages of `T` |

`query` is a string (or a sequence of strings); `filters` and `sorts` use the
[query DSL](../query-syntax.md). `options: SearchOptions` tunes relevance,
highlighting, etc.

## Command port

| Method | Signature | Notes |
|--------|-----------|-------|
| `ensure_index` | `ensure_index()` | create / update the index settings |
| `upsert` | `upsert(documents)` | add or update documents |
| `upsert_many` | `upsert_many(documents)` | batch add / update |
| `delete` | `delete(ids)` | remove by id |
| `delete_all` | `delete_all()` | empty the index |

See the [Meilisearch](../../integrations/meilisearch.md) integration.
