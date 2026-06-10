---
title: Read-only document API
icon: lucide/book-open
summary: Expose a typed query API for documents written by another service — no write side
---

When a service only *reads* an aggregate — a projection, a lookup, a reporting
view owned by someone else — give its spec `write=None`. No command port is
registered, so the API can only query, and the wiring stays minimal.

The runnable version lives at `examples/recipes/read_only/` — `just run` brings
up ephemeral Postgres, seeds a couple of rows, and serves the read API.

## A read-only spec

`write=None` is the whole opt-in. The read model still inherits `id`, `rev`, and
timestamps from `ReadDocument`:

```python
--8<-- "recipes/read_only/app.py:spec"
```

## Wire it read-only

Register the document under **`ro_documents`** (not `rw_documents`) with a
`PostgresReadOnlyDocumentConfig` — it carries only the read relation, no write
tables or bookkeeping, and no transaction route is needed:

```python
--8<-- "recipes/read_only/app.py:wiring"
```

!!! note "Data gets in elsewhere"

    A read-only doc has **no command port** — `ctx.document.command(spec)` won't
    resolve. The writer is another service (or a migration); the example seeds
    rows directly through the client just to be self-contained.

## Query routes

Resolve `ctx.document.query(spec)` and call its read methods:

```python
--8<-- "recipes/read_only/app.py:routes"
```

The query port gives you the full read surface — pick by how you want misses
handled:

| Method | Returns | On miss |
|--------|---------|---------|
| `get(id)` | the document | raises `not_found` → 404 |
| `get_many(ids)` | a list | raises `not_found` if any is missing |
| `find(filters)` | the document or `None` | returns `None` |
| `find_many(filters, pagination, sorts)` | a `CountlessPage` (`.hits`) | empty page |
| `find_page(...)` | a `Page` (adds `.count`) | empty page |
| `count(filters)` | an `int` | `0` |

Filters and sorts use the [query DSL](../reference/query-syntax.md); pagination
is `{"limit": …, "offset": …}`.
