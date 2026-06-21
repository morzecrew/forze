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

Build the document registry with only the read DTO, then call the typed facade:

```python
--8<-- "recipes/read_only/app.py:routes"
```

The facade gives driving code a typed operation surface without exposing the
query port:

| Method | Returns | On miss |
|--------|---------|---------|
| `get(DocumentIdDTO(...))` | the document | raises `not_found` → 404 |
| `list(ListRequestDTO(...))` | a `Paginated` result | empty page |

`ListRequestDTO` carries page, size, filters, and sorts. Filters and sorts use
the [query DSL](../reference/query-syntax.md).
