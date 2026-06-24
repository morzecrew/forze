---
title: Document ports
icon: lucide/file-text
summary: Every method on the document query and command ports
---

The document contract splits into a **query** port and a **command** port,
resolved from the context by spec:

```python
q = ctx.document.query(spec)    # reads
c = ctx.document.command(spec)  # writes
```

All methods are `async`. Naming is systematic: `get*`/`find*` return the read
model `R`; `project*` returns a `JsonDict` of selected fields; `select*` validates
into a caller-supplied `return_type`. The suffix sets the result container —
none → `CountlessPage` (no total), `_page` → `Page` (with `.count`), `_cursor` →
`CursorPage` (keyset), `_stream` → an async generator of batches. The `filters`,
`sorts`, and `aggregates` arguments follow the [query DSL](../query-syntax.md).

## Query port

### Fetch one

| Method | Returns | On miss |
|--------|---------|---------|
| `get(pk, *, for_update=False, skip_cache=False)` | `R` | raises `not_found` |
| `get_many(pks, *, skip_cache=False)` | `Sequence[R]` | raises `not_found` (lists missing) |
| `find(filters, *, for_update=False)` | `R \| None` | returns `None` |
| `project(filters, fields, *, for_update=False)` | `JsonDict \| None` | returns `None` |
| `select(filters, return_type, *, for_update=False)` | `T \| None` | returns `None` |

`for_update` takes a `RowLockMode` (`True` / `"nowait"` / `"skip_locked"`) to lock
the row inside a transaction.

### Fetch many

Each comes in `find` / `project` / `select` flavors and `_many` / `_page` /
`_cursor` containers. All take `filters`, `sorts`, and pagination:

| Method | Result |
|--------|--------|
| `find_many(filters=None, pagination=None, sorts=None)` | `CountlessPage[R]` (`.hits`) |
| `find_page(...)` | `Page[R]` (adds `.count`) |
| `find_cursor(filters=None, cursor=None, sorts=None)` | `CursorPage[R]` (keyset) |
| `project_many` / `project_page` / `project_cursor` `(fields, …)` | pages of `JsonDict` |
| `select_many` / `select_page` / `select_cursor` `(return_type, …)` | pages of `T` |

### Stream & aggregate

| Method | Result |
|--------|--------|
| `find_stream(filters=None, *, sorts=None, chunk_size=500)` | async generator of `Sequence[R]` |
| `project_stream` / `select_stream` | async generators of `JsonDict` / `T` batches |
| `aggregate_many(aggregates, filters=None, …)` | `CountlessPage[JsonDict]` |
| `aggregate_page(aggregates, …)` | `Page[JsonDict]` (group count) |
| `select_many_aggregated` / `select_page_aggregated` `(return_type, aggregates, …)` | typed aggregate rows |
| `count(filters=None)` | `int` |

`filters`, `sorts`, and `aggregates` use the [query DSL](../query-syntax.md);
`pagination` is `{"limit": …, "offset": …}`.

## Command port

Every mutating method takes `return_new: bool = True` — return the resulting read
model(s), or `None` when you don't need them back.

### Create

| Method | Signature | Notes |
|--------|-----------|-------|
| `create` | `create(payload, *, id=None, return_new=True)` | server-generates the PK unless `id` is given |
| `create_many` | `create_many(payloads, *, return_new=True)` | batch insert |
| `ensure` | `ensure(id, payload, *, return_new=True)` | insert-when-missing; **never mutates** an existing row (idempotent by PK) |
| `ensure_many` | `ensure_many(items, *, return_new=True)` | bulk insert-when-missing (`KeyedCreate`) |
| `upsert` | `upsert(id, create, update, *, return_new=True)` | insert `create`, else apply `update` (domain apply + OCC) |
| `upsert_many` | `upsert_many(items, *, return_new=True)` | bulk insert-or-update (`UpsertItem`) |

### Update

| Method | Signature | Notes |
|--------|-----------|-------|
| `update` | `update(pk, rev, dto, *, return_new=True, return_diff=False)` | optimistic — a stale `rev` raises `conflict`; `return_diff` adds the change `JsonDict` |
| `update_many` | `update_many(updates, *, return_new=True, return_diff=False)` | per-row `(pk, rev, dto)` with OCC |
| `update_matching` | `update_matching(filters, dto, *, return_new=True)` | fast bulk patch by filter — **no per-row OCC, no domain side effects**; `return_new=False` → rows-updated count |
| `update_matching_strict` | `update_matching_strict(filters, dto, *, return_new=True, chunk_size=None)` | like `update_many` (per-row OCC + domain apply) over a filter |
| `touch` / `touch_many` | `touch(pk, *, return_new=True)` | bump `last_update_at` only |

### Delete

`kill(pk)` and `kill_many(pks)` **hard-delete** — there is no soft-delete or
`restore` on the port (model soft-delete is a domain concern, applied via
`update`).
