---
title: Document
icon: lucide/file-text
summary: The document contract — its spec, the query/command ports, and every method
---

The document contract is Forze's CRUD aggregate store: a `DocumentSpec` binds a read
model (and optional write models) to a logical name, and the context resolves a **query**
port (reads) and a **command** port (writes) for it. The conceptual model is in
[Reading data](../../data-events/reading-data.md) and the
[writing-operation](../../writing-operation/wiring.md) chapter; this is the exhaustive
surface.

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

## Spec

`DocumentSpec[R, D, C, U]` — binds the read model and optional write types to a name,
plus per-aggregate policy:

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | logical name; the default namespace and route key |
| `read` | `type[R]` | required | the read model returned from queries |
| `write` | `DocumentWriteTypes \| None` | `None` | `{domain, create_cmd, update_cmd?}`; **omit for a read-only document** (no command port) |
| `history_enabled` | `bool` | `False` | keep an audit trail of every revision |
| `materialized` | `frozenset[str]` | `∅` | `@computed_field` names persisted as columns, so they're filterable/sortable |
| `read_conformity` | `"strict" \| "lenient"` | `"strict"` | `"lenient"` auto-derives `lenient_read_fields` from the read model (every statically-defaulted, non-identity, non-`materialized` field); explicit fields are added on top |
| `lenient_read_fields` | `frozenset[str]` | `∅` | read-model fields with **no** backing column: dropped from the projection, hydrated from their default, removed from the filter/sort/aggregate allow-sets, and tolerated by relational startup schema checks (see below) |
| `write_omit_fields` | `frozenset[str]` | `∅` | domain fields with **no** column: **silently stripped** from every write and hydrated from the domain default on read-back (the write-side of `lenient_read_fields`; explicit-only, requires `write`) |
| `default_sort` | `QuerySortExpression \| None` | `None` | sort applied when a caller omits `sorts` (required if the read model has no `id`) |
| `query_policy` | `QueryFieldPolicy \| None` | `None` | allow-sets restricting which fields a governed caller may filter / sort / aggregate |
| `query_params` | `type[BaseModel] \| None` | `None` | typed [query-parameter](../../data-events/query-parameters.md) contract, bound via `with_parameters` |
| `encryption` | `FieldEncryption \| None` | `None` | field-level [encryption](../../identity-tenancy-enc/encryption.md) policy (share the same object with the table's `SearchSpec`) |
| `cache` | `CacheSpec \| None` | `None` | read-through [cache](../../data-events/caching.md) for `get` |
| `sensitive` | `bool` | `False` | read model carries secrets; generated HTTP/MCP surfaces refuse to project it |
| `codecs` | `DocumentCodecs \| None` | `None` | codec overrides (auto-derived from the model types by default) |

`write` is a `DocumentWriteTypes` TypedDict — `domain` (the `Document` subclass),
`create_cmd`, and an optional `update_cmd`.

### Lenient read fields

By default every read-model field must map to a stored column, and a relational
backend fails at startup if one is missing — drift is caught at boot, not on the
first query. `lenient_read_fields` opts named fields out of that rule, on the read
side only:

- the field is **dropped from the read projection** and **hydrated from its model
  default** on every read (so it carries the same default for every row);
- it is **removed from the filter / sort / aggregate allow-sets** — a column that
  is not there cannot be queried;
- the Postgres startup schema check **tolerates the missing column** instead of
  failing (Mongo and Firestore are schemaless, so they tolerate it naturally).

Honored on Postgres, Mongo, and Firestore. Each name must be a non-computed
read-model field that carries a default (is non-required) and is not an
identity/audit field (`id`, `rev`, `created_at`, `last_update_at`) or a
`materialized` field. It is read-side only: if the same field is also stored on the
write/domain model over that relation, Postgres write-schema validation still
requires its column.

Use it for a field that exists in code ahead of (or independently of) the physical
column — e.g. during an expand/contract migration — or a read-model display field
the write model does not persist.

Instead of listing fields, set `read_conformity="lenient"` to **auto-derive** the set:
every statically-defaulted, non-identity read field that is not `materialized` becomes
lenient (fields with a `default_factory` are excluded — declare those explicitly to
accept a fresh value per row). Explicit `lenient_read_fields` are always added on top.
`resolved_lenient_read_fields` is the effective set every backend reads.

`write_omit_fields` is the **write-side** counterpart: a domain field with no column is
silently stripped from every insert/update and hydrates from the domain default on
read-back (Postgres, Mongo, and Firestore). Because the value is dropped (not persisted),
it is **explicit-only** — never auto-derived — requires a `write` spec, and each name must
be a defaulted, non-identity domain field. Use it for a domain field that is computed or
stored elsewhere, not on this table.

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
| `update_many` | `update_many(updates, *, return_new=True, return_diff=False)` | per-row update (`KeyedUpdate`: id, rev, dto) with OCC |
| `update_matching` | `update_matching(filters, dto, *, return_new=True)` | fast bulk patch by filter — **no per-row OCC, no domain side effects**; `return_new=False` → rows-updated count |
| `update_matching_strict` | `update_matching_strict(filters, dto, *, return_new=True, chunk_size=None)` | like `update_many` (per-row OCC + domain apply) over a filter |
| `touch` / `touch_many` | `touch(pk, *, return_new=True)` | bump `last_update_at` only |

### Delete

`kill(pk)` and `kill_many(pks)` **hard-delete** — there is no soft-delete or
`restore` on the port (model soft-delete is a domain concern, applied via
`update`).

## Implemented by

| Backend | Tenancy ceiling | Integration |
|---------|-----------------|-------------|
| Postgres | `dedicated` | [Postgres](../../integrations/postgres.md) |
| Mongo | `dedicated` | [Mongo](../../integrations/mongo.md) |
| Firestore | `dedicated` | [Firestore](../../integrations/firestore.md) |

The in-memory mock implements the full surface, so an aggregate is testable without a
backend — see [Testing](../../testing/overview.md).
