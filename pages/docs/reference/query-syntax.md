---
title: Query DSL
icon: lucide/filter
summary: The filter, sort, and aggregate expression language shared by document and search ports
---

Forze uses one expression language for **filtering**, **sorting**, and
**aggregating**, shared by document query ports (`find`, `find_many`, `count`, …)
and search requests. Learn it once; it applies everywhere — including
authorization scope filters.

## Filter expressions

A filter expression is exactly one of these shapes. **Combinators
(`$and`/`$or`/`$not`) cannot share a dict with constraints (`$values`/`$fields`)** —
that raises at parse time.

| Form | Shape |
|------|-------|
| Literal constraints | `{"$values": {…}}` |
| Field-to-field constraints | `{"$fields": {…}}` |
| Combined (implicit **AND**) | `{"$values": {…}, "$fields": {…}}` |
| Conjunction | `{"$and": [expr, …]}` |
| Disjunction | `{"$or": [expr, …]}` |
| Negation | `{"$not": expr}` (a single child object, not a list) |

## Field constraints — `$values`

Inside `$values`, each field maps to a literal shortcut or an explicit operator
map:

| Shortcut | Expands to | Meaning |
|----------|-----------|---------|
| `"active"` | `{"$eq": "active"}` | equality |
| `["a", "b"]` | `{"$in": ["a", "b"]}` | membership |
| `None` | `{"$null": true}` | is null |

```python
filters = {
    "$values": {
        "status": "active",          # → $eq
        "tags": ["backend", "api"],  # → $in
        "deleted_at": None,          # → $null
    }
}
```

### Operators

| Group | Operators | Operand |
|-------|-----------|---------|
| Comparison | `$eq` `$neq` `$gt` `$gte` `$lt` `$lte` | a scalar |
| Membership | `$in` `$nin` | a list — value is / isn't in it |
| Text | `$like` `$ilike` `$regex` | a pattern (or a list of patterns → OR) |
| Null | `$null` | `true`: is null · `false`: is not null |
| Empty | `$empty` | `true`: empty **array** · `false`: non-empty |
| Set relations | `$superset` `$subset` `$overlaps` `$disjoint` | a list (see [Array fields](#array-fields)) |
| Quantifiers | `$any` `$all` `$none` | an element predicate (see [Array fields](#array-fields)) |

`$empty` tests **array** length (not string emptiness). `$like`/`$ilike` use
`%`/`_` wildcards (`\` escapes); a list of patterns becomes an OR on that field.

### Combining operators on one field

Multiple operators on the same field are **ANDed**:

```python
{"$values": {"score": {"$gte": 1, "$lt": 10}}}   # score >= 1 AND score < 10
```

Two operators stand **alone** — `{"$null": true}` and `{"$empty": true}` can't be
combined with anything else on that field (their `false` forms can). A
[quantifier](#element-quantifiers) is also exclusive with other operators on its
field.

## Nested fields

A field key is a **dot-separated path** into a nested/embedded object — usable in
`$values`, `$fields`, sorts, `$groups`, and aggregate fields. Depth is unbounded;
each segment walks one level deeper:

```python
{"$values": {
    "address.city": "Berlin",
    "address.geo.lat": {"$gte": 52.0},
}}
```

The **root** segment must be a real column / read-model field; deeper segments
traverse a JSON/JSONB column. A few rules to know (Postgres):

- When the leaf type can't be inferred statically (a dynamic mapping, an `Any`),
  declare it via the adapter's `nested_field_hints={"address.geo.lat": float}`.
- Set operators (`$superset` / `$subset` / `$overlaps` / `$disjoint` / `$empty`)
  are **not** supported on nested JSON paths — use a top-level array column for
  those.

## Array fields

Two distinct ways to query an array column.

### Set relations

The whole array, compared against a list:

| Operator | Matches when the field… |
|----------|-------------------------|
| `$superset` | contains **all** the listed values |
| `$subset` | has **no** values outside the list |
| `$overlaps` | **intersects** the list |
| `$disjoint` | does **not** intersect the list |

```python
{"$values": {"roles": {"$superset": ["admin", "ops"]}}}   # has both roles
```

### Element quantifiers

`$any` / `$all` / `$none` apply a predicate to *individual* elements. `$all` and
`$none` are vacuously true on a missing, null, or empty array. A quantifier holds
**exactly one** of three operand forms:

```python
# 1. scalar shortcut → element equality
{"$values": {"tags": {"$any": "urgent"}}}

# 2. a single element operator (only $eq $neq $gt $gte $lt $lte $like $ilike $regex)
{"$values": {"scores": {"$all": {"$gte": 1}}}}

# 3. $values map for an array of objects — fields are element-relative
{"$values": {"line_items": {"$any": {"$values": {
    "product_id": "p-42",
    "quantity": {"$gt": 0},
}}}}}
```

Quantifiers **do not nest** (`$any` inside `$any` is rejected), and inside an
object-array `$values` the `null` / list / quantifier shortcuts aren't allowed —
only literal operators (which still AND together per field).

## Comparing fields — `$fields`

Compare one field to another, not to a literal. A bare string value is a **field
path**, and only the equality/ordering operators apply (no membership, text, or
set operators here):

```python
filters = {
    "$values": {"is_deleted": False},
    "$fields": {"starts_at": {"$lte": "ends_at"}},
}
```

## Combining expressions — `$and` / `$or` / `$not`

```python
{"$and": [
    {"$values": {"status": ["active", "trial"]}},     # $in shortcut
    {"$or": [
        {"$values": {"region": "eu"}},
        {"$not": {"$values": {"deleted_at": {"$null": False}}}},
    ]},
    {"$fields": {"updated_at": {"$gt": "created_at"}}},
]}
```

`$and`/`$or` take a list of expressions; `$not` takes a single expression.
Nesting depth is capped (see [Limits](#limits)).

## Sorting

A map of field → direction (`"asc"` / `"desc"`); keys may be nested paths, and
map order is sort priority:

```python
sorts = {"created_at": "desc", "id": "asc"}
```

The DSL has **no null-ordering control** (no `NULLS FIRST/LAST`). For
[cursor pagination](../in-depth/reading-data.md) all keys must share one
direction, and an `id` tie-breaker is appended automatically.

## Aggregates

An aggregate expression groups and computes over matched rows. Group keys go in
`$groups` (alias → source path, or a list of paths); outputs go in `$computed`
(alias → function). Functions: `$count` (use `None` for row counts), `$sum`,
`$avg`, `$min`, `$max`, `$median`.

```python
aggregates = {
    "$groups": {"category": "category"},
    "$computed": {
        "products": {"$count": None},
        "revenue": {"$sum": "price"},
        "premium_revenue": {
            "$sum": {"field": "price", "filter": {"$values": {"price": {"$gte": 20}}}},
        },
    },
}
```

A computed metric's `filter` is a **per-metric row pre-filter** (it narrows the
rows that feed *that* aggregate) — there is **no post-aggregate `HAVING`** stage.
`$count` takes no field; every other function requires one.

Calendar bucketing uses `$trunc` as a group value — `unit` is one of `hour` /
`day` / `week` (Monday-start) / `month`; `timezone` is an IANA name or fixed
offset (default UTC):

```python
"$groups": {"day_start": {"$trunc": {"field": "ts", "unit": "day", "timezone": "+3"}}}
```

## Where you pass them

Document query ports take `filters`, `sorts`, `pagination`, and (where supported)
`aggregates`:

```python
doc = ctx.document.query(project_spec)
page = await doc.find_many(
    filters=filters,
    sorts=sorts,
    pagination={"limit": 20, "offset": 0},
)
rows = page.hits
```

Search requests take the same filter and sort expressions alongside the query
text:

```python
page = await ctx.search.query(project_search_spec).search(
    "roadmap",
    filters=filters,
    pagination={"limit": 20, "offset": 0},
)
```

## Limits

Filters are validated at parse time, before any query reaches the database.
Defaults (override per gateway via `filter_limits`):

| Limit | Default | Applies to |
|-------|---------|------------|
| `max_depth` | 32 | nesting of `$and` / `$or` / `$not` |
| `max_clauses` | 256 | combinator children, `$values` / `$fields` keys, per-field operators |
| `max_in_size` | 1000 | `$in` / `$nin`, array shortcuts, set-relation operands |
| `max_pattern_length` | 256 | each `$like` / `$ilike` / `$regex` pattern |
| `max_pattern_or_branches` | 32 | patterns when a text operand is a sequence |

A violation — or an empty operator map, an unknown operator, a type mismatch, or a
regex with unsafe nesting/repetition — raises a validation `CoreException` before
the query runs.

## Backend notes

Semantics are shared; rendering is backend-specific. Text-pattern support varies:

| Operator | Postgres | MongoDB | Firestore |
|----------|----------|---------|-----------|
| `$like` | `LIKE` | `$regex` | not supported |
| `$ilike` | `ILIKE` | `$regex` + `i` | not supported |
| `$regex` | `~` | `$regex` | not supported |

Leading-`%` patterns may need a trigram index (Postgres `pg_trgm`) to stay fast on
large tables. On MongoDB, `$null: true` matches both explicit null and missing
fields; object-array quantifiers render as `$elemMatch`.
