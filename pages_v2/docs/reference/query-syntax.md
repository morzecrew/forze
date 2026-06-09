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

A filter expression is one of:

| Form | Shape |
|------|-------|
| Literal constraints | `{"$values": {…}}` |
| Field-to-field constraints | `{"$fields": {…}}` |
| Combined (implicit **AND**) | `{"$values": {…}, "$fields": {…}}` |
| Conjunction | `{"$and": [expr, …]}` |
| Disjunction | `{"$or": [expr, …]}` |
| Negation | `{"$not": expr}` (a single child object, not a list) |

`$values` and `$fields` may share one object (all constraints are ANDed). Don't
mix `$and` / `$or` / `$not` with `$values` / `$fields` in the *same* object.

## `$values` — field constraints

Inside `$values`, each field maps to a shortcut or an explicit operator map:

| Value | Expands to | Meaning |
|-------|-----------|---------|
| `"active"` | `{"$eq": "active"}` | equality |
| `["a", "b"]` | `{"$in": ["a", "b"]}` | membership |
| `None` | `{"$null": true}` | is null |

```python
filters = {
    "$values": {
        "status": "active",        # → $eq
        "tags": ["backend", "api"],  # → $in
        "deleted_at": None,          # → $null
    }
}
```

### Operators

=== "Equality & ordering"

    | Operator | Meaning |
    |----------|---------|
    | `$eq` / `$neq` | equal / not equal |
    | `$gt` / `$gte` | greater than / or equal |
    | `$lt` / `$lte` | less than / or equal |

=== "Membership & unary"

    | Operator | Meaning |
    |----------|---------|
    | `$in` / `$nin` | in / not in a list |
    | `$null` | `true`: is null · `false`: is not null |
    | `$empty` | `true`: empty array · `false`: non-empty |

    `$null` and `$empty` can't be combined with other operators on the same field.

=== "Set relations (arrays)"

    | Operator | Meaning |
    |----------|---------|
    | `$superset` | field contains all listed values |
    | `$subset` | field has no values outside the list |
    | `$overlaps` | field intersects the list |
    | `$disjoint` | field doesn't intersect the list |

=== "Text matching"

    | Operator | Meaning |
    |----------|---------|
    | `$like` | SQL `LIKE` (`%`, `_` wildcards; `\` escapes) |
    | `$ilike` | case-insensitive `LIKE` |
    | `$regex` | POSIX regex |

    A **sequence** of patterns expands to an implicit OR on that field:

    ```python
    {"$values": {"title": {"$ilike": ["%road%", "%map%"]}}}
    ```

### Array element quantifiers

Apply a predicate to *individual elements* of an array field — one quantifier per
field, not combined with other operators on that key:

| Operator | Matches when |
|----------|--------------|
| `$any` | at least one element matches |
| `$all` | every element matches (vacuously true if missing / null / `[]`) |
| `$none` | no element matches (vacuously true if missing / null / `[]`) |

```python
{"$values": {"tags": {"$any": "urgent"}}}                 # scalar array
{"$values": {"items": {"$any": {"$values": {"qty": {"$gte": 1}}}}}}  # object array
```

## `$fields` — compare field to field

Compare one field to another (not to a literal), using the equality and ordering
operators. A bare string value is a **field path**, not a literal:

```python
filters = {
    "$values": {"is_deleted": False},
    "$fields": {"starts_at": {"$lte": "ends_at"}},
}
```

## `$not` — negation

```python
filters = {"$not": {"$or": [
    {"$values": {"priority": {"$lt": 3}}},
    {"$values": {"is_deleted": True}},
]}}
```

## Sorting

A map of field → direction (`"asc"` / `"desc"`). Omitted, the layer adds no
explicit order — pass one when callers need determinism:

```python
sorts = {"created_at": "desc", "id": "asc"}
```

## Aggregates

An aggregate expression groups and computes over matched rows. Group keys go in
`$groups` (alias → source path, or a list of paths); outputs go in `$computed`
(alias → function). Functions: `$count` (use `None` for row counts), `$sum`,
`$avg`, `$min`, `$max`, `$median`. A computed field may carry its own `filter`.

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

Calendar bucketing uses `$trunc` as a group value (`unit`: `hour` / `day` /
`week` / `month`; `timezone` IANA name or fixed offset):

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

A violation — or an empty operator map, an unknown operator, or a type mismatch —
raises a validation `CoreException` before the query runs.

## Backend notes

Semantics are shared; rendering is backend-specific. Text-pattern support varies:

| Operator | Postgres | MongoDB | Firestore |
|----------|----------|---------|-----------|
| `$like` | `LIKE` | `$regex` | not supported |
| `$ilike` | `ILIKE` | `$regex` + `i` | not supported |
| `$regex` | `~` | `$regex` | not supported |

Leading-`%` patterns may need a trigram index (Postgres `pg_trgm`) to stay fast on
large tables. On MongoDB, `$null: true` matches both explicit null and missing
fields.
</content>
