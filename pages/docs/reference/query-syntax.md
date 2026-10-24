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
(`$and`/`$or`/`$not`) cannot share a dict with constraints (`$values`/`$fields`)**, nor
with each other — that raises at parse time. So does a key none of these shapes defines:
`{"$values": {…}, "$valeus": {…}}` is refused, naming `$valeus`, rather than run without
the constraint it meant to add.

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
| Hierarchy | `$descendant_of` `$ancestor_of` | a path (or a list of paths → OR) on a `TreePath`-typed field — inclusive, materialized-path semantics; backend-specific and capability-gated |

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
`$values`, `$fields`, sorts, `$groups`, aggregate fields, and the `fields` list of
`project*` calls (a dotted projection returns a nested shape, and a path crossing a
list maps over each element). Depth is unbounded;
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
  declare it via the adapter's `nested_field_hints={"address.geo.lat": float}`. A
  field inside a quantified array is keyed by its full path too
  (`{"offers.price": int}`); a hint on the array itself (`offers`) is not read.
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

A quantifier can nest. Under an object element's `$values` it reaches a sub-array of
that element, and directly inside another quantifier it ranges over an array of arrays:

```python
{"$values": {"orders": {"$any": {"$values": {"items": {"$any": {"$values": {"sku": "a"}}}}}}}}
{"$values": {"matrix": {"$any": {"$any": "hot"}}}}
```

A backend that cannot compile a nested quantifier refuses it with
`query_feature_unsupported`. Inside an object-array `$values` the `null` and list
shortcuts aren't allowed; a field takes a scalar, literal operators (which still AND
together per field) or a nested quantifier.

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

!!! note "Filters written before 0.7"
    `$fields` used to hold value predicates; those belong under `$values` now, and a refused
    filter says so. The shape that cannot be caught is a string value naming a real field, such
    as `{"$fields": {"status": "draft"}}` on a model with a `draft` field: it reads as comparing
    `status` with `draft`.

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

A key may instead be an object to control null placement —
`{"dir": "asc", "nulls": "first"}` (`"first"` / `"last"`). Omitted, nulls sort as
the smallest value (`asc` → first, `desc` → last); some backends (Mongo, Firestore,
Meilisearch, and search cursors on Postgres) support only that default and reject an
explicit override. For
[cursor pagination](../data-events/reading-data.md) directions may be mixed — each
key seeks in its own direction — and an `id` tie-breaker is appended automatically.
Cursor tokens can be HMAC-signed (`<payload>.<hmac>`) or AEAD-encrypted (a leading
`~`) — see [Cursor tokens](../identity-tenancy-enc/cursor-tokens.md).

## Aggregates

An aggregate expression groups and computes over matched rows. Group keys go in
`$groups` (alias → source path, or a list of paths); outputs go in `$computed`
(alias → function). Functions: `$count` (use `None` for row counts),
`$count_distinct`, `$sum`, `$avg`, `$min`, `$max`, `$median`, `$percentile`
(requires the application form with a `p` quantile), `$stddev_pop`, `$stddev_samp`,
`$var_pop`, `$var_samp`. `$median` / `$percentile` are exact on Postgres and the
mock but approximate on Mongo.

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
rows that feed *that* aggregate). For a post-aggregate stage, add `$having` — a
filter (same grammar) over the **aggregated** rows, referencing only the output
aliases (group keys and computed metrics). A group key or a `$min` / `$max` has
its field's type, and every other metric is a number, so a text, set or hierarchy
operator on a numeric output (`$like` on a `$count`, or on the `$max` of a number)
is refused before anything runs, and a bound written as a string is cast to the
output's type as a filter casts it. `$count` takes no field; every other function
requires one.

Calendar bucketing uses `$trunc` as a group value — `unit` is one of `hour` /
`day` / `week` (Monday-start) / `month`; `timezone` is an IANA name or fixed
offset (default UTC):

```python
{"$groups": {"day_start": {"$trunc": {"field": "ts", "unit": "day", "timezone": "+3"}}}}
```

A bucket is the instant it starts at. In `$having`, an aware bound compares as
that instant and a naive one as wall time in the bucket's zone, as the bucket was
cut. A Postgres `timestamp` column, which has no zone, is read as UTC before it
is bucketed, as a filter on it reads it.

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
The defaults below apply unless a `DocumentSpec` or `SearchSpec` sets
`filter_limits=QueryFilterLimits(...)`. Every backend serving the spec uses those
limits for its filters, aggregate metric filters and `$having`, the mock included,
and so does a generated route or tool that passes a caller's filter through.
Raising `max_in_size` for internal reads that match long id lists raises it for
those callers too. A `HubSearchSpec` has no `filter_limits`: hub search filters
always use the defaults.

| Limit | Default | Applies to |
|-------|---------|------------|
| `max_depth` | 32 | nesting of `$and` / `$or` / `$not`, and separately of element quantifiers |
| `max_clauses` | 256 | combinator children, `$values` / `$fields` keys, per-field operators |
| `max_in_size` | 1000 | `$in` / `$nin`, array shortcuts, set-relation operands |
| `max_pattern_length` | 256 | each `$like` / `$ilike` / `$regex` pattern |
| `max_pattern_or_branches` | 32 | patterns when a text operand is a sequence |

A violation — or an empty operator map, an unknown operator or key, a type mismatch, or a
regex with unsafe nesting/repetition — raises a `precondition` `CoreException`
(HTTP 400; the caller supplied a bad query) before the query runs. A request DTO built on
these types (the kit's list, search, aggregate and stored-file requests) refuses it at its
boundary; over HTTP that is a 422 whose message names the key, while an MCP tool reports
only the field it rejected.

The rule holds for every expression: an unknown key in a sort spec (`{"dir", "nulls"}`), in
an aggregates expression (`$groups`, `$computed`, `$having`, or a metric's `field`,
`filter`, `p`) or in a request's search options (`snapshot` options and highlight options
included) is refused, never dropped. A single-index search request that names a hub or
federated key (`members`, `member_weights`) is refused at the request too.

Options handed to a port directly are held to the same types. A search port refuses an
option, a highlight option or a `snapshot` option that no search options type defines.
The analytics, dynamic-read and inference ports refuse a key their run options
(`AnalyticsRunOptions`, `DynamicReadOptions`, `InferenceRunOptions`) do not define. Each
refusal is a `precondition` naming the key, so a misspelt `timeout` or `row_cap` cannot
run a call without the bound it meant to set.

A backend can cap a filter lower than any spec's limits. On Firestore an `$in` takes
at most 30 values, and a filter at most 30 disjunctions once expanded: an `$in`
counts one per value, AND multiplies its parts' counts and OR adds them, so two
6-value `$in`s under AND make 36. Either is refused with `query_feature_unsupported`
before the query is sent.

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

On Postgres, a field of an object-array element takes its type from the read model
(`list[OfferModel]`) or a `nested_field_hints` entry. A field neither types, as in
`list[dict]`, compares by the operand, which differs from the in-memory mock in places:

- A string compares as text, so a stored `6` matches `"6"`; the mock does not match it.
- A boolean or a number compares as a JSON value. A range orders only values of the
  operand's JSON type, so a stored `true` never satisfies `{"$gt": 0}`; the mock
  compares it as `1`.
- A boolean never equals a number: `{"$eq": true}` or `{"$in": [true]}` misses a
  stored `1`, which the mock matches, since Python's `True == 1`.
- A `None` member of `$in` / `$nin` matches a stored JSON null, not a missing field.
- A `Decimal`, date, time, UUID or non-finite float operand is refused with
  `query_feature_unsupported`.

A decimal stored as a JSON string is not a number to such a field: annotate the
element type to compare it.

A scalar array stored as JSON (a `jsonb` column holding `["a", "b"]`, typed `list[str]`,
`list[int]` or `list[Any]`) compares each element as a JSON value by the operand's type,
so a stored `"6"` misses `6`, as in the mock; text operators read the element's text. A
boolean never equals a number here either: `true` misses a stored `1`, which the mock
matches. A `Decimal` operand, or a `list[Decimal]` element, compares numbers and numeric
strings numerically. Each `$in` / `$nin` member compares as it would under `$eq`, so a
string beside a `Decimal` stays a string. A date or time operand compares as its ISO text, which the mock
matches only on an element typed `list[date]` or `list[time]`, not on strings in a
`list[Any]`. A datetime or non-finite float operand is refused with
`query_feature_unsupported`; keep datetimes in a `timestamptz[]` column. On a native
array with a `None` member, `$in` matches a NULL element and `$nin` excludes it; in a
`jsonb[]` or `json[]` column that holds for SQL NULL and JSON null alike.
