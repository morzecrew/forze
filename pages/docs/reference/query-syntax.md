# Query Syntax

Forze uses a shared query DSL for filtering and sorting. The same expression shape is used by document ports (`find`, `find_many`, `count`) and search requests (`filters`, `sorts`).

## Filter expression shape

A filter expression is one of:

- literal constraints: `{"$values": {...}}`
- field-to-field constraints: `{"$fields": {...}}`
- combined constraints: `{"$values": {...}, "$fields": {...}}` (implicit AND)
- conjunction: `{"$and": [expr, ...]}`
- disjunction: `{"$or": [expr, ...]}`

Where `expr` is recursively one of the shapes above.

You may combine `$values` and `$fields` in one object; all constraints are ANDed.
Do not mix `$and` / `$or` with `$values` / `$fields` in the same object.

## Literal shortcuts (`$values`)

Inside `"$values"`, each field value can use a shortcut or an explicit operator map.

| Field value | Expanded form | Meaning |
|-------------|---------------|---------|
| `"active"` | `{"$eq": "active"}` | equality |
| `["a", "b"]` | `{"$in": ["a", "b"]}` | membership |
| `null` | `{"$null": true}` | is null |

Example:

    :::python
    filters = {
        "$values": {
            "status": "active",
            "tags": ["backend", "api"],
            "deleted_at": None,
        }
    }

## Operators (for `$values`)

### Equality

| Operator | Value type | Meaning |
|----------|------------|---------|
| `$eq` | scalar | equal |
| `$neq` | scalar | not equal |

### Ordering

| Operator | Value type | Meaning |
|----------|------------|---------|
| `$gt` | numeric/date/datetime | greater than |
| `$gte` | numeric/date/datetime | greater than or equal |
| `$lt` | numeric/date/datetime | less than |
| `$lte` | numeric/date/datetime | less than or equal |

### Membership

| Operator | Value type | Meaning |
|----------|------------|---------|
| `$in` | array | field is in list |
| `$nin` | array | field is not in list |

### Unary checks

| Operator | Value type | Meaning |
|----------|------------|---------|
| `$null` | bool | `true`: is null, `false`: is not null |
| `$empty` | bool | `true`: empty array, `false`: non-empty array |

### Set relations (array fields)

| Operator | Value type | Meaning |
|----------|------------|---------|
| `$superset` | array | field contains all values from list |
| `$subset` | array | field contains no values outside list |
| `$overlaps` | array | field intersects list |
| `$disjoint` | array | field does not intersect list |

## Field-to-field compare (`$fields`)

Compare one document field to another field (not a literal). Use the same
operator names as equality and ordering (`$eq`, `$neq`, `$gt`, `$gte`, `$lt`,
`$lte`). Membership, unary, and set-relation operators are not supported under
`$fields`.

Inside `"$fields"`, each **left** field key maps to either:

| Value | Meaning |
|-------|---------|
| `"other_field"` | `$eq` shortcut: left field equals right field path |
| `{"$gte": "other_field"}` | explicit compare operator; value is always a **field path** string |

Unlike `"$values"`, string values under `"$fields"` always refer to another
field path, never a literal scalar.

Example:

    :::python
    filters = {
        "$values": {"is_deleted": False},
        "$fields": {"starts_at": {"$lte": "ends_at"}},
    }

Dot notation works for nested JSON fields (Postgres requires the same column
type metadata and read model as other nested filters).

## Complex examples

### Nested AND/OR

    :::python
    filters = {
        "$and": [
            {"$values": {"is_deleted": False}},
            {
                "$or": [
                    {"$values": {"priority": {"$gte": 5}}},
                    {"$values": {"status": {"$in": ["new", "in_progress"]}}},
                ]
            },
        ]
    }

### Range + set relation

    :::python
    filters = {
        "$values": {
            "created_at": {"$gte": "2026-01-01T00:00:00Z"},
            "labels": {"$overlaps": ["urgent", "customer"]},
        }
    }

## Sorting syntax

Sort expression is a map of field name to direction:

    :::python
    sorts = {
        "created_at": "desc",
        "id": "asc",
    }

Supported directions:

- `"asc"`
- `"desc"`

If `sorts` is omitted for regular offset pagination, this layer does not add an
explicit ordering. Pass a sort expression when callers need deterministic order.

## Aggregate syntax

Document `find_many` calls can request aggregate rows with a separate
`aggregates` expression. Aggregate rows are not document-shaped: without
`return_type` they are returned as JSON mappings; with `return_type` each row is
validated against that Pydantic model.

An aggregate expression has two sections:

- `$groups`: group keys — either a map of output alias → source field path, or a
  list/tuple of field paths (each path is used as both alias and source).
- `$computed`: output aliases mapped to one aggregate function.

Supported functions are `$count`, `$sum`, `$avg`, `$min`, `$max`, and `$median`.
Use `$count: None` for row counts. Other functions take a source field path.
Computed fields may also use an object form with `field` and an optional
`filter`. The filter uses the same query filter syntax as top-level document
filters, including `$and` and `$or`, but it applies only to that aggregate.

    :::python
    aggregates = {
        "$groups": {"category": "category"},
        "$computed": {
            "products": {"$count": None},
            "revenue": {"$sum": "price"},
            "median_price": {"$median": "price"},
            "premium_products": {
                "$count": {
                    "filter": {"$values": {"price": {"$gte": 20}}},
                },
            },
            "premium_revenue": {
                "$sum": {
                    "field": "price",
                    "filter": {"$values": {"price": {"$gte": 20}}},
                },
            },
        },
    }

    # Same group keys when alias equals source path:
    aggregates = {
        "$groups": ["detail_id", "revision_id", "warehouse_id"],
        "$computed": {"rows": {"$count": None}},
    }

    page = await doc.find_many(
        filters={"$values": {"is_deleted": False}},
        sorts={"revenue": "desc"},
        aggregates=aggregates,
        return_count=True,
    )

When `return_count=True` **with** `aggregates`, the total counts **aggregate
groups**. Sorts for aggregate queries use aggregate output aliases such as
`revenue`, not source document fields.

**`$groups` map values** are either a source path string (group by that field) or a
single-operator object. Calendar bucketing uses **`$trunc`** (output alias is the
map key):

    :::python
    "day_start": {"$trunc": {"field": "ts", "unit": "day", "timezone": "+3"}}

Units: `hour`, `day`, `week` (Monday start, aligned with Postgres `date_trunc` /
Mongo `$dateTrunc`), `month`. Default timezone is `UTC`. You may pass an **IANA**
name (`Europe/Berlin`) or a **fixed offset** (`+3`, `+03:00`). List/tuple `$groups`
accept path strings only (no `$trunc`).

MongoDB **5.0+** is required for `$dateTrunc` bucketing.

Non-aggregate **`find_many`**: you may pass `return_type` **without** `aggregates`
to validate each **document** row against a Pydantic model (same as the list read
model shape). `return_count` then counts documents, not groups.

    :::python
    aggregates = {
        "$groups": {
            "item_id": "item_id",
            "day_start": {"$trunc": {"field": "ts", "unit": "day", "timezone": "+3"}},
        },
        "$computed": {"avg_price": {"$avg": "price"}},
    }

    :::python
    page = await doc.find_many(
        filters=filters,
        pagination={"limit": 20, "offset": 0},
        return_type=MyRowDto,
    )

## Where you pass these expressions

### Document port usage

    :::python
    doc = ctx.doc_query(project_spec)

    page = await doc.find_many(
        filters=filters,
        sorts=sorts,
        pagination={"limit": 20, "offset": 0},
        return_count=True,
    )
    rows = page.hits
    total = page.count

### Search request usage

    :::python
    search = ctx.search_query(project_search_spec)

    hits, total = await search.search(
        query="roadmap",
        filters=filters,
        sorts=sorts,
        limit=20,
        offset=0,
    )

## Validation rules

- A field operator map cannot be empty.
- Unknown operators fail validation.
- Operator values must match expected types.
- `{"$null": true}` cannot be combined with other operators on the same field.
- `{"$empty": true}` cannot be combined with other operators on the same field.

## Backend notes

- Semantics are shared, but rendering is backend-specific (Postgres vs Mongo).
- In Mongo renderer defaults, `$null: true` matches both explicit `null` and missing fields.
