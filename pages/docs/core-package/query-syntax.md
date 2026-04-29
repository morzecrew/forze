# Query Syntax

Forze uses a shared query DSL for filtering and sorting. The same expression shape is used by document ports (`find`, `find_many`, `count`) and search requests (`filters`, `sorts`).

## Filter expression shape

A filter expression is one of:

- predicate: `{"$fields": {...}}`
- conjunction: `{"$and": [expr, ...]}`
- disjunction: `{"$or": [expr, ...]}`

Where `expr` is recursively one of the three shapes above.

## Field shortcuts

Inside `"$fields"`, each field value can use a shortcut or an explicit operator map.

| Field value | Expanded form | Meaning |
|-------------|---------------|---------|
| `"active"` | `{"$eq": "active"}` | equality |
| `["a", "b"]` | `{"$in": ["a", "b"]}` | membership |
| `null` | `{"$null": true}` | is null |

Example:

    :::python
    filters = {
        "$fields": {
            "status": "active",
            "tags": ["backend", "api"],
            "deleted_at": None,
        }
    }

## Operators

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

## Complex examples

### Nested AND/OR

    :::python
    filters = {
        "$and": [
            {"$fields": {"is_deleted": False}},
            {
                "$or": [
                    {"$fields": {"priority": {"$gte": 5}}},
                    {"$fields": {"status": {"$in": ["new", "in_progress"]}}},
                ]
            },
        ]
    }

### Range + set relation

    :::python
    filters = {
        "$fields": {
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

If `sorts` is omitted, adapters default to sorting by `id` descending.

## Aggregate syntax

Document `find_many` calls can request aggregate rows with a separate
`aggregates` expression. Aggregate rows are not document-shaped: without
`return_type` they are returned as JSON mappings; with `return_type` each row is
validated against that Pydantic model.

An aggregate expression has two sections:

- `fields`: output aliases mapped to source fields used as grouping keys.
- `computed_fields`: output aliases mapped to one aggregate function.

Supported functions are `$count`, `$sum`, `$avg`, `$min`, `$max`, and `$median`.
Use `$count: None` for row counts. Other functions take a source field path.
Computed fields may also use an object form with `field` and an optional
`filter`. The filter uses the same query filter syntax as top-level document
filters, including `$and` and `$or`, but it applies only to that aggregate.

    :::python
    aggregates = {
        "fields": {"category": "category"},
        "computed_fields": {
            "products": {"$count": None},
            "revenue": {"$sum": "price"},
            "median_price": {"$median": "price"},
            "premium_products": {
                "$count": {
                    "filter": {"$fields": {"price": {"$gte": 20}}},
                },
            },
            "premium_revenue": {
                "$sum": {
                    "field": "price",
                    "filter": {"$fields": {"price": {"$gte": 20}}},
                },
            },
        },
    }

    page = await doc.find_many(
        filters={"$fields": {"is_deleted": False}},
        sorts={"revenue": "desc"},
        aggregates=aggregates,
        return_count=True,
    )

When `return_count=True`, aggregate queries count aggregate result groups. Sorts
for aggregate queries use aggregate output aliases such as `revenue`, not source
document fields.

## Where you pass these expressions

### Document port usage

    :::python
    doc = ctx.doc_query(project_spec)

    rows, total = await doc.find_many(
        filters=filters,
        sorts=sorts,
        limit=20,
        offset=0,
    )

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
- For Postgres array columns, shortcuts are normalized:
  - `$eq` may behave as array superset
  - `$in` may behave as overlaps
  - `$nin` may behave as disjoint
- In Mongo renderer defaults, `$null: true` matches both explicit `null` and missing fields.
