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
            "status": "active",         # == {"$eq": "active"}
            "tags": ["backend", "api"], # == {"$in": [...]}
            "deleted_at": None,         # == {"$null": True}
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

## Where you pass these expressions

### Document port usage

    :::python
    rows, total = await doc.find_many(
        filters=filters,
        sorts=sorts,
        limit=20,
        offset=0,
    )

### Search request usage

    :::python
    body = {
        "query": "roadmap",
        "filters": filters,
        "sorts": sorts,
    }

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
