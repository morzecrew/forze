# Query DSL

## Query DSL

`filters` and `sorts` on `ListRequestDTO` (and on search requests) use the shared JSON DSL — never adapter-specific SQL/Mongo syntax in application code:

```python
filters = {
    "$and": [
        {"$values": {"status": {"$in": ["active", "paused"]}}},
        {"$values": {"created_at": {"$gte": since}}},
    ]
}
```

Common operators: `$eq`, `$neq`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$null`, `$empty`, `$superset`, `$subset`, `$overlaps`, `$disjoint`.

Field keys are **dot-separated paths** into nested objects (`"address.geo.lat"`), usable in filters, sorts, group-bys, and the `fields` list of projected (`raw_*` / `projected_*`) calls — a dotted projection returns a nested shape.

### What the DSL accepts as a value

- `Decimal` is a first-class filter and sort value on every backend — pass the `Decimal`, not a float, when the field is one.
- Range bounds may be JSON strings; they are cast to the field's own type (an exact `Decimal`, an aware `datetime` normalized to UTC), never locale-guessed. `"NaN"` and `"Infinity"` are refused everywhere.
- **Sealed fields are refused as filter and sort keys on every backend, including the mock.** Filtering a randomized-encrypted field raises `core.crypto.encrypted_field_not_filterable` (deterministic `searchable` fields keep equality); sorting *any* sealed field is refused, including a spec's default sort. This is a policy check on the declaration, so a query that cannot work in production fails identically under the mock.

## Query syntax

Filters use the shared DSL: `{"$values": {...}}`, `{"$and": [...]}`, `{"$or": [...]}`.

**Field shortcuts:**

| Value | Meaning |
|-------|---------|
| `"active"` | `$eq` |
| `["a", "b"]` | `$in` |
| `null` | `$null: true` |

**Operators:** `$eq`, `$neq`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$null`, `$empty`, `$superset`, `$subset`, `$overlaps`, `$disjoint`, `$like`, `$ilike`, `$regex`.

**Sorts:** `{"created_at": "desc", "id": "asc"}`.

```python
filters = {"$values": {"status": "active", "is_deleted": False}}
page = await doc_q.find_page(
    filters=filters,
    pagination={"limit": 20, "offset": 0},
    sorts={"created_at": "desc"},
)
rows, total = page.hits, page.count
```

## Anti-patterns

- **Sorting cursor pages without stable key fields** — include a deterministic sort key, usually `id`.
- **Passing a `float` where the field is a `Decimal`** — the DSL carries `Decimal` end to end; converting through `float` reintroduces the rounding the type exists to prevent.

## Reference

- [Query syntax](https://morzecrew.github.io/forze/latest/reference/query-syntax/)
