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

## An empty page can say why

Every page value object and kits response DTO carries an optional `abstention` reason —
`no_match`, `ambiguous` or `not_permitted` — so a permission-gated read can tell "nothing
exists" from "rows exist but you may not see them" without a second, ungated query.

It is a result, not an error. Adapters that cannot tell the causes apart leave it `None`,
and a page carrying hits never carries a reason (hits beside a reason, or an unknown
reason, are refused). On governed list operations, `AuthzDocumentScopeWrap(explain_empty=True)`
can set it for you: no policy restriction makes an empty page `no_match`, otherwise one
probe re-runs the read with the policy filters dropped and only the caller's own kept,
clamped to a single first-page row — rows there mean `not_permitted`, none mean `no_match`.

The probe's rows never reach the caller, and it is off by default. Because it re-invokes
the handler it runs only under a read-only `QUERY` invocation, and three cases leave the
page with no reason at all rather than a guessed one: any operation that is not
`QUERY`-classified, args carrying no `size` field to clamp, and a probe that raises.

## Anti-patterns

- **Sorting cursor pages without stable key fields** — include a deterministic sort key, usually `id`.
- **Passing a `float` where the field is a `Decimal`** — the DSL carries `Decimal` end to end; converting through `float` reintroduces the rounding the type exists to prevent.

## Reference

- [Query syntax](https://morzecrew.github.io/forze/latest/reference/query-syntax/)
