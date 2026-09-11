---
title: Reading data
icon: lucide/search
summary: Filters, pagination, and projections — getting back exactly what you need
---

A read is three choices: **which** records (a filter), in **what shape** to get
them, and **how** to page through them. The document query port composes all
three, and the method name spells out the combination.

## Filtering

You select records with a small filter DSL. `$values` holds field constraints,
`$fields` compares fields to each other, and `$and` / `$or` / `$not` combine
them. Inside `$values`, plain values are shorthands — a scalar means *equals*, a
list means *in*, `None` means *is null*:

```python
open_orders = await ctx.document.query(order_spec).find_many(
    {"$values": {"status": "open", "tags": ["priority"]}}
)
```

The same expression drives search filters and authorization scope filters, so
it's worth learning once. The full operator set (`$gt`, `$like`, `$overlaps`, …)
is in the [query syntax reference](../reference/query-syntax.md).

When a value must drive logic *inside* the source — a window function or a view's
own `WHERE` that a result filter can't reach — declare a typed
[query parameter](query-parameters.md) instead.

## Shape — the method prefix

What comes back is the prefix:

| Prefix | Returns |
|--------|---------|
| `find` / `get` | the full read model |
| `project` | a chosen subset of fields, as a `JsonDict` |
| `select` | rows validated as a different return type |
| `aggregate` | grouped or aggregated rows |

## Pagination — the method suffix

How many, and how you walk them, is the suffix:

| Suffix | Returns | When |
|--------|---------|------|
| *(none)* / `_many` | `CountlessPage` | lists where the total doesn't matter |
| `_page` | `Page` (with a total count) | UIs that show "X of N" |
| `_cursor` | `CursorPage` | large or infinite lists, stable under writes |
| `_stream` | an async generator of batches | full exports — walk the whole set in bounded memory |

Offset pages (`_page`, `_many`) are simple but get slower the deeper you go, and
can skip or repeat rows as data shifts underneath. **Cursor (keyset)** pages walk
by a stable key — reach for them on big result sets and live feeds. The cursor
itself is a client-held token; on a public API you can
[sign or encrypt it](../identity-tenancy-enc/cursor-tokens.md) so it can't be
forged or replayed against a different query.

The name *is* the combination: `select_page(...)` is an alternate return type,
offset-paged, with a count; `find_cursor(...)` is the read model, keyset-paged.

An empty page can also say *why* it's empty. Every page type carries an optional
`abstention` reason — `no_match`, `ambiguous`, or `not_permitted` — so a
permission-gated read can tell "nothing exists" apart from "rows exist but you
may not see them" without a second, ungated query. It's a result, not an error:
adapters that can't tell the causes apart leave it `None`, and a page with hits
never carries one. On governed list operations, `AuthzDocumentScopeWrap`'s
opt-in `explain_empty` sets the reason for you (see the
[authz recipe](../recipes/authn-authz-tenancy-fastapi.md)).

## Read fields storage doesn't hold

Three knobs decide what a read model may carry beyond its own stored columns, and
picking the wrong one is the usual mistake — they answer different questions:

| Knob | The field is… | Where its value comes from |
|---|---|---|
| `lenient_read_fields` | absent from storage | the model's own default |
| `materialized` | stored here, derived from this row | a `@computed_field`, persisted |
| `derived_read_fields` | produced by another relation | a source row, joined |

Leniency needs a default, so it refuses a required field — which a joined display
name usually is. That is what `derived_read_fields` is for. Map a field to `None`
to **mark** it derived and nothing more:

```python
ORDERS = DocumentSpec(
    name="orders",
    read=OrderRead,  # carries `supplier: SupplierRef` and `stock_quantity: float`
    derived_read_fields={"supplier": None, "stock_quantity": None},
)
```

Marking is shape-independent, and that is the point: a joined column, a nested
reference object, a `COALESCE` over sibling rows and a `CASE` expression are all
the same declaration, because none of them is computed by this aggregate. On a real
backend the declaration changes nothing at runtime — the view already produces the
column — while the write-column, sealing and settable-command guards all follow from
it.

In `forze_mock` a marked field's value comes from the stored row, which is where
[`SpecSeed(derived={...})`](../testing/overview.md) puts it — seeding goes through the
spec's create command, so a spec you intend to seed needs its `write` types. That value is fixture
data, not a derivation: nothing recomputes it, because nothing here implements the
view. For testing the handler around the read — which is what these aggregates need
— that is the whole requirement, and a mock that evaluated view expressions would be
a second query engine with divergences of its own.

Where the value genuinely is one field of one row reached by one key, it can be
**resolved** instead, and the mock performs the join:

```python
derived_read_fields={
    "catalog_code": DerivedReadField(source="catalogs", via="catalog_id", field="code"),
}
```

One hop, by primary key, no predicates and no ordering. Declare `optional=True` when
the join key is nullable; a key that resolves to no row is refused rather than
quietly `None`. A derived field is not filterable, sortable or sealable either way,
because this aggregate holds no column of its own for it.

## Searching

Full-text and vector search are a parallel surface, through the **search query
port** — the same shape × pagination naming, but results come back **ranked**:

```python
hits = await ctx.search.query(order_search).search("blue widget")
```

`search` / `search_page` / `search_cursor` / `search_stream` (with `project_`
and `select_` variants) mirror the document methods. A query can also ask for
**facet** counts and **highlighted** fragments alongside its hits — per query,
through `SearchOptions`, over fields the spec declares facetable or
highlightable (the full option surface is the
[search contract](../reference/contracts/search.md)). Engines cover full-text, vector
similarity, and **hub / federated** search that spans several relations. Vector
search ranks by **embeddings** — vectors produced by an embeddings provider
(`ctx.embeddings.provider(spec)`) — so semantically similar text scores together. Keeping
the index current — upsert and delete — is the separate **search command port**;
querying and index maintenance never mix.

Search is wired like any other capability: a `SearchSpec` resolved from the
context, with per-engine setup living in each integration.

Not every backend compiles every feature — Firestore has no aggregations, a
key-value document store no array quantifiers. The read DSL is capability-gated:
a query that reaches for a feature the wired backend can't compile is rejected
up front with a clean `precondition` naming the feature and backend, never a 500
deep in an adapter.

For a query-only service with no write side, see the
[read-only document API](../recipes/read-only-document-api.md) recipe. Reading is one
half of the story; writes that change state also emit domain events — how those
propagate is [Events & sagas](events-sagas.md).
