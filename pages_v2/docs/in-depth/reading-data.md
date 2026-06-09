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
is a reference table.

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

Offset pages (`_page`, `_many`) are simple but get slower the deeper you go, and
can skip or repeat rows as data shifts underneath. **Cursor (keyset)** pages walk
by a stable key — reach for them on big result sets and live feeds.

The name *is* the combination: `select_page(...)` is an alternate return type,
offset-paged, with a count; `find_cursor(...)` is the read model, keyset-paged.

## Searching

Full-text and vector search are a parallel surface, through the **search query
port** — the same shape × pagination naming, but results come back **ranked**:

```python
hits = await ctx.search.query(order_search).search("blue widget")
```

`search` / `search_page` / `search_cursor` (with `project_` and `select_`
variants) mirror the document methods. Engines cover full-text, vector
similarity, and **hub / federated** search that spans several relations. Keeping
the index current — upsert and delete — is the separate **search command port**;
querying and index maintenance never mix.

Search is wired like any other capability: a `SearchSpec` resolved from the
context, with per-engine setup living in each integration.
</content>
