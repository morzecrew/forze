---
title: Search
icon: lucide/search
summary: The search contract — its spec, the query and command (index-maintenance) ports
---

Full-text and vector search over a `SearchSpec`. It splits into a **query** port (run
ranked searches) and a **command** port (maintain the index); the conceptual surface is
[Reading data → Searching](../../data-events/reading-data.md#searching).

```python
q = ctx.search.query(spec)    # search
c = ctx.search.command(spec)  # index maintenance
```

`ctx.search` also exposes `.hub(spec)` and `.federated(spec)` (a query port over composed
indexes, declared with `HubSearchSpec` / `FederatedSearchSpec`) and `.snapshot(spec)` for
result-set snapshots.

## Spec

`SearchSpec[M]` — the searchable model, its indexed fields, and ranking/encryption policy:

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | logical name / index route |
| `model_type` | `type[M]` | required | the searchable Pydantic model |
| `fields` | `Sequence[str]` | required | indexed fields (non-empty, unique) |
| `default_weights` | `Mapping[str, float] \| None` | `None` | per-field relevance weights |
| `fuzzy` | `SearchFuzzySpec \| None` | `None` | fuzzy-matching configuration |
| `default_sort` | `QuerySortExpression \| None` | `None` | sort when a caller omits `sorts` (required if the model has no `id`) |
| `materialized` | `frozenset[str]` | `∅` | `@computed_field` names that are real columns on the search relation, so results can be filtered/sorted by the derived value (mirror of [`DocumentSpec.materialized`](document.md#spec); relational in-place only, **not** startup-validated) |
| `facetable_fields` | `frozenset[str]` | `∅` | fields a query may compute term (value) facet distributions over (must be real, non-lenient, non-encrypted columns) |
| `highlightable_fields` | `frozenset[str] \| None` | `None` | searchable fields a query may highlight; `None` = all searchable `fields`, `∅` = none |
| `read_conformity` | `"strict" \| "lenient"` | `"strict"` | `"lenient"` auto-derives `lenient_read_fields` (every statically-defaulted, non-identity, non-indexed, non-`materialized` field); explicit fields added on top |
| `lenient_read_fields` | `frozenset[str]` | `∅` | returned read-model fields with **no** backing column: dropped from the result projection, hydrated from their default, and excluded from sort keys (mirror of [`DocumentSpec.lenient_read_fields`](document.md#lenient-read-fields); must **not** be an indexed `fields` member) |
| `snapshot` | `SearchResultSnapshotSpec \| None` | `None` | result-ID snapshotting defaults (stable re-pagination) |
| `encryption` | `FieldEncryption \| None` | `None` | field [encryption](../../identity-tenancy-enc/encryption.md) — **the same policy** as the document table's, so in-place search reproduces its AAD |
| `sensitive` | `bool` | `False` | model carries secrets; generated surfaces refuse to project it |
| `read_codec` | `ModelCodec \| None` | `None` | row codec override (auto-derived otherwise) |

`HubSearchSpec` carries the same `read_conformity` / `lenient_read_fields` / `materialized`
over its hub-row model (a hub has no index `fields` of its own). `FederatedSearchSpec`
inherits these from each member spec.

`materialized` is for **filtering and sorting** search results by a derived value — the
column must already exist (typically written by the document side over the same table).
Returning a computed field needs no `materialized`: it recomputes from the row on decode.

## Query port

Same `search` / `project_search` / `select_search` flavors and `_page` / `_cursor`
containers as the [document query port](document.md). The query text is the first
argument; everything else mirrors the document side:

| Method | Result |
|--------|--------|
| `search(query, filters=None, pagination=None, sorts=None, *, options=None, snapshot=None)` | `CountlessPage[R]` |
| `search_page(...)` | `Page[R]` (with `.count`) |
| `search_cursor(query, filters=None, cursor=None, sorts=None, *, options=None)` | `CursorPage[R]` |
| `project_search` / `project_search_page` / `project_search_cursor` `(fields, query, …)` | pages of `JsonDict` |
| `select_search` / `select_search_page` / `select_search_cursor` `(return_type, query, …)` | pages of `T` |
| `search_stream` / `project_search_stream` / `select_search_stream` `(query, …, chunk_size=500)` | `AsyncGenerator` of chunks |

`query` is a string (or a sequence of strings); `filters` and `sorts` use the
[query DSL](../query-syntax.md). `options: SearchOptions` is the backend- and
topology-agnostic per-request surface — relevance weights, fuzzy matching, the count
policy (`search_count`), an advisory candidate cap (`max_candidates`), and the facet /
highlight requests below. Hub and federated searches resolve to a `MultiSourceSearchOptions`
port that also carries member selection (`member_weights` / `members`) and a post-merge cap
(`merge_candidates`); passing those keys to a single-index `query(...)` port is a type error.

### Streaming exports

`search_stream` (and the `project_` / `select_` variants) iterate the **whole** matching set
in bounded-memory keyset chunks — peak memory is one chunk, there is no total count. Use it to
export a ranked result set without loading it all at once:

```python
async for chunk in ctx.search.query(spec).search_stream("annual report", chunk_size=1000):
    await write_rows(chunk)
```

Streaming is capability-gated (`spec`'s adapter must advertise `SearchCapabilities.supports_stream`):
Postgres FTS / PGroonga / hub and Mongo text / Atlas stream; Meilisearch (offset-only) and the
top-k vector engines **refuse** rather than emulate it via deep offset. Pick the right export
tool for the shape:

- **Ranked live export** → `search_stream`. A concurrent write may shift a hit between chunks.
- **Filter-only export** (no query terms) → the [document port](document.md)'s `find_stream` —
  it's a plain keyset read with no ranking overhead (server-side cursor on Postgres).
- **Stable / point-in-time / Meilisearch / very deep** → a **result snapshot** (build the
  ordered-id pool once, page it by id); works where a live cursor cannot.

### Facets and highlights

A query requests term facet distributions with `options={"facets": [...]}` and per-hit
match snippets with `options={"highlight": True}` (or a `HighlightOptions` mapping to narrow
fields / customize the `<em>` markers), over the spec's `facetable_fields` /
`highlightable_fields`. Results ride the page as optional `page.facets` (one set per query,
over the full matching set) and `page.highlights` (per hit, index-aligned with `hits`),
`None` when not requested. A field or backend that cannot serve a request fails closed with
`query_feature_unsupported`.

Support is per backend (single-index) and per topology (hub / federated). **fail-closed**
raises `query_feature_unsupported`; **—** means not applicable.

| Backend / engine | Facets | Highlights |
|------------------|--------|------------|
| Mock | ✅ | ✅ |
| Meilisearch | ✅ (`facetDistribution`) | ✅ (`_formatted`) |
| Postgres — PGroonga | ✅ | ✅ |
| Postgres — FTS | ✅ | ✅ (`ts_headline`) |
| Postgres — vector | ✅ | — |
| Mongo | fail-closed | fail-closed |

| Topology | Facets | Highlights |
|----------|--------|------------|
| Hub — mock | ✅ | ✅ |
| Hub — Postgres | ✅ (`sql` exec) / fail-closed (`parallel`) | ✅ |
| Federated — RRF merge (mock, Postgres, Meilisearch) | fail-closed | ✅ (per hit) |
| Federated — Meilisearch native | fail-closed | fail-closed |

- **PGroonga / hub highlights** are marked in process (case-insensitive substring over the
  raw field text), so they fold case for any script and keep the original casing; FTS uses
  `ts_headline` for language-aware stemming.
- **Vector** ranks by distance with no lexical match, so there is no snippet to highlight
  (facets still group over the ranked set).
- **Hub** highlights apply in both `sql` and `parallel` execution; hub facets run as a
  companion `GROUP BY` over the merged set under `sql` execution and fail closed under
  `parallel` (the Python merge can't dedup-count the facet field).
- **Federated facets** are deferred — per-member distributions don't compose under the
  reciprocal-rank merge, so every federated search fails closed on facets. Highlights
  survive only the RRF-merge path (each merged hit keeps its originating leg's snippet),
  not Meilisearch native federation.
- Requesting facets or highlights bypasses result-snapshot replay (snapshots store hit ids
  only), so a sidecar request always runs against a live query.

## Command port

Data-plane document writes — `ctx.search.command(spec)`.

| Method | Signature | Notes |
|--------|-----------|-------|
| `upsert` | `upsert(documents)` | add or update documents |
| `upsert_many` | `upsert_many(documents)` | batch add / update |
| `delete` | `delete(ids)` | remove by id |

## Management port

Control-plane index provisioning — `ctx.search.management(spec)`. Kept separate from
the command port (provisioning mutates shared topology / wipes are destructive admin,
run outside the request path); acquired via the command path, so a read-only operation
cannot provision or wipe an index.

| Method | Signature | Notes |
|--------|-----------|-------|
| `ensure_index` | `ensure_index()` | create / update the index settings |
| `delete_all` | `delete_all()` | empty the index |

## Implemented by

| Backend | Mode | Integration |
|---------|------|-------------|
| Meilisearch | external index | [Meilisearch](../../integrations/meilisearch.md) |
| Postgres | in-place (FTS + pgvector) over the document table | [Postgres](../../integrations/postgres.md) |
| Mongo | in-place over the document collection | [Mongo](../../integrations/mongo.md) |

An **external index** seals encrypted fields in the index document; **in-place** search
decrypts out of the document table's results — which is why the `encryption` policy must be
shared with the `DocumentSpec`.
