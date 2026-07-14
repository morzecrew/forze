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
| `fields` | `Sequence[str]` | required | indexed fields (non-empty, unique, never field-encrypted — the index would store ciphertext and content search would silently miss) |
| `default_weights` | `Mapping[str, float] \| None` | `None` | per-field relevance weights |
| `fuzzy` | `SearchFuzzySpec \| None` | `None` | fuzzy-matching configuration |
| `default_sort` | `QuerySortExpression \| None` | `None` | sort when a caller omits `sorts` (required if the model has no `id`) |
| `materialized` | `frozenset[str]` | `∅` | `@computed_field` names that are real columns on the search relation, so results can be filtered/sorted by the derived value (mirror of [`DocumentSpec.materialized`](document.md#spec); relational in-place only, **not** startup-validated) |
| `facetable_fields` | `frozenset[str]` | `∅` | fields a query may compute term (value) facet distributions over (must be real, non-lenient, non-encrypted columns) |
| `highlightable_fields` | `frozenset[str] \| None` | `None` | searchable fields a query may highlight; `None` = all searchable `fields`, `∅` = none |
| `highlight_scan_limit` | `int \| None` | `None` | cap on the characters of a field scanned for in-process highlighting (relational search) — a match beyond the cap isn't highlighted; the hit is unaffected |
| `max_results` | `int \| None` | `None` | server-side cap on an offset search with **no** caller `limit` (an explicit `limit` is honoured as-is) — guards fetching the whole matched set |
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
topology-agnostic per-request surface — relevance `weights`, `fuzzy` matching, a per-request
`fields` narrowing, multi-term combination (`phrase_combine`: `"any"` / `"all"`), the count
policy (`search_count`), an advisory candidate cap (`max_candidates`), and the facet /
highlight requests below (`facets`, `facet_size`, `highlight`). Hub and federated searches
resolve to a `MultiSourceSearchOptions` port that also carries member selection
(`member_weights` / `members`), a post-merge cap (`merge_candidates`), and the `fusion`
strategy (`"rrf"` / `"weighted"`); passing those keys to a single-index `query(...)` port is
a type error.

### Streaming exports

`search_stream` (and the `project_` / `select_` variants) iterate the **whole** matching set
in bounded-memory keyset chunks — peak memory is one chunk, there is no total count. Use it to
export a ranked result set without loading it all at once:

```python
async for chunk in ctx.search.query(spec).search_stream("annual report", chunk_size=1000):
    await write_rows(chunk)
```

Streaming is capability-gated (`spec`'s adapter must advertise `SearchCapabilities.supports_stream`):
Postgres FTS / PGroonga and Mongo text / Atlas stream; Meilisearch (offset-only), the top-k
vector engines, and hub search (each leg is capped at `per_leg_limit`, so a full walk isn't
guaranteed) **refuse** rather than truncate. Pick the right export tool for the shape:

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
`None` when not requested. (Pages also carry optional per-hit relevance `page.scores`,
index-aligned the same way.) A field or backend that cannot serve a request fails closed with
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

## Rebuilding an index

An external index is kept in step by the index-sync bindings (`AggregateKit(search=…)`, or
`bind_search_sync` / `bind_search_sync_outbox` directly), and those are **incremental**: they
carry a row into the index when that row is *written*. Nothing carries a row that was never
written since the index existed. So an aggregate that gained `search=…` after it already held
rows, an index provisioned fresh, one restored onto new infrastructure, and one that drifted
while its sync was broken all end up the same way — correct-looking, and empty or stale for
every untouched row.

`rebuild_search_index` is the backfill. It streams the document plane and applies each row to
the index under **the same rule the incremental syncs apply**, so a rebuilt index holds exactly
what an unbroken sync would have produced:

```python
from forze_kits.integrations.search import rebuild_search_index

report = await rebuild_search_index(
    ctx.doc.query(CUSTOMER_SPEC),
    ctx.search.command(CUSTOMER_INDEX),
    document=CUSTOMER_SPEC,
    search=CUSTOMER_INDEX,
)
report.indexed, report.removed, report.scanned   # live rows, soft-deleted rows, total
```

An `AggregateKit` already holds both specs, so it exposes the same sweep as
`await kit.rebuild_search(ctx)`.

Keyset-paged, so memory is bounded by `chunk_size` whatever the collection's size; idempotent,
so an interrupted sweep is re-run rather than repaired. A soft-deleted row is **removed** from
the index rather than upserted — a sweep that merely upserted everything it read would
resurrect every soft-deleted row as a hit that `GET` then 404s. Run it once per tenant (under
`bind_identity(tenant=…)`) on a tenant-aware route.

!!! warning "Source-driven: it converges the index, it does not replace it"

    Every row the document plane still holds ends up correct. An id the document plane no
    longer holds *at all* — hard-deleted while the index kept it — is invisible to a sweep that
    reads only the source, and survives it. Where that matters (an index of unknown provenance,
    rather than a fresh one), `delete_all()` first and rebuild into the empty index. That is a
    deliberate two-step: the wipe leaves search returning nothing until the sweep finishes, and
    that outage is yours to choose.

    For an *exact* result, sweep a source nothing is writing — a fresh import, or a quiesced
    runtime. Against live traffic the sweep is best-effort: a row hard-deleted between being
    read and being upserted comes back as a ghost. On an aggregate whose sync is working this
    converges anyway, because the sync is applying the same rule to those same writes.

## Implemented by

| Backend | Mode | Integration |
|---------|------|-------------|
| Meilisearch | external index | [Meilisearch](../../integrations/meilisearch.md) |
| Postgres | in-place (FTS + pgvector) over the document table | [Postgres](../../integrations/postgres.md) |
| Mongo | in-place over the document collection | [Mongo](../../integrations/mongo.md) |

An **external index** seals encrypted fields in the index document; **in-place** search
decrypts out of the document table's results — which is why the `encryption` policy must be
shared with the `DocumentSpec`.
