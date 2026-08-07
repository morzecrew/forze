# RFC 0003 — Search facets & highlights

- **Status:** 🚧 In progress — P1–P4 + **P3 Postgres (facets + highlights, offset & cursor)** + **P6 multi-leg** shipped & tested 2026-06-29. Single-index: mock, Meilisearch, Postgres (PGroonga + FTS) complete. Multi-leg: hub facets+highlights on the mock (Postgres hub fail-closed — heterogeneous engines); federated highlights on mock/Postgres/Meilisearch-RRF (federated facets + native-Meili-federation highlights fail-closed — §8 Q7 per-member deferred). Only **P5 (OpenSearch)** remains, blocked on RFC 0002 not yet built.
- **Scope:** additive extension to `forze.application.contracts.search` (request knobs on `SearchOptions`, two capability fields on `SearchSpec`, two optional sidecar fields on the shared page envelopes, new facet/highlight value objects), then implementation across **all four** search backends — `forze_mock`, `forze_postgres` (PGroonga/FTS), `forze_meilisearch`, and the planned `forze_opensearch` ([[opensearch-integration-rfc]] / RFC 0002). Docs page.
- **Related:** the search contract (`SearchQueryPort`, `SearchSpec`, `SearchOptions`), the page value objects (`CountlessPage` / `Page` / `CursorPage` / `SearchSnapshotHandle`), the cross-backend parity harness ([[query-dsl-production-grade]]), lenient-read ([[lenient-read-fields-plan]]), the encryption×search collision ([[encryption-search-collision]]), the error-code reuse rule ([[error-code-hygiene-pass]]), the contract-surface gap log ([[contract-surface-review-2026-06]]).
- **Origin:** [[contract-surface-review-2026-06]] flags **facets and highlights as a known search-contract gap**. RFC 0002 §3 / §8 Q4 explicitly defers them to "their own RFC" and ships the OpenSearch adapter against today's contract. This is that RFC. **Vector/k-NN is out of scope and stays out** — see [[vector-search-parity-deferral]] (needs OpenSearch `knn` ⟷ Postgres `pgvector` parity design first).

---

## 1. Summary

Add two long-missing search capabilities to the **backend-agnostic** contract, then implement them everywhere:

- **Facets** — value distributions over a field: `status → {active: 812, archived: 47}`. Result-level metadata (one set per query, not per hit). Every backend supports them natively: Meilisearch `facetDistribution`, OpenSearch `terms` aggregation, Postgres `GROUP BY … COUNT(*)`, mock in-memory group-by.
- **Highlights** — matched snippets per hit: `name → ["Acme <em>Corp</em>oration"]`. Per-hit metadata. Native everywhere: Meilisearch `_formatted`/`attributesToHighlight`, OpenSearch `highlight`, Postgres `pgroonga_snippet_html`/`ts_headline`, mock substring marking.

The whole point of the contract being backend-plural is that adding a capability once propagates to all adapters. Today there is **zero scaffolding** — a repo-wide search found no `facet`, `aggregat` (in the search sense), `highlight`, or `snippet` token in the search layer. This RFC adds the minimal scaffolding and wires all four backends behind it.

**The design is deliberately additive — no breaking change, no new method on `SearchQueryPort`.** The port already has nine query variants (`search` / `project_search` / `select_search` × countless / `_page` / `_cursor`); adding `facet_*` / `highlight_*` variants would explode that to dozens. Instead:

- **Request side:** facets and highlights are requested through the existing `SearchOptions` TypedDict — the established home for per-request tuning.
- **Response side:** they ride the **existing page envelopes** as two new **optional** fields, exactly as the search-only `snapshot: SearchSnapshotHandle | None` field already rides the shared `CountlessPage`/`Page` base. Default `None`; every existing caller and every backend that hasn't implemented them is unaffected.
- **Spec side:** `SearchSpec` gains `facetable_fields` / `highlightable_fields`, mirroring the existing `fields` / weight declarations, validated the same way `lenient_read_fields` is.

So a handler that wants facets writes `options={"facets": ["status", "category"]}` and reads `page.facets`; one that wants highlights writes `options={"highlight": {...}}` and reads `page.highlights[i]`. Nothing else in the call shape changes.

## 2. Motivation

**The gap is logged and blocking a sibling RFC.** [[contract-surface-review-2026-06]] lists facets/highlights as a search-contract gap; RFC 0002 punts on them explicitly. Faceted navigation ("filter by category, see counts") and result highlighting ("show *why* this matched") are table-stakes search-UI features — their absence is the single biggest thing a search frontend cannot build on Forze today.

**Every backend already supports both natively** (verified in code):

| Backend | Facets | Highlights | Existing code today |
| --- | --- | --- | --- |
| `forze_mock` | feasible — already group-filters rows in-memory | feasible — already does substring `_text_score` | none for either |
| `forze_postgres` | `GROUP BY`/`COUNT` (count pipeline already exists) | `pgroonga_snippet_html` / `ts_headline` | none for either |
| `forze_meilisearch` | `facetDistribution` (`filterableAttributes` exist) | `_formatted` (currently **stripped** in `from_hit`) | none — `_*` fields dropped |
| `forze_opensearch` (RFC 0002) | `terms` agg (richest) | `highlight` clause (richest) | n/a (not built yet) |

The capability exists in every engine; only the Forze seam is missing. This is a "lift a native feature into the contract" RFC, not a "build a feature" RFC.

**Doing it once, uniformly, is the value.** If each adapter grew its own ad-hoc facet shape, callers couldn't write backend-portable search UIs — defeating the contract's reason to exist. A single facet/highlight value-object shape + a parity harness row makes `mock ≡ PG ≡ Meili ≡ OpenSearch` for these features, the same discipline already applied to the query DSL ([[query-dsl-production-grade]]).

## 3. Goals / Non-goals

**Goals**
- **Term facets** (value + count distributions) over declared `facetable_fields`, requested via `SearchOptions`, returned on the page envelope. All four backends.
- **Highlights** (per-hit field snippets, configurable tags) over declared `highlightable_fields`, requested via `SearchOptions`, returned index-aligned on the page envelope. All four backends.
- **Additive, non-breaking:** no new `SearchQueryPort` method; optional page fields default `None`; document queries and existing callers untouched.
- **Capability-gated, fail-closed:** a facet/highlight request a backend or field can't serve raises `query_feature_unsupported` (`precondition`/400), reusing the [[error-code-hygiene-pass]] codes — not a silent empty result.
- **Honest interplay** with `sensitive`, field encryption ([[encryption-search-collision]]), and lenient-read ([[lenient-read-fields-plan]]): refuse faceting/highlighting fields that are sealed/absent, validated at spec construction.
- **Parity harness coverage:** facet + highlight cases added to the cross-backend corpus so the four backends agree on shape and semantics.

**Non-goals (stated honestly)**
- **No vector / k-NN / hybrid search.** Out of scope and staying out until the OpenSearch `knn` ⟷ Postgres `pgvector` parity is designed ([[vector-search-parity-deferral]]).
- **No numeric range / histogram / date-bucket facets in v1.** Only **term** (value-distribution) facets — the one shape all four backends do identically. Range/histogram facets are a clean follow-on (§8 Q3), not designed-against here.
- **No nested / hierarchical / multi-level facet trees.** Flat field → buckets only.
- **No facets/highlights on snapshot-continuation pages.** They're computed on the live search call; a snapshot follow-up page (re-fetch by id) returns `None` for both (§4.7). Documented limitation, not a silent gap.
- **No new sort-by-facet-count or facet-driven filtering sugar.** Callers facet, then re-query with a filter using today's DSL. No new filter surface.
- **Not in core logic.** `forze.application` stays adapter-free; only the contract *shapes* live there (AGENTS layering).

## 4. Design

### 4.1 Request side — `SearchOptions` additions

`SearchOptions` (`src/forze/application/contracts/search/types.py:54`) is the established `TypedDict(total=False)` for per-request knobs. Add two keys:

```python
class SearchOptions(TypedDict, total=False):
    ...  # existing keys unchanged
    facets: Sequence[str]            # field names to compute value distributions over
    facet_size: int                  # max buckets per faceted field (default per-backend, e.g. 10)
    highlight: bool | HighlightOptions   # True = highlight all searchable fields with defaults
```

```python
class HighlightOptions(TypedDict, total=False):
    fields: Sequence[str]      # subset to highlight; default = all highlightable searchable fields
    pre_tag: str               # default "<em>"
    post_tag: str              # default "</em>"
    fragment_size: int         # snippet length budget (chars)
    max_fragments: int         # max snippets per field
```

Requesting `facets`/`highlight` for a field not in the spec's declared sets, or that the backend can't serve, fails at the adapter with `query_feature_unsupported` (`precondition`/400) — the same validate-before-execute posture the filter renderers already use.

### 4.2 Spec side — declaring facetable / highlightable fields

`SearchSpec` (`src/forze/application/contracts/search/specs.py`) gains two fields, mirroring the existing `fields` / `default_weights` declarations and validated in `__attrs_post_init__` the way `lenient_read_fields` is:

```python
facetable_fields: frozenset[str] = frozenset()          # explicit allow-list; empty = faceting disabled
highlightable_fields: frozenset[str] | None = None      # None = all searchable `fields`; subset to narrow
```

**Validation (reusing the lenient-read guardrail style in `contracts/lenient_read.py`):**
- A **facetable** field must exist on `model_type`, must **not** be a `lenient_read` field (faceting needs a real backing column), must **not** be encrypted (sealed → can't aggregate plaintext — the [[encryption-search-collision]] rule), and should be a keyword/scalar-typed field (analyzed `text` is rejected; faceting analyzed text is a footgun across engines). Numeric/bool/enum/`date`/UUID/string-keyword allowed.
- A **highlightable** field must be a subset of `fields` (only analyzed, searchable text can be highlighted) and must **not** be encrypted.
- If the model is `sensitive`, external surfaces refuse both (same posture as projection refusal today).

`facetable_fields` defaults to empty (faceting opt-in, since it has indexing/mapping implications — e.g. Meilisearch needs the field in `filterableAttributes`, OpenSearch needs a `keyword` mapping). `highlightable_fields` defaults to the searchable `fields` (highlighting is cheap and the natural default target).

`ensure_index()` in each management adapter reads these: Meilisearch adds facetable fields to `filterableAttributes`; OpenSearch maps them as `keyword` (and ensures `text` fields are analyzed for highlighting); Postgres ensures the column/index supports `GROUP BY` and `pgroonga_snippet`.

### 4.3 Response side — page envelope sidecars (the load-bearing decision)

The page value objects live in `src/forze/application/contracts/base/value_objects.py` — `CountlessPage[T]` (~:36), `Page[T]` (~:55, adds `count`), `CursorPage[T]` (~:69), all frozen `attrs`. **`CountlessPage`/`Page` already carry a search-only optional field — `snapshot: SearchSnapshotHandle | None = None` (~:15).** That is the precedent this RFC follows exactly: search-specific result metadata rides the shared page envelope as an optional, default-`None` field; document queries (which also return these pages) simply never populate it.

Add two optional fields to `CountlessPage` (inherited by `Page`) and `CursorPage`:

```python
facets: FacetResults | None = None              # result-level: field -> ordered buckets
highlights: list[HitHighlights] | None = None   # per-hit, index-aligned with `hits`
```

- **`facets`** is result-level (one distribution set per query), like a richer sibling of `snapshot`.
- **`highlights`** is a **parallel list aligned by index with `hits`** — `highlights[i]` describes `hits[i]`. This keeps `hits: list[R]` **bare and strongly typed** (no `SearchHit[R]` wrapper), preserving the typing of `select_search`/`project_search` and matching Forze's "only one hit wrapper exists, `FederatedSearchReadModel`, used sparingly" posture. The trade — index alignment must be preserved through the adapter — is documented and covered by the parity harness.

This is purely additive on frozen attrs classes with defaults: **no signature change to any of the nine `SearchQueryPort` methods, no break for any caller.** Facets/highlights uniformly available across `search` / `project_search` / `select_search` and all pagination shapes, because they're page-level, independent of hit type `T`.

**Acknowledged cost:** these search-only fields live on the *shared* `CountlessPage`/`Page` base used by document `aggregate_*` too, where they're always `None`. This mild pollution is the price of not breaking the port's return types — and it's a price **already paid** by `snapshot`. Alternative (search-specific envelopes + changed return types = breaking) is weighed in §6 and rejected for the same reason `snapshot` was.

### 4.4 Facet value-object shape

New value objects (frozen attrs / pydantic read models, `src/forze/application/contracts/search/`):

```python
class FacetBucket:           # one value + its count
    value: JsonValue         # the field value (str/int/bool/...)
    count: int               # number of matching documents with this value

FacetResults = Mapping[str, tuple[FacetBucket, ...]]   # facetable field name -> buckets, count-desc
```

v1 is **term facets only** — `value + count`, ordered by count desc, capped at `facet_size`. This is the lowest common denominator every backend produces identically. (Range/histogram/`other_count`/cardinality-estimate enrichments are §8 Q3.)

### 4.5 Highlight value-object shape

```python
HitHighlights = Mapping[str, tuple[str, ...]]   # highlighted field name -> snippet fragments
```

`page.highlights[i][field]` is the list of marked-up fragments for `hits[i].field`, with the requested `pre_tag`/`post_tag` already applied by the backend (or, for Postgres, by the snippet function). A field with no match is absent from the mapping; a hit with no highlights at all maps to an empty mapping (not `None`), so `highlights` stays index-aligned and non-sparse.

**`<em>`/`</em>` is the cross-industry default** (Elasticsearch/OpenSearch, Meilisearch, Algolia, Solr all default to it), so it's `HighlightOptions`' default. The non-obvious rule: **adapters must always pass tags explicitly, never inherit the engine default** — Postgres `ts_headline` defaults to `<b>`/`</b>` and PGroonga's `pgroonga_highlight_html` wraps in `<span class="keyword">`, so relying on engine defaults would make Postgres output diverge from the others and break the parity harness. Every adapter sets `StartSel`/`pre_tags`/etc. to the resolved `pre_tag`/`post_tag` so the marked-up output is byte-uniform across backends.

### 4.6 Per-backend implementation

All four sit behind the same request knobs and return the same shapes; capability-gated where an engine differs.

- **`forze_mock`** (`adapters/search/`) — after the existing in-memory filter+score (`_full_ordered_search_documents`), group matched rows by each facetable field → `FacetBucket`s; for highlights, reuse the substring logic in `_text_score` to wrap matched spans with the tags. The reference semantics the other three must match.
- **`forze_postgres`** (PGroonga/FTS, `adapters/search/`) — facets via a `GROUP BY field … COUNT(*)` companion query over the same filtered CTE the count pipeline already builds (`_search_count.py`); one extra round-trip per query when facets are requested (or a single multi-agg query — §8 Q5). Highlights via `pgroonga_snippet_html(...)` (PGroonga path) / `ts_headline(...)` (FTS path) as synthetic select columns, materialized into `HitHighlights` in `_materialize_hits.py`.
- **`forze_meilisearch`** — pass `facets=[…]` → read `result.facet_distribution` into `FacetResults`; pass `attributes_to_highlight` + `highlight_pre_tag`/`highlight_post_tag` → **read `_formatted` before `from_hit` strips `_*` fields** (the one existing-code change: `base.from_hit` currently drops all `_*`, so `_formatted` must be captured first). `facet_size` → `maxValuesPerFacet`.
- **`forze_opensearch`** (RFC 0002) — `aggs: {field: {terms: {field, size}}}` → `FacetResults`; `highlight: {fields: {…}, pre_tags, post_tags}` → `HitHighlights`. The richest native support; the parity floor is still the mock's term-facet semantics.

### 4.7 Multi-leg search: hub & federated (the asymmetry)

Hub (`HubSearchSpec`, homogeneous legs → one model `M`) and federated (`FederatedSearchSpec`, heterogeneous legs → `FederatedSearchReadModel[X]`) are both coordinator-merged through `weighted_rrf_merge_rows` (`src/forze/application/integrations/search/snapshot.py:499`), which fuses **already-materialized leg hits** keyed by `federated_record_key_string`, first-leg-wins on dedup. Highlights and facets behave **oppositely** across the two — worth stating because the naive intuition has it backwards.

**Highlights — supported for *both* hub and federated.** Each leg is a real per-index search that produces its own highlights; the merge keeps each surviving hit, so its highlights ride along. Hub is in fact *easier* than federated: hub legs share one `M`, so highlight field-keys are uniform; federated legs differ, but each hit is already `member`-tagged so `highlights[i]` is unambiguous. The one mechanical requirement: highlights are search-time artifacts **not** on the model (`from_hit` drops them), so the leg→merge path must thread a **highlight sidecar keyed by `federated_record_key_string`** and re-emit it for the surviving hit. Dedup uses the **kept leg's** highlights (matching the existing first-leg-wins model dedup, so a hit and its highlight always come from the same leg). Native-federation merge (Meilisearch `MeilisearchFederation`, not coordinator RRF) is fine too — the federated `multi_search` response carries `_formatted` per hit.

**Facets — clean for hub, awkward for federated.**
- **Hub** (shared field space) → merge by **summing per-value counts** across legs: `status:active` = Σ over legs. The flat `FacetResults` shape holds, because all legs face the same `M`. **Honest caveat:** exact only when legs are disjoint (e.g. sharding); when legs overlap on the same document, each engine counts it in its own full-match aggregation, so summed counts **over-count** the overlap. Documented; hub callers who shard get exact facets, those who overlap get approximate.
- **Federated** (heterogeneous field spaces) → there is **no shared axis** to fold into one `FacetResults` (leg A facets `status`, leg B facets `category`). So v1 federated facets are **per-member**, returned as `Mapping[str, FacetResults]` (member name → that leg's distribution), **not** the flat shape — or are simply refused (§8 Q7). Highlights stay flat/index-aligned regardless; only the *facet* shape differs for federated.

So the asymmetry, stated plainly: **highlights = hub ✓ federated ✓** (hub cleaner); **facets = hub ✓ (sum) federated = per-member or unsupported**.

### 4.8 Snapshots, encryption, sensitive, lenient-read interplay

- **Snapshots:** facets/highlights are computed on the **live** search call. A snapshot-continuation page (`SearchResultSnapshotOptions.id` set → re-fetch by stored ids) returns `facets=None`, `highlights=None`. Callers take facets from the first page. Documented; not stored in the snapshot in v1.
- **Encryption:** sealed fields can be neither faceted nor highlighted (ciphertext) — refused at spec validation, consistent with [[encryption-search-collision]]. No new guard.
- **`sensitive`:** sensitive read models refuse facet/highlight projection on external surfaces, same as they refuse field projection today.
- **Lenient-read:** a `lenient_read_fields` field has no backing column, so it can't be faceted (validation rejects it); it *can* in principle be highlighted only if it's also searchable, but lenient fields are non-searchable by construction, so it's simply excluded.

### 4.9 Capability gating + parity

- Each adapter declares which facet/highlight requests it can serve; an unservable request raises `query_feature_unsupported` (`precondition`/400) **before** hitting the engine, reusing the established error code ([[error-code-hygiene-pass]]). No silent empties.
- Add facet + highlight scenarios to the cross-backend parity corpus ([[query-dsl-production-grade]]) so `mock ≡ PG ≡ Meili ≡ OpenSearch` on bucket ordering, count semantics, tag application, and capped sizes. The mock is the reference oracle.

## 5. Phasing

- **P1 — contract scaffolding. ✅ SHIPPED 2026-06-29.** `SearchOptions.facets`/`facet_size`/`highlight` + `HighlightOptions`; `SearchSpec.facetable_fields`/`highlightable_fields` (+ `resolved_highlightable_fields`) with fail-closed validation; `FacetBucket`/`FacetResults`/`HitHighlights` value objects (defined in `contracts/base` next to the page envelopes — same rationale as `SearchSnapshotHandle` — re-exported from `contracts/search`); optional `facets`/`highlights` on `CountlessPage`/`Page`/`CursorPage`; shared request resolvers `resolve_facet_fields`/`resolve_highlight`/`facet_size_of` in `contracts/search/facet_highlight.py` so every backend validates identically; `__init__` exports. Plus the shared `offset_executor` (`OffsetRowsResult.facets`/`.highlights`, threaded through `snapshot_materialize_and_paginate` with highlights sliced in lockstep with pooled rows) — the seam P3/P5 reuse.
- **P2 — mock implementation + parity oracle. ✅ SHIPPED 2026-06-29.** In-memory facets (group-by over the full matching set, count-desc/value-asc, capped) + highlights (substring-match wrapping, consistent with the mock's scoring) in `forze_mock/adapters/search/_facets_highlights.py`. The reference semantics; unit tests in `test_search_facets_highlights.py`.
- **P3 — Postgres (PGroonga + FTS). ✅ SHIPPED 2026-06-29 (facets + highlights, offset & cursor).** Facets: a `GROUP BY {field} COUNT(*)` companion query (`adapters/search/_facets.py`) over the **uncapped** matched set, reusing `plan.count_with_clause`/`from_outer`/`params` (mirrors `fetch_count`), wired into the ranked `_PostgresSimpleOffsetHooks` path (FTS + PGroonga non-empty) **and** the PGroonga `_offset_empty_query_browse` path. Highlights (`adapters/search/_highlights.py`): synthetic `SELECT` columns spliced into the ranked data query (one per highlightable field), captured from raw rows and stripped before codec decode, returned via `OffsetRowsResult.highlights`. **FTS** uses `ts_headline(document, websearch_to_tsquery(%s), 'StartSel="…", StopSel="…"')`; **PGroonga** uses `pgroonga_snippet_html(target, pgroonga_query_extract_keywords(%s), width)` and **rewrites the fixed `<span class="keyword">` to the requested markers** in Python (§8 Q9 resolved — the snippet body is HTML-escaped by PGroonga, so the rewrite is unambiguous). Param ordering: highlight placeholders splice into `params_body` at `len - from_outer_param_count` (index-first pipelines carry the projection filter in `from_outer`). Cursor-paginated highlights use the same column-injection in `execute_ranked_pipeline_cursor` (capture + strip *after* `keyset_page_bounds` slicing so fragments stay aligned). Integration-tested both engines + offset & cursor + custom tags (`test_pg_search_facets.py`). **Caveats:** PG facet tie-break ordering is by native column type vs. the mock's `str(value)` (buckets agree; equal-count tie order may differ for non-string values); PGroonga highlights are windowed snippets and HTML-escaped (FTS/mock are whole-field, unescaped) — engine-native granularity, documented.
- **P4 — Meilisearch. ✅ SHIPPED 2026-06-29.** Native `facets`→`facet_distribution` and `attributes_to_highlight`+`highlight_pre_tag`/`highlight_post_tag`→`_formatted` (read from the raw hit, so no `from_hit` change needed); `ensure_index` adds facetable fields to `filterableAttributes`. `forze_meilisearch/adapters/search/_facets_highlights.py` + integration tests. (Note: Meilisearch facet-distribution keys are strings, so bucket values come back stringified — the engine's native behavior.)
- **P5 — OpenSearch.** Folds into RFC 0002 (its §5 P6 or a fast-follow): `terms` aggs + `highlight` clause; `keyword` mapping for facetables. Closes RFC 0002 §8 Q4.
- **P6 — multi-leg (hub & federated). ✅ SHIPPED 2026-06-29.** `HubSearchSpec` gained `facetable_fields`/`highlightable_fields` (validated against the union of member legs' `fields`). **Hub** (homogeneous): the mock computes flat facets over the merged (deduped) rows + whole-hub-row highlights; **Postgres hub fails closed** for both (`reject_unsupported_facets`/`reject_unsupported_highlight`) — its one combined SQL merges heterogeneous leg engines, so a single facet companion query and a single merged-row highlighter are ill-defined (the mock is the reference shape; deferred until demand justifies the 2-path SQL work). **Federated** (heterogeneous): per-hit highlights are threaded through the RRF merge by `federated_record_key_string` via the shared `integrations/search/multi_leg.py` (`build_federated_highlight_index` + `federated_highlights_for_hits`) — each leg runs as a full `SearchQueryPort` with the highlight option, the coordinator re-associates surviving merged hits with their originating leg's highlight. Wired on mock, Postgres, and **Meilisearch RRF** path; the **native Meilisearch federation** path fails closed (no per-hit `_formatted`). **Federated facets fail closed** everywhere (`reject_federated_facets`) — per-member (§8 Q7) deferred to avoid a speculative member-keyed page field. Tested: `test_mock_multi_leg_facets_highlights.py`, `test_pg_search_multi_leg_facets_highlights.py`, `test_meilisearch_federated.py`.
- **P7 (optional, demand-gated) — range/histogram/date-bucket facets, `other_count`/cardinality enrichments, facet-on-snapshot, per-member federated facets, exact hub facets.** Only on real demand; v1 term-facets cover the common UI.

## 6. Alternatives considered

1. **Wrap every hit in `SearchHit[R]` (`.hit`, `.score`, `.highlights`).** Rejected: breaks the bare-hit typing of `select_search`/`project_search`, forces every caller to unwrap even when they want neither score nor highlights, and contradicts the "only `FederatedSearchReadModel` wraps hits" posture. The parallel `highlights` list keeps hits bare (§4.3).
2. **New method variants (`facet_search_page`, `highlight_search_cursor`, …).** Rejected: the port already has nine variants; crossing facets×highlights×3 shapes×3 paginations explodes it. Request-via-`SearchOptions` + sidecar-on-page adds zero methods.
3. **Search-specific result envelopes + change `SearchQueryPort` return types.** Cleaner separation (no search fields on the shared base) but a **breaking** signature change across nine methods and loss of the shared pagination type. Rejected for the same reason `snapshot` was put on the shared base — additive beats clean-but-breaking here, and the precedent is already set.
4. **Surface a generic relevance `score` per hit alongside highlights.** Tempting (it'd ride the same parallel-list mechanism), but score semantics differ wildly across engines (BM25 vs Groonga rank vs mock heuristic) and aren't portably comparable — exposing them invites callers to depend on non-portable numbers. Deferred; not bundled into this RFC. (Cursor keyset already uses an internal synthetic `rank_field` without surfacing it — keep it that way.)
5. **Per-backend ad-hoc facet shapes.** Rejected outright — defeats the contract's portability, the entire reason it's backend-plural.

## 7. Relationship to what's already shipped

| Building block | State today | This RFC |
| --- | --- | --- |
| `SearchOptions` per-request knobs | shipped (`fuzzy`, `weights`, `search_count`, …) | adds `facets`/`facet_size`/`highlight` |
| `SearchSpec` field declarations | shipped (`fields`, `default_weights`, `lenient_read_fields`) | adds `facetable_fields`/`highlightable_fields`, validated the same way |
| `snapshot` optional field on shared page base | shipped — search-only metadata on `CountlessPage`/`Page` | the exact precedent for the `facets`/`highlights` sidecars |
| `FederatedSearchReadModel` hit wrapper | shipped — the only hit wrapper | not extended; highlights ride a parallel list instead |
| `weighted_rrf_merge_rows` (hub/federated merge) | shipped — fuses materialized hits, first-leg-wins | federated highlights re-associated by record key via `multi_leg.py`; federated facets refused (per-member deferred) |
| PGroonga count pipeline / `_search_count` | shipped | facets reuse its filtered CTE |
| Meilisearch `from_hit` `_*` strip | shipped | adjusted to capture `_formatted` first |
| `forze_opensearch` (RFC 0002) | drafted; defers facets/highlights | P5 here closes that deferral (RFC 0002 §8 Q4) |
| Cross-backend parity harness | shipped ([[query-dsl-production-grade]]) | gains facet/highlight rows |
| Vector / k-NN | not in contract | **still** out — [[vector-search-parity-deferral]] |

## 8. Open questions

1. **Highlight default scope.** `highlightable_fields=None` defaults to all searchable `fields`. Is highlighting-all-by-default too eager (cost on wide text fields), or is opt-out via the spec enough? *Leaning: default-on for searchable fields, opt-out via `highlightable_fields`; `highlight` still has to be requested per call, so no cost unless asked.*
2. **`facets` always-`None` on document `aggregate_*` pages.** The sidecars live on the shared base (§4.3). Accept the mild pollution (precedent: `snapshot`), or invest in search-specific envelopes later? *Leaning: accept; revisit only if the base accretes more search-only fields.*
3. **Range / histogram / date-bucket facets.** v1 is term-only. Confirm range facets are a P6/follow-on with their own request shape (`{field, ranges: [...]}` / `{field, interval}`), not retrofitted into `FacetBucket`. *Leaning: separate follow-on; keep `FacetBucket` term-only and stable.*
4. **One companion query vs multi-agg for Postgres facets.** Faceting N fields = N `GROUP BY`s. One round-trip per field, one combined query with multiple aggregates, or `grouping sets`? *Leaning: a single companion query with per-field aggregates over the shared filtered CTE; measure before optimizing.*
5. **Facet count semantics under pagination.** Facets are computed over the **full matching set**, independent of the page window (all four engines do this natively). Confirm that's the contract (it is the only portable choice) and document that `facet_size` caps buckets, not the underlying match count.
6. **Should `score` ride along now or never?** §6.4 defers per-hit score. Park it explicitly, or rule it out? *Leaning: park — revisit only with a concrete portable use-case; non-portable scores are a trap.*
7. **Federated facets: per-member or unsupported in v1?** §4.7 shows a single flat distribution is ill-defined for heterogeneous legs. **DECIDED 2026-06-29 → per-member.** P6 returns `Mapping[member, FacetResults]` for federated (a member-keyed shape distinct from the flat single-index `FacetResults`); hub stays flat (summed). Highlights stay flat/index-aligned for both.
8. **Hub facet over-count on overlapping legs.** §4.7 — summed hub facets over-count documents present in multiple legs. Accept approximate-on-overlap (exact on disjoint/sharded legs) and document it, or compute exact hub facets by deduping ids first (a second pass, more expensive)? *Leaning: accept approximate + document; exact-dedup only if a hub user reports it matters.*
9. **PGroonga highlight markers. RESOLVED 2026-06-29 → option (a), and it's robust.** PGroonga's `pgroonga_snippet_html` emits a fixed `<span class="keyword">…</span>`, but it **HTML-escapes the snippet body**, so that span is the *only* such substring in the output and rewriting it to the requested `pre_tag`/`post_tag` in Python is unambiguous (not the fragile case feared). FTS uses `ts_headline` with native `StartSel`/`StopSel`. Shipped in P3; the per-engine nuances (PGroonga = windowed + HTML-escaped, FTS/mock = whole-field + unescaped) are documented rather than forced into false uniformity.

## 9. Decisions

| # | Decision |
| --- | --- |
| 1 | Facets and highlights are additive `SearchOptions` knobs plus `SearchSpec.facetable_fields` / `highlightable_fields`, returned as **optional page sidecars**. No new port, no new page type |
| 2 | Facet counts are computed over the **full matching set**, independent of the page window — the only choice all four engines can honour natively, so the only portable one |
| 3 | Federated facets return a member-keyed `Mapping[member, FacetResults]` (decided 2026-06-29). A single flat distribution across heterogeneous legs is ill-defined, and returning one would be a wrong answer rather than a missing feature |
| 4 | Hub facets sum across legs and are therefore **approximate on overlapping legs**, exact on disjoint/sharded ones. Documented rather than deduped — exactness would cost a second id-dedup pass on every faceted hub query |
| 5 | An engine that cannot honour a requested facet/highlight combination **fails closed** (refuses), never silently degrades to an unfaceted page |
| 6 | PGroonga highlight markers use `pgroonga_snippet_html`'s fixed `<span class="keyword">…</span>`; the snippet body is HTML-escaped by the function, so that span is unambiguously the only such substring |
| 7 | v1 facets are **term-only**. Range / histogram / date-bucket facets are a follow-on with their own request shape, deliberately not retrofitted into `FacetBucket` |
| 8 | Per-hit `score` stays parked — a non-portable score leaking into a portable contract is a trap, and no concrete portable use-case has appeared |
