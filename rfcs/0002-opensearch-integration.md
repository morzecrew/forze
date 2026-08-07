# RFC 0002 — `forze_opensearch` search integration

- **Status:** 📝 Draft — not started
- **Scope:** new `src/forze_opensearch/` package (one optional integration, `opensearch` extra), implementing the existing `forze.application.contracts.search` ports; small `pyproject.toml` registration; docs page. **No core changes** beyond what the contract already exposes (one optional capability hint — see §8 Q3).
- **Related:** the search contract surface (`SearchQueryPort` / `SearchCommandPort` / `SearchManagementPort` / `SearchResultSnapshotPort`), `SearchSpec` / `HubSearchSpec` / `FederatedSearchSpec`, the `forze_meilisearch` adapter (the template this mirrors), the `forze_postgres` PGroonga ranked-search adapter (the relevance/keyset precedent), the routed-tenant-client base, lenient-read fields ([[lenient-read-fields-plan]]), encryption×search ([[encryption-search-collision]]), `Rrf` value object, [[config-consolidation-arc]].
- **Origin:** Forze ships one true external search engine (`forze_meilisearch`) plus in-database FTS (`forze_postgres` PGroonga). OpenSearch is the most-requested production search backend that neither covers: Lucene-grade relevance, mappings/analyzers, aggregations (facets), highlighting, and **native keyset pagination (`search_after`)** — a capability the Meilisearch adapter has to refuse. The search **contract already exists and is backend-agnostic**; OpenSearch is "just another adapter behind it," same as the durable-execution RFC frames Postgres durability.

---

## 1. Summary

Add `forze_opensearch` — an adapter package implementing the existing search contracts against an OpenSearch cluster via the async `opensearchpy.AsyncOpenSearch` client. It is built **by analogy to `forze_meilisearch`**: identical package shape (`kernel/client`, `kernel/relation`, `adapters/search`, `execution/deps`, `execution/lifecycle`, `_compat`), identical DI keys, identical tenancy model (filter-level `tenant_aware`, namespace-resolved index name, or dedicated routed client). The framework's `SearchSpec`, query DSL, pagination, snapshotting, weighting, hub/federated specs, encryption, and lenient-read all **carry over unchanged** — they are contract-level concerns.

What changes is everything OpenSearch does differently from Meilisearch, and only that:

1. **Filter translation** — the query AST renders to **OpenSearch Query DSL** (`bool`/`term`/`range`/`terms`/`exists`) instead of Meilisearch's `attr = value` filter string. New `_filter_render.py`; new (wider) capability set.
2. **Relevance & index settings** — `multi_match` over weighted fields + BM25, driven by **mappings/analyzers**, instead of Meilisearch's `ranking_rules` and `searchableAttributes`. New `ensure_index()` builds a mapping, not a settings blob.
3. **Indexing** — the **`_bulk` API** with `refresh="wait_for"` for read-visibility, instead of Meilisearch's task-uid polling. `_id` is the document id directly (no separate `primary_key` concept required).
4. **Keyset pagination is real** — OpenSearch supports `search_after`, so this adapter **implements `search_cursor` / `project_search_cursor` / `select_search_cursor` natively** rather than raising `precondition` the way the Meilisearch adapter does. This is the headline capability win.
5. **Federation** — no native cross-index relevance fusion, so federated/hub search uses the framework's **coordinator-side `Rrf` merge** (already shared by Meilisearch's `merge="rrf"` and Postgres). `_msearch` is used to fan out the legs in one round-trip.

The headline structural decision (the same call the durable-execution plane made): **the adapter depends on the contract, not the reverse.** Handler authors who already use `SearchDeps` get an OpenSearch backend by swapping the deps module at wiring — zero new mental model.

## 2. Motivation

**The two shipped search backends leave a real gap.**

| Backend | Engine | Relevance | Facets / highlights | Keyset (`search_after`) | Operational cost |
| --- | --- | --- | --- | --- | --- |
| `forze_postgres` (PGroonga) | in-database FTS | good (Groonga) | partial | yes (CTE keyset) | none — reuses Postgres |
| `forze_meilisearch` | Meilisearch | good, opinionated | limited | **no** (adapter raises) | a Meilisearch node |
| **`forze_opensearch`** | OpenSearch (Lucene) | Lucene/BM25, tunable | native aggregations & highlighting | **native** | an OpenSearch cluster |

OpenSearch is the default answer for teams that already run an ELK/OpenSearch cluster, need Lucene-grade relevance tuning (analyzers, synonyms, custom similarity), or need aggregations/highlighting that Meilisearch does not expose. Forze's search contract was deliberately written to be backend-plural (the capability-gating, the `Rrf` value object shared across backends, the `field_map` indirection); not having an OpenSearch adapter is a coverage hole, not a design limitation.

**The contract is ready.** `SearchQueryPort` already encodes result-shape and pagination in method names (`search` / `project_search` / `select_search` × countless / `_page` / `_cursor`); `SearchManagementPort` already separates index provisioning from the data plane; `SearchResultSnapshotPort` already exists for stateless paging; `Rrf` already standardizes rank fusion; lenient-read and field-encryption are already spec-level. An OpenSearch adapter implements these — it does **not** extend core (with one optional, additive exception in §8 Q3).

**The template is proven.** `forze_meilisearch` already solved the hard cross-cutting problems an external search engine poses inside Forze: tenant-routed clients, per-tenant index naming, encrypted-field sealing, codec/lenient-read at the index boundary, offset-with-snapshot paging, and the RRF federation merge. OpenSearch reuses all of that machinery and only re-implements the engine-specific leaves.

## 3. Goals / Non-goals

**Goals**
- A `forze_opensearch` package mirroring `forze_meilisearch`'s shape, registered as the `opensearch` extra.
- Implement, against an OpenSearch cluster: `SearchQueryPort` (single-index, hub, federated), `SearchCommandPort` (bulk upsert/delete), `SearchManagementPort` (`ensure_index` → mapping; `delete_all`).
- **Native `search_after` keyset pagination** — fill the `*_cursor` query variants the Meilisearch adapter cannot.
- Full parity with the framework query DSL filter operators **that OpenSearch supports** (a strict superset of Meilisearch's), capability-gated and fail-closed for the rest.
- Same tenancy model as Meilisearch: `tenant_aware` (term filter on `TENANT_ID_FIELD`), namespace (per-tenant index name via `NamedResourceSpec`), or dedicated (`RoutedOpensearchClient` per-tenant credentials). `required_tenant_isolation` floor enforced at wiring.
- Carry over, unchanged: lenient-read (`resolved_lenient_read_fields` dropped from `_source`, hydrated from model defaults), field-level encryption (sealed in index, decrypted on read — with the search-collision caveat), result snapshots, field weighting, hub/federated `Rrf` merge.
- Mock/contract parity: pass the existing search adapter test corpus (mirror the Meilisearch suite); cross-backend behavior matches mock ≡ Meilisearch ≡ OpenSearch where the contract is defined.

**Non-goals (stated honestly)**
- **Not k-NN / vector / hybrid semantic search** in the first cut. OpenSearch's `knn` and `hybrid` queries are a large, separate surface; the contract has no vector concept yet. Deferred (§5 P6 / future RFC), not designed-against here.
- **Not exposing aggregations/highlighting as new core contract surface** initially. OpenSearch supports both; the *contract* does not yet ([[contract-surface-review-2026-06]] flags facets/highlights as a known gap). This RFC ships the adapter against today's contract; a facets/highlights contract extension is its own RFC (§8 Q4).
- **Not Elasticsearch.** API-compatible up to a point, but we target the `opensearch-py` client and OpenSearch 2.x semantics. An ES adapter, if ever wanted, is a separate package.
- **Not in core.** `forze.application` must not import `opensearchpy` (AGENTS layering). Everything lives in `forze_opensearch` behind the existing contracts.
- **Not search-pipeline-managed RRF** (OpenSearch's normalization-processor) in the first cut — federation uses the framework's coordinator-side `Rrf` merge for backend-uniform behavior (§4.6). Native pipelines are a later optimization.

## 4. Design

Package layout mirrors `forze_meilisearch` one-for-one (only the deltas are called out):

```
src/forze_opensearch/
  __init__.py                       # public API, require_opensearch() guard first
  _compat.py                        # require_opensearch(): raise if opensearchpy missing
  kernel/
    client/{port,client,routed_client,value_objects,routing_credentials,errors}.py
    relation.py                     # resolve_opensearch_index_name() — REUSES NamedResourceSpec
  adapters/search/
    base.py                         # gateway: field map, _source projection, encryption seal, tenant filter
    _filter_render.py               # ← Query-DSL renderer (engine-specific, the load-bearing delta)
    _query_build.py                 # multi_match + filter + sort + search_after assembly
    _offset_run.py                  # from-size paging + snapshot hook (reuse framework offset executor)
    _cursor_run.py                  # ← NEW: search_after keyset paging (no Meili analog)
    _port.py                        # port mixin — implements cursor variants instead of refusing them
    _simple_base.py                 # single-index SearchQueryPort adapter
    _command.py                     # _bulk upsert/delete + ensure_index/delete_all (management)
    federated.py                    # _msearch fan-out + Rrf coordinator merge
  execution/
    deps/{configs,keys,module}.py
    deps/factories/{search,federated}.py
    lifecycle/pool.py               # AsyncOpenSearch init / aclose hooks
  py.typed
```

### 4.1 Kernel client (`kernel/client/`)

Same shape as Meilisearch's, wrapping `opensearchpy.AsyncOpenSearch` instead of `meilisearch_python_sdk.AsyncClient`.

- **`OpensearchClientPort`** (Protocol) — the minimal surface the adapters need: `health()`, `index_exists(name)`, `create_index(name, body)`, `put_mapping(name, body)`, `bulk(actions, *, refresh)`, `search(index, body)`, `msearch(searches)`, `delete_by_query(index, body)`, `aclose()`. (Meilisearch's `index()` handle / `wait_for_task` are replaced by direct `search`/`bulk` calls — OpenSearch has no task-handle model for these.)
- **`OpensearchClient`** — wraps `AsyncOpenSearch`; lazy `initialize(hosts, *, http_auth, config)` under a lock, exactly like `MeilisearchClient.initialize`. All async methods wrapped by an `exc_interceptor` built from an OpenSearch error mapper (`opensearchpy.exceptions.*` → `CoreException`; `ConnectionError`/`TransportError` → `exc.internal`, `NotFoundError` on a query path → empty result, `RequestError` from a malformed query → `exc.validation`/422 per the [[error-code-hygiene-pass]] rule).
- **`RoutedOpensearchClient`** — extends the same `StructuredSecretRoutedTenantClientBase` Meilisearch uses; per-tenant credentials `OpensearchRoutingCredentials(BaseModel)` = `{hosts, http_auth?/api_key?, verify_certs?}`; per-tenant client cache + credential fingerprinting, unchanged from the Meilisearch pattern.
- **`OpensearchConfig`** (frozen attrs) — `request_timeout: timedelta`, `verify_certs: bool`, `refresh: "true"|"false"|"wait_for"` default `"wait_for"` (read-your-writes for command ops; see §4.4), `max_retries: int`. This is the `value_objects.py` analog.

### 4.2 Relation / index naming (`kernel/relation.py`)

**Reused wholesale.** `resolve_opensearch_index_name(spec: NamedResourceSpec, tenant_id)` delegates to the framework `resolve_value`, identical to `resolve_meilisearch_index_uid`. Static names and per-tenant (namespace-isolation) names both fall out of `NamedResourceSpec`. OpenSearch index-name constraints (lowercase, no leading `_`/`-`, no spaces) are validated here.

### 4.3 The search gateway + filter renderer (the engine-specific core)

**`OpensearchSearchGateway[M]`** (`adapters/search/base.py`) plays the same role as `MeilisearchSearchGateway`: holds `SearchSpec[M]` + config, caches the logical→physical `field_map`, lazily resolves the index name (`OnceCell`), owns `TenancyMixin`, and provides `to_index_document(model)` (encode + seal encrypted fields) and `from_hit(hit)` (decode `_source` → logical fields, hydrate lenient-read defaults). **All of this is reused logic** — the only OpenSearch-specific piece is that the document body is the `_source` and the id is `_id`.

**`_filter_render.py` is the load-bearing delta.** It compiles the same `QueryFilterExpression` AST the Meilisearch and Postgres renderers consume, but emits an OpenSearch **filter-context** Query DSL tree (no scoring, cacheable):

| AST op | OpenSearch DSL |
| --- | --- |
| `$eq` / `$neq` | `term` / `bool.must_not[term]` |
| `$gt/$gte/$lt/$lte` | `range` |
| `$in` / `$nin` | `terms` / `bool.must_not[terms]` |
| `$null` | `bool.must_not[exists]` (true) / `exists` (false) |
| `$and` / `$or` / `$not` | `bool.filter[]` / `bool.should[]`+`minimum_should_match:1` / `bool.must_not[]` |

OpenSearch supports a **strict superset** of Meilisearch's operators — so the adapter declares a *wider* capability set. Operators OpenSearch can additionally support but the contract gates per-backend (e.g. `$like`/`$ilike` → `wildcard`, `$regex` → `regexp`, array quantifiers → `nested`/`terms`) are enabled where safe and otherwise refused with `query_feature_unsupported` (`precondition`/400) — same fail-closed posture and reuse-these-codes rule as [[error-code-hygiene-pass]]. The capability matrix is validated **before** rendering, exactly as Meilisearch validates today, so unsupported queries fail predictably across backends ([[query-dsl-production-grade]] parity harness applies).

Field names are passed through `field_map` then validated against a safe-identifier guard (reuse the Meilisearch `_SAFE_ATTRIBUTE` approach), and dotted/nested sort+filter paths resolve like the existing nested-sort work ([[nested-sort-resolution-plan]]).

### 4.4 Command + management (`_command.py`)

- **`OpensearchSearchCommandAdapter[M]`** implements `SearchCommandPort`:
  - `upsert` / `upsert_many` → build `index` bulk actions (`{"index": {"_index": name, "_id": doc_id}}` + sealed `_source`), chunk (default 1000 docs/batch like Meilisearch), call `client.bulk(actions, refresh=config.refresh)`. Inspect the bulk response `items[].error` and raise on partial failure (OpenSearch bulk is partial-success; this must be surfaced, not swallowed).
  - `delete(ids)` → bulk `delete` actions, same refresh.
  - Encryption: reuse `prepare_encrypt()` warmup before the sync seal, identical to Meilisearch.
- **`OpensearchSearchManagementAdapter[M]`** implements `SearchManagementPort`:
  - `ensure_index()` → if absent, `create_index(name, body)` where `body` = `{settings, mappings}`. The **mapping** is derived from `SearchSpec`: `fields` → analyzed `text` (with `keyword` sub-field for sort/filter), filterable/sortable non-text fields → `keyword`/numeric/`date`, and `TENANT_ID_FIELD` → `keyword` when `tenant_aware`. Analyzer/similarity overrides come from `OpensearchSearchConfig` (§4.7). If present, reconcile via `put_mapping` (additive only; mapping conflicts raise a clear `precondition`, since OpenSearch cannot change an existing field's type).
  - `delete_all()` → `delete_by_query(match_all)` with `refresh` (not a drop — preserves mappings/settings, matching the contract's "remove all documents" wording).

`refresh="wait_for"` (config default) is what gives command ops read-your-writes without the Meilisearch task-poll loop; teams that index in bulk and tolerate near-real-time visibility can set `"false"` for throughput.

### 4.5 Query: offset, projection, and native keyset (`_simple_base.py`, `_offset_run.py`, `_cursor_run.py`, `_port.py`)

**`_query_build.py`** assembles the request body shared by all paths:
- **Relevance:** `multi_match` over `attributes_to_search_on(spec, options, field_map)` with per-field `^weight` boosts derived from `calculate_effective_field_weights` (the same framework util Meilisearch uses) — so weighting semantics are identical across backends. Empty query (filter-only) → `match_all` in a filter context.
- **Filter:** the §4.3 DSL tree, ANDed with the tenant term filter (`merge` of base + tenant, mirroring `merge_filter_strings`).
- **Sort:** `render_user_sorts` mapped to OpenSearch `sort` clauses; `_score desc` as the implicit primary when a text query is present, falling back to `spec.default_sort`.
- **Projection:** `_source` include-list from the requested fields **minus `resolved_lenient_read_fields`** (dropped, hydrated on read) and minus `sensitive` fields on external surfaces.

**Offset paging (`_offset_run.py`)** — `from`/`size`, total from `hits.total.value` (with `track_total_hits` honoring `SearchOptions.search_count` → `true`/an integer cap/`false` for `exact`/`approximate`/`none`). Wraps in the framework's `execute_simple_offset_search_with_snapshot()` for result snapshots, exactly like Meilisearch's `_offset_run.py`.

**Keyset paging (`_cursor_run.py`) — the new capability.** OpenSearch `search_after` is true keyset pagination. The cursor token encodes the last hit's sort values; the next page sets `search_after: [...]` with a stable, total-order sort (caller sorts + a `_id`/`id` tie-breaker via the existing `ranked_search_cursor_key_spec` helper). This implements `search_cursor` / `project_search_cursor` / `select_search_cursor` **natively** — where `MeilisearchSearchPortMixin` raises `exc.precondition()`, `OpensearchSearchPortMixin` returns a real `CursorPage`. This closes a genuine contract variant that no shipped external-engine adapter fills, and aligns OpenSearch with the Postgres CTE-keyset behavior ([[db-roundtrip-p10-p12]] keyset precedent).

### 4.6 Federation + hub (`federated.py`)

`OpensearchFederatedSearchAdapter[M]` implements `SearchQueryPort[FederatedSearchReadModel[M]]`. OpenSearch has no native cross-index relevance fusion, so:

- Build one search body per member leg (each with its own `multi_match`/filter), fan them out in a **single `_msearch`** round-trip.
- Merge coordinator-side with the framework's shared `weighted_rrf_merge_rows()` using the `Rrf` value object (`k`, `per_leg_limit`) — **the exact same merge Meilisearch's `merge="rrf"` and Postgres use**, so federated relevance is backend-uniform. `member_weights` resolved via `prepare_federated_search_options` / `prepare_hub_search_options` (reused).
- Result snapshots and cursor-refusal-vs-support follow the single-index adapter. (Federated `search_after` across heterogeneous legs is genuinely hard — first cut keeps federation offset-paged + snapshot, like Meilisearch; native federated keyset is deferred.)

Hub search (homogeneous `HubSearchSpec`) is the same machinery with member result type = the hub model `M`.

### 4.7 Wiring, config, lifecycle (`execution/`)

- **`OpensearchSearchConfig`** (`configs.py`, extends `TenantAwareIntegrationConfig`, [[config-consolidation-arc]] nested style): `index_name: NamedResourceSpec`, `field_map`, `searchable_fields`/`filterable_fields`/`sortable_fields` overrides, `analyzer`/`similarity` overrides, `number_of_shards`/`number_of_replicas`, `refresh` override, `track_total_hits`. **No `primary_key`** — OpenSearch uses `_id` (the document id is taken from the model id field directly).
- **`OpensearchFederatedSearchConfig`** — `members: StrKeyMapping[OpensearchSearchConfig]` + `merge_spec` (only `Rrf()` in the first cut; the `MeilisearchFederation()` native-merge tag has no OpenSearch analog yet). Validates ≥2 non-hub members, distinct names.
- **`OpensearchClientDepKey`** (`keys.py`) — single `DepKey[OpensearchClientPort]`.
- **`OpensearchDepsModule`** (`module.py`) — registers `(SearchQueryDepKey, ConfigurableOpensearchSearch)`, `(SearchCommandDepKey, …Command)`, `(SearchManagementDepKey, …Management)`, `(FederatedSearchQueryDepKey, …Federated)` via `routed_from_mapping`, plus the client. Same `required_tenant_isolation` floor and the same tenancy-consistency validations as `MeilisearchDepsModule` (this is the module the mypy override + import-linter independence contracts must list — see §6).
- **`factories/`** — `ConfigurableOpensearchSearch` / `…Command` / `…Management` / `…Federated`, building adapters with client + config + tenant provider + result snapshot, and wrapping the read codec with the encryption codec via the shared `resolve_search_read_codec_spec()` (reused).
- **`lifecycle/pool.py`** — `opensearch_lifecycle_step()` (startup `initialize(hosts, http_auth, config)`, shutdown `aclose()`) and `routed_opensearch_lifecycle_step()` (delegates to `routed_client_lifecycle_step`), mirroring Meilisearch's hooks.

### 4.8 Encryption × search caveat (unchanged, restated)

Field-level encryption seals values in the index, so **encrypted fields are not content-searchable** and the external index holds ciphertext — the exact [[encryption-search-collision]] caveat that already applies to Meilisearch. The adapter enforces the same rule: an encrypted field may not appear in `SearchSpec.fields` (searchable) or in a filter/sort on its plaintext; it round-trips sealed and is decrypted in `from_hit`. No new guard, no new behavior — documented loudly on the OpenSearch docs page.

## 5. Phasing

- **P1 — kernel client + `_compat` + lifecycle + `OpensearchConfig` + routed client.** Connect, health, bulk, search; tenant routing. The floor everything sits on; conformance with the `opensearch-py` async client pinned.
- **P2 — single-index command + management.** `_bulk` upsert/delete, `ensure_index` mapping derivation, `delete_all`. Partial-failure surfacing. Encryption seal on write.
- **P3 — single-index query: offset + projection + snapshot.** `_filter_render.py`, `_query_build.py`, weighting, lenient-read `_source` drop, `SearchOptions.search_count` → `track_total_hits`. Parity with the Meilisearch single-index suite.
- **P4 — native keyset (`search_after`).** The `*_cursor` variants — the headline capability. Stable-sort + tie-breaker token; `CursorPage` round-trip tests.
- **P5 — federated + hub via `_msearch` + `Rrf` merge.** Backend-uniform fusion; member weights; snapshots.
- **P6 (optional, demand-gated) — k-NN / hybrid semantic search, native search-pipeline RRF, aggregations/highlighting** — each gated on a contract extension (§8 Q4) and real demand. This is where OpenSearch's surface exceeds the current contract; do **not** build speculatively.

Each phase mirrors an existing `forze_meilisearch` test module, so "does it match the template" is a concrete check, not a judgment call.

## 6. `pyproject.toml` registration (the non-code surface)

A new integration touches several hand-maintained lists (per AGENTS: extras + wheel are the authoritative package set). For `forze_opensearch`:

1. `[project.optional-dependencies]` → `opensearch = ["opensearch-py>=2.6.0"]` (async extra pulls `aiohttp`; pin checked at implementation time).
2. `[tool.hatch.build.targets.wheel].packages` → `"src/forze_opensearch"`.
3. mypy per-module override for `*/forze_opensearch/execution/deps/module.py` (matching the Meilisearch entry that exists at the analogous line — deps modules carry the relaxed override).
4. import-linter **independence** contracts (the integration-package lists at the ~326/~516 blocks) → add `forze_opensearch`, so it stays decoupled from sibling integrations.
5. coverage source list (~438) and vulture allow/path list (~473) → add `src/forze_opensearch` / `forze_opensearch`.

No change to `forze.application` or any other integration.

## 7. Relationship to what's already shipped

| Building block | State today | This RFC |
| --- | --- | --- |
| Search contracts (`SearchQuery/Command/Management/SnapshotPort`) | shipped, backend-agnostic | implemented by a new adapter; **no contract change** |
| `SearchSpec` / `HubSearchSpec` / `FederatedSearchSpec` | shipped | consumed unchanged |
| `Rrf` rank fusion | shipped, shared (Meili `rrf`, Postgres) | reused for federated/hub merge |
| `forze_meilisearch` package shape | shipped | the structural template, mirrored leaf-for-leaf |
| Routed-tenant-client base + `NamedResourceSpec` | shipped | reused for tenancy/index naming |
| Lenient-read + field encryption (spec-level) | shipped ([[lenient-read-fields-plan]]) | carried over at the `_source` boundary |
| Result snapshots + offset executor | shipped | reused |
| Keyset (`search_after`) cursor variants | **only Postgres fills them; Meili refuses** | **OpenSearch fills them natively** (the capability win) |
| Query-DSL capability gating + parity harness | shipped ([[query-dsl-production-grade]]) | extended with an OpenSearch capability row |
| Aggregations / highlighting | **contract gap** ([[contract-surface-review-2026-06]]) | out of scope; flagged for a follow-on RFC |

## 8. Open questions

1. **`opensearch-py` async client & version pin.** Target `opensearchpy.AsyncOpenSearch` (aiohttp transport). Confirm the minimum version that has stable async `bulk`/`search_after`/`_msearch` and pin in the extra. *Leaning ≥2.6.*
2. **`delete_all` semantics.** `delete_by_query(match_all)` (preserve mapping, matches "remove all documents") vs delete+recreate the index (faster, but drops settings and races `ensure_index`). *Leaning `delete_by_query` for contract fidelity; document the cost.*
3. **One optional core hint: a search capability flag.** Today the contract has **no** explicit capability model for search ("No Capability Model in Core" — adapters just refuse unsupported variants with `precondition`). Cursor support diverges sharply (Postgres yes, Meili no, OpenSearch yes). Do we (a) keep the refuse-at-call-time status quo (zero core change), or (b) add a tiny additive `supports_cursor` / capability hint so callers can branch before calling? *Leaning (a) — stay status-quo, no core change; revisit only if a caller actually needs compile-time/branch-time discovery (matches the [[capability-model-census]] "do-not-churn" posture).*
4. **Aggregations / highlighting contract extension.** OpenSearch supports both natively and the contract gap is already logged ([[contract-surface-review-2026-06]]). Ship the adapter against today's contract now, and design a facets/highlights contract slice as a **separate** RFC — or block this RFC on that surface? *Leaning: ship now, extend later; the adapter is valuable without facets.*
5. **k-NN / hybrid semantic search.** The contract has no vector concept. Confirm this is a future RFC (new `VectorSearchSpec`/port), not a bolt-on here. *Leaning: separate RFC; non-goal for this one.*
6. **Native search-pipeline RRF vs coordinator-side merge.** First cut uses the framework `Rrf` merge for backend uniformity. Is OpenSearch's normalization-processor pipeline worth adopting later for single-index hybrid queries (where it is native and cheaper)? *Leaning: defer; uniformity first, native optimization only if measured.*
7. **Index template vs per-spec `create_index`.** `ensure_index` creates one index per spec. For namespace-tenanted (per-tenant index name) deployments this is many indices — should `ensure_index` instead register an **index template** so per-tenant indices inherit the mapping on first write? *Leaning: per-spec `create_index` for the static case; offer an index-template mode for namespace tenancy as a config flag in P2.*

## 9. Decisions

| # | Decision |
| --- | --- |
| 1 | `forze_opensearch` mirrors `forze_meilisearch`'s package shape one-for-one; the adapter depends on the contract, never the reverse. **Zero core change** — `forze.application` must not import `opensearchpy` |
| 2 | Native `search_after` keyset paging fills the `*_cursor` query variants Meilisearch refuses. This is the capability that justifies the package; without it the adapter would be a second Meilisearch |
| 3 | Federation uses the framework's coordinator-side `Rrf` merge, not OpenSearch's normalization-processor search pipeline — backend-uniform behavior first. Native pipelines stay a later, single-index-only optimization |
| 4 | Filter-operator coverage is capability-gated and **fail-closed**: an operator OpenSearch cannot express is refused, never silently approximated |
| 5 | Tenancy mirrors Meilisearch exactly — tagged (term filter on `TENANT_ID_FIELD`), namespace (per-tenant index via `NamedResourceSpec`), dedicated (routed per-tenant client) — with the `required_tenant_isolation` floor enforced at wiring |
| 6 | Not Elasticsearch: `opensearch-py` and OpenSearch 2.x semantics only. An ES adapter, if ever wanted, is its own package — API compatibility is not a maintenance contract |
| 7 | No k-NN / vector / hybrid semantic search in the first cut: the contract has no vector concept, and inventing one inside an adapter would foreclose the eventual core design |
| 8 | Aggregations and highlighting ship against **today's** contract; the contract extension is the facets & highlights RFC's job, not this one's |
