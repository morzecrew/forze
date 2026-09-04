# RFC 0044 — LanceDB retrieval integration

- **Status:** 📝 Draft
- **Scope:** One new optional integration package, `forze_lancedb`, implementing the
  **existing** search contract family (`SearchQueryPort` and friends) over LanceDB —
  vector retrieval first, lexical/hybrid capability-gated behind it. **No contract
  changes:** the external request's central proposal, a parallel
  `forze.application.contracts.retrieval` family, is rejected here because
  `forze.application.contracts.search` already models everything it asks for. Document
  CRUD on Lance tables is conformance-gated, not promised. Core never imports
  `lancedb`.
- **Related:** `src/forze/application/contracts/search/` (capabilities, ports, pages),
  `src/forze/application/contracts/embeddings/` (`EmbeddingsProviderPort`,
  `EmbeddingsSpec`), `src/forze_postgres/adapters/search/vector.py` and
  `src/forze_mongo/adapters/search/vector.py` (the two shipped vector adapters this
  one imitates), `[project.optional-dependencies]` in `pyproject.toml` (the extras
  mechanism).
- **Origin:** External feature request "Retrieval Core Ports and LanceDB Integration
  for Forze" (2026-09-04). Adopted with one structural correction — see §3 and
  decision 1.

---

## 1. Summary

`forze_lancedb`: a LanceDB adapter for the search plane in the common integration
shape (`kernel/` client + settings, `adapters/`, `execution/` deps + lifecycle),
installable as the `lancedb` extra. V1 is top-k vector search with the existing
filter DSL, logical-space→column mapping, tenant predicate as a structural
invariant, and honest `SearchCapabilities`. Full-text, hybrid fusion, index
administration and document-port coverage follow only when the conformance matrix
says LanceDB serves them correctly.

## 2. Motivation

AI-shaped applications want one corpus that serves ordinary lookups, metadata
filtering, and similarity retrieval without maintaining a separate vector-store
projection. Lance's table format (versioned datasets, cheap column backfill,
object-storage-backed) fits the "embedding as enrichment column" architecture, and
an embedded/local mode gives retrieval in tests and edge deployments where the
current vector adapters (Postgres+pgvector, Mongo) demand a server. The request
also asks for a second-backend validation of the retrieval abstraction — which
forze gets for free by pointing LanceDB at the contracts pgvector and Mongo
already implement.

## 3. Current state

Verified against the tree (2026-09-04, HEAD `e4304ac58` + one local commit) — this
section is where the adopted request and the repository disagree:

- **The retrieval contract family the request proposes already exists.**
  `src/forze/application/contracts/search/capabilities.py` has `SearchCapabilities`
  with `supports_vector`, `hybrid_fusion: frozenset[FusionStrategy]` (`rrf`,
  `weighted`), `filtered_ann: "none" | "prefilter" | "postfilter"`, plus
  `validate_vector_supported` / fusion validators that refuse cleanly. Scored hits
  live on the page types (`pages.py` documents score as ordering-only, `None` when
  not meaningful). `SearchQueryPort` carries `search` / `search_page` /
  `search_cursor` / `search_stream`; `SearchManagementPort` and `SearchCommandPort`
  exist for index admin and writes.
- **Two vector adapters already implement it**:
  `PostgresVectorSearchAdapter` (pgvector, bring-your-own-vector via an adapter-held
  `embedder`, `vector_column`, `vector_distance` `l2`/`cosine`/`dot` operator
  families) and a Mongo counterpart. Their shape — spec-field→column mapping,
  dimension pinned on the adapter, filters composed around the ANN leg — is the
  template `forze_lancedb` copies.
- **Embeddings are a separate plane**, as the request wants:
  `EmbeddingsProviderPort` (`embed`, `embed_one`) and `EmbeddingsSpec` under
  `contracts/embeddings/`. Note the repo convention differs from the request's
  purism in one spot: shipped vector adapters *hold* an embedder so a text query
  can be encoded adapter-side; bring-your-own-vector remains supported.
- **Filter honesty is an existing contract concern** — capabilities distinguish
  prefilter from postfilter ANN precisely because they are not equivalent; a
  keyword-only adapter must keep `filtered_ann="none"` (enforced in
  `__attrs_post_init__`).
- Integration packaging is mechanical: one `forze_<name>` package per extra in
  `[project.optional-dependencies]` + `[tool.hatch.build.targets.wheel]`; there is
  no `lancedb` dependency anywhere today.

## 4. Goals / Non-goals

**Goals**

- LanceDB adapter for `SearchQueryPort` (vector variant) with truthful
  `SearchCapabilities` and stable forze errors — never raw `lancedb` exceptions.
- Logical vector space → physical column mapping in adapter settings, so schema
  evolution never touches application semantics.
- Existing filter DSL translated under a strict allowlist; unsupported predicates
  refuse (`query_feature_unsupported`), never silently drop.
- Tenant predicate joined into the eligible corpus *before* top-k, as a structural
  adapter invariant, conformance-tested.
- Local-path and object-storage URIs via LanceDB's own configuration; secrets
  through existing forze settings/secret facilities.

**Non-goals**

- A new `contracts/retrieval` package — that is `contracts/search`'s job (decision 1).
- Changing `EmbeddingsProviderPort` or adding vector arguments to
  `DocumentQueryPort.find_many` — the request and the repo agree these are wrong.
- Exposing Lance snapshot/version APIs as core ports; a `DatasetVersionPort` waits
  for a second integration to demand it.
- Reranking, multimodal embedding providers, engine-side embedding beyond the
  existing adapter-embedder convention.

## 5. Design

### 5.1 Package

```text
src/forze_lancedb/
├── kernel/        # client wrapper + LanceDBSettings (uri, storage_options)
├── adapters/
│   ├── search/    # LanceVectorSearchAdapter; later fts.py, hybrid
│   └── document/  # only what the conformance matrix admits (§10 Q2)
├── execution/     # deps module + lifecycle, same as sibling integrations
└── __init__.py    # public API; never re-exports lancedb
```

Settings map logical spaces to columns
(`LanceVectorSpace(logical_name, column, dimensions, distance)`); the adapter
verifies query-vector dimension and refuses unknown spaces with stable errors.
Index policy (`manual` vs `ensure`) is adapter configuration; IVF/PQ/HNSW
parameters never leave the package.

### 5.2 Capability posture

V1 advertises `supports_vector=True`, `hybrid_fusion=frozenset()`, and whichever
`filtered_ann` mode LanceDB's filtered ANN actually delivers (measured, not
assumed — §6). Lexical and hybrid land as capability flips with their adapters,
not as contract work. If LanceDB can only postfilter for some configuration, the
adapter says so; it does not emulate prefilter by oversampling silently.

### 5.3 Scores

`distance` preserved raw where the port surface carries it; `score` is a
documented monotone ordering value (the existing `pages.py` wording already
disclaims cross-backend comparability). No probability semantics.

## 6. Tests

Reuse the existing cross-backend search parity/conformance batteries and run
LanceDB as another leg wherever its capabilities claim support — ranking sanity
(nearest fixture first), dimension mismatch refusal, unknown space refusal,
filter-restricts-corpus, tenant-bypass impossibility, score/order consistency.
Filtered-ANN behavior gets a dedicated measurement test whose result *sets* the
advertised capability. Any document port the adapter claims runs the document
conformance suite — structural typing alone never counts as compatibility.
Local-path fixtures in unit tests; object-storage fixture where CI permits.

## 7. Docs

One integration page in the established per-integration shape (install extra,
settings, wiring, capability table), plus an ingestion + semantic search example
composing `EmbeddingsProviderPort` with the adapter. Score semantics worded as
ranking-only.

## 8. Out of scope

- Full `DocumentQueryPort`/`DocumentCommandPort` conformance claims — transactions,
  `for_update`, keyset guarantees, aggregation, streaming each need the matrix
  first; the escape hatch is exposing a narrower supported port (§10 Q2).
- Index administration surface beyond what `SearchManagementPort` settles into —
  that port's own scope is still open repo-wide; this RFC does not decide it.
- A `SemanticSearchService` convenience composer — application-layer, separate
  proposal if wanted.

## 9. Risks

- **LanceDB semantics drift under us** (young API surface): pin the client
  version, wrap it in `kernel/`, and keep the adapter the only importer.
- **Postfilter-only filtered ANN reads as a correctness bug**: mitigated by
  advertising the true `filtered_ann` mode and refusing what it cannot honor —
  the same honesty contract the capability model exists for.
- **Scope creep back toward the request's full surface** (new contracts, admin
  ports, multimodal): decisions 1 and the non-goals are the fence; re-litigation
  goes through this table, not through implementation.

## 10. Unresolved questions

- **Q1 (gates execution):** which client/deployment modes does v1 support —
  embedded local only, or also LanceDB Cloud/remote? Settled by what the pinned
  client version supports plus one real CI fixture.
- **Q2:** which document operations does Lance serve correctly? Settled by running
  the document conformance suite per operation; the answer defines the exposed
  port, and "none in v1" is acceptable.
- **Q3:** hybrid fusion — native Lance hybrid vs adapter-level RRF over two legs?
  Preference order is native, then deterministic adapter RRF, then refusal;
  measured when the lexical leg exists.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | No new `contracts/retrieval` family. The request's core proposal duplicates `contracts/search` (capabilities, fusion vocabulary, filtered-ANN honesty, scored pages, admin/command ports all exist and are implemented by two backends). LanceDB validates the existing abstraction; a parallel family would fork the vocabulary and every future adapter would have to choose. |
| 2 | `LOCKED` | Dependency direction: `forze_lancedb -> forze` only, shipped as the `lancedb` extra; core and other integrations never import it. Reversing this breaks the packaging model every integration shares. |
| 3 | `LOCKED` | Unsupported filters and capability gaps refuse with stable forze errors; nothing is silently ignored or emulated. This is the plane's existing honesty contract extended, not new policy. |
| 4 | `ASSUMED` | V1 slice = vector `SearchQueryPort` adapter only (spaces, dimensions, distance mapping, filter allowlist, tenant invariant). FTS/hybrid/admin/document coverage are later phases, each gated on its own evidence. |
| 5 | `ASSUMED` | Embedder placement follows the shipped adapter convention (adapter-held embedder, bring-your-own vector supported) rather than the request's stricter "retrieval never encodes" rule — matching pgvector/Mongo keeps the plane uniform. |
| 6 | `ASSUMED` | Advertised `filtered_ann` is set by measurement (§6), defaulting to the weaker claim when ambiguous. |
| 7 | `OPEN` | Index lifecycle surface: minimal `ensure_indexes`-style hook vs fuller admin, pending the repo-wide `SearchManagementPort` question. Executor aligns with whatever that port settles into and logs the choice. |
| 8 | `OPEN` | Q1–Q3 above are delegated to execution with their settlement criteria. |

## 12. Phasing

- **P1:** package skeleton + kernel + vector search adapter + parity-battery leg +
  docs page. One PR.
- **P2 (evidence-gated):** lexical search; capability flip with its conformance
  cases.
- **P3 (evidence-gated):** hybrid fusion per Q3.
- **P4 (matrix-gated):** document port coverage per Q2; index admin per decision 7.
