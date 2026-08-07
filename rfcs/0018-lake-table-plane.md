# RFC 0018 — Lake table plane: governed Iceberg tables (`contracts/lake` + `forze_iceberg`)

- **Status:** 📝 Draft (family head: 0019 = maintenance liveness, 0020 = engine closure & breadth)
- **Scope:** A new plane for **lake tables as managed application state**: `contracts/lake/` (spec, catalog/write/scan/maintenance ports, capabilities, a structured filter subset) + `forze_iceberg` (pyiceberg over an Iceberg REST catalog, object store via the secrets plane or catalog-vended credentials) + an in-memory mock with real snapshot/evolution semantics + a mock ≡ real conformance battery that ships **in P1, inside the CI battery** — not after it (the 7th-edition audit's "built the mechanism, not the gate" theme, applied preemptively). This RFC deliberately reverses a documented non-goal; §1 argues the reversal instead of pretending it isn't one.
- **Related:** G3 of the Linecust gap analysis (no columnar/lake writers anywhere; pyarrow only inside the duckdb extra). The non-goal being reversed: `pages/docs/integrations/duckdb.md` — "**Query-only.** Writing/maintaining tables (Iceberg/Delta compaction, ingest) is an ETL/ops concern, not a domain port — keep it out of the adapter" (repeated in the lake recipe). The portability plane's Parquet rejection is **unaffected** — that was about the portability archive format, and stays correct. The mock-horizon doctrine (proofs vs mock are tautology until a real-adapter differential) shapes §7. RFC 0015/0017 are the read/load siblings of the same runtime-data story.
- **Origin:** Heavy-data applications (Linecust-class: per-client Silver/Gold on a lake) make lake tables part of the *application's runtime behavior* — created per tenant by product flows, appended by pipelines, read by dashboards. The gap analysis's D2 ("defer Iceberg for MVP") was correct precisely because this plane didn't exist; this family converts the deferral into a staged adoption path instead of a permanent avoidance.

---

## 1. The doctrine reversal, argued

The "ETL/ops concern, not a domain port" non-goal was right when it was written, for two reasons that have both expired:

1. **Lake writing used to mean offline batch ETL** owned by a data-platform team with Spark. In the applications this framework now targets, lake tables are *tenant-scoped application state*: a product flow creates a table, a pipeline appends to it, a dashboard reads it — the same lifecycle as a document collection, at columnar scale. When writes are app behavior, they deserve what forze exists to provide: fail-closed tenancy, capabilities, an error taxonomy, conformance, DST. Leaving them to bare pyiceberg in app code is the raw-client posture on the plane with the **heaviest blast radius in the framework's world** — a mis-scoped write lands in another tenant's table and snapshots make it *durable history*.
2. **There was no credible Python seam.** There is now, verified 2026-07: pyiceberg ships production writes (append, **dynamic partition overwrite**, bucket-partitioned writes, upsert on identifier fields) and maintenance primitives (`expire_snapshots`, `remove_orphan_files`); the Iceberg **REST catalog** is the standardized vendor-neutral control plane (Apache Polaris graduated to TLP 2026-02; Lakekeeper is the Rust single-binary alternative; Glue/Unity/Nessie all speak it), including **credential vending**; and DuckDB's iceberg extension reads *and writes* through attached REST catalogs since v1.4 — so everything forze writes is immediately readable (and maintainable) by the engines forze already integrates.

**What does not reverse.** Forze is not becoming a query engine, a Spark replacement, or a table-optimizer service. Heavy reads stay on engines (DuckDB/ClickHouse/BigQuery via the analytics + dynamic-read planes); manifest-level rewrite compaction stays engine/operator territory (§6 names the honest middle); CDC ingestion frameworks, branching/tagging (Nessie-style), and multi-table transactions are out of scope, the last recorded as a REST-spec capability to revisit.

*(2026-08-03 addendum — RFC 0030 partially answers the branching line.* The reason branching was wanted here is write-audit-publish: validate a rebuild before it becomes visible. RFC 0030 delivers WAP through a staging relation plus an atomic swap, which needs no branch and works on this plane through `overwrite_partition` — so lake tables get the audit-before-publish gate without the Nessie dependency. Multi-table atomicity remains genuinely out: Iceberg's commit is per-table, and 0030 records that as this backend's `PublishAtomicity` ceiling rather than papering over it.*)*

## 2. Contract — `contracts/lake/`

```python
@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LakeTableSpec(BaseSpec):
    schema: type[BaseModel] | ArrowSchemaRef      # Pydantic model → derived Iceberg schema,
                                                  # or an explicit Arrow schema for wide/complex tables
    partitioning: tuple[PartitionField, ...] = () # transforms: identity | day | month | hour |
                                                  # bucket(n) | truncate(n) — declared, validated
    identifier_fields: tuple[str, ...] = ()       # upsert key; empty = upsert refused
    evolution: Literal["additive", "frozen"] = "additive"
    properties: Mapping[str, str] = {}
    description: str | None = None
```

No `encryption` field, deliberately (§5). Schema derivation (Pydantic → Iceberg types) covers the scalar/temporal/decimal/uuid/list/struct core and **fails closed** on anything it cannot map faithfully — the Decimal-union and UUID-write-gap incidents are the reason derivation refuses rather than approximates.

**Ports** (one spec, four ports — data/control separation per the port-plane doctrine):

```python
class LakeCatalogPort(Protocol):                      # control plane
    def ensure_table(self) -> Awaitable[TableRef]: ...        # create-if-absent + reconcile:
        # existing table validated against the spec; additive drift evolved when
        # evolution="additive", anything else fails closed (lake_schema_drift)
    def table_exists(self) -> Awaitable[bool]: ...
    def snapshots(self, *, limit: int = 50) -> Awaitable[Sequence[SnapshotInfo]]: ...
    def current_snapshot(self) -> Awaitable[SnapshotInfo | None]: ...
    def drop_table(self) -> Awaitable[None]: ...              # gated by config flag, provisioner-style

class LakeWritePort(Protocol):                        # data plane; write-guarded (command ops only)
    def append_rows(self, rows: Sequence[BaseModel], *,
                    commit_ref: str | None = None) -> Awaitable[AppendResult]: ...
    def append_batches(self, batches: ArrowBatchSource, *,
                       commit_ref: str | None = None) -> Awaitable[AppendResult]: ...
    def overwrite_partition(self, partition: LakeFilter, batches: ArrowBatchSource, *,
                            commit_ref: str | None = None) -> Awaitable[AppendResult]: ...
    def upsert_batches(self, batches: ArrowBatchSource) -> Awaitable[UpsertResult]: ...  # capability-gated

class LakeScanPort(Protocol):                         # data plane; read ops
    def scan_batches(self, *, filter: LakeFilter | None = None,
                     projection: Sequence[str] | None = None,
                     as_of: SnapshotRef | None = None) -> AsyncGenerator[ArrowBatch]: ...

class LakeMaintenancePort(Protocol):                  # control plane; each op capability-gated
    def expire_snapshots(self, *, older_than: timedelta, retain_last: int = 5) -> Awaitable[ExpireResult]: ...
    def remove_orphan_files(self, *, older_than: timedelta = timedelta(hours=72),
                            dry_run: bool = True) -> Awaitable[OrphanResult]: ...
    def compact_partition(self, partition: LakeFilter, *,
                          target_file_size: int) -> Awaitable[AppendResult]: ...   # §6
```

`AppendResult{snapshot_id, records_written, files_written}` and every control-plane value object are **JSON-trivial by construction** — a durable step journals the *snapshot id*, never the batches, which is exactly the durable-execution interplay this plane is designed around: `overwrite_partition` is the Pipeline-Engine write primitive because re-running a step **replaces** the partition deterministically instead of double-appending.

**Filters are structured, never strings.** `LakeFilter` is a declared **subset of the existing querying DSL** (`$values` with `$eq/$gt/$gte/$lt/$lte/$in/$null`, `$and`/`$or`) compiled to pyiceberg expressions, with a fail-closed validator (`lake_filter_unsupported`) for everything outside the subset. No engine filter strings anywhere on the plane — the RFC 0015 "engine-enforced, never parsed" doctrine holds trivially here because the input is already structured.

**Arrow at the boundary — the load-bearing decision.** Bulk currency is `pyarrow` (`RecordBatchReader` / iterables of `RecordBatch`); `append_rows` is the typed-row convenience for small governed appends (Pydantic rows through the derived schema). pyarrow lands in a new `iceberg` extra — The portability plane's "pyarrow ~100 MB" objection was about the portability *core* and is untouched; here the dependency is the plane's entire point. The JSON-trivial doctrine is split honestly: control plane JSON-trivial (journalable), data plane explicitly not (never journaled, never traced by value — DST captures counts and snapshot ids).

## 3. Tenancy — with one pleasant inversion of RFC 0015

| Tier | Mechanism | Notes |
|---|---|---|
| `namespace` | **Iceberg namespace-per-tenant** via `NamedResourceSpec` resolver | the default posture; `LakeNamespaceTenantProvisioner` creates the namespace on tenant provision |
| `dedicated` | warehouse/catalog-per-tenant via a routed catalog client | REST catalogs model this natively (Polaris catalogs, Lakekeeper warehouses) |
| `tagged` | shared table + tenant column | **allowed — unlike dynamic read** |

The inversion is worth spelling out: dynamic read refused `tagged` because a tenant predicate inside an unreadable runtime statement is unverifiable. Here **both sides are structured**, so the adapter enforces the tag itself: on write it *stamps* the tenant column (callers cannot supply it — `lake_tenant_column_reserved`); on scan it *injects* `$eq tenant` into the compiled filter. Same doctrine, opposite verdict, because verifiability — not the tier — was always the criterion. (Reads of tagged lake tables through *dynamic read/DuckDB SQL* remain governed by 0015/0016's rules; this plane's guarantee covers its own ports.)

**Credentials, two modes:** static object-store credentials resolved from the secrets plane (the `forze_duckdb` `S3Credentials` pattern), or **catalog-vended credentials** (the REST spec's table-scoped, short-lived credential vending) — vended is the documented preference where the catalog supports it, because it is the fail-closed posture: the writer never holds standing bucket-wide keys.

## 4. Idempotency and concurrency

- Iceberg commits are atomic CAS swaps at the catalog; concurrent writers conflict at commit. The adapter maps the conflict to `lake_commit_conflict` and retries **appends** under the route's resilience policy (append commits commute); `overwrite_partition` conflicts are *not* auto-retried blindly — last-writer-wins on a replace is a semantic decision the caller owns, surfaced, not swallowed.
- `commit_ref` stamps a snapshot **summary property** (`forze.commit-ref`); the adapter checks the last *N* snapshots (config, default 100) before committing and returns the existing `AppendResult` on a hit. Named honestly as **bounded-lookback defense-in-depth** — the primary idempotency mechanism remains durable-step journaling of the returned snapshot id; the stamp catches the crashed-between-commit-and-journal window.

## 5. What the plane refuses to know

- **No field encryption, v1.** Lake tables are engine-readable by design — that is their purpose — so column sealing is a direct contradiction (the encryption × search collision, worse). Tiering is bucket SSE-KMS (existing storage-config vocabulary) + catalog authorization; Parquet modular encryption is recorded as future work, blocked on pyiceberg support, not promised.
- **No raw engine strings** — no SQL, no pyiceberg expression strings; the structured filter subset or nothing.
- **No portability participation, v1.** Lake tables declare `REBUILDS_FROM` provenance (Bronze / upstream) in the spec inventory rather than exporting; a lake-export flavor is a follow-up with its own cost case.
- **No compaction-by-manifest-rewrite** (see §6) and no automatic maintenance — scheduling is RFC 0019's whole subject.

## 6. The compaction honesty (the ecosystem's sharpest edge, named)

pyiceberg is **copy-on-write with no in-library compaction**; upsert-heavy workloads accumulate small files until an engine-side rewrite runs (Spark `rewrite_data_files`, managed-catalog auto-compaction à la S3 Tables). The plane's stance, in three layers:

1. `compact_partition` ships as **rewrite-by-overwrite**: scan the partition, re-pack to `target_file_size`, `overwrite_partition` with identical logical content. It is real small-file compaction for partition-scoped tables — with its costs stated (full partition rewrite; commit-conflict window against concurrent writers, surfaced not retried).
2. Manifest-level/whole-table rewrite stays **operator/engine territory**, said in the docs with the same bluntness as the original non-goal.
3. `upsert_batches` is capability-gated *and* its docstring carries the growth warning with a pointer to the 0019 maintenance schedule — an upsert route without a compaction plan is a documented footgun, not a silent one.

## 7. Mock, DST, conformance — the plane ships inside the battery

- `MockLakeAdapter`: in-memory catalog + tables with a **real snapshot log**, schema-evolution rules (additive accepted, else refused), partition-overwrite replacement semantics, commit-ref dedup, tenant stamping/injection — deterministic, DST-legal (no I/O). This is not a stub: the semantics above *are* the contract, and the mock is where DST exercises crash-between-commit-and-journal, concurrent-append conflict, and overwrite-idempotency schedules.
- **Differential leg in P1, non-negotiable:** pyiceberg + **Lakekeeper testcontainer** (single binary, fast start) + MinIO, running the same battery as the mock. The mock-horizon rule says proofs vs mock are tautology until this leg exists; the 7th-edition finding says new planes ship with zero legs unless the leg is part of the definition of done. Both lessons are priced in here, not deferred.
- Version pins to verify at pickup (facts checked 2026-07, pin exact versions when executing): current pyiceberg release (upsert, dynamic overwrite, `expire_snapshots`, `remove_orphan_files` all present), Lakekeeper container tag, pyarrow floor shared with the duckdb extra.

## 8. Acceptance battery (mock ≡ real Lakekeeper+MinIO unless marked)

1. Rich-type round-trip through schema derivation: UUID, Decimal, tz-datetime, nested struct/list, NULL — append then scan, both `append_rows` and `append_batches` at 10⁵ rows. Derivation refuses an unmappable model at spec validation. 
2. `ensure_table` reconciliation: absent → created; additive drift → evolved under `"additive"`, refused under `"frozen"`; incompatible drift → `lake_schema_drift` always.
3. `overwrite_partition` idempotency: run twice with identical input → identical logical content, second run replaces not doubles; the Pipeline-Engine re-run story, pinned.
4. `commit_ref` dedup: duplicate append with same ref returns the original snapshot; crash-between-commit-and-journal simulated in DST, replay converges.
5. Concurrent appends: both commit (retry on conflict), all rows present exactly once; concurrent overwrite conflict surfaces `lake_commit_conflict` unretried.
6. Tagged tier: write-side stamp (caller-supplied tenant column refused), scan-side injection — two tenants, shared table, disjoint scans.
7. Namespace tier: resolver-routed namespaces; provisioner creates on provision; cross-tenant `ensure_table` collision impossible by construction.
8. Time travel: `as_of` snapshot and timestamp scans return historical content; after `expire_snapshots`, an expired `as_of` fails loudly with a mapped code (never returns partial data).
9. Filter subset: each supported op compiles and pushes down (file-pruning asserted via plan/file counts on real leg); unsupported ops fail closed.
10. `compact_partition`: small-file count drops, logical content identical (checksum), snapshot lineage records the rewrite.
11. Upsert (gated): identifier-field merge semantics; spec without `identifier_fields` refuses at wiring.
12. DST: full mock battery under forced schedules; data-plane values absent from traces, snapshot ids present.

## 9. Phases

- **P1** — contract + derivation + `forze_iceberg` (ensure/append/scan/overwrite_partition, REST catalog, static credentials) + mock + battery 1–5, 7, 9, 12 **including the Lakekeeper differential leg**.
- **P2** — tagged tier + provisioner + time travel + `expire_snapshots`/`remove_orphan_files` + battery 6, 8.
- **P3** — `compact_partition` + upsert gating + vended credentials + battery 10–11.

## 10. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Reverse the "query-only / not a domain port" non-goal for *tables as application state*; engines/optimizers/CDC stay out — the reversal is scoped, not total | locked |
| 2 | Iceberg-first, REST-catalog-first; `contracts/lake` is format-neutral, `forze_iceberg` is the backend package; Delta/Lance are RFC 0020 territory | locked |
| 3 | Arrow is the bulk currency (new `iceberg` extra); typed rows are convenience; control plane stays JSON-trivial for durable journaling — the portability plane's Parquet rejection unaffected | locked |
| 4 | Filters are a compiled subset of the querying DSL, fail-closed; no engine strings on any port | locked |
| 5 | `overwrite_partition` is the blessed pipeline write primitive (idempotent re-runs); appends auto-retry commit conflicts, overwrites never silently do | locked |
| 6 | `commit_ref` snapshot-property dedup = bounded-lookback defense-in-depth; durable journaling stays primary | locked |
| 7 | Tagged tier **allowed** — adapter-stamped writes + injected scan filters are verifiable; the 0015 refusal criterion was verifiability, and this plane meets it | locked |
| 8 | No field encryption v1 (engine-readable by design; bucket SSE + catalog authz is the tiering); Parquet modular encryption recorded, unpromised | locked |
| 9 | Compaction honesty: rewrite-by-overwrite ships; manifest-rewrite is operator territory; upsert capability-gated with the growth warning attached | locked |
| 10 | Differential leg (Lakekeeper + MinIO) is part of P1's definition of done — the plane is born inside the conformance battery | locked |
| 11 | Named consumers: Linecust Silver/Gold post-MVP (converts gap-analysis D2 into staged adoption); any heavy-data tenant-scoped analytics product | recorded |
