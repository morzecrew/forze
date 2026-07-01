# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Postgres co-located idempotency store — exactly-once across a crash** — `PostgresDepsModule(idempotencies={route: PostgresIdempotencyConfig(...)})` wires a co-located idempotency table whose result record commits *inside* the business transaction, so a crash between the business commit and the record can no longer re-execute a duplicate. It builds on a general capability: an idempotency store can report `commits_in_transaction`, and when it does the record write is driven from an `on_success` hook auto-injected at plan freeze (whenever an idempotency wrap sits on an operation with a transaction route — no per-operation wiring), committing atomically with the business writes. The middleware falls back to the out-of-transaction commit for a non-transactional store (Redis) or when no in-transaction write ran, so correctness never depends on the wiring — only atomicity does. The mock models the co-located path via `MockIdempotencyAdapter(transactional=True)`.

- **HLC monotonicity survives a restart — durable clock high-water mark** — a runtime's Hybrid Logical Clock kept its last-issued timestamp only in memory, so a process restart reset it to `(0, 0)` and could then re-issue a stamp at or below one it had already emitted (and relayed) whenever wall time had regressed or a peer merge had carried the clock ahead — breaking the causal ordering the HLC exists to guarantee. `HybridLogicalClock.resume(mark)` now seeds a rebuilt clock from a persisted floor, and a new co-located `HlcCheckpointPort` persists that floor: the outbox flush advances it (the max HLC among the rows it stamps) *inside the business transaction*, so a committed stamp is never durable without a mark covering it and a rolled-back flush never advances it. `PostgresDepsModule(hlc_checkpoint=PostgresHlcCheckpointConfig(...))` wires the Postgres store (a monotonic `GREATEST` upsert over an app-provided `(node_key, hlc)` table) and `hlc_checkpoint_recovery_lifecycle_step()` resumes the clock at startup. Opt-in and node-global; unwired, the clock resumes from `(0, 0)` exactly as before. The mock models it via `MockDepsModule(hlc_checkpoint=True)`.

- **Streaming reads on Mongo and Firestore (`find_many_chunked`)** — both read gateways gain the bounded-memory streaming read Postgres already had: yield validated batches of ≤`fetch_batch_size` (peak = one batch), reading past the implicit `find_many` cap without materializing the whole result. Mongo iterates the driver cursor (`find_many_streamed`, supports `offset`); Firestore consumes its native stream (`query_stream_batched`, rejects `offset` — use cursor pagination). Same `return_model` / `return_fields` rules as `find_many`; all three document backends now have parity.

- **`FederatedSearchSpec(thin_merge=True)` — late-materialized RRF merge** — by default a federated search holds the whole candidate union (up to `rrf_per_leg_limit` full hits per member) in memory to fuse and sort. With `thin_merge` it fetches only `id` per leg, fuses on `(member, id)`, and re-hydrates just the page from each member, so peak is the thin keys plus one page. Snapshots store only `(member, id)` and replay by re-fetching the page by id (content current, order/identities frozen, deleted hits drop out). Opt-in (one extra page-sized round trip per member); falls back to the full path for highlights, a secondary sort, or members lacking an `id` field. Postgres and Meilisearch; results identical to the full path.

- **`SearchSpec(highlight_scan_limit=…)` — bound the highlight text scan** — highlighting a PGroonga field fetches its raw text to mark matches in Python (PGroonga's snippet can't case-fold non-ASCII scripts like Cyrillic). When set, only the first `highlight_scan_limit` characters are scanned (server-side `left(...)`, for both the PGroonga column and the `ts_headline` input); a match beyond the cap isn't highlighted but the hit and its read fields are unaffected. Opt-in; `None` (default) scans the whole field.

- **`SearchSpec(max_results=…)` — bound for unbounded searches** — an offset search with no caller `limit` otherwise fetches the entire matched set (a latent OOM on a large index). `max_results` caps that unbounded fetch (an explicit caller `limit` is honoured as-is); applied in the shared offset executor, so it covers Postgres, Mongo, and Meilisearch. Opt-in; `None` (default) keeps fetch-everything.

- **Search facets & highlights** — requests can ask for term facet distributions and per-hit match highlights via the search options, declared on the spec and returned as optional page sidecars. Available on mock, Meilisearch, and Postgres single-index (PGroonga/FTS) and hub, over offset and cursor pagination, plus per-hit highlights on federated; unsupported fields or topologies fail closed. The DTO response carries them on the generated search routes.

- **Cross-aggregate (system) invariants** — `SystemInvariant` (with `ReadSet`, `SumOf`, `CountAll`) in `forze.application.contracts` (front-doored from `forze`) declares a law over a scoped read-set's aggregate that the entity-level `@invariant` can't express. `forze_kits.invariants` adds `evaluate` / `enforce` (post-commit, detective) / `enforce_preventive` (in-tx rollback, fails closed below the law's `required_isolation`) / `propose` (dry-run); `forze_dst.compile_oracle(*laws[, per_commit=True])` verifies it under simulation.

- **Transaction isolation as a fail-closed contract** — operations declare an isolation level and the kernel verifies it against the route's manager (`exc.configuration`, never silently weaker); declaring isolation without a tx route is rejected at registry freeze. `TransactionContext.current_isolation()` exposes the active level, and a `tx` `exit` trace event now carries `outcome` (`commit`/`rollback`).

- **Per-port OpenTelemetry client spans** — `DepsRegistry.with_otel_port_spans()` opts every resolved port into a per-call `CLIENT` span (child of the operation span) tagged `forze.port.{domain,surface,route,phase,op}`, inside the resilience policy (one per retry; none when breaker/bulkhead-rejected). Opt-in, zero-cost off; streaming methods pass through un-spanned.

- **W3C trace-context propagation across the async event flow** — a published event carries its span across outbox→broker→inbox so the consumer joins the same trace. Opt in per outbox with `OutboxIntegrationConfig.propagate_trace` (add a nullable `traceparent` column on relational backends first); `forze_http` injects trace context outbound. Trace-parenting only — never identity, tenancy, or dedup.

- **Document aggregates fail closed on backends that can't compute them** — `QueryCapabilities.supports_aggregates` (plus a sibling `validate_aggregate_capabilities`); a backend that can't compile group-by/aggregate pipelines (the Firestore MVP) rejects `find_many(aggregates=…)` / `find_many_aggregates` / `count_aggregates` with a clean `precondition` (code `query_feature_unsupported`) instead of an opaque `internal` (500).

- **Procedures port — governed parametrized commands/compute** — `ctx.procedure.command(spec).run(params)` runs a spec-named, parametrized statement (a function / `CALL`, a set-based recompute, or `REFRESH MATERIALIZED VIEW`). One `ProcedureSpec[In, Out]` per procedure, command-only (refused in a read-only operation), on Postgres plus a programmable mock. Tenant-aware routes fail closed at wiring unless the SQL binds `%(tenant)s`.

- **Query parameters — bound session settings for read sources** — a read declares a typed `query_params` contract and a handler binds values with `ctx.document.query(spec).with_parameters(P(...))`, applied as query-scoped session settings the relation reads internally (Postgres documents plus a programmable mock), so the full read DSL still composes. Capability-gated and fail-closed.

- **Nested-field projection** — projection `fields` may be dotted paths into nested sub-models (`project(filters, ["contract.reg_number"])`), returning the nested `{"contract": {"reg_number": ...}}` shape: sibling leaves merge under one parent, a requested root subsumes its leaves, an absent nested leaf is omitted, and a requested whole top-level field absent from the row stays present as `null` (the flat-projection contract). A path that crosses a **list** maps the selection over each element, preserving structure and length — `["items.sku", "items.qty"]` yields `{"items": [{"sku": ..., "qty": ...}, ...]}`, and nested lists recurse. Resolved like nested filters/sorts across `project_*` document reads and `project_search_*` on every backend (mock, Postgres, Mongo, Firestore), over offset and keyset-cursor pagination. A backend fetches the leaf's root column and reshapes in process; a keyset-cursor sort key must be covered by a projected field equal to it or one of its ancestors. Behavior change: the mock previously emitted a flat `"contract.reg_number"` key for a dotted projection — it now nests, matching the other backends.

- **Nested-field sorting** — sort keys may be dotted paths into nested Pydantic sub-models and `str`-keyed mappings (`sorts={"addr.city": "asc"}`), resolved like nested filters across offset and keyset-cursor reads on every backend; `default_sort` accepts nested paths too. Sorting on a nested path whose root column is field-encrypted is now rejected (it could leak the value into a cursor token).

- **Materialized derived fields** — `DocumentSpec(materialized=…)` persists selected `@computed_field`s as real columns so they're filterable/sortable (validated; create/update collisions rejected; startup column check). `SearchSpec` / `HubSearchSpec` accept it too for in-place search (Postgres, Mongo), but there it is not startup-validated.

- **Lenient read fields** — `DocumentSpec(lenient_read_fields=…)` / `SearchSpec` / `HubSearchSpec` let a read-model field have no backing column: dropped from the projection, hydrated from its default, removed from filter/sort allow-sets, and tolerated by startup schema validation. Honored on **Postgres, Mongo, Firestore** documents (and Postgres/Mongo search); read-side, opt-in. `read_conformity="lenient"` auto-derives the set (statically-defaulted, non-identity, non-materialized/indexed fields); `FederatedSearchSpec` inherits from its members. For expand/contract migrations.

- **Write-omit fields** — `DocumentSpec(write_omit_fields=…)`, the write-side counterpart: a domain field with no column is silently stripped from every insert/update and hydrates from its default on read-back. Honored on **Postgres, Mongo, Firestore**; explicit-only (never auto-derived), requires a `write` spec, warns at definition.

- **Two-phase prepare/apply handlers** — `TwoPhaseHandler` (plus a kit base): `prepare(args)` runs outside the transaction (CPU or external work) and `apply(args, payload)` inside it, so the transaction wraps only the writes. A tx route is required and `prepare` is read-only.

- **CPU-offload seam** — `run_cpu` / `run_cpu_map` run blocking or CPU-bound work off the event loop via a context-bound `CpuExecutor` (a bounded thread pool in production; inline and deterministic under simulation), honoring the invocation deadline with a cooperative `checkpoint()`.

- **Shared error helpers in core** — `error_envelope()` and `guard_frame()` give one client-safe `CoreException` projection (masking, egress context, status hint) and a shared guarded boundary, and `http_status_for_kind(kind)` maps an `ExceptionKind` to its HTTP status. FastAPI and Socket.IO render through the same helpers instead of each duplicating the logic.

- **Less CRUD boilerplate** — `build_document_registry(spec)` derives its `DocumentDTOs` from the spec when `dtos` is omitted (`create=None` / `update=None` to disable an op), and `document_facade(runtime, registry, spec)` returns a per-call typed `DocumentFacade` factory. Both additive.

- **Top-level front door** — the most-used names re-export from `forze` and `forze_kits` (`from forze import DocumentSpec, build_runtime`; `from forze_kits import DocumentFacade, build_document_registry`), resolved lazily (PEP 562) so `import forze` stays cheap. Deep paths keep working; the core never imports kits.

- **Redis stream & pub-sub transports** — `RedisDepsModule` wires the generic `StreamSpec` / `PubSubSpec` transports via new `RedisStreamConfig` / `RedisStreamGroupConfig` / `RedisPubSubConfig`, so realtime-over-Redis and outbox→stream/pub-sub relay work in production. `encryption="end_to_end"` seals payloads through the broker; `tenant_aware` adds a `tenant:{id}:` key prefix (no namespace — the stream/topic is per-call).

- **Stream consumer-group query/admin split** — the stream consumer-group adapter splits into a data-plane query adapter (read/ack/claim/pending) and a control-plane `*StreamGroupAdminAdapter` (`ensure_group`), for both Redis and the mock, so a `StreamGroupQueryPort` reference can't reach group provisioning.

- **Encryption *reach* vocabulary** — new `EncryptionReach` ladder (`none < at_rest < end_to_end`) names the outbox/messaging setting distinct from storage coverage. `OutboxEncryptionTier` is now a back-compat alias and `MessageEncryptionTier` its transport subset (no `at_rest`); field name and values unchanged.

- **`required_reach` floor** — `CryptoDepsModule(required_reach="end_to_end"|"at_rest")` refuses, at resolve, any outbox or transport route whose declared reach is weaker (`exc.configuration`). Opt-in (default `None`); a transport meets an `at_rest` floor only via `end_to_end`, enforced at both publish and subscribe/consume.

- **Fencing-token capability for distributed locks** — `DistributedLockSpec(requires_fencing_token=True)` fails closed at resolve against a backend not reporting `FencingAware` / `fencing_tokens`. Default `False`; Redis and mock report support.

- **Mock document adapter — tenant scoping on every write** — the in-memory mock now injects the tenant column on ensure, upsert, update, and touch (not only create), matching Postgres, so a tenant-aware collection using idempotent or update writes isolates correctly under the mock.

- **Realtime egress — server push** — a handler publishes a `RealtimeSignal` to a principal or topic through messaging ports and the Socket.IO gateway bridges it to a tenant-scoped room. Ephemeral at-most-once or durable exactly-once; read-only operations cannot publish.

- **Realtime offline store-and-forward** — a durable principal-addressed signal also reaches a recipient offline at emit time: the gateway mailboxes it atomically with the dedup, and on reconnect each device replays from its cursor and acks to advance it. Topic and ephemeral signals are never mailboxed.

- **Tenant-aware realtime gateway** — `TenantShardedSignalSource` runs one consume loop per assigned tenant and scopes the mailbox and rooms by a trusted tenant from the stream (not the header); `RealtimeShard` bundles a namespace-tier instance's assignment (stream, tenants, group) so the source, group-ensure step, and tenant relay can't drift. Tenant-global stays the default; an unbound tenant-aware mailbox fails closed with `realtime_mailbox_tenant_unbound`.

- **Realtime multi-node hardening** — TTL-backed presence with heartbeat re-assertion (a crashed node's rooms lapse), eviction of a connection once its credential expires, and a per-emit timeout so one stuck delivery cannot wedge the consume loop.

- **Tenant-sharded outbox relay** — pass `tenants` to the background relay step (or `realtime_tenant_relay_lifecycle_step`) and it drains each assigned tenant's partition under a bound tenant, sequentially per tick, bringing a partitioned outbox to namespace tier alongside the stream and inbox.

- **BREAKING — realtime delivery envelope** — every frame the Socket.IO gateway emits is now the uniform `{id, data}` envelope instead of the bare payload (durable carries the event id, ephemeral null). Clients must read `data` and dedup by `id`; there is no transitional dual-emit.

**Deterministic Simulation Testing (`forze_dst`)**

- **Point-at-a-real-app simulation** — `Simulation` / `SimulationConfig` with `Simulation.run(config, ...)` is the single entrypoint: one master seed reproduces a whole run (schedule, faults, latency, inputs, crashes, network partitions) over real registries and runtimes, single-process or N-node distributed, with no changes to the app under test. One config is the sole source of nondeterminism, and a violation minimizes to a reproducible counterexample.

- **Deterministic runtime + ambient seams** — `SimulationEventLoop`, `SimulationTimeSource`, `run_simulation(...)` make sleeps and backoff run in wall-milliseconds and replay byte-identically (`RealIOForbidden` on real I/O, `SimulationDeadlock` on a quiescent loop). New seams: `EntropySource` / `bind_entropy_source` and `TimeSource.monotonic()` / a free `monotonic()` (the entropy and monotonic twins of `TimeSource`), plus a composable `PortInterceptor` chain via `DepsRegistry.with_interceptors(...)` (innermost, inside tracing and resilience). A determinism quality gate fails the build on raw time/entropy use outside the seams.

- **Faults, latency, crash & partitions** — `FaultPolicy` / `FaultRule` (error/timeout/crash/drop/duplicate/delay per surface/route/op) and `LatencyProfile` (`Constant` / `Uniform` / `Exponential` plus heavy-tailed `LogNormal` / `Pareto`), declared on `SimulationConfig` and seeded by construction. `SimulationConfig.crash` drives crash-restart-recovery over persisted `MockState`; `SimulationConfig.runtime=True` runs the workload through a real `ExecutionRuntime`. `Cluster` with `Partition` / `PartitionSchedule` runs N nodes under group partitions, including lossy/asymmetric links (`Partition(loss=…)`).

- **Workload generation** — `Scenario` / `Rule` / `ModelState` with `derive_scenario` build model-based arrange-then-act workloads from the catalog; a generic fuzzer (`OpSpec`, `generate_workload`, `simulate_workload`); schedulers `PCTScheduler` (depth-bounded bug guarantees) and `SystematicScheduler` (DPOR exhaustive interleaving). Coverage-guided exploration (`behavioral_coverage`, `Simulation.coverage`) right-sizes a sweep, and `Simulation.coverage_guided(config, cases=…)` adds AFL-style feedback mutation.

- **Oracle & invariants** — a context-bound `Recorder` / `record_event` to an immutable `History`; `Invariant`s with built-ins (`no_duplicate_effect`, `mutual_exclusion`, per-key `linearizable` / `RegisterSpec`); `explore` plus greedy `minimize` to a `ViolationReport`. Reachability "sometimes" assertions (`reached(label)`, `sometimes(...)`, `SimulationConfig.reachability_targets`). Value-level invariants behind `SimulationConfig(capture_values=True)` (`read_your_writes(...)`, `expect_value(...)`; off by default, so production tracing stays id-only and PII-free).

- **Transactional-isolation oracles** — `snapshot_isolation()` / `serializable(complete=True)` (and kernel `find_snapshot_isolation_violations` / `find_serializable_violations` / `find_serializability_cycle` over `TxRecord` / `VersionedTxRecord`) detect lost update, write skew, and ≥3-transaction anti-dependency cycles including predicate/phantom edges, keyed `(namespace, id)` over the native write row. `had_isolation_conflict(history)` is a capture-free non-vacuity signal and `isolation_oracle_for(level)` maps a declared `IsolationLevel` to its oracle. The in-memory filter evaluator is now one shared core primitive — `evaluate_filter` / `compile_filter` in `forze.application.contracts.querying`.

- **Commutativity** — `OperationDescriptor.commutative` declares an operation order-independent (a declaration DST verifies but execution never consumes), and `commutative_convergence(build, *, final_state, schedule_seeds)` flags one whose interleavings reach different end states, naming the reproducing seed.

- **Trace** — `RuntimeTracer` captures the full execution surface (ports, transactions, operation boundary, domain dispatch) with virtual-time stamps and PII-free id-only keys, folded into one `History`, with exact per-call attribution via a terminal correlation id. `operation_fingerprint(op)` / `FrozenOperationRegistry.fingerprint()` tie a seed to the catalog that produced it.

- **Reporting & artifacts** — `forze_dst.report` (`CausalGraph`, `format_report`, `ViolationReport.format()`) and `timeline()` / `build_timeline` / `render_timeline` render a minimized counterexample. Regression corpus: `RegressionEntry`, `append_regression`, `load_regressions`, plus opt-in `behavioral_fingerprint` / `strict_behavior` drift detection. `FailureBundle` with `replay_bundle(bundle)` makes a bug reproduce on another machine.

- **Sweeps** — `parallel_sweep(run, seeds, workers=…)` with the picklable `SimulationSeedRunner` fans a seed sweep across processes into one `SweepResult`; `SeedOutcome.reached` and `SweepResult.reachability(targets)` fold per-label reachability. The flagship distributed-lock and hybrid-logical-clock scenarios ship as reusable seed targets (a wide nightly fuzz plus a fast per-build regression corpus).

- **Mock substrate** *(behavior change)* — `MockDepsModule(transactions="journal")` is now the default (a per-write undo journal lets transactions interleave with clean rollback; `none` / `strict` opt-in), with MVCC isolation (snapshot rejects write-write; serializable also rejects read-write and phantoms; `exc.concurrency` / `serialization_failure`). In-memory outbound HTTP via `MockHttpServicePort` / `MockHttpServiceAdapter` / `MockHttpRegistry` (`MockDepsModule(http=…)`).

- **CLI** (`forze[cli]`) — `forze dst run module:sim` (exit 1 on a violation, CI-friendly) plus `replay`, `coverage`, `topology`, and `derive`.

- **Adapter conformance — isolation anomaly battery** — `forze_dst.conformance`: a backend-agnostic battery of classic isolation anomalies (dirty/non-repeatable read, read/write skew, phantom, predicate write skew, the three-transaction read-only anomaly, lost update) as deterministic forced interleavings over the `Conductor`, with a known verdict per `IsolationLevel`, run against any `ConformanceBackend`. Verdicts normalize to permitted/prevented, and a reviewed allowed-divergence catalog (`CONTRACT_STRENGTHENINGS` / `MECHANISM_DIVERGENCES`) records expected differences.

- **Adapter conformance — real-backend differential** — the battery also runs against real Postgres and a real MongoDB replica set over testcontainers, asserting `mock ≡ real`: Postgres at every level (`SNAPSHOT`→`REPEATABLE READ`, `SERIALIZABLE`→SSI) and Mongo at `SNAPSHOT` (SI-only, write skew permitted). This is what makes "DST passed on the mock" carry weight.

### Changed

- **Search pages split from the base pagination contract** *(breaking)* — result-level facets, highlights, and the snapshot handle move off `CountlessPage` / `Page` / `CursorPage` onto a new `SearchPage` / `SearchCountlessPage` / `SearchCursorPage` family that the search ports now return. `FacetBucket`, `FacetResults`, `HitHighlights`, and `SearchSnapshotHandle` move from the base contract to the search contract — import them from there. Document and analytics pages no longer carry unused search fields.

- **`SearchFuzzySpec` is now an immutable value object** *(breaking)* — it changed from a dict to a frozen value object, exported from the search contract and constructed explicitly. The edit-distance ratio defaults to 0.34 and is validated to the 0.0–1.0 range, and the never-read prefix-length field is removed. There is no compatibility shim for the old dict form.

- **Search options de-leaked and grouped by topology** *(breaking)* — Postgres internals are gone from the backend-agnostic search options: the raw-Groonga override removed, the PGroonga plan made adapter-config only, and the advisory candidate caps renamed to `max_candidates` and `merge_candidates`. Hub and federated member keys move to a multi-source options type that single-index search rejects at type-check and that the generated hub/federated request bodies now carry (previously dropped at the DTO boundary). There are no compatibility shims.

- **Application contracts surface consolidation** *(breaking: deep imports; no runtime change)* — loose modules at the `forze.application.contracts` root are regrouped. Removed `contracts.codecs` (import codec helpers from `forze.base.serialization`); `contracts.lenient_read` / `contracts.materialized` → new `contracts.conformity`; `RowLockMode` / `row_lock_requires_transaction` → `contracts.document.value_objects` (still re-exported from `contracts.document`). System-invariant reducers `Sum` / `Count` renamed `SumOf` / `CountAll`. New `TenantSecretResolver` (`contracts.secrets`) replaces the removed `contracts.tenancy` functions `resolve_dsn_for_tenant` / `resolve_structured_for_tenant`, and `ensure_dsn_fingerprint` now takes `resolver=`. Internal `contracts.tenancy.helpers` (→ `tenant_hint` + `fingerprint`), `contracts.secrets.helpers` (→ `resolution`), and `contracts.querying.sort_resolution` (now a package) are restructured — public names re-export unchanged from their packages.

- **Search result snapshots stream their pool and expose expiry** — a snapshot is now sealed into the store one chunk at a time, so peak memory is a single chunk regardless of result size, fixing out-of-memory on wide read models. The returned snapshot handle also carries `expires_at` so a client knows how long the snapshot stays valid; semantics and replay are unchanged.

- **Postgres hub search defers its heavy projection to per-page hydration** *(no behavior change)* — a multi-leg hub search ranks, counts, and paginates over a thin candidate row and hydrates full read-model columns only for the returned page, avoiding large temp-file spills on relations backed by wide views. Single-index search over a similar view defers the same way.

- **`update_many` takes `KeyedUpdate` items, not raw tuples** *(breaking: document command port)* — the bulk update method on `DocumentCommandPort` now accepts `Sequence[KeyedUpdate[U]]` (a frozen VO with `id`, `rev`, `dto`; exported from `forze.application.contracts.document`) instead of `Sequence[tuple[UUID, int, U]]`, matching the `KeyedCreate` / `UpsertItem` shape already used by `ensure_many` / `upsert_many`. Replace `update_many([(pk, rev, dto), …])` with `update_many([KeyedUpdate(id=pk, rev=rev, dto=dto), …])`. Single-item `update(pk, rev, dto)` is unchanged.

- **Hot-path micro-optimizations (no behavior change, byte-identical output)** — faster `normalize_string` (ASCII fast-path + single `str.translate`, ~5×), keyset sort-value canonicalization (~2×), once-per-struct msgspec exclude-flag resolution, allocation-free trusted bulk decode, compile-once in-memory search/aggregate scans, and a base64-decode skip in field-decryption (new `forze.base.crypto.ENVELOPE_B64_PREFIX`).

- **Contract value types import from their contracts home, not the execution layer** *(breaking: imports)* — vestigial back-compat re-exports were removed so each type has one canonical path. Import from `forze.application.contracts.resilience`: `BreakerKey`, `CircuitBreakerStore`, `RateLimitStore`, `RateLimitKey`, `LatencyDigestStore`, `LatencyDigestKey`, `Transition` (`CircuitBreakerStore` is also removed from `forze.application.execution`). From `forze.application.contracts.execution`: `LifecycleModule`, `LifecycleStep`. From `forze.application.contracts.deps`: `RoutedDeps`, `PlainDepsMap` (and the redundant middle re-export of `Deps` / `DepsModule` — use the `forze.application.execution` kernel surface, still front-doored from `forze`, or `forze.application.contracts.deps`). From `forze.application.contracts.outbox`: `OutboxStagingContext`. In-process implementations (`InMemoryCircuitBreakerStore`, …) are unchanged.

- **`GroupRef` (query grouping) renamed to `GroupField`** *(breaking: query DSL)* — the aggregate group-by dimension in `forze.application.contracts.querying` is now `GroupField` (resolving a clash with the authz `GroupRef`, which is unchanged, and pairing it with its sibling `GroupTrunc`). Replace `GroupRef(field=…)` with `GroupField(field=…)`.

- **Search index provisioning split into a `SearchManagementPort`** *(breaking: `forze_meilisearch`)* — `ensure_index` and `delete_all` move off `SearchCommandPort` (now `upsert` / `delete` only) onto a control-plane `SearchManagementPort`, acquired via `ctx.search.management(spec)` (`SearchManagementDepKey`). Move those calls from `ctx.search.command(...)` to `ctx.search.management(...)`.

- **Search engine config as a typed value object** *(breaking: `forze_postgres`, `forze_mongo`)* — search `engine` takes a tagged-union value object instead of a flat string plus parallel kwargs, so illegal combinations are unrepresentable. Bare engine strings remain shorthands; existing reads are unchanged.

- **Shared analytics ingest target** *(breaking: `forze_postgres`, `forze_bigquery`, `forze_clickhouse`)* — warehouse analytics configs take a single shared `IngestSpec` value object instead of per-backend flat `ingest_relation` / legacy `ingest_table` fields. Postgres also drops its legacy `schema` field.

- **Shared RRF fusion settings for federated search** *(breaking: `forze_postgres`, `forze_meilisearch`)* — federated merge config uses a shared `Rrf` value object instead of flat `rrf_k` / `rrf_per_leg_limit` fields. The `federation` / `rrf` shorthands stay valid.

- **Empty filter/sort maps are no-ops on list/search requests** — a bare empty `filters` / `sorts` mapping normalizes to no filter/sort instead of raising; a structured-but-empty envelope is still rejected by the strict parser.

- **Lazy transaction acquisition, default for Postgres, Mongo, and Firestore** *(behavior change)* — a transaction scope defers connection checkout until the first operation, so it no longer holds a connection idle-in-transaction; a connect failure surfaces at the first operation. Opt out with `lazy_transaction=False`.

- **`forze_mock` internal restructure** — misplaced root modules moved under `adapters/` (re-exported from `forze_mock.adapters`) and the per-spec configurable factories to `forze_mock.execution.factories`. Top-level imports are unchanged; only deep-submodule imports need updating.

- **`forze_dst` internal restructure** *(breaking: imports)* — the harness splits into a thin `Simulation` facade over `engines/`, `oracle/`, and `artifacts/` subpackages (29 top-level modules → 15). Top-level symbols now live in submodule namespaces, and `SchedulerKind` is removed.

- **Integration logger namespaces unified to `forze_<pkg>.*`** *(behavior change: log filters)* — `forze_redis` / `forze_postgres` / `forze_http` / `forze_firestore` / `forze_temporal` previously logged under bare prefixes (`redis.*`, `postgres.*`, …). Update any log filters keyed on the old prefixes.

- **Notify kit: registration split from resolution** *(breaking: `forze_kits`)* — `NotificationRouter` is now a mutable builder (`register()` returns self, then `freeze()`); resolution (`resolve` / `resolve_or_raise`) moves to the immutable `FrozenNotificationRouter` the consumer holds. The package is reorganized into `routing` / `events` / `consumer` / `lifecycle` (public imports from `forze_kits.integrations.notify` unchanged except the new `FrozenNotificationRouter`).

### Removed

- **`msgspec` dropped as a dependency; the codec layer is Pydantic-only** *(breaking: serialization)* — `MsgspecModelCodec` and the `forze.base.serialization.msgspec` helper are removed; record models (read models, create/update commands, idempotency results, and other codec-backed contracts) must be `pydantic.BaseModel` subclasses. A non-Pydantic type passed where a codec is derived now raises `exc.configuration` instead of building a msgspec codec. The storage value objects (`UploadedObject`, `DownloadedObject`, `ObjectMetadata`, `StoredObject`) are now frozen, keyword-only `attrs` classes (previously `msgspec.Struct`) — construct them with keyword arguments. Migration: model record/payload shapes as Pydantic models. The serialization policy (Pydantic for record models, attrs for in-process framework objects and wire/value objects) is documented in [Mapping & codecs](reference/mapping.md).

### Fixed

- **Batch field decryption no longer stalls the event loop** — a document read that decrypts a large encrypted result set ran N×M synchronous AEAD opens back-to-back on the event loop (≈24 ms for 1024 rows × 4 encrypted fields), blocking every other task for the duration. The document read gateway now offloads a batch at or above 64 rows to `run_cpu_map` (a bounded worker pool) against a thread-safe decrypt *snapshot*: the keyring and the deterministic cipher resolve their per-batch keys on the loop into thread-local dicts (`EncryptingModelCodec.freeze_for_decrypt`), so the offloaded workers do pure AEAD with no shared, LRU-mutating cache access — the naive offload would have raced those caches. Small batches stay inline (a worker hand-off costs more than the crypto); output is byte-for-byte unchanged. A `tests/perf` benchmark measures the stall to tune the threshold.

- **`require_tenant_id` raises `authentication`, not `internal`** — a missing bound tenant is a caller-caused condition (the invocation carries no tenant identity), so it now egresses as a 401-class authentication failure — matching its sibling `require_tenant_if_aware` guard and the `TenantRequired` before-hook (same `tenant_required` code) — instead of a 500-class internal error.

- **Shutdown & cache-invalidation reliability** — three fixes. (1) A graceful-shutdown drain timeout no longer *abandons* still-running operations: the drain gate now tracks each in-flight operation's task and, when the drain window expires, cancels them and awaits their unwind (transaction rollback, connection release) **before** lifecycle teardown closes the clients they hold — an abandoned op can no longer run on against a closing client (a task in its shielded commit finishes that commit first, so a committed effect is never torn). (2) A document read-through cache's detached early-refresh tasks are cancelled at shutdown — via a per-runtime background-owner registry the runtime drains before teardown — instead of racing the closing cache/gateway. (3) A failed **hard-delete** cache invalidation is now surfaced at error level instead of swallowed at debug: a deleted document served from the distributed cache until its TTL is a correctness hazard, not the benign self-healing miss a failed *warm* is; it stays best-effort so a cache outage never blocks a delete.

- **Resilience hardening — breaker health, bounded state, safe retries** — three fixes to the resilience layer. (1) The circuit breaker now classifies an outcome by downstream *health* rather than retryability: a downstream throttle (`429`) or an optimistic-concurrency conflict no longer trips it open, and a timeout now counts as a failure (it previously counted as a *success*). (2) The per-`(policy, route)` state maps — breaker, rate-limit, bulkhead, budget, throttle, hedge-delay, latency-digest — are LRU-bounded (`InProcessResilienceExecutor.max_state_entries` and each store's `max_entries`, default 4096), so a high-cardinality `route` can no longer grow them without bound. (3) A port policy that blanket-applies a policy retrying an *ambiguous* failure (infrastructure / per-attempt timeout, e.g. `transient`) to **every** method is now refused at build (`resilience.blanket_write_retry`) — retrying such a failure can duplicate a non-idempotent write, so the author must name the methods safe to retry. Concurrency/throttle-only retries (e.g. `occ`) stay unrestricted.

- **Idempotency: safer TTL default and no false failure on a record-write blip** — the dedup window defaulted to 30s, far shorter than an at-least-once queue-redelivery horizon, so the guarantee silently lapsed for async workloads; it now defaults to 24h and the spec documents that the TTL must be at least the operation's max retry/redelivery horizon. Separately, a store failure recording the result *after* the business effect already committed no longer turns the successful operation into a failure — it is logged and the result returned (the pending claim then lapses at its TTL). The contract now states the guarantee plainly: at-least-once with a dedup window, not exactly-once.

- **A deadline that tears a transaction commit is now non-retryable** — when the invocation deadline fired at or after the root transaction's commit, the operation raised a retryable `deadline_exceeded` even though the commit may have (or had already) landed, so an at-least-once caller could retry into a duplicate. The transaction scope now marks the commit point and the boundary surfaces a non-retryable `internal` error (code `commit_ambiguous`) for a deadline at or after it — including through the shielded post-commit drain — so a retry cannot double-execute; a deadline during the body still rolls back and stays retryable. Reconcile a `commit_ambiguous` rather than blindly retrying. (Shielding the commit outright isn't safe in-process — the driver commit resets context-local state that a cancellation-shield's copied context would break — so the failure is made honest instead.)

- **Opt-in guard against outbox dual-writes** — `flush()` persisted staged rows via the wired `flush_rows` with nothing checking it ran inside the business transaction; a flush with no open transaction persists events in a *separate* transaction from the business writes (the classic dual-write — state commits but events are lost, or the reverse). New `OutboxSpec(require_transaction=True)` makes flush-inside-a-transaction a checked precondition (`exc.configuration`, code `core.outbox.flush_outside_transaction`) via a transaction-depth probe the execution-boundary builder injects. Default `False`, because the stage-then-relay (`OutboxRelay`) and standalone-emit patterns deliberately flush outside a transaction — so no behavior change unless a route opts in.

- **A resilience store outage no longer takes down all protected traffic** — the circuit-breaker `admit` / `record` and rate-limiter `try_acquire` calls were unguarded, so with a *distributed* store (e.g. a Redis-backed breaker) an unreachable store surfaced as the call's own failure — the resilience layer became the outage — and a post-call `record` failure could even turn a successful call into a failure. Store errors on admission now **fail open by default** (the call proceeds), overridable per policy with `ResiliencePolicy.fail_open_on_store_error=False` (fail closed with a retryable `infrastructure` error); a `record` failure is always swallowed so it can never fail a succeeded call or mask an in-flight domain error. Both surface as `breaker_store_error` / `rate_limit_store_error` metrics. The in-memory default stores never raise, so single-process deployments are unaffected.

- **Bounded lifecycle teardown — a wedged shutdown hook can't hang process exit** — the graceful-drain window was bounded (`drain_timeout`) but the lifecycle teardown that followed it was not: a shutdown hook that never returns (a broker flush that wedges, a connection that won't drain) hung `ExecutionRuntime.shutdown` — and thus scope and process exit — indefinitely, defeating the drain design. Each shutdown hook now gets `ExecutionRuntime.shutdown_step_timeout` (default 10s); a hook that exceeds it is abandoned with a logged error and teardown continues to the next step. Extends the existing best-effort teardown posture (step errors already swallowed-and-logged) to hangs. Raise it for legitimately long flushes; a very large value restores unbounded teardown.

- **Postgres `update_matching` now honours `batch_size`** — it ran a single unbounded `UPDATE … WHERE filters RETURNING …`, ignoring `batch_size` (which Mongo already used to chunk). It now keyset-pages matched rows by primary key and updates one `batch_size` chunk per statement (history per chunk) within the one transaction, bounding the per-statement working set. Behaviour unchanged: every matching row updated exactly once, full updated set still returned.

- **Streamed offline-mailbox replay** — on reconnect the gateway loaded a device's entire retained backlog (up to the mailbox `cap`, default 1000) into memory before emitting, so a reconnect storm held N-devices × cap at once. It now streams the backlog through a new `RealtimeMailbox.replay_since` and emits page-by-page; the document-backed mailbox keyset-pages by HLC (`replay_page_size`, default 100), so peak is one small page per device. `replay_since` is optional — a mailbox with only `read_since` falls back.

- **Unbounded in-process caches now have bounded defaults** — four process-lifetime structures gain caps: the Postgres introspector's filtered-row-estimate lane (`max_filtered_estimate_entries=2048`; schema-keyed lanes stay unbounded); the Redis circuit-breaker local cache (`max_cache_entries=4096`, FIFO eviction); the document-cache background early-refresh fan-out (`max_inflight_refresh=64`, dropping new elections when saturated); and the L1 live-store registry now sweeps dead weak references on every append. All have escape hatches; behaviour unchanged below the cap.

- **Bounded memory on more read/list paths** — GCS `list_objects` streams the listing page by page (holding only the window + current page) while keeping the exact total; the realtime mailbox `trim` projects only `id` instead of hydrating each stale row's full `payload`/`event`; and the Postgres hub `execution="parallel"` multi-FK path restricts its hub-row scan to rows a leg actually matched (the union of per-leg, `per_leg_limit`-capped results) instead of the whole filtered relation. Results unchanged.

- **HTTP outbound response-size guard** — `HttpConfig(max_response_bytes=…)` caps the in-memory response body: it streams the response, refuses one whose `Content-Length` exceeds the cap before reading, and aborts a chunked/unsized response once the body crosses the cap. Default `None` keeps the unbounded behaviour.

- **Mongo ranked search late-materialization** — `$text` / browse offset search ran `$sort` / `$skip` / `$limit` over the full matched documents, pushing the whole set through Mongo's 100 MB in-memory sort. It now projects to thin `{_id, sort-key, rank}` before the sort and hydrates only the page's documents by `_id` (mirroring the Postgres hub fix); the count pipeline drops the rank `$addFields`/`$sort` before `$count`. Atlas `$search` / `$vectorSearch` and the cursor path are unchanged; order/content identical.

- **Analytics `run_chunked` now actually streams** — `run_chunked` / `select_run_chunked` / `project_run_chunked` on the DuckDB, ClickHouse, and BigQuery adapters buffered the whole result set (raw rows and typed models) before the first yield. They now consume a new client `run_query_streamed` generator through a shared `stream_shaped_chunks` re-chunker that decrypts+decodes one `fetch_batch_size` window at a time, so peak is a single chunk. The buffered `run_query_all_pages` stays for whole-result retry; the streaming path trades mid-stream retry for bounded memory (BigQuery still retries per page).

- **Client-caused errors no longer masquerade as 500s** — across the query / cursor / aggregate / storage / search paths, the `exc.internal` raises a caller can trigger are reclassified: an unsupported backend feature or capability limit now raises `precondition` (400), and a malformed or out-of-range value (cursor `after`+`before` together, a non-positive `limit` / `offset`, an invalid cursor token, an unknown projection field) now raises `validation` (422), instead of an opaque `internal` (500) deep in an adapter. The same situation maps to the same kind across mock ≡ Postgres ≡ Mongo ≡ Firestore, the aggregate DSL now reports parse errors like the filter DSL, and newly client-visible messages were scrubbed of internal detail. Genuine server-fault guards stay `internal` (500).

- **Bad query fields are a client error (400), not a server error (500)** — a sort / filter / direction value naming a field absent from the read model now raises `precondition` (`field_not_on_read_model` / `invalid_sort_value`) instead of a `configuration` error masked as a 500, uniform across Postgres, Mongo, Firestore, and the mock (covers computed fields). A spec's own `default_sort` naming an unknown field stays a `configuration` error (500) — that's author misconfiguration.

- **Mock rev-conflict now matches the real adapters** — a stale-revision write on the in-memory mock raised a generic `CONCURRENCY` error; it now raises the identical `exc.precondition("Revision mismatch", code="revision_mismatch")` every real adapter raises, so optimistic-concurrency handling (catch/retry on `revision_mismatch`) behaves the same in tests/DST as in production.

- **Faithful read-committed in the mock (no dirty reads)** — the mock's default transaction manager buffers document writes and publishes them at commit, so a concurrent transaction never observes an uncommitted (later rolled-back) write. Read-committed reads through to the latest committed state per statement (non-repeatable reads / read skew still permitted); snapshot/serializable keep their as-of-begin snapshot. Removes a class of DST false positives; production adapters are unchanged.

- **Notifications can run through the queue consumer** — `notification_consumer_lifecycle_step(...)` and `notification_queue_consumer_handler(...)` route notifications through `QueueConsumer`, so an at-least-once redelivery no longer re-sends (inbox dedup on the deterministic event id) and poison messages are parked.

- **Queue consumer warns when a poison ceiling can't be enforced** — when `max_deliveries` is set but the backend does not report a delivery count, the consumer logs one warning per run pointing at the broker's dead-letter/redrive policy, instead of silently looping a poison message forever.

- **CQRS read-only guard now covers eager (factory-time) port acquisition** — a QUERY handler factory that acquired a command (write) port at build time (`lambda ctx: Handler(port=ctx.document.command(spec))`, the common kit pattern) is now built under the read-only flag, so eager acquisition hits the same guard as a call-time one (raised at first resolve). Read-port acquisition and COMMAND operations are unaffected.

- **Filtering a randomized-encrypted field now fails closed** — a query predicate on a field encrypted with randomized (non-searchable) encryption silently matched nothing; it now raises `precondition` (`core.crypto.encrypted_field_not_filterable`) at the shared codec seam, so every document/search backend inherits it. Query by equality using a deterministic searchable field instead.

- **Encrypted-sort rejection now covers every search backend** — refusing a sort on a field-encrypted column (no usable order at rest, and the raw value leaks into the keyset cursor token) was wired only on Mongo; the guard now runs once in the shared offset executor (Postgres, Mongo, Meilisearch) plus the Postgres cursor path, raising `core.search.encrypted_sort_field`.

- **VK login no longer copies the untrusted introspection envelope into claims** — the `public_info` verifier now keeps only the masked `user` object the subject is derived from, so attacker-influenceable envelope fields cannot reach downstream claim/tenant mappers.

- **A missing dependency now reports as a legible configuration error** — looking up an unregistered port (a forgotten `DepsModule` entry) now raises `configuration` and names what *is* registered (with a "did you forget a DepsModule entry?" hint) instead of an opaque `internal`. It stays a server-side 500, but the log message is now actionable.

- **Log scrubbing no longer corrupts ordinary text** — sensitive-word scrubbing of log string values now requires a secret-bearing shape (`session=…`), not a bare word, so paths like `/v1/authn/login` survive intact while the value after a sensitive key is fully masked.

- **Outbox relay tenancy** — the background relay now binds each claim's tenant before publishing, so a tenant-aware destination routes per-tenant instead of the global key; a tenant-aware outbox on the plain (non-sharded) relay fails closed with `outbox_relay_tenant_unbound`.

- **Keyring fill-lock stripe is now cross-process stable** — the per-`key_id` crypto fill-lock stripe used Python's `hash()` (PYTHONHASHSEED-randomized), breaking deterministic-simulation replay; it now uses a stable hash, and a guard bans the `hash(x) % n` pattern.

- **FastAPI API-key `prefix:key` parsing fixed** — the `X-API-Key` resolver now splits on the first colon so `prefix:secret` yields the bare secret (matching `forze_mcp`); previously it split on whitespace and passed the whole value. Bare keys still authenticate.

- **Database error classification keys on error codes, not message text** — mappers no longer match English substrings in driver messages (which break under non-English locales): Postgres keys on SQLSTATE, ClickHouse on the numeric code, Mongo on the operation-failure code, and Redis on the leading RESP error token.

- **Mongo query renderer rejects `$`-prefixed field names** — a field path segment beginning with `$` (e.g. `$where`) is rejected with `exc.precondition` instead of being emitted as an operator, closing an injection path when untrusted field names reach a filter. Stored fields never start with `$`.

- **Mongo index introspection no longer crashes on special indexes** — `listIndexes` direction is kept verbatim instead of cast to int, so text, 2dsphere, 2d, hashed, and vector indexes (string directions) no longer raise during validation.

- **Per-tenant routed clients no longer crash on multi-host DSNs** — `connection_string_fingerprint` now fingerprints the full host list from the raw authority instead of a single parsed host, which raised `ValueError` on the comma-separated form used by Mongo replica sets, Redis Sentinel, and AMQP clusters.

- **Postgres schema validation accepts parameterized column types** — a field over `NUMERIC(10,2)` or `TIMESTAMP(3) WITH TIME ZONE` is no longer rejected: type compatibility compares modifier-insensitively while still carrying the modifier.

- **`forze_postgres` search index-definition parsing hardened** — index expressions parse via a balanced-delimiter, quote- and dollar-quote-aware scanner; PGroonga resolution accepts more array and cast forms but fails closed on ones it cannot reproduce, and GIN-to-FTS detection keys on a real `to_tsvector(...)` call.

- **BigQuery array and null query parameters are typed from field annotations** — an empty list parameter emits a typed `ARRAY` instead of an invalid one, a `None` for an optional field carries its real type rather than always string, and the array element type prefers the annotation.

- **Meilisearch search terms strip embedded quotes** — an embedded `"` is removed so it can no longer break phrase boundaries or split the query.

- **Numeric timezone offsets validated** — offsets require two-digit hours (so `+123` no longer parses as `1:23`) and reject values beyond the real ±14:00 maximum.

- **`forze dst --seeds` parsing fails loud** — a reversed range, leading dash, or non-numeric input raises a parameter error, and ranges inside comma lists are accepted, instead of silently running zero seeds.

- **S3 multipart part-ETags normalized** — a whitespace-padded ETag is collapsed to a single quote pair instead of being double-wrapped.

- **S3 range downloads handle an unknown total** — a `Content-Range` whose total is unknown (`*`, seen on S3-compatible gateways) synthesizes the total from the satisfied range instead of returning zero.

- **`If-None-Match` parsed per RFC 7232** — quoted entity-tags are extracted with quote-aware, list-anchored matching, so an opaque tag containing a comma no longer shreds the list, `*` matches, and a malformed weak tag is not treated as weak.

- **`forze_http` suppresses its default bearer when an Authorization header is already set** — under any header casing, avoiding duplicate, conflicting credentials.

- **GCS rejects reserved object-metadata keys** — keys in the `forze-tag-` namespace are rejected at write time, since they would otherwise be misread as tags on read-back.

## [0.4.1] - 2026-06-17

### Added

- **Mergeable quantile sketch (`DDSketch`)** — `forze.base.primitives` adds `DDSketch`/`WindowedDDSketch`: a relative-error sketch answering any quantile and mergeable across streams (fleet-wide, multi-quantile latency). Complements `P2Quantile`.

- **Hybrid Logical Clock (`HybridLogicalClock`)** — `forze.base.primitives` adds `HybridLogicalClock`/`HlcTimestamp`: a skew-tolerant causal clock (reading the ambient `TimeSource`) with an optional drift guard.

- **Causal outbox ordering** — opt-in `hlc_ordering=True` on the Postgres and Mongo outbox configs stamps events with a hybrid logical clock and claims them in causal order across replicas (drift-guarded). Off by default; Postgres needs a new `hlc` column, and legacy rows fall back to `created_at`.

- **Fleet-wide adaptive-bulkhead congestion signal** — the AIMD latency-quantile signal flows through a pluggable `LatencyDigestStore` (default in-process windowed-P², behavior-preserving), and `forze_redis` adds a Redis store so the limit reacts to the fleet's p95. Opt-in.

- **Prioritized load shedding** — opt-in `prioritized=True` on the bulkhead strategies makes the wait queue criticality-aware via a new task-scoped `Criticality` and `bind_criticality`. No-op until enabled; requires a non-zero max queue.

- **Delay-based bulkhead (`GradientBulkheadStrategy`)** — a third bulkhead kind that tunes concurrency from the latency gradient with no latency threshold. Mutually exclusive with the other bulkhead kinds.

### Changed

- **Quantile estimators relocated** — `P2Quantile`/`WindowedP2Quantile` moved from the resilience module to `forze.base.primitives` (co-located with `DDSketch`, now public exports). The old module path is removed; internal resilience wiring is unaffected.

### Fixed

- **Typing annotations** — type-only imports moved under `TYPE_CHECKING` with forward references (including the runtime-optional OpenTelemetry types), so affected modules import cleanly without those optional dependencies installed and skip needless runtime imports.

## [0.4.0] - 2026-06-17

### Added

- **Envelope-encryption core** — `forze.base.crypto` adds `EncryptedEnvelope`, a `KeyManagementPort` BYOK seam (the KEK stays backend), a `FieldEncryption` policy, and a fail-closed `required_encryption` floor (none < field < envelope). Adds `cryptography` to core dependencies. Opt-in, off by default.

- **Per-tenant keyring and wiring** — `KeyDirectoryPort` resolves tenant to KEK, `CryptoDepsModule` composes the stack, and `forze_mock` ships a dev-only `MockKeyManagement`.

- **At-rest sealing across persistence and transport planes** — each plane takes a `…Spec(encryption=…)` or `encrypt=` policy, fail-closed at wiring and tolerant of legacy plaintext.

- **Object-storage encryption** — `S3StorageConfig`/`GCSStorageConfig` `encrypt=True`; presigned URLs are refused on encrypting routes.

- **Document-field encryption** — `DocumentSpec(encryption=FieldEncryption(...))`; `binds_record_id=True` binds the record id into AAD (bulk-update of a bound field is refused), and `reencrypt_documents` upgrades legacy ciphertext.

- **Searchable deterministic-field encryption** — `FieldEncryption(searchable={…})` (AES-SIV, no KMS) so equality and membership filters rewrite to ciphertext; root rotation is supported via a previous-root match plus re-encrypt. Trade-off: leaks equality and frequency within a tenant.

- **Encrypted search reads** — `SearchSpec.encryption` (the same policy object as the document spec) decrypts out of results across every read path.

- **Analytics and graph encryption** — `AnalyticsSpec`/`GraphNodeSpec`/`GraphEdgeSpec` `encryption`; sealed on write and decrypted out of every read and traversal. Encrypted columns are not analyzable or matchable; analytics rejects record-id binding and graph binds the kind's key field.

- **Outbox and direct-messaging encryption** — `OutboxSpec.encryption` (none/at_rest/end_to_end) and queue, stream, and pubsub spec `encryption` (none/end_to_end); AAD binds tenant and event id. `QueueCommandPort.enqueue_many` gains `message_headers`.

- **Durable-payload encryption** — Temporal (`encrypt_payloads=True`) and Inngest (`encrypt=True`), per-tenant BYOK. A Temporal worker must be built from the same encrypting client to decode.

- **Cache, search-snapshot, and idempotency encryption** — sealed via `IdempotencySpec(encrypt_result=True)` and similar when the underlying route encrypts. The in-process L1 stays plaintext in memory.

- **Vault Transit KMS (`forze_vault`)** — `VaultTransitKeyManagement` implements `KeyManagementPort` on Transit, and `VaultTransitTenantProvisioner` idempotently creates a tenant's Transit key.

- **BYOK access-token signing and JWKS** — pluggable `SignerPort` (`Hs256Signer` default, plus local-asymmetric and Vault Transit signers); `attach_jwks_route` publishes JWKS. Breaking: `AccessTokenService` now takes a `signer=`, `issue_token`/`verify_token` are awaitable, and `AccessTokenConfig.algorithm` is removed.

- **Crypto and signing observability** — `instrument_crypto(...)` and `instrument_signing(...)`, always-on.

- **Declared-minimum tenant isolation, fail-closed at wiring** — every deps module accepts `required_tenant_isolation` over none < tagged < namespace < dedicated, enforced per route, and each integration declares its supported ceiling. Additive, with the `None` default unchanged.

- **Neo4j reaches namespace and dedicated isolation** — `Neo4jGraphConfig.database` accepts a per-tenant resolver and a new `RoutedNeo4jClient` resolves per-tenant Bolt URI and credentials (failing closed on partial auth), wired via a routed lifecycle step.

- **Tenant infrastructure provisioning** — idempotent `provision`/`deprovision` via `TenantProvisionerPort` on `TenancyDepsModule`, with reference object-storage and Postgres-schema provisioners. Opt-in.

- **Analytics per-tenant namespace routing and advisory binding** — query operations route into the tenant's namespace, and `tenant_aware` routes bind the tenant id and fail closed if unbound. Off by default.

- **Tenant-safe structured graph walk and raw gating** — `GraphQueryPort.scoped_walk(...)` runs an adapter-owned full-path tenant-scoped traversal, and the raw hatch is disabled by default. Breaking: deployments using the raw graph query must set `allow_raw_query=True`.

- **Fluent query builder `Q`** — `Q.field("age").gt(18) & Q.field("name").like("a%")` lowers to the same filter AST. New exports `Q`, `QueryCondition`, `FieldRef`. Additive.

- **Hierarchy operators** — `$descendant_of`/`$ancestor_of` on a `TreePath` field, using Postgres `ltree` or a text-prefix fallback, gated by a capability flag. New exports `TreePath`, `HierarchyOp`, `HierarchyValue`.

- **Aggregation operators** — `$count_distinct`, `$stddev`, `$var`, `$percentile`, and post-group `$having` on Postgres and Mongo (`$first`/`$last` deferred).

- **Full and array-of-arrays nested quantifiers** on every document backend; the previous capability gate is dropped. `validate_query_field_types` now runs in the gateway and the mock, rejecting mismatches.

- **Mixed-direction keyset pagination with per-key null ordering** — coherent null ordering across backends; old cursor tokens stay valid, and Mongo opts in via `computed_null_ordering`.

- **Query discovery metadata** — `build_query_discovery` projects a read model's filterable, sortable, and aggregatable surface as an OpenAPI extension plus an MCP line.

- **Tenant selector self-service** — `GET /tenants`, an activate endpoint that re-mints a tenant-scoped token pair, and a leave endpoint, via the new `attach_tenancy_routes`.

- **Tenant admin** (`forze_kits.aggregates.tenancy_admin`) — create, list-members, invite, remove, and deactivate via `attach_tenancy_admin_routes`. Ships unguarded, so bind authn and authz per op. Breaking for `TenantManagementPort` implementers: adds two listing methods.

- **Self-service API-key management** — issue, list, and revoke as `POST`/`GET`/`DELETE /api-keys`, with the secret returned once. Breaking for `ApiKeyLifecyclePort`; the account table gains hint and label columns.

- **Delegation-aware API keys (user to agent)** — `issue_api_key(actor_principal_id=…)` binds a delegation actor (an RFC 8693 `act` claim). Breaking for `ApiKeyLifecyclePort`; the account table gains an actor-principal column.

- **MCP boundary API-key auth** — `ForzeApiKeyVerifier` plus `AccessTokenIdentityResolver` protect a FastMCP server with the forze_identity brain (no OAuth flow), reads-only by default.

- **OpenAPI security from configured authn** — `apply_openapi_security` derives security schemes from the authn requirement, and principal-requiring ops are flagged.

- **Authn plane** — `AuthnOrchestrator` with a full mock identity plane and `attach_authn_routes` (login, refresh, logout, change-password, deactivate, reset) plus self-service password reset. `deactivate_principal` ships unguarded.

- **In-process L1 document cache** — `CacheSpec(l1=L1Spec(…))` ahead of the distributed cache: tenant-scoped, pluggable eviction, with Redis invalidation push and `CachePort.exists`. Off by default.

- **Stampede protection and adaptive freshness** — singleflight on read-through misses, probabilistic early refresh, per-entry age and sliding TTLs, and a keyword `ttl=` on every setter.

- **New resilience strategies** — adaptive bulkhead (AIMD concurrency), adaptive throttle, tail-based hedging, and a token-bucket rate limit; configurable via `ResilienceDepsModule(port_policies=[…])`.

- **Invocation deadlines** — per-operation budgets via `with_deadline(…)`; expiry raises `exc.timeout` (504).

- **Distributed rate limits** — a pluggable `RateLimitStore` (`RedisRateLimitStore`, fails open) lets N replicas share one rate; bulkheads and budgets stay process-local.

- **App assembly and deployment** — `build_runtime` plus `runtime_lifespan`, graceful drain (default 10s), and `DeploymentProfile.FLEET`/`SERVERLESS` (the latter rejects long-running ops).

- **Envelope headers and correlation propagation** — messages gain headers and a delivery count, the relay forwards the full envelope, and `process_with_inbox` rebinds correlation and causation across broker hops.

- **Outbox ordering key** — per-aggregate ordering (SQS FIFO message group, stream partition key). Requires a new `ordering_key` column.

- **Kits queue-consumer runner** — `run_consumer` plus a background lifecycle step: inbox exactly-once, requeue, poison parking, and envelope rebinding.

- **Stream pending-entry recovery** — `StreamGroupQueryPort.claim` (XAUTOCLAIM) and `pending` (XPENDING). Breaking for port implementers.

- **Presigned object-storage URLs** — `presign_download` and `presign_upload` (S3 SigV4, GCS V4, mock). Breaking for port implementers, since minting an upload URL is a CQRS write.

- **Object-storage metadata and access ops** — `head`, ranged download (206), conditional download (304), copy/move, and object tags; generated routes honour `Range` and `If-None-Match`. Breaking for the storage and client ports.

- **Resumable multipart uploads** — `StorageUploadSessionPort` (begin, presign-part, complete, abort), CQRS-write-guarded. Refused on object-encrypting routes.

- **Storage HTTP edge** — kit ops and generated FastAPI routes for presigned download and upload and the full multipart session. Minting an upload URL is a command op, so bind authn and authz.

- **Server-side encryption at rest (SSE/CMEK)** — `S3StorageConfig.sse` and `GCSStorageConfig.kms_key_name`. A separate axis from client-side `encrypt` (it does not satisfy a client-side encryption floor). Off by default.

- **Catalog and registry ergonomics** — `OperationCatalogEntry` gains idempotency-key and required-permissions facts, duplicate merge keys raise (with an override hatch), and `registry.register(…)` is one step.

- **Generated-route mount ergonomics** — every `attach_*_routes` helper gains `resource=` (mutually exclusive with `ns=`) and `path_overrides=`. Additive.

- **Scoped, materialized patch authoring** — `registry.patch(selector, namespace=ns)` matches only ops under a namespace, and `materialize_patches` folds patches into per-op plans. Merge now raises when a patch from one registry matches another's ops (breaking only there; pass `cross_registry=True`).

### Changed

- **Queue consumer and outbox relay are now configurable classes** — `run_consumer(...)` becomes `QueueConsumer(...).run(...)` and the relay helpers become `OutboxRelay(...)` methods. Lifecycle steps keep flat params. Breaking for direct callers of the old functions.

- **Tenant-isolation tier model made coherent** — the ladder is none < tagged < namespace < dedicated (the `relation` rung removed), each integration owns its supported ceiling, and namespace resolution is unified. Key and path formats are unchanged.

- **Argon2 hashing off the event loop** — `hash_password`, `verify_password`, and the timing dummy are now async on a bounded pool (default concurrency 4); the `*_sync` variants remain.

- **Performance (measured)** — engine hot path roughly halved (hookless op 2.5 to 1.2 µs, query −56%, memoized resolve); `Document.update()` copies only changed subtrees; Postgres and Mongo write paths cut round-trips (Mongo outbox claim −90%); lazy error-context and opt-in tracing cut overhead.

- **FastAPI `style="rpc"` uses REST verbs and query params** — e.g. `GET /notes.get?id=`, `PATCH /notes.update?id=&rev=`. Breaking: RPC clients must switch from `POST /<op>`; REST and MCP are unchanged.

- **`singleton_lifecycle_step` takes a `DistributedLockSpec`, not a live port** — breaking: pass `spec=DistributedLockSpec(name=...)`.

- **Release-coherence sweep** — the relay logs the at-least-once to fire-and-forget downgrade, Temporal query/update/result deserialize into declared types, the API-key prefix is validated, and saga `step_failed` stays a domain error.

### Fixed

- **Tenant-isolation correctness and parity** — Postgres outbox and inbox enforce the declared isolation floor, a missing bound tenant fails closed consistently as an authentication error, and mock durable, graph, and document adapters now tenant-partition their stores.

- **Post-commit work survives task cancellation** — the after-commit drain runs as a cancellation-protected critical section and then re-raises; cancellation during the body still rolls back.

- **PGroonga search honors tenant isolation regardless of plan** — a tenant-aware search now always uses `filter_first`, overriding index-first plans that scanned cross-tenant rows and could truncate results.

## [0.3.0] - 2026-06-11

### Added

- **Generated FastAPI routes** (`attach_document_routes`, `attach_search_routes`, `attach_storage_routes`) — project a frozen registry's operations onto a user's `APIRouter` with a required `style` (rest or rpc), dispatching through `run_operation`. Idempotency is now engine-level.

- **`forze_mcp`** (`forze[mcp]`) — expose operations as MCP tools (read-only MVP): `register_tools(...)` adds a frozen registry's operations as FastMCP tools, read-only by default (commands need `include_writes=True`).

- **`forze_duckdb`** (`forze[duckdb]`) — in-process DuckDB analytics over object storage (query-only): `AnalyticsQueryPort` over a Parquet/CSV/Iceberg/Delta lake on S3, GCS, or local, with no standing warehouse. Wire with `DuckDbDepsModule`.

- **Delegated identity (on-behalf-of, RFC 8693)** — `AuthnIdentity.actor` carries the acting principal, and `AuthzBeforeAuthorize` enforces a least-privilege intersection. Explicit authority via `DelegationPort.may_act`.

- **Operation-level CQRS** (`OperationKind` QUERY/COMMAND) — `as_query()` runs read-only: command ports are unacquirable and the tx opens `READ ONLY` (DB-enforced). Untagged defaults to COMMAND.

- **Operation catalog descriptors** — `OperationDescriptor` plus `FrozenOperationRegistry.catalog()`: interface-agnostic request/response-schema metadata for projecting operations onto MCP or HTTP, joined with operation kind.

- **Queryable-field policy** (`QueryFieldPolicy` on `DocumentSpec`) — per-aggregate filterable, sortable, and aggregatable allow-sets, powering MCP schema discovery and boundary enforcement. Direct port calls are unrestricted.

- **OpenTelemetry traces and metrics** (`instrument_operations`) — wraps every operation in an OTel span plus an operations counter and duration histogram. Opt-in, additive.

- **`@invariant` — declarative domain invariants** — an always-true rule enforced on both create and update, closing the merge-patch bypass of `@model_validator`s.

- **Saga / process orchestration** — `SagaDefinition` plus an in-process executor for declarative multi-step processes across aggregates, with typed steps and reverse compensation before the pivot. `run_saga(...)` must run outside an enclosing transaction.

- **DDD domain events and aggregate roots to outbox** — `DomainEvent`/`AggregateRoot` buffer events; persisting an aggregate drains and dispatches them in the operation's transaction. Wired via `DomainEventsDepsModule`.

- **End-to-end worked example** (`examples/recipes/order_fulfillment/`) — the first runnable, test-backed example: checkout saga to outbox to relay to inbox to downstream, plus compensation, on `forze_mock`.

- **Deterministic time and ids** (`TimeSource` seam) — `utcnow()`/`uuid7()` read a context-active source, and `bind_time_source(FrozenTimeSource(...))` makes every read deterministic with no call-site changes.

- **Resilience policy pipeline** — composable strategies into a validated `ResiliencePolicy`, run via `ctx.resilience().run(...)` or `ResilienceWrap`. Adds hedging and a distributed breaker (`RedisCircuitBreakerStore`, fails open).

- **Inbox / consumer-side dedup** — `InboxPort.mark_if_unseen`; `process_with_inbox` marks and runs the handler in one transaction for an exactly-once effect. Adds a Postgres store plus mock.

- **Graph contracts plus `forze_neo4j`** (`forze[neo4j]`) — graph ports via `ctx.graph.query`/`.command`/`.raw`; a Neo4j async Bolt adapter (CRUD, neighbors, expand, shortest path, raw Cypher hatch) and an in-memory mock.

- **`forze_kits` — consolidated kit package** — kits, aggregates, mapping, DTOs, outbox/notify, secrets, and scopes. Absorbs former `forze_patterns`, several `forze.application.*` modules, and `forze_secrets`.

- **`forze_http`** (`forze[http]`) — outbound HTTP: `HttpServiceSpec`/`HttpServicePort`, `HttpClient`/`RoutedHttpClient`, and `HttpDepsModule`; `ctx.http` resolves services by name. httpx-backed.

- **`forze_meilisearch`** (`forze[meilisearch]`) — async Meilisearch: offset `SearchQueryPort`, `SearchCommandPort`, and federated search (native or weighted RRF).

- **Transactional outbox, notify, and search-command** — `forze.application.contracts.outbox` (`OutboxSpec`, `IntegrationEvent`) with Postgres, Mongo, and mock stores plus relay helpers; a notify kit; and a core `SearchCommandPort` for external-index maintenance.

- **Tenant routing** — declarative per-request backend targets across all integrations, with per-tenant `Routed*Client` variants, routed lifecycle steps, LRU pool dedup, and `TenantClientRegistry`.

- **Identity — IdP presets** (`forze_identity.builtin.idp`) — OIDC presets for Google, VK ID, and Telegram Login; `oidc_bootstrap_identity_deps`; PKCE helpers. Authn adds API-key rotation and single-use password invites.

- **Execution — freeze/resolve pipeline** — an authoring `DepsRegistry` (freeze to a frozen registry, resolve to `FrozenDeps`) separates registration from per-scope resolution, with a matching `LifecyclePlan`. Per-scope caches default on.

- **Codecs** — `default_model_codec`, `DocumentCodecs`/`document_codecs_for_spec`/`DocumentSpec.resolved_codecs`, optional read and ingest codecs, and trusted-row read validation.

- **Postgres / Mongo search** — Postgres strict/trusted read validation, PGroonga plan modes, hub parallel legs plus `SearchOptions`; Mongo `MongoDepsModule.searches` (text, Atlas, vector; offset plus cursor).

- **Document adapters** — `max_scan_pages`/`max_stream_pages`/`max_chunked_command_pages` (default 100 000, `None` unlimited) with cursor-stall detection.

- **Durable workflow** — `DurableWorkflowRunStatus`/`Description` plus `describe()` on `DurableWorkflowQueryPort` (`forze_temporal`).

- **`forze_temporal` secure connections** — `TemporalConfig` TLS, API key, RPC metadata, and data-converter override; defaults unchanged.

- **AWS — long-lived clients and credential chain (SQS/S3)** — one aiobotocore client is reused, and access key, secret, and region are optional (defaulting to the standard credential and region chain). Per-tenant routed creds still require explicit keys and region.

- **Vault — token renewal, metadata existence, health** — an opt-in self-renew loop, `kv_exists` via KV v2 metadata, and a standard `health()`.

- **`forze_fastapi` upload cap and attach-time validation** — chunked upload streaming under `max_upload_size` (default 64 MiB, `None` disables) with early Content-Length rejection.

- **`forze_socketio` error translation and identity** — handler exceptions become structured ack payloads honoring egress redaction, with an optional connect-time identity resolver.

- **Distributed-lock fencing tokens** (breaking for port implementers) — `DistributedLockCommandPort.acquire` returns an `AcquiredLock | None` carrying a monotonic fencing token. Backends without tokens return `token=None`.

- **Object-storage tags end-to-end** — `UploadObjectRequestDTO.tags` and an `include_tags` flag on head and list (`True` makes S3 pay for `GetObjectTagging`).

- **`IdempotencyPort.fail()`** (breaking for port implementers) — releases a pending claim on handler failure so legitimate retries are not rejected (Redis plus mock).

- **`AuthnFacade.deactivate_principal`** — the existing tested handler is now registered into `build_authn_registry`, exposed, and exported.

- **`forze_mock` parity** — strict transactions, queue and idempotency parity, consumer groups with real ack, keyset cursor pagination, and tenancy, dlock, search, durable, and identity adapters.

- **`forze.base` primitives** — `CacheLane`, `SimpleLruRegistry`/`GuardedLruRegistry`, `InflightLane`, `OnceCell`, `frozen_mapping`, and fingerprint helpers.

### Changed

- **Breaking: document write identity is an explicit argument** — `CreateDocumentCmd` no longer carries `id`/`created_at`; the write surface becomes `create(payload, *, id=None)`, `ensure(id, payload)`, and `upsert(...)` with `KeyedCreate`/`UpsertItem` value objects.

- **Breaking: storage CQRS split** — `StoragePort`/`StorageDepKey` split into query (`download`, `list`) and command (`upload`, `delete`) ports, resolved via `ctx.storage.query(spec)` / `.command(spec)`. S3 and GCS factories are renamed.

- **Breaking: coordinators to adapters** — `DocumentCoordinator` to `DocumentAdapter`, the cache and outbox coordinators likewise, and `DistributedLockCoordinator` to `DistributedLockScope`; `forze.application.coordinators` is removed.

- **Breaking: codecs unified on `ModelCodec`** — document, search, and analytics paths materialize through spec-owned codecs, and document kernel gateways require explicit codecs (build via `read_gw`/`doc_write_gw`).

- **Breaking: frozen `attrs` integration configs** — all integration wiring configs are frozen `attrs` (no dict or `TypedDict`); module-level validators are removed (validation at construction or `.validate()`), and some timeout fields move to `timedelta`.

- **Breaking: `ensure_bucket` is create-if-missing on both backends (S3)** — both now create idempotently and race-safe (was a not-found). Use `bucket_exists()` for existence assertions.

- **Breaking: `nack(requeue=...)` semantics aligned (SQS)** — `requeue=False` no longer deletes the message but leaves it for the redrive policy, and `requeue=True` means immediate redelivery. Apps relying on nack-to-drop must `ack`.

- **Breaking: `workflow_id_template` to `workflow_id_base`** — the schedule field is passed verbatim (Temporal appends the fire timestamp); renamed across contract, adapter, and mock with no alias.

- **Idempotency reshaped to engine-level result idempotency** — `IdempotencySnapshot` is replaced by an interface-agnostic `IdempotencyRecord(result: bytes)`, and a new `IdempotencyWrap` hook returns the stored typed result early. FastAPI middleware reads `Idempotency-Key`.

- **OCC retry routed through the resilience pipeline** — Postgres, Mongo, and Firestore write gateways drop their own retry library for the shared `occ_retry` policy. Attempt counts are unchanged.

- **Write gateways — unified OCC/history validation** — Postgres and Mongo share one history-OCC mixin, and a missing history snapshot now raises a retryable precondition error on both.

- **Async contract protocols standardized on `Awaitable[X]` returns** — remaining `async def` Protocol ports converted (type-only; call sites unaffected). Async-generator methods are unchanged.

- **Transaction nesting contract** — nested scopes are savepoints, isolation and read-only are honored only at root, and a conflicting nested read-only raises. `TransactionHandle.id` is removed and it gained `read_only`.

- **Unbounded-read protection unified on the implicit cap** — Mongo and Firestore gain an implicit find limit (default 10 000, `None` disables), and the hard "filters or limit required" precondition is dropped.

- **Analytics SQL pagination wraps in a subquery** — `apply_limit_offset` wraps Postgres and ClickHouse too, and a negative limit or offset now raises.

- **`forze_mock` adapters are stricter (potentially breaking for tests)** — the password verifier actually compares, authz and scope deny by default, and a duplicate-id create raises a conflict.

- **Graph contracts (evolving, pre-1.0)** — dual-addressing `EdgeRef.by_key`/`by_endpoints`, a single-path `shortest_path` plus new `k_shortest_paths`, and a config-raising spec validator.

- **Execution-context lifecycle tripwire, import-linter, kernel consolidation** — constructing an `ExecutionContext` mid-operation warns, plane layering is now lint-enforced (14 contracts), and kernel-client boilerplate folds onto shared lifecycle helpers.

- **Internal package layout** — integration `kernel` to `kernel.client`, `execution` to `lifecycle/` plus deps sub-modules, and registry/planning/facade/run move under `forze.application.execution.operations`. Package-root imports are unchanged; direct internal-module imports must update.

- **Performance** — hookless operations skip body-stage scaffolding (~30%), per-scope caches reuse gateways/adapters/codecs, and JSON logs render via `orjson`.

- **Misc** — Postgres streaming uses a server-side named cursor; outbox uses bulk insert on conflict plus stale-processing reclaim (default 5 min) and `requeue_failed`; `forze[oidc]` now bundles `httpx`.

### Deprecated

- **`forze_identity.oidc`** — `OidcTokenVerifier.enforce_issuer_and_audience` now defaults to `True`, so construction requires both an issuer and an audience unless explicitly opted out.

### Removed

- **Dead public surface removed** — the `forze[arango]` extra, `AccessTokenService.try_decode_token`, `ISSUER_FORZE_JWT`, `EffectiveGrantsAdapter`, the GCS head and listed-object aliases, `PostgresQualifiedName.from_string`, an internal fingerprint module, and a never-honored `delete_many` batch size.

- **`python-dateutil` core dependency** — dropped; `datetime_to_uuid7` parses ISO-8601 via the stdlib `datetime.fromisoformat` (a trailing `Z` is accepted).

- **`forze[casbin]` extra** — dropped (no integration shipped).

- **`forze_identity.local` (breaking)** — use `forze_identity.builtin.local`; local verifiers and factories are no longer exported from authn or tenancy.

- **`forze_identity.builtin.telegram`** — the Telegram Mini App `initData` HMAC preset, superseded by Telegram Login OIDC under `forze_identity.builtin.idp.telegram`.

- **Execution** — `forze.application.coordinators`; the registry, planning, facade, and running modules; `OperationRunner`; and `lifecycle_graph_from_sequence` (use `steps_graph_from_sequence`).

- **Validation helpers from public APIs** — Postgres and integration `validate_*_conf` helpers; validation now lives on the config types. Also dict/mapping coercion for the configurable Postgres document specs.

- **Codecs** — `RecordMappingCodec`, the Pydantic and Msgspec codec families, `codec_for_model`, and public helper functions (use `ModelCodec`/`default_model_codec`); plus `SearchSpec.row_codec` and the effective-row-codec accessor (use `read_codec`).

- **Relocated to `forze_kits` (breaking)** — former `forze_patterns`, `forze.application.{composition,kit,handlers,mapping,dto}`, and `forze_secrets` now live under `forze_kits`; `Mapper`/`MapperFactory` stay on `forze.application.contracts.mapping`. `OutboxDestination` is now the discriminated `OutboxDestination.queue(route=…, channel=…)`.

### Fixed

- **Package error mappers were dead code in 12 integrations** — `ChainExceptionMapper` now flattens nested chains, so Postgres serialization and deadlock errors (plus Mongo and Neo4j conflicts) map to concurrency and OCC retry fires on real serialization conflicts.

- **Firestore transactions** — aborts map to concurrency, rollback happens on `BaseException`, `count_documents` joins the ambient tx, and a mismatched database raises configuration.

- **ClickHouse `run_query_all_pages` is one streaming execution** — a consistent snapshot with no growing-offset duplicates.

- **Redis pipelines fail loud on reads** — value-returning methods inside `pipeline()` raise `redis_read_in_pipeline`.

- **RabbitMQ robustness** — `close()` nacks and requeues pending unacked, poison messages are dead-lettered, and there is one delay queue per distinct delay. The same poison handling applies on SQS.

- **Outbox relay failure model** — codec-decode poison fails immediately; publish failures reschedule with backoff until `max_attempts` (default 5). Adds durable attempt-tracking columns and `mark_retry` (breaking for port implementers), and `requeue_failed` resets the counter. At-least-once delivery.

- **Outbox staging is per-route and per-task** — fixes a process-global flag dropping events and shared buffers. Adds per-route buffer, flushed, and peek accessors.

- **`GuardedLruRegistry` use-after-dispose race** — refcount transitions and eviction reads happen under the registry lock, and a dispose error during drain deregisters and propagates.

- **After-commit callbacks run to completion** — a failing post-commit callback no longer skips the rest; failures aggregate into one error.

- **Lifecycle steps are shut down exactly once** — per-scope started-state tracking ends double-shutdown on failed startup.

- **`finally` hooks observe before-hook denials** — before hooks run inside the try/finally, while `on_failure` stays handler-only.

- **OCC history validation hardened** — records are re-keyed by id and revision, and comparisons run in canonical space so no-op resends do not falsely conflict.

- **`Document.update()` re-validates the patched state** — it merges into a python-mode dump and re-validates, so semantic no-ops yield an empty diff, partial nested dicts and ISO datetimes are no longer raw, computed-field keys are excluded, and validators run on update.

- **Concurrent graph waves report all failures** — an exception group for two or more; a single failure raises directly.

- **Per-scope port cache works for per-call specs** — value equality with an identity fast-path first.

- **`kill()`/`kill_many()` verify row counts on every path** — all paths raise not-found on missing rows.

- **SQS message identity fixed (was breaking inbox dedup)** — `QueueMessage.id` is now the broker message id, and the receipt handle moves to the SQS-specific message type.

- **Postgres transaction options no longer leak across pooled connections** — read-only and isolation are emitted as `SET TRANSACTION …` inside the root tx.

- **Mongo write conflicts retry under OCC** — write-conflict and transient-transaction errors map to concurrency.

- **`forze_fastapi` middleware errors return proper status codes** — core exceptions in forze middlewares render the standard JSON error instead of a 500.

- **RabbitMQ/SQS receive and consume defaults** — bounded receive windows and a uniform idle-timeout consume (`None` is forever, finite is a clean stop).

- **`DistributedLockScope` no longer loses the lock silently** — a lost heartbeat is raised as concurrency at scope exit without masking the body's exception.

- **Notify consumer dedup** — the event id is derived deterministically from the broker message identity (was a random UUID).

- **All integration kernel clients** — `initialize()`/`close()` serialize on an internal lock, and partial-failure assignment is hardened (BigQuery, Postgres, Redis).

- **Analytics adapters** — chunked execution rejects a non-positive fetch batch size via a shared validator.

- **Misc fixes** — Postgres on-conflict targets, PGroonga index-first cap and exact count, Mongo bulk-upsert miss detection, Meilisearch federated finalization, identity duplicate/ambiguous-login detection, and sorted query params in the connection fingerprint.

- **`forze_temporal` and `forze[mcp]` workflow sandbox** — the sandbox runner passes `beartype` and `coverage` through, fixing circular-import failures and a coverage-induced test hang.

### Security

- **Password change revokes existing sessions (breaking by default)** — `change_password` revokes all sessions (refresh families plus session-bound access JWTs); opt out explicitly, and missing session ports fail at startup.

- **Rehash-on-login (opt-in)** — the Argon2 verifier persists parameter-upgraded hashes after login, OCC- and fire-safe.

- **`sensitive=True` spec marker keeps credentials off generated surfaces** — route, tool, and resource generators refuse sensitive specs at attach time, and shipped authn specs are marked.

- **Owner-override permission keys configurable and documented** — the admin bypass moves to `AuthzKernelConfig.owner_override_permissions` (defaults unchanged, an empty set disables it).

- **`tenancy_mode="global"` warns over tenant-partitioned stores** — grants are shared across tenants; set `require_invocation_tenant` for isolation.

- **OIDC nonce value binding** — `verify_id_token_nonce` (constant-time) plus `generate_nonce()`/`generate_state()`; the VK and Telegram exchanges accept an expected nonce.

- **Secret values masked in reprs framework-wide** — credential value objects become non-repr, and several ClickHouse and Inngest fields become secret. Direct readers must call the secret accessor.

- **Outbound HTTP does not follow redirects by default** — prevents custom credential headers from following a malicious 30x to an attacker host.

- **`AuthnDepsModule` rejects a token-verifier override without a resolver override** — a principal-collision hazard; it fails at startup naming the route.

- **Tenancy adapters enforce the cache/history guard** — a cached principal-to-tenant binding could otherwise keep a detached principal resolving after revocation.

- **Cursor pagination tokens validated as client input** — malformed, stale, or tampered tokens raise 4xx (was a 500), and values are restricted to JSON scalars.

- **Log message text is scrubbed** — string scrub rules apply to the rendered message after interpolation, not just structured extras.

- **Postgres sort direction whitelisted** (asc/desc only); S3 object tags URL-encoded; `OidcClaimMapper` rejects an empty issuer or subject.

- **5xx responses no longer leak internal diagnostics** — generic detail for status 500 and above, sanitized context restricted below 500, and configuration-kind details are no longer sent to clients.

- **Authz document-scope filters fail closed** — a scope port returning row filters with no DTO attribute to carry them raises configuration (was silently dropped to an unscoped query).

- **Raw-query tenancy hardening** — the raw graph query (`forze_neo4j`) fails closed in a tenant-aware module and binds the tenant; adds `ctx.tenancy.current()`/`require_current_id()`.

- **Missing authentication surfaces as authentication (401), not authorization (403).**

- **`builtin.local` API-key verification no longer 500s on non-ASCII input** — via a UTF-8 bytes comparison.

- **`asyncio.CancelledError` passes through exception interceptors** — it was converted to a core exception, breaking timeouts, structured concurrency, and graceful shutdown.

- **`forze_identity.authn` session enforcement (breaking)** — access JWTs carry a session id cross-checked against the session store. Pre-upgrade tokens without it fail until re-login (or register a stateless verifier override).

- **`forze_identity.authn` `change_password` requires the current password (breaking)** — it re-authenticates first, so a hijacked session cannot escalate to account takeover.

- **`forze_identity.authn` principal eligibility (breaking)** — the authn and credential lifecycle is gated on an active flag, deactivation cascades, and API keys enforce expiry.

- **`forze_identity.authn` login hardening** — a generic 401 for all failures, always running an Argon2 verify (anti-enumeration and timing).

- **`forze_identity.authz` fail-closed tenant isolation** — grant-resolution adapters refuse to construct when a tenant-scoped route has a non-tenant-aware binding or catalog port.

- **`forze_identity.oidc`** — resolves JWKS signing keys in a worker thread, so a cache miss does not block the event loop.

- **Secret-field redaction** — JWT signing keys and HMAC peppers become non-repr, and several Vault, S3, GCS, and HTTP routing credentials become secret or redacted.

- **`forze_fastapi` — `X-Tenant-Id`/`X-Forwarded-Host` not trusted by default (breaking)** — a raw tenant header is ignored unless trust is enabled, the forwarded host is gated likewise, and Scalar docs default to not persisting auth.

- **Input/identifier hardening** — Meilisearch filter attribute names validated, PGroonga terms quoted as literal phrases, SQS rejects absolute-URL queue names on tenant-aware adapters, object-storage keys validated, and tenancy rejects invalid hints and inactive tenants.

- **Misc** — BigQuery and GCS routed clients unlink the temp service-account JSON on close, and logging can scrub error message and stack and omit stacks from JSON logs.

## [0.2.0] - 2026-05-28

### Added

- **Execution** — `OperationRegistry`/`Handler` with stage hooks, `OperationRegistry.patch()`, and `run_operation`; `ResolvedOperationPlan` drives hooks, tx scopes, and after-commit dispatch.

- **Execution context** — nested resolvers `ctx.document`, `ctx.deps`, `ctx.tx_ctx`, `ctx.authz`.

- **Tracing** — `ResolutionTracer`/`RuntimeTracer` with `DepsPlan.with_tracing()` and dev runtime tracing.

- **Composition catalogs** — `DOCUMENT_OPERATIONS` (and search, storage, authn) under `forze_kits.*.catalog`, plus plan hooks.

- **Query DSL** — literal and field filters, `$not`, array quantifiers, text patterns, aggregate groups and truncation, and `QueryFilterLimits`.

- **Document and search** — `DocumentCoordinator`, `update_matching`/`ensure`, method-specific ports (`find_page`/`find_cursor`/…), federated search, `RowLockMode`, stream methods, and `default_sort`.

- **Durable functions** — contracts under `forze.application.contracts.durable.function` plus `run_durable_function`.

- **`forze_inngest`** (`inngest` extra) — Inngest adapter with registry-backed runs and a FastAPI serve.

- **Workflow schedules** — schedule contracts and Temporal Schedules via declarative schedule bootstraps.

- **Queue delayed delivery** — `enqueue`/`enqueue_many` accept a delay or a not-before time.

- **`forze_identity`** (plus `oidc` extra) — consolidated authn, authz, tenancy, and OIDC with `AuthnOrchestrator` and `AuthzPolicyService`.

- **Analytics** — `AnalyticsSpec`/`AnalyticsQueryPort` with Postgres, ClickHouse, and BigQuery adapters.

- **`forze_firestore`, `forze_gcs`, `forze_secrets`, `forze_vault`** — document, object-storage, and secrets integrations.

- **Postgres startup validation** — Pydantic-to-column compatibility and tenancy-wiring checks on `PostgresDepsModule`.

- **Scrubbing and logging** — `forze.base.scrubbing` (`sanitize`, `configure_logging(sanitize_logs=True)`).

- **Integrations** — Redis distributed locks, Pydantic and Msgspec model codecs, and optional kit domain mixins.

### Changed

- **Breaking — execution and composition** — `Usecase`/`UsecaseRegistry` replaced by `Handler` plus `OperationRegistry`. Register with `set_handler`, compose via patch and bind methods, freeze, then resolve per operation and context.

- **Breaking — `ExecutionContext`** — the doc, dep, transaction, and call-context accessors are renamed onto the nested resolvers.

- **Breaking — document and search ports** — result shape and pagination are chosen by method name (`find_page` vs `find_cursor`); `find_many_with_cursor` is removed.

- **Breaking — query DSL** — filter literals, field compares, and grouping move to new operator keys; the top-level time-bucket key is removed.

- **Breaking — identity** — legacy `forze_authnz` consolidated into `forze_identity`. `AuthnIdentity` is principal-only, `AuthnPort` returns an `AuthnResult`, and tenant hints are validated via a resolver port.

- **Breaking — authorization** — `AuthzPort.permits(...)` removed; use `AuthzDecisionPort.authorize(...)`. Import plan helpers from the authz hooks module.

- **Breaking — durable workflows** — contracts move under `forze.application.contracts.durable.workflow` with renamed types and dep keys.

- **Breaking — errors** — `forze.base.errors` removed in favor of `forze.base.exceptions`; the HTTP error-code header defaults to `core.<kind>`.

- **Breaking — tracing** — runtime tracing renamed to `forze.application.execution.tracing`; `Deps.merge()` no longer propagates tracer flags.

- **Breaking — FastAPI** — the endpoints and HTTP-transport packages are removed; the package now ships middleware, exception handlers, OpenAPI helpers, and security resolvers only.

- **Breaking — Mongo** — the client db and collection accessors and the gateway collection accessor are async.

- **Document/search pagination** — omitting sorts no longer emits an order-by-id when the read model has no id field; configure `default_sort` or pass explicit sorts.

- **Messaging contracts** — queue, pubsub, and stream messages are frozen attrs value objects, and specs require a model codec.

- **`forze_gcs`** uses native async storage; Postgres PGroonga match and weights follow index order; Postgres and Redis get safer batched writes and atomic mset.

- **Scrubbing/console** — the log scrub mask changed and traceback frames grew; Socket.IO bind takes an operation resolver; unhandled FastAPI route exceptions return a generic JSON 500.

### Removed

- **Execution** — `Usecase`, `UsecaseRegistry`, `UsecasePlan`, the bucket module, `facade_call`, and registry graph introspection types.

- **FastAPI** — the endpoints package, the HTTP-transport package, `ForzeAPIRouter`, and attach-based route helpers.

- **Authn and identity** — the monolithic authn adapter, the header authn resolver, `OAuth2Tokens`, and principal codec ports.

- **Query/search/domain** — deprecated predicate aliases, the legacy Postgres FTS and PGroonga search adapters, and `forze.domain.mixins` (use kit mixins).

### Fixed

- **`forze_fastapi`** — the exception handlers critical-log tracebacks for unhandled exceptions, and a deliberate causeless 5xx logs at error level.

- **Errors** — error details and FastAPI context responses no longer expose raw credentials or Pydantic validation input.

- **Postgres** — batched updates cast nullable cells correctly and read-only is set before opening transactions; empty FTS queries no longer emit invalid rank SQL.

- **Redis** — script result normalization avoids rare type-check failures. S3 user-metadata decoding is fixed and default keys use a fresh UUID v7. The API-key lifecycle unpacks prefix and secret in the correct order.

## [0.1.14] - 2026-04-08

### Added

- `forze.base.logging` — structlog-based logging (structured records, a trace level, Rich/JSON renderers, request/context binding, per-namespace levels, optional dual pretty-stderr plus JSON-stdout, and a global unhandled-exception handler). Replaces the previous Loguru stack.

- `forze_fastapi` — ANSI-colored HTTP status in access logs, plus an optional unhandled-exception handler and registration for non-core exceptions.

- `forze.application.contracts.workflow` — port protocols and specs for workflow engines (start, signal, update, query, cancel, terminate).

- `forze_temporal` — Temporal integration package: deps module and lifecycle, a workflow adapter implementing the command port, and client/worker interceptors propagating context and running payload codecs.

- `forze_fastapi.middlewares.context` — an ASGI middleware to bind call and principal context and emit call-context headers.

### Changed

- `Deps` replaces `DepRouter` — spec-based routing and the router module are removed; route selection now lives on `Deps` with plain and routed registration and updated merge and removal helpers.

- `DepKey`/`DepsPort` imports moved to `forze.application.contracts.base` — the old deps package (keys, ports, router) is gone; import the base types and drop router types.

- `DepsModule` wiring — integration packages now build `Deps` through module callables with routed registration; review each package's execution deps.

- Contracts — ports, specs, and dep keys updated across domains (document, search, workflow, cache, queue, pubsub, stream, tx), including renames and new overloads; search parse helpers removed; the mapper port relocated.

- `forze_fastapi` — HTTP integration reorganized under an endpoints package with attach helpers and route features for idempotency and ETag; the custom router and routing package are removed.

- `forze.base.logging` — a new logger API (configure, get-logger, message sub vs extras); migrate code that relied on Loguru-specific helpers.

- `forze.base.logging` — OpenTelemetry-aware processors, an exception-info formatter, configurable dim keys, and level-aware Rich console styling.

- `forze_fastapi` — idempotent routes do not record idempotency when the body is invalid JSON (422), so the key can be reused after fixing the body.

- `forze_fastapi` — a batch HTTP route registrar, plus an exclude-none option on the document, http, and search attach helpers.

- `forze.application.execution` — `UsecaseRegistry.finalize` supports an in-place mode.

- `forze.application.contracts.document` and adapters — optional return-new and return-diff on create, update, touch, and batch variants.

### Removed

- `DepRouter` and the deps package — use `Deps` routing and the base module for `DepKey`/`DepsPort`.

- `TenantContextPort` and the tenant contract module.

- `ActorContextPort` and the actor contract module — caller identity is modeled via the execution context, auth identity, and the FastAPI context middleware.

- The Loguru-based implementation and the `loguru` dependency, including the old configure-prefixes, render-message, and safe-preview helpers in favor of the structlog logger.

### Fixed

- `forze_postgres`/`forze_mongo` — document deps modules register each read-write route's read and query port from that route's read config, fixing incorrect reuse of the read-only route.

- `forze_postgres`/`forze_mongo` — tenant-aware write gateways include the tenant id in update and hard-delete predicates; Postgres still raises not-found when no row matches the scoped delete.

- `forze_postgres` — the FTS search adapter reads rows from the configured source relation and uses the index only for catalog metadata; empty-query FTS uses a valid order-by.

## [0.1.13] - 2026-03-15

### Added

- `hybridmethod` descriptor in `forze.base.descriptors` for class/instance dual methods.

- `Pagination` DTO with page and size fields for list and search request payloads.

- `DocumentDTOs` with list and raw-list keys for custom list request DTO types.

- `SearchDTOs` with read, typed, and raw keys for search facade DTO configuration.

- `build_document_list_mapper` and `build_document_raw_list_mapper` in document composition.

- `build_search_typed_mapper` and `build_search_raw_mapper` in search composition.

- `LoggingMiddleware` in `forze_fastapi.middlewares` for request/response logging with scope.

- `Logger.opt` for passing options (depth, exception) to the underlying logger.

- `UVICORN_LOG_CONFIG_TEMPLATE` and `InterceptHandler` in `forze_fastapi.logging` for uvicorn log-config integration.

- Storage application layer — upload, list, download, and delete usecases plus the storage facade, DTOs, and registry builder.

### Changed

- `OperationPlan.merge`, `UsecasePlan.merge`, and `UsecaseRegistry.merge` are now hybridmethods (callable on class or instance).

- The document and search endpoint-name overrides are renamed to path overrides, and the name-overrides parameter to path-overrides.

- Document and search facades now take a DTOs object instead of a read DTO, and the registry builders require it.

- `DTOMapper` now requires the source model type in addition to the output; update existing mappers.

- `MappingStep` is now generic over the source type; custom steps should specify it.

- `CoreModel` no longer includes `Decimal` in its JSON encoders; custom Decimal serialization must be handled elsewhere.

- The list and search request DTOs extend `Pagination`, so page and size now live in the request body.

- List and search usecases take the request DTO directly instead of a TypedDict with body, page, and size.

- Postgres and Mongo document adapters — write operations now return results via the read gateway.

- Logging — scope-based contextualization, a section helper for structured spans, and `safe_preview` replacing the old argument-safety helper.

### Fixed

- Document list endpoints now correctly pass pagination to the usecase.

- Logging format — escape the extras dict to avoid a key error, and exclude a redundant logger-name field.

### Removed

- `Pagination` and the pagination parameter from `forze_fastapi.routing.params`; use the request body instead.

- `Usecase.log_parameters` and the private argument-safety helper; use `safe_preview` from logging.

- `register_uvicorn_logging_interceptor`; use the log-config template in uvicorn's `log_config`.

## [0.1.12] - 2026-03-11

### Added

- A paginated list-documents endpoint in `forze_fastapi` with typed and raw variants, the list request DTOs, and a list usecase.

- Name overrides on the document and search routers for customizing operation ids and paths.

- `attach_document_routes` and `attach_search_routes` for attaching routes to existing routers.

### Changed

- `attach_search_router` renamed to `attach_search_routes` in the search router module. Update imports.

### Fixed

- Postgres bulk update — correct table alias in the returning clause, and English error messages for consistency errors.

## [0.1.11] - 2026-03-11

### Added

- Route-level HTTP ETag support in `forze_fastapi` with an ETag provider protocol, an ETag route, and a route-class factory.

- Per-route and per-router ETag configuration (enabled, provider, auto-304).

- A document ETag provider deriving the ETag from the document id and revision without response hashing.

- ETag and `If-None-Match`/304 support on the document metadata endpoint.

- A `get()` override on the router with ETag parameters.

- A route-feature protocol and a route-class composition engine for composable route behaviors.

- ETag and idempotency as standalone route features.

- A route-features parameter on the router's route methods.

- Document update validators now run even when the update produces an empty diff.

- A model-hash helper that normalizes `Decimal` for stable hashing; `CoreModel` adds `Decimal` to its JSON encoders.

### Changed

- The router now composes idempotency, ETag, and custom route features into a single route class, replacing the single-feature override pattern.

- The validation helper's default for forbidding extra keys changed from true to false; extra keys are now ignored by default.

- `Document.touch()` now returns a new instance via a model copy instead of mutating in place.

- The Postgres document gateway raises a conflict with a revision-mismatch code when history is disabled.

- The Postgres query renderer requires array column types for array operators.

### Fixed

- The document metadata endpoint path is corrected to `/metadata`.

- Cache operations in the Postgres and Mongo document adapters are now non-fatal, so failures are suppressed and primary operations succeed.

## [0.1.10] - 2026-03-11

### Added

- An error handler for `forze_mongo` mapping PyMongo exceptions to core error subtypes.

- Optimistic retry on Mongo write-gateway write operations for concurrency errors.

- A default adaptive retry configuration for the S3 client when none is provided.

### Changed

- Replaced the DeepDiff-based dict diff with a lightweight recursive implementation (a large speedup on diff and patch).

- Removed the `deepdiff` and `mergedeep` runtime dependencies from the core package.

- Cached the middleware chain in the usecase call to avoid rebuilding closures per invocation.

- Cached signature lookups in the error-handling decorators.

- Cached module lookups in the introspection helpers.

- Cached type-adapter instances per payload type in the Socket.IO emitter.

- The document update now uses a shallow model copy for scalar-only diffs.

- The S3 storage adapter list now fetches object metadata concurrently.

- Used list-extend over augmented assignment for middleware chain construction.

- Eliminated per-call signature binding; the operation name resolves once at decoration time.

- Postgres dict-row fetch uses a dedicated row-to-dict method.

- SQS queue-name sanitization uses pre-compiled regex patterns.

- RabbitMQ ack/nack acquires the pending-messages lock once per batch.

- Cached field-name lookups and narrowed the return type to a frozenset.

- Cached Postgres type normalization in the introspection utilities.

- Pre-computed query-operator sets as module-level constants in the filter parser.

- S3 list now exits pagination early when the limit window is fully collected.

## [0.1.9] - 2026-03-10

### Added

- Socket.IO integration package `forze_socketio` with typed command-event routing, usecase dispatch through the execution context, a typed server-event emitter, ASGI and server builders, and an optional extra.

### Changed

- Contracts refactor — removed the conformity protocols and their dep variants; port protocols remain the source of truth.

- Removed `forze.base.typing`; type checking is now enforced via mypy strict mode.

## [0.1.8] - 2026-03-10

### Added

- A strict content-type parameter (default true) on the router and route methods.

- Tenant context support in the S3 storage adapter.

- An `S3Config` TypedDict for abstracting botocore configuration in `forze_s3`.

- Socket and connect timeouts on the Redis config.

- Prefix validation on the S3 storage adapter.

- A Mongo document adapter with dependency factories and CRUD/query support.

- PubSub contracts and a Redis pubsub adapter with execution wiring.

- RabbitMQ integration package `forze_rabbitmq` with queue contracts, client and adapters, an execution module and lifecycle, and test coverage.

- In-memory integration package `forze_mock` with shared-state adapters and deps for document, search, counter, cache, idempotency, storage, queue, pubsub, stream, and the tx manager.

- SQS integration package `forze_sqs` with an async client and adapters, an execution module and lifecycle, an optional extra, and LocalStack coverage.

### Changed

- Search router — split building from attachment.

- Response body chunk processing in the idempotent route (performance).

- The Postgres patch-many loop now gathers concurrently (performance).

- Postgres document write operations avoid redundant reads (performance).

- The Mongo integration now mirrors Postgres composition with dedicated read, write, and history gateways and configurable revision and history strategies.

- RabbitMQ batch enqueue now publishes via a single channel scope and queue declaration per batch (performance).

### Fixed

- Tenant-context dep resolution in the S3 storage adapter (invoke the dep as a factory).

- Read-gateway fallback on cache failure.

- Deterministic UUID generation now uses SHA-256 instead of MD5 (security).

## [0.1.7] - 2026-03-08

### Changed

- The package is now published on PyPI instead of OCI.

- `register_scalar_docs` — the version parameter is renamed, and the docs page title now uses the app title.

## [0.1.6] - 2026-03-04

Execution and mapping refactor, middleware-first usecases, split search/cache/document contracts.

### Added

- A mapping module with `DTOMapper`, mapping steps, and a mapping policy for composable async DTO mapping.

- Document plan and create-mapper builders in the document registry builder.

- Namespaced document and storage operation values.

- A creator-id field constant in the domain constants.

- A search contract with read and write ports, specs, and a parser, plus a Postgres search adapter.

- A FastAPI search router and facade dependency.

### Changed

- Document and storage operations and the document facade move out of the facades package, which is removed.

- The effect, guard, middleware, and next-call types move from the usecase module to a middleware module.

- `Deps` adopts a constructor-based API; the builder methods are removed.

- `Usecase` now requires an execution context, and guards and effects are replaced by middlewares.

- `TxUsecase` removed; transaction handling moves to a tx middleware in the plan.

- The document facade provider now requires the registry and plan.

- The create and update document usecases use the async mapper instead of sync callables; the numbered create usecase is removed in favor of a numbered create mapper.

- Search specs split into public TypedDicts and internal attrs, with per-index source and ordered groups.

- Router subclasses must set the dep key as a class attribute when using the attrs decorator.

### Fixed

- The Postgres history gateway consistency error messages are now in English.

- The Postgres search adapter uses a correct attrs mutable default for its gateway cache.

- Postgres index introspection uses a lateral unnest and simplified detection.

- The Postgres error handler covers grouping errors.

## [0.1.5] - 2026-02-28

### Added

- A `scalar-fastapi` dependency and a Scalar docs registrar in `forze_fastapi.openapi`.

- An exception-handlers module in `forze_fastapi.handlers`.

- Stable OpenAPI operation ids on all document router endpoints.

- Public exports in `forze_postgres`, `forze_redis`, and `forze_s3` for the deps modules, client dep keys, and lifecycle steps.

- An idempotency dep key in the idempotency contract.

- A route-level idempotency route and route-class factory in `forze_fastapi.routing.routes`.

- A deps module and deps plan in the execution deps module.

- From-modules and from-steps factory methods for the deps and lifecycle plans.

- A lifecycle plan and step in the execution lifecycle module.

- An `ExecutionRuntime` combining the deps plan, lifecycle, and context scope.

### Changed

- `Deps` moved from the deps contract to the execution package. Update imports.

- Postgres, Redis, and S3 restructure — the dependencies package is removed and modules move to execution with attrs-based deps modules and lifecycle steps. Replace the old module helpers with the new deps modules.

- The router's from-deps factory now accepts a deps port and returns an optional remainder.

- The doc, counter, tx-manager, and storage port resolvers consolidate into a single resolver namespace class.

- The DTO spec is renamed to the document DTO spec. Update imports.

- Document router — request body params now use a body annotation with override annotations for a correct OpenAPI schema.

- The router and document-router builder no longer accept idempotency parameters; idempotency is applied via a custom route class and resolved from the context via the idempotency dep key.

## [0.1.4] - 2026-02-27

### Added

- A configurable revision-bump strategy in `forze_postgres` (database vs application) and a configurable document factory.

- A middleware protocol and chain composition in the usecase.

- An outbox feature module with buffer middleware and a flush effect.

- A middleware factory and middleware support in the usecase plan.

### Changed

- `TxContextScopedPort` renamed to `TxScopedPort` (the context requirement is removed). Update imports.

- The tx-scope-match decorator is removed; tx-scope validation is now handled by the execution context when resolving dependencies.

- The Postgres document adapter no longer requires the context; it uses the tx-scoped port instead.

### Fixed

- Duplicate guards, middlewares, and effects are now deduplicated by priority when merging usecase-plan operations.

## [0.1.3] - 2026-02-27

### Added

- A filter query DSL in `forze.application.dsl.query` — AST nodes, parser, and value coercion.

- A Mongo query renderer for compiling filter expressions to MongoDB queries.

- A buffer primitive in `forze.base.primitives`.

### Changed

- Application-layer restructure — the kernel splits into a contracts package (ports, specs, deps, schemas) and an execution package (context, usecase, plan, registry, resolvers). Update imports.

- Contracts flattening — top-level re-exports, with internal modules moved to underscored sub-packages.

- Tx contracts rename — the tx-manager port and related contracts move from the tx-manager module to the tx module. Update imports.

- Postgres filter builder — replaced the old builder with the DSL-based query renderer; the old builder is removed.

## [0.1.2] - 2026-02-26

### Added

- `forze.base.typing` with protocol conformance helpers.

- Domain document support in `forze.domain` built from the domain document model with name, number, and soft-deletion mixins and update-validator infrastructure.

- A document kernel in `forze.application.kernel` — pluggable usecase plans, a document facade factory, the document port with explicit read and write ports, and a document operation enum.

- An optional FastAPI integration package with routing helpers, idempotent POST support, and a prebuilt document router.

- Optional provider packages `forze_postgres`, `forze_redis`, `forze_s3`, `forze_temporal`, and `forze_mongo` with platform clients, gateways and adapters, and dependency keys.

### Changed

- Kernel — transaction handling and dependency resolution refactored around the execution context and kernel deps; the tx-manager and app-runtime ports are removed from the kernel ports.

- Postgres filter builder — filter input accepts only canonical operator names; aliases such as `==`, `ge`, and `in_` are no longer accepted and raise a validation error.

- Infrastructure previously under `forze.infra` moved into optional packages; core `forze` no longer ships Postgres, Redis, S3, or Temporal implementations.

### Fixed

- Correct UUIDv7 datetime conversion in `forze.base.primitives.uuid` so round-trips preserve timestamp semantics.

## [0.1.1] - 2026-02-23

### Added

- Initial DDD/Hex contracts — ports, results, errors.

### Fixed

- Packaging metadata for PyOCI classifiers.

[0.4.1]: https://github.com/morzecrew/forze/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/morzecrew/forze/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/morzecrew/forze/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/morzecrew/forze/compare/v0.1.14...v0.2.0
[0.1.14]: https://github.com/morzecrew/forze/compare/v0.1.13...v0.1.14
[0.1.13]: https://github.com/morzecrew/forze/compare/v0.1.12...v0.1.13
[0.1.12]: https://github.com/morzecrew/forze/compare/v0.1.11...v0.1.12
[0.1.11]: https://github.com/morzecrew/forze/compare/v0.1.10...v0.1.11
[0.1.10]: https://github.com/morzecrew/forze/compare/v0.1.9...v0.1.10
[0.1.9]: https://github.com/morzecrew/forze/compare/v0.1.8...v0.1.9
[0.1.8]: https://github.com/morzecrew/forze/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/morzecrew/forze/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/morzecrew/forze/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/morzecrew/forze/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/morzecrew/forze/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/morzecrew/forze/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/morzecrew/forze/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/morzecrew/forze/releases/tag/v0.1.1
