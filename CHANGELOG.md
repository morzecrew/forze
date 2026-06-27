# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Document aggregates fail closed on backends that can't compute them** — `QueryCapabilities` gains a `supports_aggregates` axis (default on) plus a sibling `validate_aggregate_capabilities`, the aggregate counterpart to the filter-capability check. A backend that can't compile group-by / aggregate pipelines — the Firestore MVP adapter — declares `supports_aggregates=False` and now rejects `find_many(aggregates=…)` / `find_many_aggregates` / `count_aggregates` with a clean `precondition` (code `query_feature_unsupported`, naming the backend) at the renderer's aggregate entry, instead of an opaque `internal` (500) deep in the read gateway. The check runs in every backend's `render_aggregates` (a no-op for Mongo / Postgres, which support aggregates), and Firestore's `find_many` now delegates to `find_many_aggregates` like the other adapters, so all aggregate paths share the one gate.

- **W3C trace-context propagation across the async event flow** — a published event now carries its publishing operation's span across the outbox→broker→inbox hop, so the consume side joins the **same distributed trace** — the gap OpenTelemetry's transport auto-instrumentation can't bridge for a custom envelope. Opt in per outbox backend with `OutboxIntegrationConfig.propagate_trace` (off by default; on a relational backend add a nullable `traceparent` column first, exactly like `hlc_ordering`): the staging enricher captures the active span's W3C `traceparent`, the relay forwards it as the standard `traceparent` header, and `process_with_inbox` rebuilds the context so the handler's spans link to the publish span. Outbound HTTP (`forze_http`) also injects the active trace context into requests (no-op without a span; honours the app's configured propagator). The header is plain broker metadata — it influences trace *parenting only*, never identity / tenancy / dedup. Inbound FastAPI extraction is left to standard `opentelemetry-instrumentation-fastapi`.

- **Per-port OpenTelemetry client spans** — `DepsRegistry.with_otel_port_spans()` opts every resolved configurable port into a per-call OTel `CLIENT` span (a child of the operation span from `instrument_operations`), so in a hexagonal app — where every external call is a port — the port seam yields a complete outbound-I/O trace: each document / cache / queue / search / http call a span carrying `forze.port.{domain,surface,route,phase,op}` (the route a high-cardinality attribute, never the span name). The span sits *inside* the resilience policy (a retried call yields one span per attempt; a breaker/bulkhead-rejected call emits none), and error status follows the exception *kind* — an infrastructure / internal / configuration `CoreException` (a 5xx kind) or any non-`CoreException` sets `ERROR`, while a client-class domain failure (not-found / conflict / precondition — a 4xx kind the caller may handle) leaves the span clean, as a 404 does not red an HTTP client span. Opt-in and zero-cost when off; emits through the global OTel tracer (or one passed explicitly), independent of the dev DST trace. Streaming (async-generator) port methods are passed through un-spanned.

- **Cross-aggregate (system) invariants — declaration + detective & preventive enforcement + dry-run proposal** — a new `SystemInvariant` (with `ReadSet`, `Sum`, `Count`) in `forze.application.contracts` (front-doored from `forze`) declares a law that spans more than one record — a conservation law (a ledger's balances sum to zero), a cardinality law (≤1 captured payment per order) — as a predicate over an aggregate of a scoped read-set, which the entity-level `@invariant` structurally cannot express. `forze_kits.invariants.evaluate` reads the read-set aggregate (`count` / no-group `$sum`) and checks the predicate. Two enforcement modes: `enforce` schedules the check post-commit (`ctx.tx_ctx.run_or_defer`) and raises on a violation — *detective* (reports a breach, does not prevent it); `enforce_preventive` runs the check *inside* the writing transaction and raises before commit, so the violating write is **rolled back**. Preventive mode fails closed unless the transaction meets the law's `required_isolation` (default `SERIALIZABLE`), readable via the new `TransactionContext.current_isolation()`, with the backend-capability check reusing the existing `scope(isolation=…)` fail-closed against `TxCapabilities`. `forze_kits.invariants.propose` is a dry-run filter for agent-authoring: it applies a proposed write and checks laws inside a transaction, then rolls it back (nothing persists), returning a machine-checkable `ProposalVerdict` of whether the write would be accepted — a filter (TOCTOU under concurrency, conformance-bounded), never a proof. And `forze_dst.compile_oracle(*laws)` compiles a declared law into a DST oracle (an `observe` hook + invariants) that, over final simulated state, groups by the law's scope and checks **every scope the run produced** — catching e.g. the dst_payments double-charge under the seed sweep. (`ReadSet` declares `scope_keys` + a constant `where` rather than a filter callable, so runtime and oracle derive the same read-set.) `compile_oracle(*laws, per_commit=True)` is the per-commit variant: it folds the value-trace (`SimulationConfig.capture_values`) to reconstruct the read-set as-of each committed transaction and flags a violation at the commit where it existed — catching a transient a later transaction heals (one a final-state check misses). It reconstructs the *faithful* view, so it is only as sound as the backend's conformance.

- **Transaction-scope exit records its outcome** — a `tx` `exit` trace event now carries `outcome` (`"commit"` vs `"rollback"`); the exit fires from a `finally`, so it marks scope teardown either way, and the outcome distinguishes a real commit from a rollback. `TransactionContext.scope` and the `TxTracer` protocol thread a `committed` flag. (Enables the per-commit cross-aggregate oracle to count only committed transactions; production tracing is unaffected.)

- **DST transactional-isolation oracles** — `forze_dst` gains `snapshot_isolation()` / `serializable()` invariants (and the `TxRecord` kernel `find_snapshot_isolation_violations` / `find_serializable_violations`) that detect lost update and write skew across a run's committed transactions. They rest on a new per-event transaction-id seam on the runtime trace (a run-global counter minted at root transaction entry, threaded through the port proxy and folded into the DST history), so port calls are grouped into the transaction that issued them soundly — operation-span attribution is unsound under interleaving. Production trace stays id-only and unaffected (the id is `None` when no run counter is bound). `serializable(complete=True)` upgrades the default pairwise check to a **complete** conflict-serializability check via a dependency serialization graph (`find_serializability_cycle` over `VersionedTxRecord`), so anti-dependency cycles spanning **three or more** transactions — e.g. the snapshot-isolation read-only anomaly — are caught, not just two-transaction write skew. Edges are directed by the entity `rev` version order, so the complete mode reads the value-trace (requires `SimulationConfig.capture_values`, fails closed without it); keys are `(namespace, id)` so documents never conflate across specs. The complete check also adds **predicate (phantom) edges**: a scan's filter is captured on the trace and a concurrent committed write whose row satisfies it — but which the scan provably did not see (it committed later in trace order) — is a phantom anti-dependency, so generated workloads that produce a phantom cycle are caught, not just hand-scripted ones. To make the oracle's predicate semantics match the backend's exactly, the in-memory filter evaluator is now a single shared core primitive — `evaluate_filter` / `compile_filter` in `forze.application.contracts.querying` (the mock re-exports it) — matched against the write row's **native** representation (a new `result_native` capture field), so a JSON-vs-native gap can't manufacture a false match. Bounds are false-negative-only (documented on `find_serializability_cycle`): `count`/`exists` and `project_*`/`select_*`/`aggregate_*`/`find_stream` scans don't generate the forward phantom edge.

- **DST isolation oracle — make it fire, and trust the green** — the serializability oracle can *detect* anti-dependency cycles + phantoms, but a random workload rarely *produces* them, leaving it latent. `forze_dst` now ships `had_isolation_conflict(history)` — a capture-free **non-vacuity** signal (did the run produce a real conflict *opportunity*: concurrent committed transactions sharing a key with a write, or — under capture — a scanner overlapping a writer in a scanned namespace?) so `any(had_isolation_conflict(h) for h in histories)` asserts a sweep actually stressed isolation (a green over a workload that never conflicted is meaningless). `isolation_oracle_for(level)` maps a *declared* `IsolationLevel` to the oracle that checks it (`SERIALIZABLE` → `serializable(complete=True)`, `SNAPSHOT` → `snapshot_isolation()`, `READ_COMMITTED` raises — no serialization-graph guarantee), so a run is checked at the level it claims. A reusable contended stress scenario (bounded key pool + read-modify-write / write-skew / scan-then-insert shapes) drives the structures: the oracle now catches a *generated* write-skew cycle and a *generated* predicate phantom at SNAPSHOT and stays clean at SERIALIZABLE (the mock's SSI), non-vacuously.

- **DST continuous sweep + regression corpus for the flagship scenarios** — the distributed-lock and hybrid-logical-clock scenarios are now reusable picklable seed targets (`tests/support/dst_flagship.py`): a `fuzz`-marked wide sweep fans them across processes via `parallel_sweep` (the nightly substance), and a fast regression corpus runs them every build as a merge guard (`sweep` over a small band plus any seed that ever found a bug). Previously the only swept target was a toy in-memory counter; the flagship primitives now actually get fuzzed.

- **DST sweep reachability fold** — a seed sweep now carries reachability: `SeedOutcome.reached` records each run's reached labels, the sweep folds per-label run counts into `SweepResult.reached_runs`, and `SweepResult.reachability(targets)` returns a `ReachabilityReport`. The flagship corpus and wide fuzz assert `result.reachability(TARGETS).satisfied` — that the dangerous interleaving actually fired across the band — so a green sweep that never drove the hard state is now caught (previously only the per-history smoke tests checked it).

- **DST commutativity assertion** — an operation can declare itself order-independent (`OperationDescriptor.commutative`, a declaration DST verifies but execution never consumes), and `forze_dst.invariants.commutative_convergence(build, *, final_state, schedule_seeds)` checks it: it reruns the workload across a band of schedule seeds (fresh state per run) and flags a declared-commutative op whose interleavings reach different end states, naming the reproducing seed. A cross-history checker, not a single-history invariant.

- **Adapter conformance — real-Mongo differential (snapshot isolation)** — the isolation battery also runs against a real MongoDB replica set over testcontainers (`tests/integration/test_forze_mongo/test_mongo_isolation_conformance.py`), asserting `mock ≡ real Mongo` at `SNAPSHOT` (write skew permitted — Mongo is SI-only, no SSI). Mongo advertises only `{READ_COMMITTED, SNAPSHOT}`; its in-transaction `READ_COMMITTED` is snapshot-isolated (a non-repeatable read is prevented even there), so the differential runs at `SNAPSHOT`.

- **Adapter conformance — real-Postgres differential** — the isolation battery now runs against real Postgres over testcontainers (`tests/integration/test_forze_postgres/test_pg_isolation_conformance.py`), asserting the same `expected_verdict` the mock passes: a green run means **mock ≡ real** for the isolation family (`SNAPSHOT`→`REPEATABLE READ`, `SERIALIZABLE`→SSI), and verifies the Postgres tx manager's self-attested `TxCapabilities` against the real engine. This is what makes "DST passed on the mock" carry weight.

- **Adapter conformance — isolation anomaly battery** — new `forze_dst.conformance`: a backend-agnostic battery of classic isolation anomalies (dirty read, non-repeatable read, read skew, phantom, write skew, predicate write skew, the three-transaction read-only anomaly, and lost update) as deterministic forced interleavings (over the shipped `Conductor`, including a third session for the read-only anomaly) with a known verdict per `IsolationLevel`, run against any `ConformanceBackend`. The predicate cases (`phantom`, `predicate_write_skew`) cover predicate/phantom (G2) anti-dependencies — a re-run scan seeing a concurrent insert, and two scans each inserting a matching row — and the `read_only_anomaly` shows snapshot isolation is non-serializable even for a read-only transaction; all verified `mock ≡ real Postgres` at every level and `mock ≡ real Mongo` at snapshot. Verdicts normalize to permitted/prevented so a differential compares the anomaly *outcome* at the declared level — never the mechanism, error code, or victim — and a reviewed allowed-divergence catalog (`CONTRACT_STRENGTHENINGS` / `MECHANISM_DIVERGENCES`) records expected differences (e.g. Forze's rev-OCC prevents lost update at every level). Mock-only first leg; the real-backend (testcontainers) differential that turns "passed on the mock" into "matches the real engine" follows.

- **Redis stream & pub-sub transports** — `RedisDepsModule` wires the generic `StreamSpec` / `PubSubSpec` transports (`streams` / `stream_groups` / `pubsub` route maps → the six stream/pub-sub dep keys) via new `RedisStreamConfig` / `RedisStreamGroupConfig` / `RedisPubSubConfig`, so realtime-over-Redis and outbox→stream/pub-sub relay work in production. No namespace (the stream/topic is per-call; `tenant_aware` adds a `tenant:{id}:` key prefix).

- **End-to-end encryption + reach floor on Redis transports** — `encryption="end_to_end"` on a `StreamSpec` / `PubSubSpec` seals payloads through the broker, and the deployment `required_reach` floor is enforced at every resolve point (publish *and* subscribe/consume).

- **Stream consumer-group query/admin split** — the stream consumer-group adapter is split into a data-plane query adapter (read/ack/claim/pending) and a control-plane `*StreamGroupAdminAdapter` (`ensure_group`), for both Redis and the mock, so a `StreamGroupQueryPort` reference can't reach group provisioning.

- **Encryption *reach* vocabulary** — new canonical `EncryptionReach` type names the outbox/messaging setting as a *reach* ladder (`none < at_rest < end_to_end`), distinct from storage coverage. `OutboxEncryptionTier` is now a back-compat alias and `MessageEncryptionTier` its transport subset (no `at_rest`); field name and values unchanged.

- **`required_reach` floor** — `CryptoDepsModule(required_reach="end_to_end"|"at_rest")` refuses, at resolve, any outbox or transport route whose declared reach is weaker (`exc.configuration`). Opt-in (default `None`); a transport meets an `at_rest` floor only via `end_to_end`.

- **Fencing-token capability for distributed locks** — `DistributedLockSpec(requires_fencing_token=True)` fails closed at resolve against a backend not reporting `FencingAware`/`fencing_tokens`. Default `False`; Redis and mock report support.

- **Less CRUD boilerplate** — `build_document_registry(spec)` derives its `DocumentDTOs` from the spec when `dtos` is omitted (override, or `create=None`/`update=None` to disable an op), and new `document_facade(runtime, registry, spec)` returns a per-call typed `DocumentFacade` factory. Both additive — the explicit `DocumentDTOs` and `DocumentFacade(...)` forms keep working.

- **Top-level front door** — the most-used names re-export from `forze` and `forze_kits` (`from forze import DocumentSpec, build_runtime`; `from forze_kits import DocumentFacade, build_document_registry`), resolved lazily (PEP 562) so `import forze` stays cheap. Deep paths keep working; the core never imports kits.

- **Procedures port — governed parametrized commands/compute** — `ctx.procedure.command(spec).run(params)` runs a spec-named, parametrized statement (a function/`CALL`, set-based recompute, or `REFRESH MATERIALIZED VIEW`): analytics' write/compute twin, for recomputing over an ingested batch in one statement instead of per-row triggers. One `ProcedureSpec[In, Out]` per procedure, command-only (refused in a read-only operation), on Postgres plus a programmable mock. Tenant-aware routes fail closed at wiring unless the SQL binds `%(tenant)s`.

- **Query parameters — bound session settings for read sources** — a read resource declares a typed `query_params` contract and a handler binds values with `ctx.document.query(spec).with_parameters(P(...))`, which the backend applies as query-scoped session settings the relation reads internally (Postgres documents plus a programmable mock), so the full read DSL still composes on top. Capability-gated and fail-closed: unsupported backends and declared-but-unbound reads raise.

- **Nested-field sorting** — sort keys may now be dotted paths into nested Pydantic sub-models and `str`-keyed mappings (`sorts={"addr.city": "asc"}`), resolved the same way nested filters already are, across offset and keyset-cursor reads and every backend. `default_sort` on document/search specs accepts nested paths too. Tightening: sorting on a nested path whose **root** column is field-encrypted is now rejected (it was silently allowed and could leak the value in a cursor token).

- **CPU-offload seam** — `run_cpu` / `run_cpu_map` run blocking or CPU-bound work off the event loop via a context-bound `CpuExecutor` (bounded thread pool in production; inline and deterministic under simulation, so offloading handlers stay testable), honoring the invocation deadline with a cooperative `checkpoint()`.

- **HTTP status mapping in core** — `http_status_for_kind(kind)` in `forze.base.exceptions` maps an `ExceptionKind` to its conventional HTTP status (404/409/422/…, else 500), so any HTTP-serving layer can reuse it. FastAPI's response builder now uses this shared helper instead of a private one.

- **Shared error boundary in core** — `error_envelope()` and `guard_frame()` give one client-safe projection of a `CoreException` (masking, egress context, status hint) plus a shared guarded boundary, so FastAPI and Socket.IO render the same envelope instead of each duplicating that logic.

- **Realtime egress — server push** — a handler publishes a `RealtimeSignal` to a principal or topic through messaging ports, and the Socket.IO gateway bridges it to a tenant-scoped room. Ephemeral at-most-once or durable exactly-once; read-only operations cannot publish.

- **Realtime multi-node hardening** — TTL-backed presence with heartbeat re-assertion so a crashed node's rooms lapse, eviction of a connection once its credential expires, and a per-emit timeout so one stuck delivery cannot wedge the gateway's consume loop.

- **Realtime offline store-and-forward** — a durable principal-addressed signal also reaches a recipient offline at emit time: the gateway mailboxes it atomically with the dedup, and on reconnect each device replays from its cursor and acks to advance it. Topic and ephemeral signals are never mailboxed.

- **Tenant-aware realtime gateway** — `TenantShardedSignalSource` puts per-tenant realtime isolation on the standard tenancy tier ladder: it runs one consume loop per assigned tenant and scopes the mailbox and rooms by a trusted tenant from the stream, not the header. Tenant-global stays the default.

- **`RealtimeShard`** — one value object bundling a namespace-tier instance's assignment (stream, tenants, group). Hand the same shard to `TenantShardedSignalSource`, the group-ensure step, and the tenant relay so the three can't drift on which tenants, stream, or group an instance owns.

- **Tenant-sharded outbox relay** — pass `tenants` to the background relay step (or `realtime_tenant_relay_lifecycle_step`) and it drains each assigned tenant's partition under a bound tenant, sequentially per tick. This brings a partitioned (tenant-aware) outbox to namespace tier, alongside the stream and inbox.

- **Tenant-aware realtime mailbox fails closed clearly** — when the gateway has no bound tenant to scope a tenant-aware mailbox, it now raises an actionable `realtime_mailbox_tenant_unbound` error naming the fix, instead of an opaque tenant-required failure deep in the adapter.

- **BREAKING — realtime delivery envelope** — every frame the Socket.IO gateway emits is now the uniform `{id, data}` envelope instead of the bare payload (durable carries the event id, ephemeral null). Clients must read `data` and dedup by `id`; there is no transitional dual-emit.

- **Mock document adapter — tenant scoping on every write** — the in-memory mock now injects the tenant column on ensure, upsert, update, and touch (not only create), matching Postgres, so a tenant-aware collection using idempotent or update writes isolates correctly under the mock.

- **Materialized derived fields** — `DocumentSpec(materialized=…)` persists selected computed fields as real columns, making them filterable and sortable. Names are validated, create/update collisions are rejected, and startup checks require matching columns.

- **Two-phase prepare/apply handlers** — `prepare(args)` runs outside the transaction (CPU or external work) and `apply(args, payload)` inside it, so the transaction wraps only the writes. Adds the `TwoPhaseHandler` contract and a kit base; a tx route is required and `prepare` is read-only.

- **Deterministic Simulation Testing (`forze_dst`)** — point it at a real Forze app and one master seed reproduces the whole run (schedule, faults, latency, inputs, crashes, network partitions) across single-process and N-node distributed runs, over real registries and runtimes, with no changes to the app under test.

- **Deterministic runtime** — `SimulationEventLoop`, `SimulationTimeSource`, and `run_simulation(...)` make hours of sleeps and backoff run in real-wall milliseconds and replay byte-identically for a seed. Real I/O raises `RealIOForbidden`; a quiescent loop raises `SimulationDeadlock`.

- **Ambient entropy seam** — adds `EntropySource` and `bind_entropy_source` to `forze.base.primitives`, the entropy twin of `TimeSource`: uuid randomness, nonces, jitter, and tokens route through it so a seeded source and frozen clock make runs byte-identical. Default stays the system CSPRNG.

- **Monotonic time seam** — `TimeSource.monotonic()` and a free `monotonic()`; deadlines, resilience clocks, caches, and the mock queue read it (default `time.monotonic()`). A determinism guard wired into the quality gate fails the build if raw time or entropy primitives are used outside the seams.

- **Port interception seam** — a composable `PortInterceptor` chain around resolved ports via `DepsRegistry.with_interceptors(...)`, running innermost (inside tracing and resilience) and zero-cost when unused. DST plugs latency, faults, crashes, and partitions through it without touching handlers.

- **Operation-catalog fingerprint** — `FrozenOperationRegistry.fingerprint()` and `operation_fingerprint(op)` give a stable structural hash of the operation catalog (kind, schemas, idempotency, authn, deadline facts) that ties a seed to the code that produced it.

- **In-memory outbound HTTP** — `MockHttpServicePort` with `MockHttpServiceAdapter`/`MockHttpRegistry` via `MockDepsModule(http=…)`, so an app runs under DST with no external services.

- **Mock transactions default to a journal** *(behavior change)* — `MockDepsModule(transactions="journal")` is now the default: a per-write undo journal lets concurrent transactions interleave while an aborted operation leaves no partial writes. The `none` and `strict` modes remain opt-in.

- **Mock MVCC isolation** — the journal manager enforces snapshot (rejects write-write) and serializable (also rejects read-write and phantoms) isolation via a buffered overlay; a conflict raises `exc.concurrency` with `serialization_failure` and no global lock.

- **Transaction isolation as a fail-closed contract** — operations declare an isolation level and the kernel verifies it against the route's manager, raising `exc.configuration` rather than silently running weaker isolation. Declaring isolation without a tx route is rejected at registry freeze.

- **Turnkey harness and unified config** — `forze_dst.Simulation` and `SimulationConfig`: given a registry, deps factory, and invariants, `Simulation.run(config, ...)` is the single exploration entrypoint. One config is the sole source of nondeterminism, and a violation minimizes to a reproducible counterexample.

- **Generative scenario model** — `Scenario`/`Rule`/`ModelState`, auto-derivation via `derive_scenario`, and reactive topology build model-based arrange-then-act workloads that pass domain validation. The scenario is inferred from the catalog and refined by probing the engine trace to drop cascade-only operations.

- **Schedulers** — `PCTScheduler` (depth-bounded bug guarantees), `SystematicScheduler` (DPOR exhaustive interleaving with effect-equivalence reduction), and seeded shuffle perturbation, all reproducible. Adds a generic workload fuzzer (`OpSpec`, `generate_workload`, `simulate_workload`).

- **Oracle** — a context-bound `Recorder`/`record_event` to an immutable `History`; `Invariant`s with built-ins like `no_duplicate_effect` and `mutual_exclusion`; `explore` plus greedy `minimize` to a reproducible `ViolationReport`. Adds a per-key linearizability checker (`linearizable`, `RegisterSpec`).

- **Coverage-guided exploration** — `behavioral_coverage` and `Simulation.coverage`: a PII-free signal (operation outcomes, port edges, injected faults) drives a self-right-sizing sweep that stops once coverage plateaus, returning `CoverageStats` and any counterexample it hits.

- **Declarative seeded faults and latency** — `FaultPolicy`/`FaultRule` and `LatencyProfile` set per-surface/route/op rates for error, timeout, crash, drop, duplicate, and delay plus per-route latency distributions, declared on `SimulationConfig` and seeded by construction.

- **Crash, restart, and recovery** — `SimulationConfig.crash` turns a run into a crash-restart-recovery scenario: the tx rolls back, a fresh runtime restarts over persisted `MockState`, and an optional recovery pass runs. `SimulationConfig.runtime=True` instead drives the workload through a real `ExecutionRuntime`.

- **Simulated I/O latency and cooperative scheduling** — under `run_simulation` a cooperative interceptor makes each port call a yield point so concurrency interleaves at real boundaries, and optionally advances the virtual clock by a per-port latency, surfacing races and time-dependent bugs with no artificial sleeps.

- **Engine trace folded into history** — `RuntimeTracer` captures the full execution surface (ports, transactions, operation boundary, domain dispatch) with virtual-time stamps and PII-free id-only keys; the harness folds it and projects operation outcomes from this one source. Adds trace-driven invariants.

- **Counterexample report** — `forze_dst.report` (`CausalGraph`, `format_report`, `ViolationReport.format()`) renders the minimized workload, the concurrency that triggered it, the per-span causal trace, an injected-environment timeline of faults, latency, and partitions, recorded facts, and the violated invariant.

- **Regression corpus** — `RegressionEntry`, `append_regression`, and `load_regressions` turn a found seed into a permanent, replayable JSON-Lines entry stamped with the registry fingerprint and violated invariants.

- **`forze` CLI** (`forze[cli]` extra) — `forze dst run module:sim` (exit 1 on a violation, CI-friendly) plus `replay`, `coverage`, `topology`, and `derive`. Import strings are forgiving and a bare registry gets an auto-mock with a safety-net invariant. Test-backed recipes ship for concurrency and virtual-time TTL.

- **Multi-runtime distributed DST** — `forze_dst.Cluster` with `Partition`/`PartitionSchedule` runs N real `ExecutionRuntime` nodes over one shared `MockState` from a single seed, under group-based network partitions and per-node faults, checked by ordinary distributed invariants. Violations minimize reproducibly.

- **Reachability ("sometimes") assertions** — `reached(label)` marks a hard state from inside code under simulation, and `SimulationConfig.reachability_targets` fails a sweep when a declared state was never reached, so a green invariant means the dangerous interleaving actually fired. Adds `sometimes(...)`.

- **Exact per-call DST attribution** — each operation terminal carries a correlation id and cascade invocations are flagged nested, so the trace projection pairs a terminal to the precise invoke it belongs to (not per-op FIFO). Per-call verdicts and the report's `call_id` are now exact even for concurrent same-op calls.

- **Lossy / asymmetric network partitions** — `Partition(loss=…)` makes a split a flaky link (each gated call drops with a seeded per-node probability) rather than a clean cut; `loss=1.0` (default) stays a hard partition, byte-identical to before. Overlapping windows take the strongest loss.

- **Strict behavioral-fingerprint regressions (opt-in)** — `behavioral_fingerprint(history)` digests a run's trace shape; `entry_from_report(…, strict_behavior=True)` stores it and `RegressionEntry.behavior_drifted` flags a replay that drifted despite an unchanged catalog fingerprint. Default stays structural-only.

- **Coverage-guided mutation** — `Simulation.coverage_guided(config, cases=…)` replaces the uniform seed sweep with feedback fuzzing: it keeps inputs that unlocked new coverage and mutates the productive ones under an AFL-style power schedule, reaching rare op combinations far sooner. Reproduces from the master seed.

- **Parallel timelines** — `forze_dst.parallel_sweep(run, seeds, workers=…)` fans a seed sweep across a process pool (seeds are independent), folding every worker's result into one `SweepResult`. The picklable `SimulationSeedRunner` resolves an import string per worker, so a whole app sweeps in parallel.

- **Failure artifact bundle** — `FailureBundle` captures a counterexample as one portable JSON file: the seed, the full `SimulationConfig` that produced it, the minimized workload, and the app import string. `replay_bundle(bundle)` rebuilds and re-runs it, so a bug reproduces on another machine.

- **Heavy-tailed latency distributions** — `LogNormal` and `Pareto` join `Constant`/`Uniform`/`Exponential` in a `LatencyProfile`. Their long right tail models realistic p99 blowups, surfacing timeout bugs a fixed delay never reaches. Sampled through the seeded latency RNG, so runs stay reproducible.

- **Value-level DST invariants** — `SimulationConfig(capture_values=True)` makes the trace carry a redacted view of write payloads and read results (off by default, so production tracing stays id-only and PII-free). New `read_your_writes(...)` and `expect_value(...)` assert on what was written or read, not just the key.

- **Time-travel timeline** — `ViolationReport.timeline()` / `build_timeline(history)` flatten a counterexample into a virtual-time-ordered stream of `TimelineEntry` steps covering operations, port calls with value flow, injected environment, and recorded facts. `render_timeline` prints it and each entry is JSON.

### Changed

- **Contract types are imported from their contracts home, not re-exported through the execution layer** — vestigial back-compat re-exports of contract value types from `forze.application.execution` subpackages were removed, so each type has a single canonical import path. Removed: `BreakerKey`, `CircuitBreakerStore`, `RateLimitStore`, `RateLimitKey`, `LatencyDigestStore`, `LatencyDigestKey`, `Transition` from `forze.application.execution.resilience` (and `CircuitBreakerStore` from `forze.application.execution`) — import from `forze.application.contracts.resilience`; `LifecycleModule` from `forze.application.execution[.lifecycle]` — import from `forze.application.contracts.execution`; `RoutedDeps`/`PlainDepsMap` from `forze.application.execution.deps` — import from `forze.application.contracts.deps`; `OutboxStagingContext` from `forze.application.execution.context` — import from `forze.application.contracts.outbox`; `LifecycleStep` from `forze.application.execution.lifecycle` — import from `forze.application.contracts.execution`; and the redundant middle re-export of `Deps`/`DepsModule` from `forze.application.execution.deps` (import them from the kernel surface `forze.application.execution` — unchanged, still front-doored from `forze` — or from `forze.application.contracts.deps`). The in-process *implementations* (`InMemoryCircuitBreakerStore`, …) are unchanged.

- **`GroupRef` (query grouping) renamed to `GroupField`** *(breaking: query DSL)* — the aggregate group-by dimension in `forze.application.contracts.querying` is now `GroupField`, resolving a name clash with the unrelated `GroupRef` (a group catalog reference) in `forze.application.contracts.authz` and pairing it accurately with its sibling `GroupTrunc` (it groups by a field path; it is a dimension, not a reference). Replace `GroupRef(field=…)` with `GroupField(field=…)` in aggregate group specs. The authz `GroupRef` is unchanged.

- **Search index provisioning split into a `SearchManagementPort`** *(breaking: `forze_meilisearch`)* — `ensure_index` and `delete_all` move off `SearchCommandPort` (now document writes only: `upsert`/`delete`) onto a new control-plane `SearchManagementPort`, acquired via `ctx.search.management(spec)` (registered under `SearchManagementDepKey`). Mirrors the `StreamGroupAdminPort` split — provisioning/wipe runs outside the request path and a read-only operation can't reach it. Move `ensure_index`/`delete_all` calls from `ctx.search.command(...)` to `ctx.search.management(...)`.

- **Search engine config as typed value objects** *(breaking: `forze_postgres`, `forze_mongo`)* — search `engine` now takes a tagged-union value object instead of a flat string plus parallel engine kwargs, so illegal combinations are unrepresentable. Bare engine strings remain shorthands; existing reads are unchanged.

- **Shared analytics ingest target** *(breaking: `forze_postgres`, `forze_bigquery`, `forze_clickhouse`)* — warehouse analytics configs take a single shared `IngestSpec` value object instead of per-backend flat `ingest_relation` and legacy `ingest_table` fields. Postgres also drops its legacy `schema` field.

- **Shared RRF fusion settings for federated search** *(breaking: `forze_postgres`, `forze_meilisearch`)* — federated merge config uses a shared `Rrf` value object instead of flat `rrf_k`/`rrf_per_leg_limit` fields, tying the fusion knobs to the rrf merge mode. The `federation`/`rrf` shorthands stay valid.

- **Empty filter/sort maps are no-ops on list/search requests** — a bare empty mapping for `filters` or `sorts` on the kit list/search DTOs normalizes to no filter/sort instead of raising. A structured-but-empty envelope is still rejected by the strict parser as a probable bug.

- **Lazy transaction acquisition, default for Postgres, Mongo, and Firestore** *(behavior change)* — a transaction scope defers connection checkout until the first operation, so it no longer holds a connection idle-in-transaction. A connect failure surfaces at the first operation; opt out with `lazy_transaction=False`.

- **`forze_mock` internal restructure** — misplaced root modules moved under `adapters/` (outbox, embeddings, resilience, re-exported from `forze_mock.adapters`) and the per-spec configurable factories moved to `forze_mock.execution.factories`. Top-level imports are unchanged; only deep-submodule imports need updating.

- **`forze_dst` internal restructure** *(breaking: imports)* — the harness splits into a thin `Simulation` facade over `engines/`, `oracle/`, and `artifacts/` subpackages, dropping top-level modules from 29 to 15. Top-level symbols now live in submodule namespaces, and `SchedulerKind` is removed.

- **Integration logger namespaces unified to `forze_<pkg>.*`** *(behavior change: log filters)* — `forze_redis`/`forze_postgres`/`forze_http`/`forze_firestore`/`forze_temporal` previously logged under bare prefixes (`redis.*`, `postgres.*`, …), matching the rest of the integrations now. Besides consistency this stops `redis.*` from inheriting the `redis` driver's own logger configuration. Update any log filters keyed on the old prefixes.

- **Notify kit: registration split from resolution** *(breaking: `forze_kits`)* — `NotificationRouter` is now a mutable builder (`register()` returns self, then `freeze()`); resolution (`resolve`/`resolve_or_raise`) moves to the immutable `FrozenNotificationRouter` the consumer holds, so the routing table can't change under a running consumer. Notification command models are frozen, and the package is reorganized into `routing` / `events` / `consumer` / `lifecycle` (public imports from `forze_kits.integrations.notify` unchanged except the new `FrozenNotificationRouter`).

### Fixed

- **Client-caused errors no longer masquerade as 500s** — a sweep across the query / cursor / aggregate / storage / search paths reclassified the `exc.internal` raises a caller can trigger and fix into the right kind. An **unsupported backend feature / capability limit** — a backend that can't aggregate, sort-only-by-primary-key cursors, `search_cursor` on an offset-only engine, the neo4j adapter's not-yet-implemented methods, Postgres array/text/ltree column-shape gaps, a duplicate-id batch, `mset` with `nx`+`xx`, backward analytics cursors — now raises `precondition` (HTTP 400); a **malformed / out-of-range value** — cursor `after`+`before` together, a non-positive `limit`/`offset`, an invalid cursor token, an unknown projection field — now raises `validation` (HTTP 422); instead of an opaque `internal` (500) deep in an adapter. The same situation maps to the same kind across every backend (mock ≡ Postgres ≡ Mongo ≡ Firestore), the caller-supplied aggregate DSL now reports parse errors like the filter DSL already did, and newly client-visible messages were scrubbed of internal detail (SQL type tokens, internal package names). Genuine server-fault guards are unchanged (still `internal`/500).

- **Mock rev-conflict now matches the real adapters** — a stale-revision write on the in-memory mock raised a generic `CONCURRENCY` error, while every real adapter (the shared persistence gateway) raises a `PRECONDITION` with code `revision_mismatch`. The mock — the outlier — now raises the identical `exc.precondition("Revision mismatch", code="revision_mismatch")`, so optimistic-concurrency handling (catch/retry on `revision_mismatch`) behaves the same in tests/DST as in production. Surfaced by the new real-Postgres conformance differential.

- **Faithful read-committed in the mock (no dirty reads)** — the in-memory mock's default transaction manager no longer writes through: at every isolation level a transaction buffers its document writes and publishes them at commit, so a concurrent transaction can never observe an uncommitted (later rolled-back) write. Read-committed reads through to the latest committed state per statement (non-repeatable reads / read skew still permitted); snapshot/serializable keep their as-of-begin snapshot. Concurrent rev-guarded updates to a row still conflict (first-committer-wins, matching Postgres read-committed's row serialization) while a blind rev-less write still loses silently. This removes a class of DST false positives the old write-through behavior could produce. Mainly affects tests/DST; production adapters are unchanged.

- **Notifications can run through the queue consumer (dedup + poison parking)** — `notification_consumer_lifecycle_step(...)` and `notification_queue_consumer_handler(...)` route notifications through `QueueConsumer`, so an at-least-once redelivery no longer re-sends (inbox dedup on the deterministic event id) and poison messages are parked. The transactional-notifications recipe now drains via the consumer instead of a hand-rolled receive/ack loop.

- **Queue consumer warns when a poison ceiling can't be enforced** — when `max_deliveries` is set but the backend does not report a delivery count (so in-app parking can never trigger), the consumer logs one warning per run pointing at the broker's dead-letter/redrive policy, instead of silently looping a poison message forever.

- **CQRS read-only guard now covers eager (factory-time) port acquisition** — a QUERY operation whose handler factory acquired a command (write) port at build time — `lambda ctx: Handler(port=ctx.document.command(spec))`, the common kit pattern — previously slipped past the read-only guard, which was only set later inside the call. The handler is now built under the read-only flag for any QUERY operation, so eager acquisition hits the same guard as a call-time one (raised at first resolve). Read-port acquisition and COMMAND operations are unaffected.

- **Filtering a randomized-encrypted field now fails closed** — a query predicate on a field encrypted with randomized (non-searchable) encryption silently matched nothing (plaintext literal compared against ciphertext at rest); it now raises `precondition` (`code="core.crypto.encrypted_field_not_filterable"`) at the shared codec seam, so every document/search backend inherits it. Query by equality using a deterministic searchable field instead.

- **Encrypted-sort rejection now covers every search backend** — refusing a sort on a field-encrypted column (no usable order at rest, and the raw value leaks into the keyset cursor token) was wired only on Mongo. The guard now runs once in the shared offset executor — so Postgres, Mongo, and Meilisearch all inherit it — plus the Postgres cursor path (Mongo's cursor guard stays inline). Raises `core.search.encrypted_sort_field`.

- **VK login no longer copies the untrusted introspection envelope into claims** — the `public_info` verifier emitted the full unverified response (including top-level protocol/envelope fields) as identity claims; it now keeps only the masked `user` object the subject is derived from, so attacker-influenceable envelope fields cannot reach downstream claim/tenant mappers.

- **A missing dependency now reports as a legible configuration error** — looking up an unregistered port (a forgotten `DepsModule` entry) raised an opaque `internal` error; it now raises `configuration` and names what *is* registered (the plain dependency or route inventory) with a "did you forget a DepsModule entry?" hint. It stays a server-side 500 (a wiring fault is the server's, and the detail is never exposed to clients) — the win is an actionable message in logs instead of a generic internal error indistinguishable from a crash.

- **Log scrubbing no longer corrupts ordinary text** — sensitive-word scrubbing of log string values now requires a secret-bearing shape (`session=…`), not a bare word, so paths like `/v1/authn/login` survive intact while the value after a sensitive key is fully masked. Key-name masking of structured fields is unchanged.

- **Outbox relay tenancy** — the background relay now binds each claim's tenant before publishing, so a tenant-aware destination routes per-tenant instead of the global key. A tenant-aware outbox on the plain (non-sharded) relay fails closed with a clear `outbox_relay_tenant_unbound` error.

- **Keyring fill-lock stripe is now cross-process stable** — the per-`key_id` crypto fill-lock stripe used Python's `hash()` (PYTHONHASHSEED-randomized), so it varied per process and broke deterministic-simulation replay. It now uses a stable hash, and the guard bans the `hash(x) % n` pattern.

- **Bad query fields are a client error (400), not a server error (500)** — a query-time sort, filter, or sort-direction value that names a field absent from the read model (or an invalid direction/null placement) now raises a `precondition` (HTTP 400, `code="field_not_on_read_model"` / `"invalid_sort_value"`) instead of a `configuration` error masked as a 500. This is caller-supplied input, so the status now reflects who's at fault, and the detail reaches the client. A spec's own `default_sort` naming an unknown field stays a `configuration` error (500) — that's the author's misconfiguration. Behavior is uniform across Postgres, Mongo, Firestore, and the mock, and still covers computed (never-stored) fields.

- **FastAPI API-key `prefix:key` parsing fixed** — the `X-API-Key` resolver now splits on the first colon so `prefix:secret` yields the bare secret, matching `forze_mcp`; previously it split on whitespace and passed the whole value, failing verification. Bare keys still authenticate.

- **Meilisearch search terms strip embedded quotes** — an embedded `"` is removed so it can no longer break phrase boundaries or split the query.

- **Numeric timezone offsets validated** — offsets require two-digit hours (so `+123` no longer parses as `1:23`) and reject values beyond the real ±14:00 maximum.

- **`forze dst --seeds` parsing fails loud** — a reversed range, leading dash, or non-numeric input raises a parameter error, and ranges inside comma lists are accepted, instead of crashing or silently running zero seeds.

- **S3 multipart part-ETags normalized** — a whitespace-padded ETag is collapsed to a single quote pair instead of being double-wrapped.

- **`If-None-Match` parsed per RFC 7232** — quoted entity-tags are extracted with quote-aware, list-anchored matching, so an opaque tag containing a comma no longer shreds the list, `*` matches, and a malformed weak tag is not treated as weak.

- **S3 range downloads handle an unknown total** — a `Content-Range` whose total is unknown (`*`, seen on S3-compatible gateways) synthesizes the total from the satisfied range instead of returning zero.

- **`forze_http` suppresses its default bearer when an Authorization header is already set** — under any header casing, avoiding duplicate, conflicting credentials.

- **GCS rejects reserved object-metadata keys** — keys in the `forze-tag-` namespace are rejected at write time, since they would otherwise be misread as tags on read-back.

- **Database error classification keys on error codes, not message text** — mappers no longer match English substrings in driver messages (which break under non-English locales). Postgres keys on SQLSTATE, ClickHouse on the numeric code, Mongo on the operation-failure code, and Redis on the leading RESP error token.

- **Mongo query renderer rejects `$`-prefixed field names** — a field path segment beginning with `$` (e.g. `$where`) is rejected with `exc.precondition` instead of being emitted as an operator, closing an injection path when untrusted field names reach a filter. Stored fields never start with `$`.

- **Mongo index introspection no longer crashes on special indexes** — `listIndexes` direction is kept verbatim instead of being cast to int, so text, 2dsphere, 2d, hashed, and vector indexes (string directions) no longer raise during index validation.

- **BigQuery array and null query parameters are typed from field annotations** — an empty list parameter emits a typed `ARRAY` from the annotation instead of an invalid one BigQuery rejects, a `None` for an optional field carries its real type rather than always string, and the array element type prefers the annotation.

- **Per-tenant routed clients no longer crash on multi-host DSNs** — `connection_string_fingerprint` now fingerprints the full host list from the raw authority instead of a single parsed host, which raised `ValueError` on the comma-separated form used by Mongo replica sets, Redis Sentinel, and AMQP clusters.

- **Postgres schema validation accepts parameterized column types** — a field over `NUMERIC(10,2)` or `TIMESTAMP(3) WITH TIME ZONE` is no longer rejected: type compatibility compares modifier-insensitively while still carrying the modifier, so casts keep precision and scale.

- **`forze_postgres` search index-definition parsing hardened** — index expressions parse via a balanced-delimiter, quote- and dollar-quote-aware scanner. PGroonga resolution accepts more array and cast forms but fails closed on ones it cannot reproduce; GIN-to-FTS detection keys on a real `to_tsvector(...)` call.

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
