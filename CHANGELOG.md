# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Faithful mock transactions (`MockDepsModule(transactions="journal")`, now the default)** — the in-memory mock transaction manager is now **atomic without serializing**: each write to a participating store records an undo entry in a per-transaction journal, and an aborted transaction replays the journal in reverse to undo *only its own* writes — never a whole-store restore — so concurrent transactions interleave freely (the basis DST needs) while a failed operation leaves no partial writes. Write-write conflicts are caught by the document row `rev` (optimistic concurrency); a read-only root rejects writes to participating stores. This makes simulation/DST findings **trustworthy**: e.g. the payments example's "double charge" was a *false positive* under the old no-op manager (the loser's aborted payment persisted); under faithful atomicity the loser's whole transaction rolls back and the app is correctly seen as race-free. The legacy no-op manager (`transactions="none"`, writes persist through rollback) and the serializing strict manager (`transactions="strict"`, or `strict_tx=True`) remain opt-in. Operations participate when they carry a transaction route (`OperationPlan().bind_tx().set_route(...)`), as real write operations do.
- **Turnkey simulation harness (`forze_dst.Simulation`)** — drive an app's real operations under DST with almost no boilerplate: give it a `FrozenOperationRegistry`, a deps factory (typically `lambda: MockDepsModule(...)` — one module auto-mocks every port, fresh per run), and the invariants that must hold, then call `.explore(cases=[OperationCase(op=…)], count, concurrency, seeds)`. It generates a seeded workload of real operations (inputs auto-built from each operation's `input_type` via polyfactory — `forze[dst]` extra — or a per-case override), invokes them through `run_operation` on the virtual-time loop under scheduler perturbation, records every operation automatically, checks the invariants, and on a failure minimizes to a reproducible counterexample stamped with the operation-catalog fingerprint. `setup`/`observe` hooks seed and inspect state.
- **Systematic interleaving exploration (`Simulation.explore_scenario_dpor`)** — the complete, deterministic complement to the probabilistic PCT/Hypothesis search. It fixes one act workload, then walks the tree of per-tick scheduling choices depth-first (via a new `SystematicScheduler`), guaranteed to find any violation reachable by *reordering* that workload within `max_runs`. A partial-order reduction prunes the search — an interleaving whose observable effect order matches one already seen is not expanded. In the DPOR family but at the loop's tick granularity (reduction by observed effect-equivalence rather than a per-memory-access independence relation): sound and robust, not optimal per-access DPOR. The violating `schedule` reproduces exactly.
- **PCT scheduler (`forze_dst.PCTScheduler`)** — a principled interleaving search to replace the uniform shuffle. Probabilistic Concurrency Testing (Burckhardt et al., ASPLOS 2010) gives each task a random priority, runs ready tasks highest-first, and inserts `d-1` random priority-change points that demote the running task — provably finding any depth-`d` concurrency bug with probability ≥ `1/(n·k^(d-1))`, far better than a random walk for deep, specific orderings. The simulation loop and `run_simulation` take a pluggable `scheduler`; `Simulation.explore_scenario(..., scheduler_factory=pct_scheduler_factory(depth=, steps=))` drives it per seed. Seeded and reproducible like the shuffle; applied at the loop's tick granularity.
- **Hypothesis-driven exploration (`Simulation.explore_scenario_hypothesis`)** — drives a generative scenario with Hypothesis (an optional `forze[dst]` dependency) as the generate-and-shrink engine: it searches the `(seed, act-plan)` space and, on a violation, shrinks to a minimal counterexample with its general-purpose shrinker — simplifying the seed and the act sequence far past `explore_scenario`'s greedy drop. Each candidate still runs on the deterministic loop, so the report reproduces exactly. Finds and shrinks the double-charge to the two minimal racing payments.
- **`forze` CLI (optional `forze[cli]` extra)** — a Typer console-script to run and inspect DST against an app's operations with no driver script: `forze dst run module:simulation` loads a `Simulation` from an import string, explores the auto-derived scenario (`--strategy scenario|hypothesis|dpor`, `--pct`, `--seeds`), and prints the counterexample (exit 1 if a violation is found — CI-friendly); `forze dst topology` prints the recovered reactive cascade map; `forze dst derive` prints the inferred scenario; `forze --version` reports the installed version. A thin surface over the framework's introspectable assets — it deliberately does not wrap dev tasks (those stay in the `justfile`). The `cli` extra pulls only Typer; the `dst` commands additionally need the `dst` extra (a `require_dst()` guard prints a clear hint if it's missing), and the always-installed `forze` script fails gracefully with an install hint when `cli` itself is absent. The import string is forgiving: `module:attr` resolves a `Simulation`, a bare `FrozenOperationRegistry` (auto-wrapped with an auto-mocking `MockDepsModule` **plus the built-in `no_unexpected_error` safety net** — so an uninstrumented app is still checked for operations that crash under concurrency/faults), or a callable returning either; `module` alone discovers the one a module exposes. A runnable, test-backed example lives at `examples/recipes/dst_payments/` — a *real* forze app (handlers talk to `ctx.document` and emit a domain event, with **no DST calls in the handlers**; observation is a test-side `observe` hook), where a charge written before the optimistic-concurrency-guarded transition double-charges under concurrent payment.
- **`no_unexpected_error` invariant** — a zero-instrumentation built-in: every operation that raised a non-`CoreException` exception (a bug — `KeyError`, `TypeError`, …) is a violation, while declared domain failures pass. The default check for the CLI's ad-hoc/bare-registry path, and the basis for "point DST at any forze app and immediately find crash bugs".
- **Reactive cascade topology (`Simulation.reactive_map` / `forze_dst.ReactiveMap`)** — recover the saga/event wiring that's invisible to static inspection (operation registries hold opaque callables). Probing each operation and reading the hardened engine trace reveals, per operation, the operations it triggers as a *cascade* (B3 op-invokes the harness never drove) and the **domain event types** dispatched along the way (B4 dispatch records) — recovering the event→operation linkage at runtime. `ReactiveMap` exposes `cascades`, `events`, `entry_points()`, and a `format()` rendering; `derive_scenario` uses it to drop cascade-only operations from the act set. The `Simulation.deps` factory may now also return *several* modules (app plane + e.g. a domain-event-wiring module).
- **Auto-derived scenarios (`forze_dst.derive_scenario`)** — infer a draft `Scenario` from the operation catalog so authors hand-write zero rules in the common case. A name-driven heuristic reads the data-dependency graph: a creation-verb op (`create_order` → produces `order`) becomes an arrange rule capturing the real returned id; an op with an input field referencing a known entity (`order_id` → requires `order`) becomes an act rule that fills the field from the arranged pool (other fields auto-generated). Best-effort, not an oracle — the draft is a starting point to refine, with overridable verb set / `arrange_each`. Drives the full create→pay→double-charge counterexample with no hand-written model. `Simulation.derive_scenario(...)` goes further: after the static derivation it **probes** (firing each candidate op once and diffing the engine trace) to drop operations that only ever run as cascades — saga steps and domain-event handlers — since those fire automatically and shouldn't be driven as standalone entry points (the registries hold opaque callables, so this reactive structure is only knowable at runtime).
- **Generative scenario model (`forze_dst.Scenario`)** — meaningful workloads instead of random noise. Independent random ops bounce off domain validation (you can't `pay` an order never created); a `Scenario` fixes that with a two-phase, model-based design driven by `Simulation.explore_scenario(...)`: an **arrange** phase fires `Rule`s serially to build valid state, capturing each op's real return (e.g. a created id) into a `ModelState` pool; an **act** phase samples rules that operate on the arranged state (building inputs from the captured handles) and runs them concurrently under perturbation. A `Rule` declares its `requires` (state pools that gate it — the default precondition), how it builds its `arg`, and what it `produces`. Because the run is seeded, arrange replays identically, so the act phase is minimized to a 1-minimal counterexample whose handles stay valid. Surfaces coherent concurrency bugs (e.g. a check-then-set double-charge across racing payments on a real arranged order) the random harness can't reach.
- **Observed causal graph + counterexample report (`forze_dst.report`)** — a recorded run is no longer a flat event log: `CausalGraph.from_history(history)` reconstructs operation **spans** (each call's invoke→return interval, in recorder-sequence space so concurrent ops sharing a virtual-time stamp are still seen as a race), the trace **steps** (port/transaction/dispatch side effects) each op caused, and which spans overlapped. `ViolationReport.format()` (and the free `format_report`) renders it as a readable counterexample — the minimized workload, the concurrency that triggered it, the per-span causal trace, recorded facts, and the violated invariant — instead of a wall of events. The harness emits an `op_start` marker per call to anchor spans.
- **Port interception seam (`forze.application.execution.interception`)** — a public, composable middleware chain around resolved configurable ports: a `PortInterceptor` (onion `around(call, nxt)`) can yield, delay, short-circuit, raise, or post-process each async port call. Register deps-scoped via `DepsRegistry.with_interceptors(...)` or run-scoped via `bind_interceptors(...)`; the chain runs **innermost** — inside the runtime-tracing and resilience-policy wraps, so a fault interceptor's transient error stays retryable by the policy. Zero cost in production (no interceptors ⇒ the port is returned bare). This is the seam DST plugs cooperative yielding, simulated latency, and (next) fault injection into, without touching application handlers.
- **Simulated I/O latency + cooperative scheduling under simulation** — under `run_simulation`, the built-in `CooperativeInterceptor` (registered run-scoped on the interception seam above) makes each port call (a) **yield** to the loop, so concurrent operations interleave at the real port boundaries (a real adapter suspends on I/O; the in-memory mocks don't) — the scheduler (PCT / DPOR / perturbation) then explores those interleavings; and (b) optionally **advance the virtual clock** by a per-port *latency* — a `(surface, route, op) → seconds` model passed as `Simulation(latency=…)` / `run_simulation(latency=…)`, so a real downstream's round-trip costs simulated time. Together these mean **both concurrency races and time-dependent bugs surface with no artificial `await asyncio.sleep(...)` in application handlers** — the I/O characteristics live in the simulated environment, configured test-side, not in production code (a no-op in production, where no interceptor is registered). The folded causal trace also shows each step's **route** (the spec / transaction route), e.g. `document_command[orders].create` vs `document_command[payments].create`.
- **Time-travel example (`examples/recipes/dst_reservation_ttl/`)** — a test-backed example demonstrating DST over *virtual time*: a reservation whose confirm charges through a **slow payment downstream** (latency configured on the Simulation, not a handler sleep) that outlasts the TTL, so it confirms after expiry. DST fast-forwards the latency instantly and deterministically (counterexample stamped at `t=600s` while the run takes real-wall milliseconds) — the time twin of the concurrency example, with handlers that are ordinary forze code.
- **Engine trace folded into the DST history** — the core `RuntimeTracer` now captures the full execution surface (async-generator port methods like queue `consume`, an `operation` invoke/complete/error boundary in `run_operation`, and `domain` event `dispatch`), and every `TracingEvent` carries an `at` virtual-time stamp (via the `monotonic()` seam). `Simulation` enables runtime tracing and folds the engine's trace into the recorded `History` (as `trace` events, each keeping its own stamp), so invariants and counterexample reports see port/transaction/operation/dispatch activity — high-granularity failure capture — without handler code recording anything explicitly.
- **Operation catalog fingerprint** — `FrozenOperationRegistry.fingerprint()` / `operation_fingerprint(op)` produce a stable, structural hash of the operation catalog (kind, input/output JSON schema, idempotency/authn/required-permission/deadline facts, tags). A version tag for catalog/contract drift and for tying a simulation seed to the code that produced it. Structural, not behavioral — it does not hash handler code or a hook's internal config, so a differing fingerprint means "cannot be trusted to reproduce" while a matching one means "same contract, probably reproducible".
- **Ambient entropy seam (`EntropySource`)** — `forze.base.primitives` adds `EntropySource`/`SystemEntropySource`/`SeededEntropySource` + `current_entropy_source`/`bind_entropy_source` (and a seam-routed `token_urlsafe` helper), the entropy twin of the `TimeSource` seam: random bytes/bits/floats, the stdlib-`random` API (via `EntropySource.as_random()`), and random `uuid4` ids read the context-active source, so a scope can make them deterministic and seed-replayable without changing call sites. Default is the system CSPRNG (production unchanged); `SeededEntropySource` is simulation-only and not cryptographically secure. AEAD nonces, backoff/relay jitter, opaque identity tokens, and the no-arg `uuid4()` / `uuid7()`'s random bits now route through it — binding both a `FrozenTimeSource` and a `SeededEntropySource` makes the full UUID (and an end-to-end aggregate→outbox→inbox run) byte-identical (previously a uuid's low 54 bits were always random). A determinism guard (wired into `just quality`) fails the build if raw entropy primitives reappear outside the seam.
- **In-memory outbound HTTP (`MockHttpServicePort`)** — `forze_mock` adds `MockHttpServiceAdapter` + `MockHttpRegistry`, wired into `MockDepsModule` via a new `http=` knob, so an app's `HttpServicePort` calls resolve in-process against registered per-operation handlers (args validated against `args_type`, results coerced to `return_type`) — closing the last gap to running a full app with **zero external services**. Unprogrammed operations fail loudly (`code="mock.http.unprogrammed"`). The handler seam is where simulation fault injection will later attach.
- **Monotonic time seam** — `TimeSource` gains `monotonic()` and `forze.base.primitives` adds a free `monotonic()` (the relative-timing twin of `utcnow()`). Deadlines, resilience (executor/breaker/rate-limit/bulkhead/latency-digest clocks), tenancy fingerprint TTLs, the L1/document cache, and the mock queue/dlock now read it instead of `time.monotonic()` directly — so a bound clock controls relative time too. Default is `time.monotonic()` (production unchanged); `FrozenTimeSource` keeps real monotonic (only its wall clock is frozen).
- **Deterministic simulation runtime (`forze_dst`)** — a dedicated, **native**, dependency-free virtual-time event loop package: `SimulationEventLoop` (a `BaseEventLoop` with a virtual clock + null, I/O-refusing selector), `SimulationTimeSource` (wall/monotonic/ids off the loop clock), and `run_simulation(scenario, *, seed, epoch)`. A workload spanning hours of `asyncio.sleep` / deadlines / backoff runs in real-wall milliseconds, and same `(scenario, seed)` reproduces byte-identically — the full aggregate→outbox→inbox flow included. Real I/O and thread executors are refused (`RealIOForbidden`) as a leak guard; a quiescent loop raises `SimulationDeadlock` instead of hanging. The determinism guard now also bans raw `time.monotonic`/`time`/`datetime.now` outside the seam (`perf_counter` stays allowed for observability).
- **Seeded workloads & scheduler perturbation** — `run_simulation(..., schedule_seed=)` (opt-in) shuffles the ready-callback queue each tick from a separate seeded RNG, exploring concurrent interleavings FIFO never reaches (kept distinct from app entropy, so scheduling and values vary independently); same `schedule_seed` replays the interleaving, so any bug reproduces. `forze_dst` adds a generic workload fuzzer — `OpSpec` catalog, `generate_workload(seed, count)` (weighted, deterministic), `run_workload(concurrency=)`, and `simulate_workload(...)` (generate + perturbed run) — so a single seed produces a reproducible, interleaving-exploring fuzz of any app's operations. Surfaces order-dependent concurrency bugs (e.g. lost updates) and reproduces them from the seed.
- **Recorder + invariant oracle + minimization** — `forze_dst` adds the oracle layer that turns a simulation into a checkable history. A context-bound `Recorder` collects `record_event(kind, **fields)` calls (virtual-time stamped) into an immutable `History`; `Invariant`s assert over it — built-ins `no_duplicate_effect` (exactly-once effect), `monotonic_per` (HLC/sequence monotonicity), and a generic `expect(...)`, plus any app callable. `explore(build, items, invariants, *, seeds)` searches seeds (optionally perturbing the scheduler) for a violation, then `minimize`s the workload (greedy delta-debugging) to a 1-minimal counterexample and returns a `ViolationReport` with the seed, the minimal workload, and its history — reproducible exactly via `run_recorded`.
- **Multi-node simulation + DST in CI** — N replicas run concurrently on the deterministic loop over one shared in-memory backend (the existing primitives compose into a cluster: `asyncio.gather` of replica coroutines sharing a `MockState`, under scheduler perturbation). A new `mutual_exclusion` invariant validates a distributed lock / critical section across replicas (no overlapping holds); a correct dlock serializes contenders across every seed while skipping the lock is found and minimized. The DST exit tests run in the normal suite every CI, plus a fixed **seed corpus** (regression band — correct components stay clean, known bugs stay caught) and a `fuzz`-marked extended sweep run via `just fuzz` (excluded from the default `just test`; intended for a nightly job).
- **Linearizability checking** — `forze_dst` adds a Wing-Gong/Lowe linearizability checker (memoized recursive linearize/lift), **P-compositional** (partitioned per key) so it scales. Record operation intervals with `record_operation(key, op, args)`, define a `SequentialSpec` (built-in `RegisterSpec` provided), and assert the `linearizable(spec)` invariant — it flags any key whose recorded history admits no real-time-consistent total order reproducing the observed results (e.g. a stale read after a completed write). Plugs into `explore`, so a non-linearizable object is found and minimized like any other violation.
- **Seam-based port fault injection (`forze_dst.PortFaultInterceptor`)** — a seeded `PortInterceptor` that raises a transient `exc.infrastructure` at a matched port boundary (filter by `surface`/`route`/`op`) over **any** resolved port, via the core interception seam — the registry-wide successor to `FaultyQueueCommand` (which wraps one queue port by hand). Registered per-run on `Simulation(interceptors=lambda seed: …)` (a seed-derived chain, fresh per run → reproducible) and placed inside the resilience wrap, so the injected transient is retryable by a declared policy. A partial-failure invariant violation it surfaces (a charge that persists when the subsequent state transition is faulted) is rolled back cleanly when the operation is transaction-routed — faults compose with faithful transactions.
- **Transport fault injection** — `forze_dst` adds `TransportFaultPolicy` + `FaultyQueueCommand`, a seeded wrapper over any `QueueCommandPort` that injects the failures a real broker exhibits: transient enqueue errors (forcing at-least-once retry), duplicate delivery (exercising inbox idempotency), silent drops (broker loss), and per-message delays (which reorder arrivals in virtual time). Faults are driven by a dedicated RNG — independent of app entropy and the scheduler — so a fixed fault seed replays the exact failure sequence and any bug reproduces. Under it, an idempotent (inbox-deduped) outbox→queue→inbox flow holds at-least-once + exactly-once-effect, while a non-idempotent consumer's double-apply and a lossy broker's at-least-once violation are both caught. The wrappers target the core port interfaces, so `forze_dst` depends only on `forze` core.

### Changed

- **`forze_mock` internal restructure** — the package's misplaced root modules moved under `adapters/` (`forze_mock.outbox_adapter` → `forze_mock.adapters.outbox`, `forze_mock.embeddings` → `forze_mock.adapters.embeddings`, `forze_mock.resilience` → `forze_mock.adapters.resilience`; all also re-exported from `forze_mock.adapters`), and the per-spec `Configurable*` factories were extracted out of the `MockDepsModule` module into `forze_mock.execution.factories`. Public top-level imports (`from forze_mock import …`) are unchanged; only direct deep-submodule imports of those three modules need updating. `forze_mock` is now under coverage enforcement (95.8%).

## [0.4.1] - 2026-06-17

### Added

- **Mergeable quantile sketch (`DDSketch`)** — `forze.base.primitives` adds `DDSketch`/`WindowedDDSketch`: a relative-error sketch answering any quantile and mergeable across streams (fleet-wide / multi-quantile latency). Complements `P2Quantile`.
- **Hybrid Logical Clock (`HybridLogicalClock`)** — `forze.base.primitives` adds `HybridLogicalClock`/`HlcTimestamp`: a skew-tolerant causal clock (reads the ambient `TimeSource`) with an optional `max_drift` guard.
- **Causal outbox ordering** — opt-in `hlc_ordering=True` on `PostgresOutboxConfig`/`MongoOutboxConfig` stamps events with an HLC and claims them in causal order across replicas (relay forwards the untrusted `HEADER_HLC`, drift-guarded). Off by default; **Postgres requires adding an `hlc BIGINT` column first** (legacy rows fall back to `created_at`).
- **Fleet-wide adaptive-bulkhead congestion signal** — the AIMD latency-quantile signal flows through a pluggable `LatencyDigestStore` (default in-process windowed-P², behavior-preserving); `forze_redis` adds `RedisLatencyDigestStore` so the limit reacts to the fleet's p95. Opt-in via `ResilienceDepsModule(latency_digest_store=…)`.
- **Prioritized load shedding** — opt-in `prioritized=True` on `BulkheadStrategy`/`AdaptiveBulkheadStrategy` makes the wait queue criticality-aware via the new task-scoped `Criticality` + `bind_criticality`. No-op until enabled; requires `max_queue >= 1`.
- **Delay-based bulkhead (`GradientBulkheadStrategy`)** — a third bulkhead kind (Gradient2) that tunes concurrency from the latency gradient with no `latency_threshold`. Mutually exclusive with the other bulkhead kinds.

### Changed

- **Quantile estimators relocated** — `P2Quantile`/`WindowedP2Quantile` moved from `forze.application.execution.resilience.quantile` to `forze.base.primitives` (co-located with `DDSketch`; now public `base.primitives` exports). The old module path is removed; internal resilience wiring is unaffected.

### Fixed

- **Typing annotations** — type-only imports moved under `TYPE_CHECKING` with forward references (including the runtime-optional OpenTelemetry types), so affected modules import cleanly without those optional dependencies installed and skip needless runtime imports.

## [0.4.0] - 2026-06-17

### Added

**Encryption — envelope encryption, BYOK & at-rest sealing across every plane (opt-in, off by default):**

- **Envelope-encryption core.** `forze.base.crypto` ships the self-describing `EncryptedEnvelope` wire format (`pack_envelope`/`unpack_envelope`/`is_envelope`; rotated keys still decrypt historical data), the `Aead` protocol, and `AesGcmAead` (AES-256-GCM) / `ChaCha20Poly1305Aead`. `contracts.crypto` adds the async `KeyManagementPort` (`generate_data_key`/`unwrap_data_key` — the BYOK seam, the KEK never leaves the backend), `KeyRef`/`DataKey`, `EnvelopeCipher`, the shared `FieldEncryption` policy (`encrypted`/`searchable`/`binds_record_id`), and a fail-closed `required_encryption` floor (`none < field < envelope`; `EncryptionTier`, `encryption_satisfies`, `validate_required_encryption`). Adds `cryptography` to core deps.
- **Per-tenant keyring + wiring.** `KeyDirectoryPort` (`StaticKeyDirectory` / `TenantTemplateKeyDirectory` for BYOK) resolves a tenant→KEK; `Keyring` (`forze.application.integrations.crypto`, a `BytesCipherPort`) is the async caching bridge — DEK reuse bounded by `max_dek_messages`, `warm(tenant)`, striped fill-locks + bounded LRU. `CryptoDepsModule(deterministic_root=…)` composes the stack and registers the dep keys; `forze_mock` ships real `AesGcmAead` + dev-only `MockKeyManagement`.
- **At-rest sealing across every persistence & transport plane** — each driven by a `…Spec(encryption=…)` / `encrypt=` policy, fail-closed at wiring (`core.<plane>.encryption_wiring`), tolerant of legacy plaintext (envelope sniff), no-op for plaintext routes:
  - **Object storage** — `S3StorageConfig`/`GCSStorageConfig` `encrypt=True`; AAD binds bucket/key/tenant; presigned URLs refused (bypass the keyring).
  - **Document fields** — Postgres/Mongo/Firestore via `DocumentSpec(encryption=FieldEncryption(encrypted={…}))` + `EncryptingModelCodec` (sync codec bridged to async KMS by a warm pre-pass; cold call raises `core.crypto.cipher_not_warm`). `binds_record_id=True` binds the record `id` into the AAD (transplant resistance; bulk-update of a bound field refused with `core.crypto.record_id_required`); `reencrypt_documents` upgrades legacy ciphertext. Typed and raw projections decrypt transparently.
  - **Searchable (deterministic) fields** — `FieldEncryption(searchable={…})` via `DeterministicFieldCipher` (AES-SIV, per-`(tenant,field)` HKDF, no KMS) so equality/membership filters rewrite to match ciphertext at the shared chokepoint (all document backends; unsupported positions raise `core.crypto.searchable_op_unsupported`). Zero-downtime root rotation: `deterministic_previous_root` matches **both** keys (`search_variants`), `reencrypt_documents`, then drop. Trade: leaks equality/frequency within a tenant.
  - **Search reads** — `SearchSpec.encryption` (the *same* policy object as the document spec, so they can't drift): Meilisearch seals on `upsert`, in-place Postgres FTS/vector + Mongo decrypt sealed fields out of results. Decryption once on raw rows (`decrypt_search_rows`) before decode, across every read path — offset/cursor, `select_search`, raw `project_search`, hub (`HubSearchSpec.encryption`), federated, and snapshot re-pagination.
  - **Analytics & graph** — `AnalyticsSpec` / `GraphNodeSpec`/`GraphEdgeSpec` `encryption`; sealed on ingest/write, decrypted out of every read/traversal path (shared `decrypt_rows`) on Postgres/ClickHouse/BigQuery/DuckDB and Neo4j get/neighbors/expand/shortest-path/scoped-walk. Encrypted columns/properties are confidential, *not* analyzable/matchable; analytics rejects `binds_record_id` (no stable id), graph binds the kind's `key_field`.
  - **Outbox & direct messaging** — `OutboxSpec.encryption` (`none` / `at_rest` / `end_to_end`) and `QueueSpec`/`StreamSpec`/`PubSubSpec` `encryption` (`none` / `end_to_end` — a transport has no at-rest store; the outbox owns that tier); AAD binds `(tenant, event_id)` from forwarded headers; works over SQS/RabbitMQ/Redis streams+pubsub. Backend-agnostic decorators (`encrypting_{queue,stream,pubsub}_command`) seal direct-published payloads so they are **interchangeable** with relayed ones; `decrypt_consumed_payload` handles both. `QueueCommandPort.enqueue_many` gains `message_headers` (per-message) so an encrypted batch still ships in one `SendMessageBatch`.
  - **Durable payloads** — Temporal (`TemporalConfig(encrypt_payloads=True)` via the native `PayloadCodec` seam, `encrypting_data_converter(...)`, runs outside the workflow sandbox) and Inngest (`DurableFunctionEventSpec(encrypt=True)`; the `_forze` routing envelope stays plaintext). Per-tenant BYOK; *a Temporal worker must be built from the same encrypting client to decode.*
  - **Cache, search snapshots & idempotency results** — the distributed cache body (bound `(tenant, pk)`), ranked-search snapshots (`(tenant, run id)`), and `IdempotencySpec(encrypt_result=True)` results (`EncryptingIdempotencyPort`, AAD `(tenant, op:key)`) are sealed automatically when the underlying route encrypts, closing plaintext re-exposure in Redis/Postgres. The in-process L1 stays plaintext in memory (process-scoped).
- **Vault Transit KMS (`forze_vault`)** — `VaultTransitKeyManagement` implements `KeyManagementPort` on Transit (`transit_generate_data_key`/`transit_decrypt`/`transit_rewrap`, `VaultConfig.transit_mount`, opt-in `VaultDepsModule(key_management=…)`); `VaultTransitTenantProvisioner` creates a tenant's Transit key from the same `KeyDirectoryPort` (idempotent; teardown opt-in via `allow_deletion`).
- **BYOK access-token signing + JWKS** — pluggable `SignerPort`; ships `Hs256Signer` (default, behavior-preserving), `LocalAsymmetricSigner` (RS256/ES256), `forze_vault.VaultTransitSigner`; `kid`-aware verification across the issuer + `AuthnKernelConfig.access_token_verifiers` (rotate by overlap); `forze_fastapi.attach_jwks_route` + `jwks_document(*signers)` publish `/.well-known/jwks.json` (symmetric secrets never published). *Breaking: `AccessTokenService(secret_key=…)` → `AccessTokenService(signer=Hs256Signer(secret=…))`; `issue_token`/`verify_token` are now awaitable; `AccessTokenConfig.algorithm` removed.*
- **Crypto & signing observability** — `instrument_crypto(...)` (`CryptoKeyringStats`: data-keys generated/unwrapped, cache hits, cold miss) and `forze_identity.authn.instrument_signing(...)` (`SigningStats`: tokens signed/verified/verify_failed, by `kid`/alg); always-on.

**Multi-tenancy hardening:**

- **Declared-minimum tenant isolation, fail-closed at wiring** — every deps module accepts `required_tenant_isolation` over `none < tagged < namespace < dedicated`, enforced **per route** (a single unscoped sibling fails it); each integration declares its `max_supported_isolation` ceiling so an unreachable floor fails as a capability mismatch. One declarative `validate_module_tenancy(groups=[TenancyRouteGroup(…)])`. New exports `TenancyRouteGroup`, `validate_module_tenancy`, `isolation_satisfies`, `validate_required_isolation`, `derive_tenant_isolation_mode`. Additive (`None` default unchanged).
- **Neo4j reaches `namespace`/`dedicated`** — `Neo4jGraphConfig.database` accepts a `(tenant_id)->str` resolver (per-tenant database → `namespace`); new `RoutedNeo4jClient` resolves per-tenant Bolt URI/credentials from secrets (→ `dedicated`; fails closed on partial auth), wired via `routed_neo4j_lifecycle_step`. New exports `RoutedNeo4jClient`, `Neo4jRoutingCredentials`, `routed_neo4j_lifecycle_step`.
- **Tenant infrastructure provisioning (`TenantProvisionerPort`)** — idempotent `provision`/`deprovision` on `TenantManagementPort.provision_tenant`/`deprovision_tenant`, wired via `TenancyDepsModule.tenant_provisioner`; `Noop`/`Function`/`Composite` + reference `ObjectStorageTenantProvisioner` (ensures a bucket) and `PostgresSchemaTenantProvisioner` (`CREATE SCHEMA`, opt-in `drop_on_deprovision`). Opt-in.
- **Analytics per-tenant namespace routing + advisory binding** — `ClickHouseAnalyticsConfig.query_database` / `BigQueryAnalyticsConfig.query_dataset` / `PostgresAnalyticsConfig.query_schema` resolve an unqualified table in the tenant's namespace; `tenant_aware` routes bind the tenant id as a query param, fail closed if unbound, and reject SQL that never references it. Helpers `bind_tenant_param`/`assert_tenant_param_referenced`/`TENANT_PARAM`. Off by default.
- **Tenant-safe structured graph walk + raw gating** — `GraphQueryPort.scoped_walk(anchor, ScopedWalkParams(…))` runs an adapter-owned, fully-structured, full-path tenant-scoped traversal; the whole-query raw hatch is **disabled by default** (`Neo4jGraphConfig.allow_raw_query` defaults `False`). New exports `GraphPathStep`, `ScopedWalkParams`. *(Breaking: deployments using `ctx.graph.raw` must set `allow_raw_query=True`.)*

**Query DSL:**

- **Fluent builder `Q`** — `Q.field("age").gt(18) & Q.field("name").like("a%")` lowers to the same filter AST (`.build()`/`.to_ast()`); covers every value operator, `&`/`|`/`~`, field compares, and array quantifiers. New exports `Q`, `QueryCondition`, `FieldRef`. Additive (lowers faithfully, does not re-validate).
- **Hierarchy operators** (`$descendant_of`/`$ancestor_of`) on a `TreePath` field — inclusive, label-boundary-correct containment; Postgres native `ltree` or `text` prefix fallback, mock oracle; gated by `QueryCapabilities.supports_hierarchy`. New exports `TreePath`, `HierarchyOp`, `HierarchyValue`.
- **Aggregation** — `$count_distinct`, `$stddev_pop`/`samp`, `$var_pop`/`samp`, `$percentile`, and post-group `$having` on Postgres/Mongo (mock oracle). (`$first`/`$last` deferred.)
- **Full + array-of-arrays nested quantifiers** on every document backend (Postgres nested `EXISTS`, Mongo `$expr`); `supports_nested_quantifiers` gate dropped. Operator/field-type validation (`validate_query_field_types`) now runs in the gateway *and* the mock, rejecting mismatches with `query_operator_type_mismatch`.
- **Mixed-direction keyset pagination + per-key `NULLS FIRST/LAST`** — coherent null ordering across backends (fixes a Postgres keyset predicate that dropped null-keyed rows); cursor tokens carry null placement (old tokens stay valid); Mongo opt-in `computed_null_ordering`. New helpers `QuerySortNulls`/`QuerySortKeySpec`, `resolve_sort_keys`, `parse_sort_value`, `ordered_compare`.
- **Query discovery metadata** — `build_query_discovery` projects a read model's filterable/sortable/aggregatable surface as OpenAPI `x-forze-query` + MCP line. New helpers `classify_field_type`, `field_value_operators`, `is_quantifiable_field`.

**Identity & API keys:**

- **Tenant selector self-service** — `GET /tenants` (active memberships), `POST /tenants/{id}/activate` (validates membership via `TenantResolverPort` → `tenant_mismatch`/`tenant_inactive`, re-mints a tenant-scoped token pair — Pattern B: the signed `tid` is re-validated against live membership each request), `DELETE /tenants/{id}` (drops the caller's own membership). New aggregate `forze_kits.aggregates.tenancy.build_tenancy_registry(authn_spec)` + `forze_fastapi.routes.attach_tenancy_routes` (all `AuthnRequired`, tenant-unaware); `TenantManagementPort.list_principal_tenants`, `TenancyDeps.require_manager`.
- **Tenant admin (`forze_kits.aggregates.tenancy_admin`)** — the privileged inverse: `create_tenant`/`list_members`/`invite_member`/`remove_member`/`deactivate_tenant` via `attach_tenancy_admin_routes` (`POST /tenants`, `GET /tenants/{id}/members`, `POST /tenants/{id}/deactivate`, `POST`/`DELETE /memberships`). Ships **unguarded** — bind `AuthnRequired` + `AuthzBeforeAuthorize` per op before exposing. New `TenantManagementPort.list_tenant_principals`. *(Breaking for `TenantManagementPort` implementers: new `list_principal_tenants` + `list_tenant_principals`.)*
- **Self-service API-key management** — `issue_api_key`/`list_api_keys`/`revoke_api_key` as `POST/GET/DELETE /api-keys` (secret returned once; `hint`/`label`). New `ApiKeyInfo`. *Breaking for `ApiKeyLifecyclePort`. Migration: `ALTER TABLE <api_key_accounts> ADD COLUMN hint text, ADD COLUMN label text`.*
- **Delegation-aware API keys (user→agent)** — `issue_api_key(actor_principal_id=…)` binds a delegation actor (RFC 8693 `act` claim → `AuthnIdentity.actor`; engine enforces the user×agent grant intersection). New `ACT_CLAIM`. *Breaking for `ApiKeyLifecyclePort`. Migration: `ALTER TABLE <api_key_accounts> ADD COLUMN actor_principal_id uuid`.*
- **MCP boundary API-key auth** — `ForzeApiKeyVerifier` + `AccessTokenIdentityResolver` protect a FastMCP server with the forze_identity brain (no OAuth flow); reads-only by default.
- **OpenAPI security from configured authn** — `apply_openapi_security` derives `securitySchemes` from the `AuthnRequirement`; principal-requiring ops are flagged `x-requires-authn` (new `OperationCatalogEntry.requires_authn`) with a matching MCP line.
- **Authn plane** — `AuthnOrchestrator` in `forze.application.integrations.authn` with a full mock identity plane; `attach_authn_routes` (login/refresh/logout/change-password/deactivate + reset); self-service `PasswordResetPort` (single-use, no enumeration); `AuthnEventSink` + fixed-window login `lockout`. `deactivate_principal` ships unguarded.

**Cache:**

- **In-process L1 document cache** (`CacheSpec(l1=L1Spec(…))`) ahead of the distributed cache — tenant-scoped, TTL staleness budget, pluggable eviction (LRU+TTL / scan-resistant `TinyLfuStore`); `RedisCacheConfig(invalidation_push=True)` shrinks the window to one round-trip (RESP3 `CLIENT TRACKING`); `instrument_document_l1`; `CachePort.exists`. Off by default.
- **Stampede protection & adaptive freshness** — singleflight on read-through misses; probabilistic early refresh (`early_refresh_beta`, optional `early_refresh_background`); per-entry `age_ttl`/`sliding_ttl` + keyword `ttl=` on every setter.

**Resilience & runtime:**

- **New strategies** — `AdaptiveBulkheadStrategy` (AIMD concurrency, optional `latency_quantile`; CoDel shedding + adaptive LIFO), `AdaptiveThrottleStrategy`, tail-based `HedgeStrategy.adaptive_delay_quantile`, token-bucket `RateLimitStrategy` + `THROTTLED`/`TIMEOUT` exception kinds; `ResilienceDepsModule(port_policies=[…])`.
- **Invocation deadlines** — per-operation budgets (`registry.bind(op).with_deadline(…)`, `bind_deadline`); expiry raises `exc.timeout` (504), projected to catalog/routes/MCP.
- **Distributed limits** — pluggable `RateLimitStore` (`RedisRateLimitStore`, fails open) so N replicas share one rate; bulkheads/budgets stay process-local.
- **App assembly & deployment** — `build_runtime` + `forze_fastapi.runtime_lifespan` / `forze_mcp.runtime_lifespan` (warm scope for the app lifetime); graceful drain (`drain_timeout`, default 10s); `DeploymentProfile.FLEET` (singleton-guard, `singleton_lifecycle_step`, readiness probe, deadline-budget HTTP propagation) and `SERVERLESS` (rejects `requires_long_running`, zero default drain); `instrument_resilience` / `instrument_tenant_pools`.

**Messaging & storage:**

- **Envelope headers + correlation propagation** — messages gain `headers`/`delivery_count`; the relay forwards the full envelope and `process_with_inbox` rebinds correlation/causation across broker hops (optional documented-trust tenant rebinding).
- **Outbox `ordering_key`** — per-aggregate ordering (SQS FIFO `MessageGroupId`, stream partition key); dedup keyed on the event-id header. *Migration: `ALTER TABLE … ADD COLUMN ordering_key TEXT`.*
- **Kits queue-consumer runner** (`run_consumer` / `queue_consumer_background_lifecycle_step`) — inbox exactly-once, requeue, poison parking, envelope rebinding.
- **Stream pending-entry recovery** — `StreamGroupQueryPort.claim` (XAUTOCLAIM) + `pending` (XPENDING). *Breaking for port implementers.*
- **Presigned object-storage URLs** — `StorageQueryPort.presign_download` / `StorageCommandPort.presign_upload` (S3 SigV4, GCS V4, mock). *Breaking for port implementers (minting an upload URL is a CQRS write).*
- **Object-storage metadata & access ops** — `head` (completion-seam enabler for presigned uploads), `download_range` (HTTP Range, 206), `download_if_changed` (`None` on 304), `copy`/`move` (server-side; S3 5 GiB single-copy cap, GCS `rewrite`; refused on object-encrypting routes), `put_object_tags`; generated FastAPI download routes honour `Range`/`If-None-Match`. *Breaking for `StorageQueryPort`/`StorageCommandPort`/`ObjectStorageClientPort` implementers* (per-object `expires_at` omitted — S3/GCS have no per-object TTL).
- **Resumable multipart uploads** — `StorageUploadSessionPort` (`ctx.storage.uploads(spec)`, CQRS-write-guarded): `begin_upload` / `presign_part` / `list_parts` / `complete_upload` / `abort_upload` for large direct-to-storage uploads with the app out of the data path. S3 native multipart; GCS temp-keys + `compose`; mock `deposit_part` seam. Refused on object-encrypting routes.
- **Storage HTTP edge** — kit ops + generated FastAPI routes for presigned download/upload and the full multipart session (`POST /presign/{download,upload}`, `POST /uploads`, `/uploads/parts/url`, `/uploads/parts`, `/uploads/complete`, `/uploads/abort`) so a browser/Uppy client drives direct uploads. The presigned URL rides the response body but never a log line; minting an upload URL is a command op — bind authn/authz.
- **Server-side encryption at rest (SSE/CMEK)** — for direct-upload flows where client-side envelope encryption can't reach. `S3StorageConfig.sse` (`S3ServerSideEncryption(mode="none|s3|kms", kms_key_id=…)`) applies SSE-S3/SSE-KMS to upload/copy/presign/multipart (SSE-KMS signed headers ride `PresignedUrl.headers`); `GCSStorageConfig.kms_key_name` (CMEK) covers app-path upload/compose, while presigned/resumable PUTs rely on the bucket default (documented divergence). A separate axis from client-side `encrypt` (combinable; does not satisfy a client-side `required_encryption` floor). Off by default.

**Misc:**

- **Catalog/registry ergonomics** — `OperationCatalogEntry` gains `supports_idempotency_key`/`required_permissions` (projected to routes/MCP); duplicate `merge` keys raise (`override=True` hatch); one-step `registry.register(…)`. Plus `RecordingNotificationSenders`, `AnalyticsDeps.command`, `TenancyDeps.require_resolver()`, `OperationDescriptor.tags` → FastAPI route tags.
- **Generated-route mount ergonomics** — every `attach_*_routes` helper gains `resource=` (a prefix string, mutually exclusive with `ns=`) and `path_overrides=` (per-op path replacements; `operationId` stays the verbatim catalog key). Additive.
- **Patch authoring — scoped, materialized, fail-closed reach** — `registry.patch(selector, namespace=ns)` / `commit_patch(…, namespace=ns)` match only ops under `ns` (`str_key_selector.in_namespace` builds the scoped selector); `registry.materialize_patches(*selectors)` folds patches into per-op plans so a later `OperationRegistry.merge` can't reach a sibling. `merge` now **raises** when a patch authored in one registry matches another's ops, naming the selectors/ops. *(Breaking only for a registry that merged a broad pre-merge patch onto another's ops — pass `merge(…, cross_registry=True)`.)*

### Changed

- **Queue consumer and outbox relay are now configurable classes** — attrs classes that validate config once on construction, replacing the function-runners that took `ctx` + a long kwarg list: `run_consumer(ctx, …)` → `QueueConsumer(queue=…, queue_spec=…, handler=…, inbox_spec=…, tx_route=…, …).run(ctx, *, timeout=…)`; `relay_outbox_*`/`relay_outbox` → `OutboxRelay(outbox_spec=…, …).to_queue/.to_stream/.to_pubsub/.run`. Lifecycle steps keep flat params; `relay_outbox_claims` unchanged. *Breaking for direct `run_consumer` / `relay_outbox_*` callers.*
- **Tenant-isolation tier model made coherent** — ladder `none < tagged < namespace < dedicated` (`relation` rung removed); per-tenant collection/index names reach `namespace` via dynamic-resolver detection; each integration owns its `max_supported_isolation` ceiling (required, fail-closed); namespace resolution unified into `resolve_scoped_namespace` across nine adapters (key/path formats unchanged).
- **Argon2 hashing off the event loop** — `PasswordService.hash_password`/`verify_password`/`timing_dummy_hash` are now `async` on a bounded pool (`PasswordConfig.hashing_concurrency`, default 4); `*_sync` variants remain.
- **Performance (measured):**
  - *Engine hot path* — hookless op ~2.5→1.2 µs (−52%), QUERY −56%, `bind` −50%; `resolve_simple` memoized per scope (−73%); aggregate `load` skips the dump roundtrip (−22% flat / −67% nested).
  - *Data access* — `Document.update()` copies only changed subtrees (−21%/−44%, OCC history −30%); Postgres root-tx rides `BEGIN` (−21%), out-of-tx on autocommit (−37%); Mongo `create` skips read-back (−49%), single updates use `find_one_and_update` (−30%), outbox claims in 3 round-trips (−90%, needs a sparse `claim_token` index); `trusted` decode 1.5–2.6×, msgspec `forbid_extra` 3–13×.
  - *Observability / cold start* — lazy error-context (~8–13 µs→0.2 µs), batched relay marks (~18.4k rows/s), opt-in `trace` (26×), memoized log scrubbing (~53×/27×); s3/sqs type-stubs + `opentelemetry` confined to `TYPE_CHECKING`/lazy import.
- **FastAPI `style="rpc"` uses REST verbs + query params** — `GET /notes.get?id=`, `PATCH /notes.update?id=&rev=`, `DELETE /notes.kill?id=`, etc. *Breaking: RPC clients must switch from `POST /<op>`; REST and MCP unchanged.*
- **`singleton_lifecycle_step` takes a `DistributedLockSpec`, not a live port** — *Breaking: pass `spec=DistributedLockSpec(name=...)`.*
- **Release-coherence sweep** — relay logs the at-least-once → fire-and-forget downgrade; Temporal `query`/`update`/`result` deserialize into declared types; `ApiKeyConfig.prefix` validated; saga `step_failed` stays `DOMAIN`.

### Fixed

- **Tenant-isolation correctness & parity** — Postgres outbox/inbox now enforce the declared isolation floor (were excluded); a missing bound tenant fails closed consistently as `authentication`/`tenant_required` (was 500/401 split) via `TenancyMixin._tenant_id_for_resolve`; the mock durable/graph/document adapters now tenant-partition their stores, pinned by a mock-tenancy-parity meta-test.
- **Post-commit work survives task cancellation** — the after-commit drain runs as a cancellation-protected critical section (`forze.base.asyncio.run_to_completion`), then re-raises; cancellation during the body still rolls back.
- **PGroonga search honors tenant isolation regardless of plan** — a tenant-aware PGroonga search now always uses `filter_first`, overriding `pgroonga_plan="index_first"`/`"auto"` (which ranked a heap top-K across **all** tenants and post-filtered, scanning cross-tenant rows and possibly truncating a tenant's results to a slice of the global top-K).

## [0.3.0] - 2026-06-11

### Added

- **Generated FastAPI routes (`attach_document_routes` / `attach_search_routes` / `attach_storage_routes`):** project a frozen registry's operations onto a user-owned `APIRouter` (the HTTP sibling of `register_tools`), `operationId` = operation key verbatim. Required `style` (`"rest"` resource paths / `"rpc"` operation-named); capability-aware with `include=` narrowing; dispatches through `run_operation` so plans, read-only enforcement, and hooks apply. Merging a soft-deletion registry adds delete/restore. No ETag or route-feature framework — idempotency is now engine-level.
- **`forze_mcp` (`forze[mcp]`) — expose operations to AI frameworks as MCP tools (read-only MVP):** `register_tools(server, registry, ctx_factory, …)` adds a frozen registry's operations as tools onto your own FastMCP server (flat arg signature from the input DTO, `OperationKind` → `readOnlyHint`/`destructiveHint`, governed `run_operation` dispatch). Read-only by default (`include_writes=True` to expose commands). Also `register_dsl_query_prompts` (querying-grammar prompts), `register_schema_resources` (per-spec JSON schema + queryable fields), `register_resource_templates` (get-by-id as `notes://{id}`), `LoggingMiddleware`, and the `build_mcp_server` convenience. Pluggable identity (`StaticIdentityResolver` / `DelegatedIdentityResolver`). Built on FastMCP 3.x; test-backed example under `examples/recipes/mcp_server/`.
- **`forze_duckdb` (`forze[duckdb]`) — in-process DuckDB analytics over object storage (query-only):** `AnalyticsQueryPort` over a Parquet/CSV/Iceberg/Delta lake on S3/GCS/local files without a standing warehouse. Typed source descriptors (`ParquetSource`/`CsvSource`/`JsonSource`/`IcebergSource`/`DeltaSource`) auto-derive DuckDB extensions; typed object-store credentials (`S3Credentials`/`GcsCredentials`) render `CREATE SECRET`, resolved via `SecretsPort` or supplied inline. Bridged to asyncio on a bounded executor (cursor-per-query, native timeout interrupt), Arrow-internal. Wire with `DuckDbDepsModule` + `duckdb_lifecycle_step`; also serves as a real-engine analytics test double.
- **Delegated identity (on-behalf-of, RFC 8693):** `AuthnIdentity.actor` carries the principal performing the action; `AuthzBeforeAuthorize` enforces least-privilege intersection (both subject and actor must be permitted — the confused-deputy defense). Token-derived via `AuthnDepsModule(actor_claim="act")` (multi-hop chains); explicit pairwise authority via `DelegationPort.may_act` + `DelegationGrantPort` (`AuthzSpec.enforce_delegation_grant`, fail-loud). Document-backed adapters in `forze_identity`, mocks in `forze_mock`; `forze_mcp` ships a `DelegatedIdentityResolver`.
- **Operation-level CQRS (`OperationKind` QUERY/COMMAND):** `registry.bind(op).as_query()` runs read-only — a command port cannot be acquired (by construction) and the transaction opens `READ ONLY` (enforced at the DB, including the raw-query hatch); covers every state-write accessor (document/outbox/search/graph/dlock/storage/analytics/authz/authn). Untagged defaults to COMMAND (behavior-preserving). Replica routing via `as_query().bind_tx().set_route(...)`.
- **Operation catalog descriptors (`OperationDescriptor` + `FrozenOperationRegistry.catalog()`):** interface-agnostic request/response-schema + description metadata for projecting operations onto MCP/HTTP without re-deriving schemas; `catalog()` joins the descriptor with the plan's `OperationKind`. All kit builders populated.
- **Queryable-field policy (`QueryFieldPolicy` on `DocumentSpec`):** per-aggregate `filterable`/`sortable`/`aggregatable` allow-sets (validated against the read model). Powers MCP schema discovery and boundary-only enforcement (`QueryFieldGuard` in the kit list/search/aggregate handlers); direct port calls stay unrestricted.
- **OpenTelemetry traces + metrics (`instrument_operations`):** wraps every operation in an OTel span (kind, ids, tenant, principal) plus a `forze.operations` counter and `forze.operation.duration` histogram, via the global providers. OpenTelemetry is already a core dependency. Opt-in, additive.
- **`@invariant` — declarative domain invariants:** an always-true `(self) -> None` rule enforced on **both** create and update — closes the footgun that merge-patch updates (via `model_copy`) bypass Pydantic `@model_validator`s. Positioned against `@update_validator` (transition rules) and raw `@model_validator` (escape hatch, documented as not running on updates).
- **Saga / process orchestration (`SagaDefinition` + in-process executor):** declarative multi-step processes across aggregates that can't share a transaction; typed `SagaStep`s with `SagaStepKind` (`COMPENSATABLE`/`PIVOT`/`RETRYABLE`) modeling a point of no return, per-step `tx_route`/`retry_policy`, reverse compensation before the pivot, forward-only after. `run_saga(ctx, definition, initial)` via `SagaExecutorPort`; must run outside an enclosing transaction. A shared backend-agnostic `SagaProgress` coordinator drives both the in-process executor and `forze_temporal.TemporalSaga` (Temporal owns durability).
- **DDD domain events + aggregate roots → outbox:** `DomainEvent`/`AggregateRoot` (`forze.domain.models`) buffer events (`record_event`/`collect_events`); the declarative `@event_emitter` raises an event from an `(before, after, diff)` transition on `Document.update`. Persisting an aggregate drains and dispatches its events **in the operation's transaction** via `DomainEventDispatcherPort` (`InProcessDomainEventDispatcher`, factory-registered handlers); the `outbox_event_handler` bridge stages them in the transactional outbox. `forze_kits.aggregates.AggregateRepository` (`load`/`add`/`apply`) supports the functional-decider pattern. Wired via `DomainEventsDepsModule`.
- **End-to-end worked example (`examples/recipes/order_fulfillment/`):** the first runnable, test-backed example — checkout saga → aggregate `@event_emitter` → outbox → relay → inbox → downstream, plus the compensation path, all in-process on `forze_mock`.
- **Deterministic time & ids (`TimeSource` seam):** `forze.base.primitives.utcnow()` / `uuid7()` read a context-active `TimeSource`, so `bind_time_source(FrozenTimeSource(...))` makes every read (including domain self-stamping) deterministic with no call-site changes; the Temporal worker binds a replay-deterministic source.
- **Resilience policy pipeline (`forze.application.contracts.resilience`):** Polly-style composable strategies (`BulkheadStrategy`, `CircuitBreakerStrategy`, `RetryStrategy` with jittered/decorrelated backoff + `RetryBudget`, `TimeoutStrategy`, `FallbackStrategy`) compose into a validated `ResiliencePolicy`; named via `ResilienceSpec`, run through `ctx.resilience().run(...)` or attached per-op as `ResilienceWrap` (retry re-runs with a fresh transaction per attempt). Ships `InProcessResilienceExecutor` with default `"occ"`/`"transient"` policies; `forze_mock` no-op passthrough. **Hedging** (`HedgeWrap`/`HedgeStrategy`) races a redundant attempt after `delay`, gated by a freeze-time safety check (idempotent/read-only only). **Distributed breaker** (`CircuitBreakerStore` seam + `RedisCircuitBreakerStore`) so a fleet trips/recovers together (two-tier, server-clock Lua, fails open).
- **Inbox / consumer-side dedup (`forze.application.contracts.inbox`):** `InboxPort.mark_if_unseen` (atomic seen/not-seen); `process_with_inbox` marks and runs the handler in one transaction (exactly-once effect for at-least-once delivery). `PostgresInboxStore` + mock. Distinct from idempotency (operation-level result replay).
- **Graph contracts + `forze_neo4j` (`forze[neo4j]`):** graph ports resolvable via `ctx.graph.query`/`.command`/`.raw`; Neo4j adapter over the async Bolt driver (vertex/edge CRUD, both edge-identity modes, `neighbors`/`expand`/`shortest_path`, tenant isolation, raw Cypher hatch); reusable `kernel.cypher`. In-memory `MockGraphAdapter`.
- **`forze_kits` — consolidated kit package:** domain kits, aggregate registries/facades, mapping, DTOs, outbox/notify integrations, secrets adapters, runtime scopes (`DistributedLockScope`). Absorbs former `forze_patterns`, `forze.application.{composition,handlers,mapping,dto,kit}`, and `forze_secrets` (see the Removed migration table). Includes a closed-schema, document-backed **stored-file kit** (`StoredFileKitSpec`).
- **`forze_http` (`forze[http]`):** outbound HTTP integration — `HttpServiceSpec`/`HttpServicePort`, `HttpClient`/`RoutedHttpClient` (tenant routing), `HttpDepsModule`, declarative `BaseHttpIntegration` + `async_http_op`; `ctx.http` resolves services by name. httpx under the hood.
- **`forze_meilisearch` (`forze[meilisearch]`):** async Meilisearch — offset `SearchQueryPort`, `SearchCommandPort`, federated search (native federation or weighted RRF).
- **Transactional outbox + notify + search-command:** `forze.application.contracts.outbox` (`OutboxSpec`, `IntegrationEvent`, command/query ports, request-scoped `OutboxStaging`) with Postgres/Mongo/Mock stores; relay helpers + `outbox_relay_background_lifecycle_step` (at-least-once claim/reclaim) in `forze_kits.integrations.outbox`. `forze_kits.integrations.notify` — typed notification commands, routing, dispatch, queue-consumer helper. Core `SearchCommandPort` (`ensure_index`/`upsert`/`delete`/…) for external index maintenance.
- **Tenant routing:** declarative per-request backend targets (`RelationSpec`/`NamedResourceSpec` + `coerce_*`/`require_static_*`, `forze.application.contracts.resolution`) adopted across all integrations; per-tenant `Routed*Client` variants with `*RoutingCredentials`, `routed_*_lifecycle_step`, LRU pool dedup by connection fingerprint, backed by `TenantClientRegistry` and tenancy/secret helpers in `forze.application.contracts.tenancy`.
- **Identity — IdP presets (`forze_identity.builtin.idp`):** OIDC presets for Google Sign-In, VK ID (server-side introspection), and Telegram Login; `oidc_bootstrap_identity_deps` for external `id_token` JWTs; `OidcIdpPreset`/`ConfigurableOidcIdpVerifier`. PKCE helpers (`generate_pkce`), `OidcTokenVerifier.require_nonce`. Authn: `refresh_api_key` rotation; single-use password invites (HMAC-digest storage); custom `token_verifiers` skip access-secret validation.
- **Execution — freeze/resolve pipeline:** authoring `DepsRegistry` (`freeze()` → `FrozenDepsRegistry.resolve()` → `FrozenDeps`) separates registration from per-scope resolution; matching `LifecyclePlan` → frozen → resolved with `LifecycleModule`, topological ordering, and `routed_client_lifecycle_step`. Per-scope caches (`cache_resolved_operations`/`cache_resolved_ports`, default on) with tenant-scoped resolvers staying per-call.
- **Codecs:** `default_model_codec`, `stored_field_names_for`, `DocumentCodecs`/`document_codecs_for_spec`/`DocumentSpec.resolved_codecs`; optional `read_codec`/`ingest_codec` on search/analytics specs; trusted-row read validation.
- **Postgres / Mongo search:** Postgres `read_validation` strict/trusted, PGroonga plan modes + candidate caps, hub `per_leg_limit`/`combo_*`/parallel legs + `SearchOptions` overrides; Mongo `MongoDepsModule.searches` (text/Atlas/vector, offset + cursor, optional Redis snapshots, index-validation lifecycle step).
- **Document adapters:** `max_scan_pages`/`max_stream_pages`/`max_chunked_command_pages` (default 100 000, `None` unlimited) with cursor-stall detection.
- **Durable workflow:** `DurableWorkflowRunStatus`/`Description` + `describe()` on `DurableWorkflowQueryPort` (`forze_temporal`).
- **`forze_temporal` secure connections:** `TemporalConfig.tls` / `api_key` (Temporal Cloud) / `rpc_metadata` / `data_converter` override; defaults unchanged (plaintext localhost, pydantic converter).
- **AWS — long-lived clients + credential chain (SQS/S3):** one aiobotocore client opened at `initialize()` and reused; `access_key_id`/`secret_access_key` and `region_name` become optional (default credential/region chain — env, profile, IAM role, SSO, IMDS); S3 derives `LocationConstraint` from the resolved region. Per-tenant routed credentials still require explicit keys and region.
- **Vault — token renewal, metadata existence, health:** opt-in self-renew loop; `kv_exists` via the KV v2 metadata endpoint; standard `health()` for the first time.
- **`forze_fastapi` upload cap + attach-time validation:** chunked upload streaming under `max_upload_size` (default 64 MiB, `None` disables) with early Content-Length rejection; id / id+rev route builders validate DTO shape at attach time.
- **`forze_socketio` error translation + identity:** handler exceptions become structured ack payloads honoring egress redaction; optional connect-time `identity_resolver` bound per event.
- **Distributed-lock fencing tokens (breaking for port implementers):** `DistributedLockCommandPort.acquire` returns `AcquiredLock | None` carrying a monotonic fencing token; `DistributedLockScope` yields the handle. Backends that cannot issue tokens return `token=None`.
- **Object-storage tags end-to-end:** `UploadObjectRequestDTO.tags` (S3 native tagging / GCS prefixed metadata / mock); `include_tags` guarantee flag on head/list (`True` makes S3 pay `GetObjectTagging`); tags on head/listed value objects.
- **`IdempotencyPort.fail()` (breaking for port implementers):** releases a pending claim on handler failure so legitimate retries aren't rejected as duplicates (Redis + mock).
- **`AuthnFacade.deactivate_principal`:** the existing tested handler is now registered into `build_authn_registry`, exposed on the facade, and exported.
- **`forze_mock` parity:** strict transactions (`MockDepsModule(strict_tx=True)` — snapshots DB-backed stores, savepoint nesting, read-only enforcement; queues/streams/storage deliberately don't roll back, matching production); queue/idempotency parity (idle-timeout `consume`, visibility-timeout redelivery, dead-letter list, TTL'd idempotency); consumer groups (one-consumer-per-group with real `ack`) and true keyset cursor pagination; tenancy helpers and distributed-lock/search/durable/identity adapters.
- **`forze.base` primitives:** `CacheLane`, `SimpleLruRegistry`/`GuardedLruRegistry`, `InflightLane` (singleflight), `OnceCell`, `frozen_mapping`, and fingerprint helpers (`stable_json_bytes`/`stable_payload_fingerprint`/`stable_fingerprint`/`connection_string_fingerprint`).

### Changed

- **Breaking — document write identity is an explicit argument:** `CreateDocumentCmd` no longer carries `id`/`created_at`; the command write surface becomes `create(payload, *, id=None)` / `ensure(id, payload)` / `upsert(id, create, update)` with `KeyedCreate`/`UpsertItem` bulk value objects (the gateway mirrors with parallel sequences). Restore via `forze_kits.dto.ImportTimestamps` + `ensure`. **Migration:** move `id`/`created_at` into the new arguments; replace bulk lists with the value objects.
- **Breaking — storage CQRS split:** `StoragePort`/`StorageDepKey` split into `StorageQueryPort` (`download`, `list`) / `StorageCommandPort` (`upload`, `delete`) with separate dep keys; resolve via `ctx.storage.query(spec)` / `.command(spec)`. S3/GCS factory renames (`ConfigurableS3Storage` → `…StorageQuery`/`…StorageCommand`).
- **Breaking — coordinators → adapters:** `DocumentCoordinator`→`DocumentAdapter`, `DocumentCacheCoordinator`→`DocumentCache`, `SearchResultSnapshotCoordinator`→`SearchResultSnapshot`, `OutboxStagingCoordinator`→`OutboxStaging`, `DistributedLockCoordinator`→`DistributedLockScope`; helpers moved under `forze.application.integrations`; `forze.application.coordinators` removed.
- **Breaking — codecs unified on `ModelCodec`:** document/search/analytics paths materialize through spec-owned codecs; document kernel gateways require explicit codecs at construction (build via `read_gw`/`doc_write_gw`). `read_validation="trusted"` decode on Postgres/Mongo/Firestore; the versioned cache stores compact JSON bytes.
- **Breaking — frozen `attrs` integration configs:** all integration wiring configs are frozen `attrs` classes (no dict/`TypedDict` literals); `tenant_aware` inherited from `TenantAwareIntegrationConfig`; module-level `validate_*_conf` removed (validation at construction / `.validate()`); several timeout fields move to `timedelta`.
- **Breaking — `ensure_bucket` is create-if-missing on both backends (S3):** S3 previously raised `not_found`; both now create idempotently and race-safe. Use `bucket_exists()` for existence assertions.
- **Breaking — `nack(requeue=...)` semantics aligned (SQS):** `requeue=False` no longer deletes the SQS message (silent loss) — it leaves it for the redrive policy to dead-letter; `requeue=True` = immediate redelivery (best-effort). Apps relying on nack-to-drop must `ack`.
- **Breaking — `workflow_id_template` → `workflow_id_base`:** the schedule field is passed verbatim (Temporal appends the fire timestamp); renamed across contract/adapter/mock, no alias.
- **Idempotency reshaped to engine-level result idempotency:** `IdempotencySnapshot` (HTTP-shaped) replaced by interface-agnostic `IdempotencyRecord(result: bytes)`; a new `IdempotencyWrap` hook reads a context-bound `idempotency_key`, hashes the args, and returns the stored typed result early (skipping the handler and its transaction). The FastAPI middleware reads the canonical `Idempotency-Key` header.
- **OCC retry routed through the resilience pipeline:** Postgres/Mongo/Firestore write gateways drop their own `tenacity` decorators for the shared `occ_retry` (`"occ"` policy, decorrelated backoff, 3 attempts); the executor is resolved per scope with a shared default, so apps keep OCC retries with no wiring change. Attempt counts unchanged; registering `ResilienceDepsModule` lets an app override `"occ"`.
- **Write gateways — unified OCC/history validation:** Postgres/Mongo share one `HistoryOccMixin`; a missing history snapshot now raises retryable `exc.precondition` (`history_not_found_retry`) on both (Mongo previously raised `not_found`).
- **Async contract protocols standardized on `def … -> Awaitable[X]`:** the remaining `async def`-declared Protocol ports are converted (type-only; implementations and `await` call sites unaffected; makes contracts decorator-friendly). Async-generator methods unchanged.
- **Transaction nesting contract:** nested scopes are savepoints; isolation and `read_only` are honored only at the root; a nested scope requesting a conflicting `read_only` raises `tx_nested_read_only_conflict`. `TransactionHandle.id` removed; gained `read_only`.
- **Unbounded-read protection unified on the implicit cap:** Mongo/Firestore gain the Postgres `find_many_implicit_limit` (default 10 000, `None` disables); the hard "filters or limit required" precondition is dropped.
- **Analytics SQL pagination wraps in a subquery:** the shared `apply_limit_offset` wraps Postgres/ClickHouse too (fixes registered queries already ending in `LIMIT`); negative limit/offset now raise; catch-all driver-error summaries normalized via a shared `fallback_exception_mapper`.
- **`forze_mock` adapters are stricter (potentially breaking for tests):** the mock password verifier actually compares; `MockAuthzDecisionPort`/scope port are deny-by-default; `MockTenantResolverPort` mirrors real membership/ambiguity/inactive checks; `MockDocumentAdapter.create` raises `conflict` on a duplicate id. Lenient-mock false passes now fail honestly.
- **Graph contracts (evolving, pre-1.0):** dual-addressing `EdgeRef.by_key`/`by_endpoints` (per-kind `GraphEdgeSpec.identity`); `key_field`s; `shortest_path` single path + new `k_shortest_paths`; `validate_graph_module_spec` raises `configuration`.
- **Execution-context lifecycle tripwire + import-linter + kernel consolidation:** constructing an `ExecutionContext` while an operation is in flight now logs a warning (the unsupported per-request pattern); plane layering (`forze_kits` consumers, nothing imports `forze_identity`) is now `lint-imports`-enforced (14 contracts); kernel-client boilerplate consolidated onto `GuardedLifecycle`/`ContextScopedResource` (http/GCS/Temporal/Vault/RabbitMQ; behavior-preserving).
- **Internal package layout:** integration `kernel`→`kernel.client`, `execution`→`lifecycle/` + `execution.deps.{configs,factories}`; the operation registry/planning/facade/run modules move under `forze.application.execution.operations`. Package-root imports unchanged; direct internal-module imports must update.
- **Performance:** hookless operations skip body-stage scaffolding and fold the middleware chain iteratively (~30%); per-scope caches reuse gateways/adapters/codecs; trusted bulk decode hoists the field set + construct loop; JSON logs render via `orjson`.
- **Misc:** Postgres streaming reads use a server-side named cursor (bounded client memory); outbox (Postgres/Mongo) bulk `INSERT … ON CONFLICT DO NOTHING` + `claim_pending`/stale-`processing` reclaim (`reclaim_stale_after`, default 5 min) + `requeue_failed`; Mongo uses a single `index_name`; storage/analytics internals move to `forze.application.integrations`; search snapshot fingerprints re-baseline once; `forze[oidc]` now bundles `httpx`.

### Deprecated

- **`forze_identity.oidc`:** `OidcTokenVerifier.enforce_issuer_and_audience` now defaults to `True` — construction requires both `issuer` and `audience` unless explicitly opted out.

### Removed

- **Dead public surface (pre-release cleanup, all verified unreferenced):** the orphan `forze[arango]` extra; `AccessTokenService.try_decode_token`; the `ISSUER_FORZE_JWT` constant; `EffectiveGrantsAdapter`; `HeaderTokenAuthn.scheme`/`bearer_format`; `GCSHead`/`GCSListedObject` aliases; the unused `FORZE_*_LOGGER_NAMES` tuples and `MIDDLEWARES` enum members; `PostgresQualifiedName.from_string`; the `forze_postgres.kernel.client.fingerprint` module; the never-honored `batch_size` of `MongoClientPort.delete_many`.
- **`python-dateutil` core dependency:** dropped; `datetime_to_uuid7` parses ISO-8601 via stdlib `datetime.fromisoformat` (trailing `Z` accepted).
- **`forze[casbin]` extra:** dropped (no integration shipped against it).
- **`forze_identity.local` (breaking):** use `forze_identity.builtin.local`; local verifiers/factories no longer exported from `forze_identity.authn`/`.tenancy`.
- **`forze_identity.builtin.telegram`:** Telegram Mini App `initData` HMAC preset, superseded by Telegram Login OIDC under `forze_identity.builtin.idp.telegram`.
- **Execution:** `forze.application.coordinators`; `forze.application.execution.{registry,planning,facade,running}`; `OperationRunner`; `lifecycle_graph_from_sequence` (use `steps_graph_from_sequence`).
- **Validation helpers from public APIs:** Postgres (`validate_pg_search_conf`, `validate_postgres_hub_search_conf`, …) and integrations (`validate_mongo_search_conf`, `validate_clickhouse_analytics_config`, …); validation now lives on the config types / instance validation. Also dict/mapping coercion for `ConfigurablePostgresDocument`/`…ReadOnlyDocument`.
- **Codecs:** `RecordMappingCodec`/`Pydantic*`/`Msgspec*`, `codec_for_model`, `pydantic_cache_dump*`, and public `pydantic_*`/`msgspec_*` helpers in `forze.base.serialization` (use `ModelCodec`/`default_model_codec`); `SearchSpec.row_codec`/`resolved_row_codec` and `DocumentReadGatewayPort.effective_row_codec` (use `read_codec`).
- **Relocated to `forze_kits` (breaking):** the former `forze_patterns`, `forze.application.{composition,kit,handlers.*,mapping,dto}`, and `forze_secrets` modules now live under `forze_kits`. `Mapper`/`MapperFactory` stay on `forze.application.contracts.mapping`. `OutboxDestination(queue_route=…, queue=…)` replaced by the discriminated `OutboxDestination.queue(route=…, channel=…)` (also `.stream`, `.pubsub`).

| Old import | New import |
|------------|------------|
| `forze_patterns.soft_deletion` | `forze_kits.domain.soft_deletion` |
| `forze.application.composition.document` | `forze_kits.aggregates.document` |
| `forze.application.composition.outbox` | `forze_kits.integrations.outbox` |
| `forze.application.kit.DistributedLockScope` | `forze_kits.scopes.DistributedLockScope` |
| `forze_secrets` | `forze_kits.adapters.secrets` |
| `forze.application.handlers.document` | `forze_kits.aggregates.document.handlers` |
| `forze.application.handlers.search` | `forze_kits.aggregates.search.handlers` |
| `forze.application.handlers.storage` | `forze_kits.aggregates.storage.handlers` |
| `forze.application.handlers.authn` | `forze_kits.aggregates.authn.handlers` |
| `forze.application.mapping` | `forze_kits.mapping` |
| `forze.application.dto` | `forze_kits.dto` |
| `OutboxDestination(queue_route=..., queue=...)` | `OutboxDestination.queue(route=..., channel=...)` |
| `StoragePort` | `StorageQueryPort` / `StorageCommandPort` |
| `StorageDepKey` | `StorageQueryDepKey` / `StorageCommandDepKey` |
| `ctx.storage(spec)` | `ctx.storage.query(spec)` / `ctx.storage.command(spec)` |
| `RecordMappingCodec` | `ModelCodec` |
| `SearchSpec.row_codec` | `SearchSpec.read_codec` |
| `PostgresReadGateway(...)` without `codec=` | Pass `codec=` or build via `read_gw` |
| `PostgresWriteGateway(...)` without write codecs | Pass `create_codec` / `update_codec` / `codec=`, or use `doc_write_gw` |
| `PostgresHistoryGateway(...)` without `history_codec` | Pass `history_codec` and `codec=`, or use `doc_write_gw` |

### Fixed

- **Package error mappers were dead code in 12 integrations:** `ChainExceptionMapper` now flattens nested chains so package mappers are consulted — most critically Postgres `SerializationFailure`/`DeadlockDetected` (and Mongo/Neo4j conflicts) now map to `CONCURRENCY`, so **OCC retry fires on real serialization conflicts**. Mapped errors carry the interception `site`.
- **Firestore transactions:** `Aborted` (contention) → `CONCURRENCY` (OCC retry); rollback on `BaseException` (no leaked server-side tx); `count_documents` joins the ambient tx; a mismatched `database` raises configuration.
- **ClickHouse `run_query_all_pages` is one streaming execution** (consistent snapshot, no growing-OFFSET duplicates, no `(attempts+1)²` retry blow-up).
- **Redis pipelines fail loud on reads:** value-returning methods inside `pipeline()` now raise `redis_read_in_pipeline` instead of returning garbage coerced from the pipeline object.
- **RabbitMQ robustness:** `close()` nacks/requeues pending unacked messages; a pending watermark warns on growth; poison messages are dead-lettered (consumer continues); one delay queue per distinct delay value (no head-of-line blocking). Same poison handling on SQS.
- **Outbox relay failure model (transient retry / poison / drain):** codec decode (poison) failures fail immediately; broker publish failures reschedule with exponential backoff + jitter until `max_attempts` (default 5). New durable `attempts`/`available_at` columns (**migration:** `ALTER TABLE … ADD COLUMN attempts INT NOT NULL DEFAULT 0, ADD COLUMN available_at TIMESTAMPTZ`), `mark_retry(...)` (breaking for port implementers), `requeue_failed` resets the counter; the relay drains the backlog per tick. At-least-once, ordering not preserved across retries — key on `event_id`.
- **Outbox staging is per-route and per-task:** fixes a process-global `flushed` flag that silently dropped events after the first flush, and shared buffers that handed every route's rows to whichever store flushed first. New `buffer_for(route)`/`flushed_for(route)`/`peek(route?)`.
- **`GuardedLruRegistry` use-after-dispose race:** refcount 0→1 transitions and eviction reads now happen under the registry lock (these guard live connection pools); a dispose error during drain deregisters and propagates.
- **After-commit callbacks run to completion:** a failing post-commit callback no longer skips the rest — all run, failures aggregate into one `after_commit_failed` (raised after, rolls nothing back).
- **Lifecycle steps are shut down exactly once:** per-scope started-state tracking ends the double-shutdown on failed startup.
- **`finally` hooks observe before-hook denials:** before hooks now run inside the try/finally so audit/metrics `finally` hooks see denials; `on_failure` stays handler-only.
- **OCC history validation hardened:** records re-keyed by `(id, rev)` (no positional zip); all comparisons run in canonical python-mode space so no-op datetime/UUID resends don't falsely conflict.
- **`Document.update()` re-validates the patched state:** merges into a python-mode dump and `model_validate`s the result — semantic no-ops yield an empty diff (no spurious `rev`/history/event), partial nested dicts and ISO-string datetimes are no longer left raw, `@computed_field` keys are excluded at every depth, and `@model_validator` now runs on update (invalid patches raise `ValidationError`).
- **Concurrent graph waves report all failures** (`ExceptionGroup` for 2+; a single failure still raises directly).
- **Per-scope port cache works for per-call specs** (value equality, identity fast-path first).
- **`kill()`/`kill_many()` verify row counts on every path** (all paths raise `not_found` on missing rows).
- **SQS message identity fixed (was breaking inbox dedup):** `QueueMessage.id` is now the broker `MessageId` (stable across redeliveries); the ReceiptHandle moved to `SQSQueueMessage.receipt_handle`.
- **Postgres transaction options no longer leak across pooled connections:** read-only/isolation are now emitted as `SET TRANSACTION …` inside the root tx (was persistent psycopg attributes causing intermittent read-only write failures).
- **Mongo write conflicts retry under OCC:** WriteConflict (112) and `TransientTransactionError` → `CONCURRENCY`.
- **`forze_fastapi` middleware errors return proper status codes:** `CoreException`s raised in forze middlewares render the standard JSON error (via `build_core_exception_response`) instead of 500s; a malformed correlation/causation header falls back to a generated id.
- **RabbitMQ/SQS receive & consume defaults:** bounded `receive` windows; uniform idle-timeout `consume` (`None` = forever, finite = clean stop) — no more indefinite block, 1s death, or zero-wait busy-loop.
- **`DistributedLockScope` no longer loses the lock silently:** a lost heartbeat is recorded and raised (`CONCURRENCY`) at scope exit without masking the body's own exception.
- **Notify consumer dedup:** the event id is derived deterministically from the broker message identity (was a random UUID that defeated dedup).
- **All integration kernel clients:** `initialize()`/`close()` serialize on an internal lock (no double-create/leak under concurrency); partial-failure assignment hardened (BigQuery/Postgres/Redis).
- **Analytics adapters:** `run_chunked`/`select_run_chunked` reject non-positive `fetch_batch_size` up front (shared `validate_fetch_batch_size`).
- **Misc fixes:** Postgres `ON CONFLICT` from `conflict_target`/inferred PKs (composite PKs, extra UNIQUE indexes); PGroonga `index_first` cap and `search_count=exact` corrections; Mongo bulk-upsert miss → `mongo_ensure_bulk_miss`; Meilisearch federated snapshot finalization; identity duplicate/ambiguous-login detection; `forze_fastapi` tenant-hint resolution; `connection_string_fingerprint` includes sorted query params.
- **`forze_temporal` + `forze[mcp]` workflow sandbox:** `sandboxed_workflow_runner()` / `default_sandbox_restrictions()` pass `beartype` (fastmcp's transitive import hook) and `coverage` through the workflow sandbox, fixing circular-import validation failures and a coverage-induced test hang.

### Security

- **Password change revokes existing sessions (breaking by default):** `change_password` revokes all of the principal's sessions (refresh families + `sid`-bound access JWTs); `revoke_sessions_on_password_change=True`, opt-out explicit, missing session ports fail at startup.
- **Rehash-on-login (opt-in):** `Argon2PasswordVerifier` persists parameter-upgraded hashes after login (`password_rehash_on_login=True`), OCC- and fire-safe.
- **`sensitive=True` spec marker keeps credentials off generated surfaces:** `attach_*_routes` / `register_tools` / `register_schema_resources` / `register_resource_templates` refuse sensitive specs at attach time; the shipped authn specs are marked (would otherwise have served Argon2 hashes / HMAC digests).
- **Owner-override permission keys configurable + documented:** the `"admin"` / `"{resource_type}.admin"` bypass moves to `AuthzKernelConfig.owner_override_permissions` (defaults unchanged, empty set disables) — previously hardcoded, so an unrelated `admin` permission silently granted a global bypass.
- **`tenancy_mode="global"` warns over tenant-partitioned stores** (grants shared across tenants — set `require_invocation_tenant` for isolation).
- **OIDC nonce value binding:** `verify_id_token_nonce` (constant-time, single error) + `generate_nonce()`/`generate_state()`; VK/Telegram exchange accept `expected_nonce`; callback hardening checklist added.
- **Secret values masked in reprs framework-wide:** credential value objects become `repr=False`; `ClickHouseConfig.password`/routing creds and `InngestConfig` keys → `SecretStr`; `LocalIdentityConfig.api_keys` out of repr. Direct readers must call `.get_secret_value()`.
- **Outbound HTTP does not follow redirects by default** (`HttpConfig.follow_redirects=False`): httpx only strips `Authorization` on cross-origin redirects, so custom credential headers would otherwise follow a malicious 30x to an attacker host.
- **`AuthnDepsModule` rejects a token-verifier override without a resolver override** (principal-collision hazard; fails at startup naming the route).
- **Tenancy adapters enforce the cache/history guard** (a cached principal→tenant binding could keep a detached principal resolving after revocation).
- **Cursor pagination tokens validated as client input:** malformed/stale/tampered tokens raise 4xx (was `INTERNAL` 500); values restricted to JSON scalars; mixed-type sort-key compares surface as an invalid-cursor error.
- **Log message text is scrubbed:** string scrub rules apply to the rendered message after interpolation, not just structured extras.
- **Postgres sort direction whitelisted** (`asc`/`desc` only; uppercase now raises). **S3 object tags URL-encoded** (no `Tagging` query-string corruption/injection). **`OidcClaimMapper` rejects empty `iss`/`sub`** (empty subjects collapsed onto one principal).
- **5xx responses no longer leak internal diagnostics:** generic detail for status ≥ 500, sanitized `context` restricted to < 500, catch-all mapper summaries made static (driver text moved into suppressed-and-scrubbed `details`); `CONFIGURATION`-kind details no longer sent to clients.
- **Authz document-scope filters fail closed:** a scope port returning row filters with no DTO attribute to carry them now raises `CONFIGURATION` (was silently dropped → unscoped query).
- **Raw-query tenancy hardening:** `ctx.graph.raw(spec)` (`forze_neo4j`) fails closed in a tenant-aware module (was unscoped across all tenants) and binds `$tenant`; new `ctx.tenancy.current()`/`require_current_id()` for kernel-client ports.
- **Missing authentication surfaces as `AUTHENTICATION` (401), not `AUTHORIZATION` (403).**
- **`builtin.local` API-key verification no longer 500s on non-ASCII input** (UTF-8 bytes comparison).
- **`asyncio.CancelledError` passes through exception interceptors** (was converted to `CoreException`, breaking timeouts, structured concurrency, and graceful shutdown framework-wide).
- **`forze_identity.authn` session enforcement (breaking):** access JWTs carry a `sid` claim cross-checked against the session store, so logout / refresh-rotation invalidate access before `exp`. Pre-upgrade tokens without `sid` fail until re-login (or register a stateless verifier override).
- **`forze_identity.authn` `change_password` requires the current password (breaking):** re-authenticates first, so a hijacked session can't escalate to account takeover.
- **`forze_identity.authn` principal eligibility (breaking):** authn and credential lifecycle gated on `is_active` via `PrincipalEligibilityPort`; `PrincipalDeactivationPort` cascades policy/session/credential deactivation; API keys persist and enforce `expires_at`; key ownership checks take `identity`.
- **`forze_identity.authn` login hardening:** generic 401 for all failures, always runs Argon2 verify (anti-enumeration/timing).
- **`forze_identity.authz` fail-closed tenant isolation:** grant-resolution adapters refuse to construct when a tenant-scoped route has a non-tenant-aware binding/catalog port (`"global"` routes unaffected).
- **`forze_identity.oidc`** resolves JWKS signing keys in a worker thread (no event-loop block on a cache miss).
- **Secret-field redaction:** JWT signing keys / HMAC peppers become `repr=False`; `VaultConfig.token`, `S3RoutingCredentials.secret_access_key`, `GCSRoutingCredentials.service_account_json` → `SecretStr`; `HttpRoutingCredentials.headers` redacted and routed through the one-way KDF in fingerprints.
- **`forze_fastapi` — `X-Tenant-Id`/`X-Forwarded-Host` not trusted by default (breaking):** a raw `X-Tenant-Id` is ignored unless `trust_tenant_header=True` (verified-credential tenants still honored); forwarded host gated on `trust_forwarded_host=True`; Scalar docs default `persist_auth=False`.
- **Input/identifier hardening:** Meilisearch filter attribute names validated (no filter-expression injection / tenant-filter bypass); Postgres PGroonga terms quoted as literal phrases (operator chars can't alter match scope/cost); SQS rejects absolute-URL queue names on tenant-aware adapters; object-storage keys validated (safe charset, no `..`/absolute) before forwarding; `forze_identity.tenancy` rejects invalid hints and inactive tenants.
- **Misc:** BigQuery/GCS routed clients unlink temp service-account JSON files on close; `configure_logging(sanitize_logs=True)` scrubs `error.message`/`error.stack`, and `include_exception_stack=False` omits stacks from JSON logs.

## [0.2.0] - 2026-05-28

### Added

- **Execution:** `OperationRegistry`, `FrozenOperationRegistry`, `Handler`, stage hooks, `OperationRegistry.patch()` / `PlanPatch`, `make_registry_operation_resolver`, `run_operation`, and `facade_op` on document/search/storage/authn facades. `ResolvedOperationPlan` drives runtime hooks, transaction scopes, and after-commit dispatch.
- **Execution context:** nested resolvers — `ctx.document`, `ctx.search`, `ctx.deps`, `ctx.tx_ctx`, `ctx.inv_ctx`, `ctx.authz`, `ctx.analytics`.
- **Tracing:** `ResolutionTracer`/`RuntimeTracer` with `DepsPlan.with_tracing()` and `DepsResolutionTrace.to_key_dag()`; development runtime tracing (`forze.application.execution.tracing`, `FORZE_RUNTIME_TRACE`, `validate_runtime_trace`); optional `TxTracer` on `TransactionContext`.
- **Composition catalogs:** `DOCUMENT_OPERATIONS`, `SEARCH_OPERATIONS`, `STORAGE_OPERATIONS`, `AUTHN_OPERATIONS` under `forze_kits.*.catalog`; operation-plan hooks `forze.application.hooks.{authz,authn,tenancy}`.
- **Query DSL:** literal `$values` / field `$fields` filters, `$not`, array quantifiers (`$any`/`$all`/`$none`), text patterns (`$like`/`$ilike`/`$regex`), aggregate `$computed`/`$groups`/`$trunc`, configurable `QueryFilterLimits`, pre-parsed `QueryExpr` on gateways.
- **Document & search:** `DocumentCoordinator`/`DocumentCacheCoordinator`/`SearchResultSnapshotCoordinator`; `update_matching`/`ensure`; method-specific ports (`find_page`, `find_cursor`, `search_page`, `project_*`, `select_*`, …); hub and federated search (FTS/PGroonga v2, weighted RRF); `RowLockMode` on `for_update`; `select_cursor`; stream methods (`find_stream`/`project_stream`/`select_stream`); `hydrate_from_write`; `default_sort` with shared sort helpers.
- **Durable functions:** contracts under `forze.application.contracts.durable.function`; optional `DurableFunctionSpec.operation`; `handler_for_registry_operation` and `run_durable_function`.
- **`forze_inngest` (`inngest` extra):** Inngest adapter with registry-backed cron/event runs, `inngest_lifecycle_step`, and FastAPI `serve`.
- **Workflow schedules:** schedule contracts and `forze_temporal` Temporal Schedules (create/upsert/update/delete/pause/unpause/trigger/describe/list) with declarative `TemporalDepsModule.schedule_bootstraps`.
- **Queue delayed delivery:** `enqueue`/`enqueue_many` accept `delay`/`not_before` (SQS `DelaySeconds`, Mock `visible_at`, RabbitMQ DLX delay queues when `delayed_delivery=True`).
- **`forze_identity` (+ `oidc` extra):** consolidated authn/authz/tenancy/OIDC with verify-then-resolve ports, `AuthnOrchestrator`, `AuthzPolicyService`; `forze_identity.local` demo file/env API-key identity.
- **Analytics:** `AnalyticsSpec`, `AnalyticsQueryPort`, optional `AnalyticsIngestPort`, and Postgres / ClickHouse (`clickhouse`) / BigQuery (`bigquery`) adapters.
- **`forze_firestore` (`firestore`), `forze_gcs` (`gcs`), `forze_secrets`, `forze_vault` (`vault`):** document, object-storage, and secrets integrations with routed clients and lifecycle steps.
- **Postgres startup validation:** Pydantic↔column compatibility, bookkeeping triggers, and tenancy-wiring checks on `PostgresDepsModule`.
- **Scrubbing & logging:** `forze.base.scrubbing` (`sanitize(value, context=...)`, default structlog field scrubbing via `configure_logging(sanitize_logs=True)`); `ForzeConsoleRenderer.max_traceback_frames` (default 20).
- **Integrations:** Redis distributed locks; `PydanticModelCodec`/`MsgspecModelCodec`; `StrKeySelector`/`StrKeyNamespace`; optional domain mixins in `forze_kits`.

### Changed

- **Breaking — execution & composition:** `Usecase`/`UsecaseRegistry` replaced by `Handler` + `OperationRegistry`. Register with `set_handler`, compose plans via `.patch()`/`.bind()`/`.bind_outer()`/`.bind_tx()`, then `.freeze()`; resolve with `registry.resolve(operation, ctx)`.
- **Breaking — `ExecutionContext`:** `ctx.doc_query`/`ctx.doc_command` → `ctx.document.query`/`.command`; `ctx.dep(...)` → `ctx.deps.provide`/`ctx.deps.resolve_configurable`; `ctx.transaction(...)` → `ctx.tx_ctx.scope(...)`; `CallContext` → `InvocationMetadata` via `ctx.inv_ctx`.
- **Breaking — document & search ports:** result shape and pagination mode are chosen by method name (`find_page` vs `find_cursor`, …); `find_many_with_cursor` removed.
- **Breaking — query DSL:** filter literals use `"$values"` (was `"$fields"`); field compares use `"$fields"` (was `"$compare"`); grouping uses `"$groups"`/`"$trunc"` (top-level `"$time_bucket"` removed).
- **Breaking — identity:** legacy `forze_authnz` consolidated into `forze_identity` (authn/authz/tenancy/oidc). `AuthnIdentity` is principal-only; `AuthnPort` returns `AuthnResult`; tenant hints validated via `TenantResolverPort`.
- **Breaking — authorization:** `AuthzPort.permits(...)` removed; use `AuthzDecisionPort.authorize(AuthzRequest)` with `Authz*` types. Import plan helpers from `forze.application.hooks.authz`.
- **Breaking — durable workflows:** contracts under `forze.application.contracts.durable.workflow` with `DurableWorkflow*` types and renamed dep keys.
- **Breaking — errors:** `forze.base.errors` removed in favor of `forze.base.exceptions`; HTTP `X-Error-Code` defaults to `core.<kind>`.
- **Breaking — tracing:** runtime tracing renamed to `forze.application.execution.tracing` (`RuntimeTrace`, `trace_runtime`, `validate_runtime_trace`); `Deps.merge()` no longer propagates tracer flags (use `DepsPlan.with_tracing()`).
- **Breaking — FastAPI:** `forze_fastapi.endpoints/` and `transport.http/` removed; the package now ships middleware, exception handlers, OpenAPI helpers, and security resolvers only.
- **Breaking — Mongo:** `MongoClient.db`/`collection` and `MongoGateway.coll` are async.
- **Document/search pagination:** omitting `sorts` no longer emits `ORDER BY id` when the read model has no `id` field; configure `default_sort` or pass explicit `sorts`. `@computed_field` names excluded from persistence; `ensure`/`upsert` skip redundant read round-trips on insert.
- **Messaging contracts:** `QueueMessage`/`PubSubMessage`/`StreamMessage` are frozen attrs value objects; queue/pubsub/stream specs require a `ModelCodec`.
- **`forze_gcs`:** native async `gcloud-aio-storage` instead of threaded `google-cloud-storage`. **Postgres PGroonga:** match and `weights` follow index declaration order; every indexed column must appear in `SearchSpec.fields`. **Postgres & Redis:** safer batched writes, implicit read limits, routed pool locking, `get`/`mget` → `bytes | None`, atomic `mset` with `NX`/`XX`, concurrent cache I/O.
- **Scrubbing/console:** log-context scrub uses `**********` and Logfire-aligned substring rules; default Rich traceback visibility 8 → 20 frames. **Socket.IO:** `ForzeSocketIOAdapter.bind` takes `operation_resolver`. **`forze_fastapi`:** unhandled route exceptions return a generic JSON 500 when `register_exception_handlers(app)` is used.

### Removed

- **Execution:** `Usecase`, `UsecaseRegistry`, `UsecasePlan`, the `bucket` module, `facade_call`, `FacadeOpRef`, `OpKeySpace`, `GuardSkip`, and registry graph introspection types.
- **FastAPI:** the `endpoints/` package, `transport.http/`, `ForzeAPIRouter`, `facade_dependency`, and attach-based route helpers.
- **Authn & identity:** monolithic `AuthnAdapter`, `HeaderAuthnIdentityResolver`, `OAuth2Tokens`, `PrincipalContext`, and principal codec ports.
- **Query/search/domain:** deprecated predicate aliases (`QueryPredicate`, …); legacy `PostgresFTSSearchAdapter`/`PostgresPGroongaSearchAdapter` and the `hub_pgroonga` module; `forze.domain.mixins` (use `forze_kits` mixins).

### Fixed

- **`forze_fastapi`:** `register_exception_handlers` CRITICAL-logs tracebacks for unhandled exceptions and 5xx `CoreException` with a chained cause; deliberate causeless 5xx logs at ERROR with structured fields only.
- **Errors:** `CoreError.details` and FastAPI `context` responses no longer expose raw credentials or Pydantic validation `input`.
- **Postgres:** batched `UPDATE … FROM (VALUES …)` casts nullable cells correctly; no duplicate `rev` in `VALUES`; `read_only` set before opening transactions; `text[]` array coercion. **Postgres search:** hub/PGroonga empty queries no longer emit invalid rank SQL; offset snapshot pages reuse validated rows.
- **Redis:** script result normalization avoids rare `isinstance` failures on union types. **S3:** user-metadata decoding on download/list; upload persists optional `description`; default keys use a fresh UUID v7 per call. **Authn:** API-key lifecycle unpacks `(prefix, secret)` in the correct order.

## [0.1.14] - 2026-04-08

### Added

- `forze.base.logging`: structlog-based logging—structured records, TRACE level, Rich console and JSON renderers, request/context binding, per-namespace levels, optional dual pretty (stderr) + JSON (stdout) output, and global unhandled-exception hooks (`register_unhandled_exception_handler`). Replaces the previous Loguru stack.
- `forze_fastapi`: ANSI-colored HTTP status in access logs (`format_status_for_log`); optional `forze_unhandled_exception_handler` / `register_exception_handlers` for non-`CoreError` exceptions (CRITICAL log + 500).
- `forze.application.contracts.workflow`: port protocols and specs for workflow engines (start, signal, update, query, cancel, terminate, handle types).
- `forze_temporal`: Temporal integration package—`TemporalDepsModule` and lifecycle; **workflow adapter** implementing `WorkflowCommandPort`; **client- and worker-side interceptors** to propagate `ExecutionContext`, map headers/metadata, and run **payload codecs** (workflow/activity inputs and results); platform client wiring for workers.
- `forze_fastapi.middlewares.context`: ASGI `ContextBindingMiddleware` to bind call and principal context and emit call-context headers on responses.

### Changed

- **`Deps` replaces `DepRouter`**: spec-based **`DepRouter`** and **`contracts/deps/router.py`** are removed. **Route selection lives on `Deps`**: `plain_deps` vs `routed_deps`, `provide(key, route=..., fallback_to_plain=...)`, `Deps.plain` / `Deps.routed` / `Deps.routed_group`, and updated merge / `without` / `without_route` semantics—no separate router objects in the container.
- **`DepKey` / `DepsPort` imports**: moved to **`forze.application.contracts.base`**; the old **`forze.application.contracts.deps`** package (keys, ports, **router**) is **gone**—replace `from forze.application.contracts.deps import …` with **`from forze.application.contracts.base import DepKey, DepsPort`** (and drop router types).
- **`DepsModule` wiring**: integration packages (**`forze_postgres`**, **`forze_mongo`**, **`forze_redis`**, **`forze_s3`**, **`forze_rabbitmq`**, **`forze_sqs`**, **`forze_temporal`**, …) now build **`Deps` through module callables**, shared **config** types, and **routed** registration aligned with the new container—review each package’s `execution/deps/` for factory signatures and keys.
- **Contracts**: **ports, specs, and dependency keys** updated across domains (document, search, workflow, cache, queue, pubsub, stream, tx, …)—including **renames**, **new overloads** (e.g. document command/query), **search** types/specs reshaped (**`internal/`** parse helpers removed), **`MapperPort`** under **`forze.application.contracts.mapping`**, and **workflow** **deps** + **specs** (signals, queries, updates) expanded.
- **`forze_fastapi`**: HTTP integration **reorganized** under **`endpoints/`** (`attach_document`, `attach_search`, `attach_http`, route **features** for idempotency and ETag); **`ForzeAPIRouter` and the `forze_fastapi.routing` package are removed**—compose a standard **`APIRouter`** and use the **`attach_*`** helpers.
- `forze.base.logging`: new configuration and `Logger` API (`configure`, `getLogger`, message `sub` vs extras); layout and rendering options are documented on the module—migrate any code that relied on Loguru-specific helpers.
- `forze.base.logging`: OpenTelemetry-aware processors, `ExceptionInfoFormatter`, optional custom console renderer when bridging foreign loggers, configurable dim keys, and level-aware Rich console styling.
- `forze_fastapi`: idempotent routes do not record idempotency when the request body is invalid JSON (422), so the same idempotency key can be reused after fixing the body.
- `forze_fastapi`: `attach_http_endpoints` for batch HTTP route registration; `exclude_none` on `attach_document`, `attach_http`, and `attach_search` to control `response_model_exclude_none`.
- `forze.application.execution`: `UsecaseRegistry.finalize` supports `inplace=True` to finalize a registry in place without copying.
- `forze.application.contracts.document` and document adapters (`forze_postgres`, `forze_mongo`, `forze_mock`): optional `return_new` and `return_diff` on create, update, touch, and batch variants—skip repeat reads when the hydrated document is not needed, or return JSON update diffs (and paired results where applicable).

### Removed

- **`DepRouter`** and the **`forze.application.contracts.deps`** package (keys/ports/router split); use **`Deps`** routing and **`forze.application.contracts.base`** for **`DepKey` / `DepsPort`**.
- **`TenantContextPort`** and **`forze.application.contracts.tenant`**.
- **`ActorContextPort`** and **`forze.application.contracts.actor`** (caller identity is modeled via **`ExecutionContext`** / **`AuthIdentity`** and related codecs—see FastAPI **`ContextBindingMiddleware`**).
- Loguru-based implementation and the `loguru` dependency; removed helpers such as `configure(prefixes=...)`, `render_message`, and `safe_preview` in favor of the structlog pipeline and `Logger`.

### Fixed

- `forze_postgres` / `forze_mongo`: document deps modules register each `rw_documents` route’s read/query port from that route’s `read` config (fixes incorrect reuse of `ro_documents` and broken or duplicated routing).
- `forze_postgres` / `forze_mongo`: tenant-aware write gateways include `tenant_id` in UPDATE and hard-delete predicates so writes match read isolation; Postgres still raises `NotFoundError` when no row matches the scoped delete.
- `forze_postgres`: `PostgresFTSSearchAdapter` reads rows from the configured source relation and uses the index only for catalog `tsvector` metadata; empty-query FTS uses a valid `ORDER BY` when no rank is computed.

## [0.1.13] - 2026-03-15

### Added

- `hybridmethod` descriptor in `forze.base.descriptors` for class/instance dual method support.
- `Pagination` DTO with `page` and `size` fields for list and search request payloads.
- `DocumentDTOs` with `list` and `raw_list` keys for custom list request DTO types.
- `SearchDTOs` with `read`, `typed`, and `raw` keys for search facade DTO configuration.
- `build_document_list_mapper` and `build_document_raw_list_mapper` in document composition.
- `build_search_typed_mapper` and `build_search_raw_mapper` in search composition.
- `LoggingMiddleware` in `forze_fastapi.middlewares` for request/response logging with scope.
- `Logger.opt` for passing options (depth, exception, etc.) to the underlying logger.
- `UVICORN_LOG_CONFIG_TEMPLATE` and `InterceptHandler` in `forze_fastapi.logging` for uvicorn log_config integration.
- Storage application layer additions: `UploadObject`, `ListObjects`, `DownloadObject`, `DeleteObject` usecases plus storage composition `StorageUsecasesFacade`, `StorageDTOs`, and `build_storage_registry`.

### Changed

- `OperationPlan.merge`, `UsecasePlan.merge`, and `UsecaseRegistry.merge` are now hybridmethods (callable on class or instance).
- `OverrideDocumentEndpointNames` renamed to `OverrideDocumentEndpointPaths`; `name_overrides` renamed to `path_overrides` in document router.
- `OverrideSearchEndpointNames` renamed to `OverrideSearchEndpointPaths`; `name_overrides` renamed to `path_overrides` in search router.
- Document and search facades now use `dtos: DocumentDTOs` / `dtos: SearchDTOs` instead of `read_dto`; `build_document_registry` and `build_search_registry` require `dtos`.
- `DTOMapper` now requires `in_` (source model type) in addition to `out`; update existing mappers accordingly.
- `MappingStep` protocol is now generic (`MappingStep[In: BaseModel]`); custom step implementations should specify the source type.
- `CoreModel` no longer includes `Decimal` in `json_encoders`; custom serialization for Decimal fields must be handled elsewhere.
- `ListRequestDTO` and `SearchRequestDTO` extend `Pagination`; pagination (`page`, `size`) now in request body.
- List and search usecases take request DTO directly instead of TypedDict with body/page/size.
- Postgres and Mongo document adapters: write operations now return results via read gateway for consistent read/write source separation.
- Logging: scope-based contextualization across execution modules; `logger.section()` for structured spans; usecase scope in log format; `safe_preview` replaces `_args_safe_for_logging` for argument preview.

### Fixed

- Document list endpoints now correctly pass pagination to the usecase.
- Logging format: escape extra dict in output to avoid loguru KeyError; exclude redundant `logger_name` from displayed extra.

### Removed

- `Pagination` and `pagination` from `forze_fastapi.routing.params`; use request body instead.
- `Usecase.log_parameters` and `Usecase._args_safe_for_logging`; use `safe_preview` from `forze.base.logging` instead.
- `register_uvicorn_logging_interceptor`; use `UVICORN_LOG_CONFIG_TEMPLATE` in uvicorn `log_config` instead.

## [0.1.12] - 2026-03-11

### Added

- Paginated list documents endpoint in `forze_fastapi` document router with typed (`list`) and raw (`raw-list`) variants, `ListRequestDTO`, `RawListRequestDTO`, and `ListDocument` usecase.
- `name_overrides` on document and search routers: `OverrideDocumentEndpointNames` and `OverrideSearchEndpointNames` for customizing operation IDs and endpoint paths.
- `attach_document_routes` and `attach_search_routes` for attaching document/search routes to existing routers.

### Changed

- `attach_search_router` renamed to `attach_search_routes` in `forze_fastapi.routers.search`. Update imports accordingly.

### Fixed

- Postgres bulk update: correct table alias in RETURNING clause; use English error messages for consistency errors.

## [0.1.11] - 2026-03-11

### Added

- Route-level HTTP ETag support in `forze_fastapi` with `ETagProvider` protocol, `ETagRoute`, and `make_etag_route_class` for reusable conditional GET handling.
- `RouteETagConfig` and `RouterETagConfig` for per-route and per-router ETag configuration (enabled, provider, auto_304).
- `DocumentETagProvider` that derives ETag values from document `id:rev` for stable version identity without response hashing.
- ETag and `If-None-Match` / 304 Not Modified support on the document metadata endpoint.
- `get()` override on `ForzeAPIRouter` with `etag` and `etag_config` parameters.
- `RouteFeature` protocol and `compose_route_class` engine in `forze_fastapi.routing.routes.feature` for composable route-level behaviors (ETag, idempotency, tracing, etc.) without subclass conflicts.
- `ETagFeature` and `IdempotencyFeature` as standalone `RouteFeature` implementations, decoupled from their `APIRoute` subclasses.
- `route_features` parameter on `ForzeAPIRouter.add_api_route`, `.get()`, and `.post()` for explicit feature composition on individual routes.
- Document update validators now run even when the update produces an empty diff.
- `pydantic_model_hash` normalizes `Decimal` values for stable hashing; `CoreModel` adds `Decimal` to `json_encoders` for consistent serialization.

### Changed

- `ForzeAPIRouter` now composes idempotency, ETag, and custom `RouteFeature` instances into a single route class via `compose_route_class`, replacing the sequential `route_class_override` pattern that only supported one feature per route.
- `pydantic_validate` default `forbid_extra` changed from `True` to `False`; extra keys are now ignored by default.
- `Document.touch()` now returns a new instance via `model_copy` instead of mutating in place.
- Postgres document gateway: revision mismatch now raises `ConflictError` with `code="revision_mismatch"` when history is disabled.
- Postgres query renderer: array operators (`$subset`, `$disjoint`, `$overlaps`) now require array column types via `raise_on_scalar_t`.

### Fixed

- Document metadata endpoint path corrected from `/medatada` to `/metadata`.
- Cache operations in Postgres and Mongo document adapters are now non-fatal; failures are suppressed so primary operations succeed when cache is unavailable.

## [0.1.10] - 2026-03-11

### Added

- Error handler for `forze_mongo` (`mongo_handled`) that maps PyMongo exceptions to `CoreError` subtypes, bringing Mongo in line with Postgres, Redis, S3, SQS, and RabbitMQ error handling.
- Optimistic retry with tenacity on `MongoWriteGateway` write operations (`create`, `create_many`, `_patch`, `_patch_many`), mirroring the existing Postgres retry strategy for `ConcurrencyError`.
- Default adaptive retry configuration (3 attempts) for S3 client when no explicit retries config is provided.

### Changed

- Replaced `DeepDiff`-based dict diff with a lightweight recursive implementation, yielding 50–250× speedup on `calculate_dict_difference` and 10–150× speedup on `apply_dict_patch`.
- Removed `deepdiff` and `mergedeep` runtime dependencies from the core package.
- Cached middleware chain in `Usecase.__call__` to avoid rebuilding closures on every invocation.
- Cached `inspect.signature` lookups in error-handling decorators via `lru_cache`.
- Cached `inspect.getmodule` lookups in introspection helpers via `lru_cache`.
- Cached `TypeAdapter` instances per payload type in `SocketIOEventEmitter` to avoid repeated construction.
- Pre-computed `MappingStep.produces()` results in `DTOMapper` to avoid redundant calls per mapping pass.
- `Document._apply_update` now uses `model_copy(deep=False)` for scalar-only diffs.
- S3 storage adapter `list` now fetches object metadata concurrently via `asyncio.gather` instead of sequential `head_object` calls.
- Used `list.extend` over `+=` for middleware chain construction in `UsecasesPlanRegistry`.
- Added `slots=True` to `_CmWrapper` and `_AsyncCmWrapper` in error utilities.
- Eliminated per-call `inspect.signature().bind_partial()` overhead from error-handling decorators; operation name is now resolved once at decoration time.
- Postgres `fetch_one` with dict row factory uses a dedicated `_row_to_dict` method instead of wrapping in a list.
- SQS queue name sanitization uses pre-compiled regex patterns.
- RabbitMQ `ack`/`nack` now acquire the pending-messages lock once per batch instead of per message.
- Cached `pydantic_field_names` via `lru_cache`; return type narrowed to `frozenset[str]` for immutability.
- Cached `normalize_pg_type` in Postgres introspection utilities via `lru_cache`.
- Pre-computed query operator sets as module-level `frozenset` constants in the filter expression parser, replacing per-call `get_args()` lookups.
- S3 `list_objects` now exits pagination early when the requested limit window has been fully collected.

## [0.1.9] - 2026-03-10

### Added

- Socket.IO integration package `forze_socketio` with typed command-event routing, usecase dispatch through `ExecutionContext`, typed server-event emitter, ASGI/server builders, and optional `forze[socketio]` extra.

### Changed

- **Contracts refactor:** Removed conformity protocols (`DocumentConformity`, `PubSubConformity`, `QueueConformity`, `SearchConformity`, `StreamConformity` and their dep variants). Port protocols remain the source of truth for contract conformance.
- Removed `forze.base.typing`; type checking now enforced via mypy strict mode.

## [0.1.8] - 2026-03-10

### Added

- `strict_content_type` parameter (default True) to `ForzeAPIRouter` and route methods.
- Tenant context support in S3 storage adapter (`forze_s3`).
- `S3Config` TypedDict for abstracting botocore configuration in `forze_s3`.
- Socket and connect timeouts to `RedisConfig` in `forze_redis`.
- Prefix validation to `S3StorageAdapter`.
- Mongo document adapter with dependency factories and CRUD/query support in `forze_mongo`.
- PubSub contracts (`PubSubSpec`, conformity protocols, dep keys/ports) in core and Redis pubsub adapter/execution wiring with publish-subscribe support.
- RabbitMQ integration package `forze_rabbitmq` with queue contracts wiring, client/adapters, execution module/lifecycle, and unit/integration test coverage.
- In-memory integration package `forze_mock` with shared-state adapters/deps for document, search, counter, and additional contracts (cache, idempotency, storage, queue, pubsub, stream, tx manager) for local mock backends without external services.
- SQS integration package `forze_sqs` with async aioboto3 client/adapters, execution module/lifecycle, optional `forze[sqs]` extras, and unit/integration coverage via LocalStack.

### Changed

- Search router: split building and attachment for flexibility.
- Response body chunk processing in idempotent route (performance).
- Postgres `__patch_many` loop now uses `asyncio.gather` (performance).
- Postgres document write operations avoid redundant reads (performance).
- Mongo integration now mirrors Postgres composition with dedicated read/write/history gateways, configurable rev/history strategies (application-managed), and execution module wiring.
- RabbitMQ batch enqueue now publishes via a single channel scope and queue declaration per batch (performance).

### Fixed

- Tenant context dep resolution in S3 storage adapter (invoke dep as factory).
- Read gateway fallback on cache failure.
- Deterministic UUID generation now uses SHA-256 instead of MD5 (security).

## [0.1.7] - 2026-03-08

### Changed

- Package is now published on PyPI instead of OCI (ghcr.io).
- `register_scalar_docs`: parameter `version` renamed to `scalar_version`; docs page title now uses `app.title`.

## [0.1.6] - 2026-03-04

Execution and mapping refactor, middleware-first approach for usecases, split search, cache, and document contracts.

### Added

- `forze.application.mapping` module with `DTOMapper`, `MappingStep`, `NumberIdStep`, `CreatorIdStep`, `MappingPolicy` for composable async DTO mapping.
- `build_document_plan`, `build_document_create_mapper`, and `replace_create_mapper` in `build_document_registry` for document lifecycle and custom create mappers.
- Namespaced `DocumentOperation` and `StorageOperation` values (`document.*`, `storage.*`).
- `CREATOR_ID_FIELD` constant in `forze.domain.constants`.
- Search contract in `forze.application.contracts.search`: `SearchReadPort`, `SearchWritePort`, `SearchSpec`, `SearchIndexSpec`, `SearchFieldSpec`, `parse_search_spec`; `PostgresSearchAdapter` in forze_postgres.
- FastAPI search router: `build_search_router`, `search_facade_dependency` in `forze_fastapi.routers.search`.

### Changed

- `DocumentOperation`, `DocumentUsecasesFacade` moved from `forze.application.facades` to `forze_kits.aggregates.document`. `StorageOperation` moved to `forze.application.usecases.storage`. Facades package removed.
- `Effect`, `Guard`, `Middleware`, `NextCall` moved from `forze.application.execution.usecase` to `forze.application.execution.middleware`.
- `Deps` constructor-based API: use `Deps(deps={...})` instead of `register`/`register_many`. Builder methods removed.
- `Usecase` now requires `ctx: ExecutionContext`; `with_guards`/`with_effects` replaced by `with_middlewares`.
- `TxUsecase` removed; transaction handling via `TxMiddleware` in plan.
- `DocumentUsecasesFacadeProvider` now requires `reg` and `plan` (no longer optional).
- `CreateDocument` and `UpdateDocument` use async `DTOMapper` instead of sync `Callable` mappers. `CreateNumberedDocument` removed; use `build_document_create_mapper(spec, numbered=True)` with `replace_create_mapper` in registry.
- Search spec: public TypedDict specs vs internal attrs; per-index `source`; `SearchGroups` from dict to list for ordering.
- `DepRouter` subclasses: `dep_key` must be set as class attribute when using `@attrs.define` (no longer as class-definition kwarg).

### Fixed

- Postgres history gateway: consistency error messages now in English.
- Postgres search adapter: correct attrs mutable default for gateway cache.
- Postgres index introspection: LATERAL unnest, simplified has_tsvector_col detection.
- Postgres error handler: `GroupingError` handling.

## [0.1.5] - 2026-02-28

### Added

- `scalar-fastapi` dependency and `register_scalar_docs` in `forze_fastapi.openapi` for Scalar API reference UI.
- Exception handlers module in `forze_fastapi.handlers` with `register_exception_handlers`.
- `operation_id` on all document router endpoints for stable OpenAPI operation IDs.
- Exports in `forze_postgres`, `forze_redis`, `forze_s3`: `PostgresDepsModule`, `RedisDepsModule`, `S3DepsModule`, client dep keys, and lifecycle steps.
- `IdempotencyDepKey` in `forze.application.contracts.idempotency` for registering idempotency implementation in the execution context.
- `forze_fastapi.routing.routes` with `IdempotentRoute` and `make_idempotent_route_class` for route-level idempotency (replaces endpoint wrapping).
- `DepsModule`, `DepsPlan` in `forze.application.execution.deps` for dependency composition.
- `DepsPlan.from_modules` and `LifecyclePlan.from_steps`, `with_steps` factory methods.
- `LifecyclePlan` and `LifecycleStep` in `forze.application.execution.lifecycle` for startup/shutdown hooks.
- `ExecutionRuntime` in `forze.application.execution.runtime` combining deps plan, lifecycle, and context scope.

### Changed

- `Deps` moved from `forze.application.contracts.deps` to `forze.application.execution`. Update imports accordingly.
- **Postgres, Redis, S3 restructure:** `dependencies/` removed; modules moved to `execution/` with `PostgresDepsModule`, `RedisDepsModule`, `S3DepsModule` (attrs-based classes) and lifecycle steps (`postgres_lifecycle_step`, `redis_lifecycle_step`, `s3_lifecycle_step`). Replace `postgres_module(client)` with `PostgresDepsModule(client=client)()` and similarly for redis/s3.
- `DepRouter.from_deps` now accepts `DepsPort` and returns optional remainder.
- Port resolvers `doc`, `counter`, `txmanager`, `storage` consolidated into `PortResolver` namespace class. Replace `doc(ctx, spec)` with `PortResolver.doc(ctx, spec)` and similarly for `counter`, `txmanager`, `storage`.
- `DTOSpec` renamed to `DocumentDTOSpec` in `forze_kits.aggregates.document`. Update imports accordingly.
- Document router: request body params now use `Body(...)` with `override_annotations` for correct OpenAPI schema generation.
- `ForzeAPIRouter` and `build_document_router` no longer accept idempotency parameters; idempotency is applied via custom route class and resolved from `ExecutionContext` via `IdempotencyDepKey`. Register your `IdempotencyDepPort` with the key.

## [0.1.4] - 2026-02-27

### Added

- Configurable revision bump strategy in `forze_postgres`: `PostgresRevBumpStrategy` enum (DATABASE vs APPLICATION) and `postgres_document_configurable` factory with `rev_bump_strategy` parameter.
- Middleware protocol and chain composition in `forze.application.execution.usecase.Usecase`.
- `forze.application.features.outbox` module with buffer middleware and flush effect.
- `MiddlewareFactory` and middleware support in `UsecasePlan`.

### Changed

- `TxContextScopedPort` renamed to `TxScopedPort` (simplified: removed `ctx` requirement). Update imports from `TxContextScopedPort` to `TxScopedPort`.
- `require_tx_scope_match` decorator removed; tx scope validation is now handled by `ExecutionContext` when resolving dependencies.
- `PostgresDocumentAdapter` no longer requires `ctx`; uses `TxScopedPort` instead.

### Fixed

- Duplicate guards, middlewares, and effects are now deduplicated by priority when merging `UsecasePlan` operations.

## [0.1.3] - 2026-02-27

### Added

- Filter query DSL in `forze.application.dsl.query`: AST nodes, parser, and value coercion.
- Mongo query renderer in `forze_mongo.kernel.query` for compiling filter expressions to MongoDB queries.
- `forze.base.primitives.buffer` for buffer handling.

### Changed

- **Application layer restructure:** `forze.application.kernel` split into `forze.application.contracts` (ports, specs, deps, schemas) and `forze.application.execution` (context, usecase, plan, registry, resolvers). Update imports accordingly.
- **Contracts flattening:** Top-level re-exports (`contracts.document`, `contracts.deps`, etc.); internal modules moved to `_ports`, `_deps`, `_schemas`, `_specs`.
- **Tx contracts rename:** `TxManagerPort` and related contracts moved from `contracts.txmanager` to `contracts.tx`. Update imports from `forze.application.contracts.txmanager` to `forze.application.contracts.tx`.
- **Postgres filter builder:** Replaced `forze_postgres.kernel.builder` with DSL-based `forze_postgres.kernel.query` renderer. Old builder (coerce, filters, sorts) removed.

## [0.1.2] - 2026-02-26

### Added

- `forze.base.typing` with protocol conformance helpers.
- Domain document support in `forze.domain` built from `forze.domain.models.Document` with name/number/soft-deletion mixins and update-validator infrastructure for safer incremental updates.
- Document kernel in `forze.application.kernel`: pluggable usecase plans, `DocumentUsecasesFacade` factory, `DocumentPort` with explicit `DocumentSearchPort` and `DocumentReadPort`/`DocumentWritePort`, and `DocumentOperation` enum for operation keys.
- Optional FastAPI integration package `forze_fastapi`: routing helpers, idempotent POST support, and prebuilt document router.
- Optional provider packages: `forze_postgres`, `forze_redis`, `forze_s3`, `forze_temporal`, `forze_mongo` with platform clients, gateways/adapters, and dependency keys for composition.

### Changed

- **Kernel:** Transaction handling and dependency resolution refactored around `ExecutionContext` and `forze.application.kernel.deps.*`; `TxManagerPort`/`AppRuntimePort` removed from `forze.application.kernel.ports`. Usecase base now relies on the new context and tx ports.
- **Postgres filter builder** (in `forze_postgres.kernel.builder`): filter input accepts only canonical operator names (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `is_null`, `or`, plus array and ltree ops). Aliases such as `==`, `ge`, `not in`, `in_`, `or_` are no longer accepted and raise `ValidationError`. Use `in` and `or` for membership and disjunction.
- Infrastructure previously under `forze.infra` has been moved into optional packages; core `forze` no longer ships Postgres, Redis, S3, or Temporal implementations.

### Fixed

- Correct UUIDv7 datetime conversion in `forze.base.primitives.uuid` so round-trips between datetimes and UUIDs preserve timestamp semantics.

## [0.1.1] - 2026-02-23

### Added

- Initial DDD/Hex contracts: ports, results, errors.

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
