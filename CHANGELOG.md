# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

Lands **Deterministic Simulation Testing (DST)** as a native framework capability (`forze_dst`): point it at a real Forze app and one master seed reproduces the whole run — schedule, faults, latency, inputs, crashes, network partitions — across single-process and N-node distributed runs, over real registries and real `ExecutionRuntime`s, with zero touches to the app under test.

**Engine & determinism seams**

- **`forze_dst` deterministic runtime** — `SimulationEventLoop` (virtual-clock `BaseEventLoop` + I/O-refusing selector), `SimulationTimeSource`, and `run_simulation(scenario, *, seed, epoch, schedule_seed, scheduler, latency)`: hours of `asyncio.sleep`/deadlines/backoff run in real-wall ms and `(scenario, seed)` replays byte-identically. Real I/O / thread executors raise `RealIOForbidden`; a quiescent loop raises `SimulationDeadlock`.
- **Ambient entropy seam** — `forze.base.primitives` adds `EntropySource`/`SystemEntropySource`/`SeededEntropySource` + `current_entropy_source`/`bind_entropy_source` (+ `token_urlsafe`), the entropy twin of `TimeSource`: `uuid4`/`uuid7` random bits, AEAD nonces, jitter, and identity tokens route through it, so binding a `SeededEntropySource` + `FrozenTimeSource` makes ids and full runs byte-identical (default stays the system CSPRNG). Plus `derive_seed(seed, label)` (stable, order-insensitive blake2b) splitting one master seed into independent per-stream sub-seeds.
- **Monotonic time seam** — `TimeSource.monotonic()` + a free `monotonic()`; deadlines, resilience clocks, caches, and the mock queue/dlock read it (default `time.monotonic()`). A determinism guard wired into `just quality` **fails the build if raw time/entropy primitives are used outside the seams**.
- **Port interception seam (`forze.application.execution.interception`)** — a composable `PortInterceptor` (`around(call, nxt)`) chain around resolved ports, registered via `DepsRegistry.with_interceptors(...)` / `bind_interceptors(...)`, running innermost (inside tracing + resilience), zero-cost when unused — the seam DST plugs cooperative yielding, latency, faults, crashes, and partitions into without touching handlers.
- **`FrozenOperationRegistry.fingerprint()` / `operation_fingerprint(op)`** — a stable, structural hash of the operation catalog (kind, schemas, idempotency/authn/deadline facts) that ties a seed to the code that produced it.
- **In-memory outbound HTTP (`MockHttpServicePort`)** — `MockHttpServiceAdapter`/`MockHttpRegistry` via `MockDepsModule(http=…)`, so an app runs under DST with zero external services.

**Faithful mock transactions & isolation**

- **`MockDepsModule(transactions="journal")` — now the default** *(behavior change)* — atomic-without-serializing: a per-write undo journal across every participating store (documents, outbox, inbox, identity) lets concurrent transactions interleave while an aborted operation leaves no partial writes — making DST findings trustworthy (the payments "double charge" was a false positive under the old no-op manager). `transactions="none"` / `"strict"` remain opt-in.
- **Mock MVCC isolation (snapshot & serializable)** — the journal manager enforces snapshot (rejects write-write) and serializable (also rejects read-write, plus phantoms — a concurrent write to a namespace this tx scanned) via a buffered overlay; a conflict raises `exc.concurrency(code="serialization_failure")` without a global lock.
- **Transaction isolation as a fail-closed contract** — operations declare `OperationPlan().bind_tx().set_isolation(IsolationLevel.…)` and the kernel verifies it against the route's manager (opt-in `IsolationAware` → `TxCapabilities`), raising `exc.configuration(code="tx_isolation_unsupported")` rather than silently running weaker isolation. `TransactionManagerPort` is unchanged; an operation declaring no isolation is unaffected.

**Harness, oracle & exploration**

- **Turnkey harness (`forze_dst.Simulation`) + unified config (`SimulationConfig` / `Simulation.run`)** — give it a registry, a deps factory (`lambda: MockDepsModule(...)`), and invariants; `run(config, *, scenario=/cases=)` is the single exploration entrypoint (strategy via `config.strategy`: op-case / scenario / Hypothesis / DPOR; scheduler via `config.scheduler`: FIFO / random / PCT). One config object is the sole source of nondeterminism — each seed derives independent schedule / fault / entropy / input sub-seeds. On a violation it minimizes to a reproducible counterexample stamped with the catalog fingerprint.
- **Generative scenario model (`Scenario` / `Rule` / `ModelState`), auto-derivation (`derive_scenario`), reactive topology (`Simulation.reactive_map` / `ReactiveMap`)** — model-based arrange→act workloads that pass domain validation; the scenario is inferred from the catalog and refined by probing the engine trace to drop cascade-only (saga / event-handler) operations.
- **Schedulers** — `PCTScheduler` (Burckhardt PCT, depth-`d` bug guarantees), `SystematicScheduler` (DPOR-family exhaustive interleaving search with effect-equivalence reduction), and seeded shuffle perturbation (`run_simulation(schedule_seed=)`) — all reproducible. Plus a generic workload fuzzer (`OpSpec` / `generate_workload` / `simulate_workload`).
- **Oracle** — a context-bound `Recorder` / `record_event` → immutable `History`; `Invariant`s with built-ins `no_duplicate_effect` / `monotonic_per` / `mutual_exclusion` / `no_unexpected_error` / `expect`; `explore` + greedy `minimize` → reproducible `ViolationReport` (replay via `run_recorded`). Plus a Wing-Gong, per-key **linearizability** checker (`linearizable(spec)`, `record_operation`, `RegisterSpec`).
- **Coverage-guided exploration (`behavioral_coverage` / `Simulation.coverage`)** — a PII-free behavioral signal (operation outcomes, port edges, injected faults) drives a self-right-sizing sweep that stops once coverage plateaus (`config.coverage_plateau`), returning `CoverageStats` (and a counterexample if it hits one).

**Injected environment — faults, latency, crashes, partitions**

- **Declarative seeded faults & latency (`FaultPolicy` / `FaultRule`, `LatencyProfile` + `Constant` / `Uniform` / `Exponential`)** — per-`(surface, route, op)` rates for `error` / `timeout` / `crash` / `drop` / `duplicate` / `delay` and per-route latency distributions, declared on `SimulationConfig` and **seeded by construction** (no caller RNG) over any resolved port via the seam (`drop` / `duplicate` apply only to transport-delivery ops; a failing call still yields, so interleavings on failure paths are explored). `PortFaultInterceptor` / `CrashInterceptor` are single-kind primitives. **Replaces the hand-wrapping `FaultyQueueCommand` (removed).**
- **Crash / restart / recovery (`SimulatedCrash`, `CrashInterceptor`, `CrashPolicy`) + the real-runtime path** — `SimulationConfig.crash` turns a run into a crash→restart→recovery scenario (a `BaseException` crash bypasses `except Exception`; the tx rolls back; a fresh `ExecutionRuntime` restarts over the persisted `MockState` and an optional `Simulation.recover` pass runs); `SimulationConfig.runtime=True` drives the plain workload through real `ExecutionRuntime.scope()` (lifecycle + graceful drain) via a `Simulation.lifecycle` plan, beside the bare-context default.
- **Simulated I/O latency + cooperative scheduling** — under `run_simulation` a `CooperativeInterceptor` makes each port call a yield point (so concurrency interleaves at real boundaries for the scheduler to explore) and optionally advances the virtual clock by a per-port latency — so both races and time-dependent bugs surface with no artificial `sleep` in handlers.

**Product loop — trace, reporting & CLI**

- **Engine trace folded into the history + trace convergence** — the core `RuntimeTracer` captures the full execution surface (ports, transactions, an `operation` boundary classified `ok` / `failed` / `error`, `domain` dispatch) with virtual-time stamps and id-only entity **keys** (PII-free); the harness folds it and **projects** operation outcomes from this single source. Trace-driven built-in invariants need no handler instrumentation: `operation_succeeds`, `completes_within`, `single_key_per_operation` (the wrong-entity guard).
- **Counterexample report (`forze_dst.report`)** — `CausalGraph` / `format_report` / `ViolationReport.format()` render the minimized workload, the concurrency that triggered it, the per-span causal trace, an **injected-environment timeline** (faults + latency + partitions, in virtual-time order), recorded facts, and the violated invariant.
- **Regression corpus (`RegressionEntry`, `append_regression`, `load_regressions`)** — a JSON-Lines corpus that turns a found seed into a permanent, replayable entry (with registry fingerprint + violated invariants).
- **`forze` CLI (`forze[cli]` extra)** — `forze dst run module:sim [--strategy --seeds --pct --fault-error --latency --save-regression]` (exit 1 on a violation — CI-friendly), `replay` (re-run the corpus — the regression guard), `coverage`, `topology`, `derive`. Forgiving import strings; a bare registry gets an auto-mock + the `no_unexpected_error` safety net. Test-backed examples at `examples/recipes/dst_payments/` (concurrency) and `dst_reservation_ttl/` (virtual-time TTL).

**Distributed capstone**

- **Multi-runtime distributed DST (`forze_dst.Cluster`, `ClusterConfig`, `Partition` / `PartitionSchedule`)** — N real `ExecutionRuntime` nodes over one shared `MockState` from a single master seed, under group-based network partitions + per-node seeded faults, checked by ordinary distributed invariants; on a violation it minimizes by dropping nodes into a reproducible `ViolationReport`. A partition cuts a node-group off from gated surfaces for a virtual-time window (modeled at the seam as unreachable → retryable), so a correct retry/outbox flow heals while a fire-and-forget one loses work.

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

- **Envelope-encryption core** — `forze.base.crypto` `EncryptedEnvelope` + `KeyManagementPort` (BYOK seam, KEK stays backend), `FieldEncryption` policy, fail-closed `required_encryption` floor (`none < field < envelope`). Adds `cryptography` to core deps.
- **Per-tenant keyring + wiring** — `KeyDirectoryPort` resolves tenant→KEK; `CryptoDepsModule(deterministic_root=…)` composes the stack; `forze_mock` ships dev-only `MockKeyManagement`.
- **At-rest sealing across every persistence & transport plane** — each driven by a `…Spec(encryption=…)` / `encrypt=` policy, fail-closed at wiring, tolerant of legacy plaintext:
  - **Object storage** — `S3StorageConfig`/`GCSStorageConfig` `encrypt=True`; presigned URLs refused.
  - **Document fields** — `DocumentSpec(encryption=FieldEncryption(encrypted={…}))`. `binds_record_id=True` binds record `id` into AAD (bulk-update of a bound field refused with `core.crypto.record_id_required`); `reencrypt_documents` upgrades legacy ciphertext.
  - **Searchable (deterministic) fields** — `FieldEncryption(searchable={…})` (AES-SIV, no KMS) so equality/membership filters rewrite to ciphertext. Root rotation: `deterministic_previous_root` matches both keys, `reencrypt_documents`, then drop. Trade: leaks equality/frequency within a tenant.
  - **Search reads** — `SearchSpec.encryption` (same policy object as the document spec); decrypt out of results across every read path.
  - **Analytics & graph** — `AnalyticsSpec`/`GraphNodeSpec`/`GraphEdgeSpec` `encryption`; sealed on write, decrypted out of every read/traversal. Encrypted columns not analyzable/matchable; analytics rejects `binds_record_id`, graph binds the kind's `key_field`.
  - **Outbox & direct messaging** — `OutboxSpec.encryption` (`none`/`at_rest`/`end_to_end`) and `QueueSpec`/`StreamSpec`/`PubSubSpec` `encryption` (`none`/`end_to_end`); AAD binds `(tenant, event_id)`. `QueueCommandPort.enqueue_many` gains `message_headers`.
  - **Durable payloads** — Temporal (`TemporalConfig(encrypt_payloads=True)`) and Inngest (`DurableFunctionEventSpec(encrypt=True)`). Per-tenant BYOK; *a Temporal worker must be built from the same encrypting client to decode.*
  - **Cache, search snapshots & idempotency results** — sealed via `IdempotencySpec(encrypt_result=True)` etc. when the underlying route encrypts. The in-process L1 stays plaintext in memory.
- **Vault Transit KMS (`forze_vault`)** — `VaultTransitKeyManagement` implements `KeyManagementPort` on Transit; `VaultTransitTenantProvisioner` creates a tenant's Transit key (idempotent).
- **BYOK access-token signing + JWKS** — pluggable `SignerPort` (`Hs256Signer` default, `LocalAsymmetricSigner`, `VaultTransitSigner`); `attach_jwks_route` publishes `/.well-known/jwks.json`. *Breaking: `AccessTokenService(secret_key=…)` → `AccessTokenService(signer=Hs256Signer(secret=…))`; `issue_token`/`verify_token` are now awaitable; `AccessTokenConfig.algorithm` removed.*
- **Crypto & signing observability** — `instrument_crypto(...)` and `forze_identity.authn.instrument_signing(...)`; always-on.

**Multi-tenancy hardening:**

- **Declared-minimum tenant isolation, fail-closed at wiring** — every deps module accepts `required_tenant_isolation` over `none < tagged < namespace < dedicated`, enforced per route; each integration declares its `max_supported_isolation` ceiling. `validate_module_tenancy(...)`. Additive (`None` default unchanged).
- **Neo4j reaches `namespace`/`dedicated`** — `Neo4jGraphConfig.database` accepts a `(tenant_id)->str` resolver; new `RoutedNeo4jClient` resolves per-tenant Bolt URI/credentials (fails closed on partial auth), wired via `routed_neo4j_lifecycle_step`.
- **Tenant infrastructure provisioning (`TenantProvisionerPort`)** — idempotent `provision`/`deprovision` via `TenancyDepsModule.tenant_provisioner`; reference `ObjectStorageTenantProvisioner` + `PostgresSchemaTenantProvisioner`. Opt-in.
- **Analytics per-tenant namespace routing + advisory binding** — `query_database`/`query_dataset`/`query_schema` route into the tenant's namespace; `tenant_aware` routes bind the tenant id, fail closed if unbound. Off by default.
- **Tenant-safe structured graph walk + raw gating** — `GraphQueryPort.scoped_walk(...)` runs an adapter-owned full-path tenant-scoped traversal; the raw hatch is disabled by default (`Neo4jGraphConfig.allow_raw_query` defaults `False`). *(Breaking: deployments using `ctx.graph.raw` must set `allow_raw_query=True`.)*

**Query DSL:**

- **Fluent builder `Q`** — `Q.field("age").gt(18) & Q.field("name").like("a%")` lowers to the same filter AST. New exports `Q`, `QueryCondition`, `FieldRef`. Additive.
- **Hierarchy operators** (`$descendant_of`/`$ancestor_of`) on a `TreePath` field — Postgres native `ltree` or `text` prefix fallback; gated by `QueryCapabilities.supports_hierarchy`. New exports `TreePath`, `HierarchyOp`, `HierarchyValue`.
- **Aggregation** — `$count_distinct`, `$stddev_pop`/`samp`, `$var_pop`/`samp`, `$percentile`, and post-group `$having` on Postgres/Mongo. (`$first`/`$last` deferred.)
- **Full + array-of-arrays nested quantifiers** on every document backend; `supports_nested_quantifiers` gate dropped. `validate_query_field_types` now runs in the gateway and the mock, rejecting mismatches with `query_operator_type_mismatch`.
- **Mixed-direction keyset pagination + per-key `NULLS FIRST/LAST`** — coherent null ordering across backends; old cursor tokens stay valid; Mongo opt-in `computed_null_ordering`.
- **Query discovery metadata** — `build_query_discovery` projects a read model's filterable/sortable/aggregatable surface as OpenAPI `x-forze-query` + MCP line.

**Identity & API keys:**

- **Tenant selector self-service** — `GET /tenants`, `POST /tenants/{id}/activate` (re-mints a tenant-scoped token pair, Pattern B), `DELETE /tenants/{id}`. New `attach_tenancy_routes`.
- **Tenant admin (`forze_kits.aggregates.tenancy_admin`)** — `create_tenant`/`list_members`/`invite_member`/`remove_member`/`deactivate_tenant` via `attach_tenancy_admin_routes`. Ships unguarded — bind `AuthnRequired` + `AuthzBeforeAuthorize` per op. *(Breaking for `TenantManagementPort` implementers: new `list_principal_tenants` + `list_tenant_principals`.)*
- **Self-service API-key management** — `issue_api_key`/`list_api_keys`/`revoke_api_key` as `POST/GET/DELETE /api-keys` (secret returned once). *Breaking for `ApiKeyLifecyclePort`. Migration: `ALTER TABLE <api_key_accounts> ADD COLUMN hint text, ADD COLUMN label text`.*
- **Delegation-aware API keys (user→agent)** — `issue_api_key(actor_principal_id=…)` binds a delegation actor (RFC 8693 `act` claim → `AuthnIdentity.actor`). *Breaking for `ApiKeyLifecyclePort`. Migration: `ALTER TABLE <api_key_accounts> ADD COLUMN actor_principal_id uuid`.*
- **MCP boundary API-key auth** — `ForzeApiKeyVerifier` + `AccessTokenIdentityResolver` protect a FastMCP server with the forze_identity brain (no OAuth flow); reads-only by default.
- **OpenAPI security from configured authn** — `apply_openapi_security` derives `securitySchemes` from the `AuthnRequirement`; principal-requiring ops flagged `x-requires-authn`.
- **Authn plane** — `AuthnOrchestrator` with a full mock identity plane; `attach_authn_routes` (login/refresh/logout/change-password/deactivate + reset); self-service `PasswordResetPort`. `deactivate_principal` ships unguarded.

**Cache:**

- **In-process L1 document cache** (`CacheSpec(l1=L1Spec(…))`) ahead of the distributed cache — tenant-scoped, pluggable eviction; `RedisCacheConfig(invalidation_push=True)`; `CachePort.exists`. Off by default.
- **Stampede protection & adaptive freshness** — singleflight on read-through misses; probabilistic early refresh (`early_refresh_beta`); per-entry `age_ttl`/`sliding_ttl` + keyword `ttl=` on every setter.

**Resilience & runtime:**

- **New strategies** — `AdaptiveBulkheadStrategy` (AIMD concurrency), `AdaptiveThrottleStrategy`, tail-based `HedgeStrategy.adaptive_delay_quantile`, token-bucket `RateLimitStrategy`; `ResilienceDepsModule(port_policies=[…])`.
- **Invocation deadlines** — per-operation budgets (`registry.bind(op).with_deadline(…)`); expiry raises `exc.timeout` (504).
- **Distributed limits** — pluggable `RateLimitStore` (`RedisRateLimitStore`, fails open) so N replicas share one rate; bulkheads/budgets stay process-local.
- **App assembly & deployment** — `build_runtime` + `runtime_lifespan`; graceful drain (`drain_timeout`, default 10s); `DeploymentProfile.FLEET` and `SERVERLESS` (rejects `requires_long_running`).

**Messaging & storage:**

- **Envelope headers + correlation propagation** — messages gain `headers`/`delivery_count`; the relay forwards the full envelope and `process_with_inbox` rebinds correlation/causation across broker hops.
- **Outbox `ordering_key`** — per-aggregate ordering (SQS FIFO `MessageGroupId`, stream partition key). *Migration: `ALTER TABLE … ADD COLUMN ordering_key TEXT`.*
- **Kits queue-consumer runner** (`run_consumer` / `queue_consumer_background_lifecycle_step`) — inbox exactly-once, requeue, poison parking, envelope rebinding.
- **Stream pending-entry recovery** — `StreamGroupQueryPort.claim` (XAUTOCLAIM) + `pending` (XPENDING). *Breaking for port implementers.*
- **Presigned object-storage URLs** — `StorageQueryPort.presign_download` / `StorageCommandPort.presign_upload` (S3 SigV4, GCS V4, mock). *Breaking for port implementers (minting an upload URL is a CQRS write).*
- **Object-storage metadata & access ops** — `head`, `download_range` (206), `download_if_changed` (304), `copy`/`move`, `put_object_tags`; generated routes honour `Range`/`If-None-Match`. *Breaking for `StorageQueryPort`/`StorageCommandPort`/`ObjectStorageClientPort` implementers* (per-object `expires_at` omitted).
- **Resumable multipart uploads** — `StorageUploadSessionPort` (`ctx.storage.uploads(spec)`, CQRS-write-guarded): `begin_upload`/`presign_part`/`complete_upload`/`abort_upload`. Refused on object-encrypting routes.
- **Storage HTTP edge** — kit ops + generated FastAPI routes for presigned download/upload and the full multipart session. Minting an upload URL is a command op — bind authn/authz.
- **Server-side encryption at rest (SSE/CMEK)** — `S3StorageConfig.sse` (`S3ServerSideEncryption(mode="none|s3|kms", kms_key_id=…)`); `GCSStorageConfig.kms_key_name` (CMEK). Separate axis from client-side `encrypt` (does not satisfy a client-side `required_encryption` floor). Off by default.

**Misc:**

- **Catalog/registry ergonomics** — `OperationCatalogEntry` gains `supports_idempotency_key`/`required_permissions`; duplicate `merge` keys raise (`override=True` hatch); one-step `registry.register(…)`.
- **Generated-route mount ergonomics** — every `attach_*_routes` helper gains `resource=` (mutually exclusive with `ns=`) and `path_overrides=`. Additive.
- **Patch authoring — scoped, materialized, fail-closed reach** — `registry.patch(selector, namespace=ns)` / `commit_patch(…, namespace=ns)` match only ops under `ns`; `registry.materialize_patches(*selectors)` folds patches into per-op plans. `merge` now raises when a patch authored in one registry matches another's ops. *(Breaking only for a registry that merged a broad pre-merge patch onto another's ops — pass `merge(…, cross_registry=True)`.)*

### Changed

- **Queue consumer and outbox relay are now configurable classes** — `run_consumer(ctx, …)` → `QueueConsumer(...).run(ctx, *, timeout=…)`; `relay_outbox_*`/`relay_outbox` → `OutboxRelay(...).to_queue/.to_stream/.to_pubsub/.run`. Lifecycle steps keep flat params. *Breaking for direct `run_consumer` / `relay_outbox_*` callers.*
- **Tenant-isolation tier model made coherent** — ladder `none < tagged < namespace < dedicated` (`relation` rung removed); each integration owns its `max_supported_isolation` ceiling (fail-closed); namespace resolution unified into `resolve_scoped_namespace` (key/path formats unchanged).
- **Argon2 hashing off the event loop** — `PasswordService.hash_password`/`verify_password`/`timing_dummy_hash` are now `async` on a bounded pool (`PasswordConfig.hashing_concurrency`, default 4); `*_sync` variants remain.
- **Performance (measured):**
  - *Engine hot path* — hookless op ~2.5→1.2 µs (−52%), QUERY −56%, `bind` −50%; `resolve_simple` memoized (−73%); aggregate `load` skips the dump roundtrip (−22% flat / −67% nested).
  - *Data access* — `Document.update()` copies only changed subtrees (−21%/−44%); Postgres root-tx rides `BEGIN` (−21%), out-of-tx on autocommit (−37%); Mongo `create` skips read-back (−49%), outbox claims in 3 round-trips (−90%, needs a sparse `claim_token` index).
  - *Observability / cold start* — lazy error-context (~8–13 µs→0.2 µs), batched relay marks, opt-in `trace` (26×), memoized log scrubbing; `opentelemetry` confined to `TYPE_CHECKING`/lazy import.
- **FastAPI `style="rpc"` uses REST verbs + query params** — `GET /notes.get?id=`, `PATCH /notes.update?id=&rev=`, `DELETE /notes.kill?id=`, etc. *Breaking: RPC clients must switch from `POST /<op>`; REST and MCP unchanged.*
- **`singleton_lifecycle_step` takes a `DistributedLockSpec`, not a live port** — *Breaking: pass `spec=DistributedLockSpec(name=...)`.*
- **Release-coherence sweep** — relay logs the at-least-once → fire-and-forget downgrade; Temporal `query`/`update`/`result` deserialize into declared types; `ApiKeyConfig.prefix` validated; saga `step_failed` stays `DOMAIN`.

### Fixed

- **Tenant-isolation correctness & parity** — Postgres outbox/inbox now enforce the declared isolation floor; a missing bound tenant fails closed consistently as `authentication`/`tenant_required` (was 500/401 split); mock durable/graph/document adapters now tenant-partition their stores.
- **Post-commit work survives task cancellation** — the after-commit drain runs as a cancellation-protected critical section (`forze.base.asyncio.run_to_completion`), then re-raises; cancellation during the body still rolls back.
- **PGroonga search honors tenant isolation regardless of plan** — a tenant-aware PGroonga search now always uses `filter_first`, overriding `pgroonga_plan="index_first"`/`"auto"` (which scanned cross-tenant rows and could truncate results to a slice of the global top-K).

## [0.3.0] - 2026-06-11

### Added

- **Generated FastAPI routes (`attach_document_routes` / `attach_search_routes` / `attach_storage_routes`):** project a frozen registry's operations onto a user's `APIRouter`; required `style` (`"rest"`/`"rpc"`), dispatches through `run_operation`. Idempotency is now engine-level.
- **`forze_mcp` (`forze[mcp]`) — expose operations as MCP tools (read-only MVP):** `register_tools(server, registry, ctx_factory, …)` adds a frozen registry's operations as FastMCP tools; read-only by default (`include_writes=True` for commands).
- **`forze_duckdb` (`forze[duckdb]`) — in-process DuckDB analytics over object storage (query-only):** `AnalyticsQueryPort` over a Parquet/CSV/Iceberg/Delta lake on S3/GCS/local, no standing warehouse. Wire with `DuckDbDepsModule` + `duckdb_lifecycle_step`.
- **Delegated identity (on-behalf-of, RFC 8693):** `AuthnIdentity.actor` carries the acting principal; `AuthzBeforeAuthorize` enforces least-privilege intersection. Explicit authority via `DelegationPort.may_act`.
- **Operation-level CQRS (`OperationKind` QUERY/COMMAND):** `registry.bind(op).as_query()` runs read-only: command ports unacquirable, tx opens `READ ONLY` (DB-enforced). Untagged defaults to COMMAND.
- **Operation catalog descriptors (`OperationDescriptor` + `FrozenOperationRegistry.catalog()`):** interface-agnostic request/response-schema metadata for projecting operations onto MCP/HTTP; `catalog()` joins descriptor with `OperationKind`.
- **Queryable-field policy (`QueryFieldPolicy` on `DocumentSpec`):** per-aggregate `filterable`/`sortable`/`aggregatable` allow-sets; powers MCP schema discovery + boundary enforcement (`QueryFieldGuard`); direct port calls unrestricted.
- **OpenTelemetry traces + metrics (`instrument_operations`):** wraps every operation in an OTel span plus `forze.operations` counter and `forze.operation.duration` histogram. Opt-in, additive.
- **`@invariant` — declarative domain invariants:** an always-true `(self) -> None` rule enforced on **both** create and update, closing the merge-patch (`model_copy`) bypass of `@model_validator`s.
- **Saga / process orchestration (`SagaDefinition` + in-process executor):** declarative multi-step processes across aggregates; typed `SagaStep`s with `SagaStepKind`, reverse compensation before the pivot. `run_saga(ctx, definition, initial)` must run outside an enclosing transaction.
- **DDD domain events + aggregate roots → outbox:** `DomainEvent`/`AggregateRoot` buffer events; persisting an aggregate drains/dispatches them **in the operation's transaction** via `DomainEventDispatcherPort`. Wired via `DomainEventsDepsModule`.
- **End-to-end worked example (`examples/recipes/order_fulfillment/`):** first runnable, test-backed example: checkout saga → outbox → relay → inbox → downstream, plus compensation, on `forze_mock`.
- **Deterministic time & ids (`TimeSource` seam):** `utcnow()`/`uuid7()` read a context-active `TimeSource`; `bind_time_source(FrozenTimeSource(...))` makes every read deterministic with no call-site changes.
- **Resilience policy pipeline (`forze.application.contracts.resilience`):** composable strategies into a validated `ResiliencePolicy`, run via `ctx.resilience().run(...)` or `ResilienceWrap`. **Hedging** (`HedgeWrap`) and **distributed breaker** (`RedisCircuitBreakerStore`, fails open).
- **Inbox / consumer-side dedup (`forze.application.contracts.inbox`):** `InboxPort.mark_if_unseen`; `process_with_inbox` marks and runs the handler in one transaction (exactly-once effect). `PostgresInboxStore` + mock.
- **Graph contracts + `forze_neo4j` (`forze[neo4j]`):** graph ports via `ctx.graph.query`/`.command`/`.raw`; Neo4j async Bolt adapter (CRUD, `neighbors`/`expand`/`shortest_path`, raw Cypher hatch). In-memory `MockGraphAdapter`.
- **`forze_kits` — consolidated kit package:** kits, aggregates, mapping, DTOs, outbox/notify, secrets, scopes. Absorbs former `forze_patterns`, `forze.application.{composition,handlers,mapping,dto,kit}`, and `forze_secrets` (see Removed migration table).
- **`forze_http` (`forze[http]`):** outbound HTTP: `HttpServiceSpec`/`HttpServicePort`, `HttpClient`/`RoutedHttpClient`, `HttpDepsModule`; `ctx.http` resolves services by name. httpx-backed.
- **`forze_meilisearch` (`forze[meilisearch]`):** async Meilisearch: offset `SearchQueryPort`, `SearchCommandPort`, federated search (native or weighted RRF).
- **Transactional outbox + notify + search-command:** `forze.application.contracts.outbox` (`OutboxSpec`, `IntegrationEvent`) with Postgres/Mongo/Mock stores + relay helpers; `forze_kits.integrations.notify`; core `SearchCommandPort` for external index maintenance.
- **Tenant routing:** declarative per-request backend targets (`RelationSpec`/`NamedResourceSpec`) across all integrations; per-tenant `Routed*Client` variants, `routed_*_lifecycle_step`, LRU pool dedup, `TenantClientRegistry`.
- **Identity — IdP presets (`forze_identity.builtin.idp`):** OIDC presets for Google, VK ID, Telegram Login; `oidc_bootstrap_identity_deps`; PKCE helpers. Authn: `refresh_api_key` rotation, single-use password invites.
- **Execution — freeze/resolve pipeline:** authoring `DepsRegistry` (`freeze()` → `FrozenDepsRegistry.resolve()` → `FrozenDeps`) separates registration from per-scope resolution; matching `LifecyclePlan`. Per-scope caches default on.
- **Codecs:** `default_model_codec`, `DocumentCodecs`/`document_codecs_for_spec`/`DocumentSpec.resolved_codecs`; optional `read_codec`/`ingest_codec`; trusted-row read validation.
- **Postgres / Mongo search:** Postgres `read_validation` strict/trusted, PGroonga plan modes, hub parallel legs + `SearchOptions`; Mongo `MongoDepsModule.searches` (text/Atlas/vector, offset + cursor).
- **Document adapters:** `max_scan_pages`/`max_stream_pages`/`max_chunked_command_pages` (default 100 000, `None` unlimited) with cursor-stall detection.
- **Durable workflow:** `DurableWorkflowRunStatus`/`Description` + `describe()` on `DurableWorkflowQueryPort` (`forze_temporal`).
- **`forze_temporal` secure connections:** `TemporalConfig.tls` / `api_key` / `rpc_metadata` / `data_converter` override; defaults unchanged.
- **AWS — long-lived clients + credential chain (SQS/S3):** one aiobotocore client reused; `access_key_id`/`secret_access_key`/`region_name` optional (default credential/region chain). Per-tenant routed creds still require explicit keys and region.
- **Vault — token renewal, metadata existence, health:** opt-in self-renew loop; `kv_exists` via KV v2 metadata; standard `health()`.
- **`forze_fastapi` upload cap + attach-time validation:** chunked upload streaming under `max_upload_size` (default 64 MiB, `None` disables) with early Content-Length rejection.
- **`forze_socketio` error translation + identity:** handler exceptions become structured ack payloads honoring egress redaction; optional connect-time `identity_resolver`.
- **Distributed-lock fencing tokens (breaking for port implementers):** `DistributedLockCommandPort.acquire` returns `AcquiredLock | None` carrying a monotonic fencing token. Backends without tokens return `token=None`.
- **Object-storage tags end-to-end:** `UploadObjectRequestDTO.tags`; `include_tags` flag on head/list (`True` makes S3 pay `GetObjectTagging`).
- **`IdempotencyPort.fail()` (breaking for port implementers):** releases a pending claim on handler failure so legitimate retries aren't rejected (Redis + mock).
- **`AuthnFacade.deactivate_principal`:** the existing tested handler is now registered into `build_authn_registry`, exposed, and exported.
- **`forze_mock` parity:** strict transactions (`MockDepsModule(strict_tx=True)`); queue/idempotency parity; consumer groups with real `ack`; keyset cursor pagination; tenancy/dlock/search/durable/identity adapters.
- **`forze.base` primitives:** `CacheLane`, `SimpleLruRegistry`/`GuardedLruRegistry`, `InflightLane`, `OnceCell`, `frozen_mapping`, and fingerprint helpers.

### Changed

- **Breaking — document write identity is an explicit argument:** `CreateDocumentCmd` no longer carries `id`/`created_at`; write surface becomes `create(payload, *, id=None)` / `ensure(id, payload)` / `upsert(id, create, update)` with `KeyedCreate`/`UpsertItem`. **Migration:** move `id`/`created_at` into the new arguments; replace bulk lists with the value objects.
- **Breaking — storage CQRS split:** `StoragePort`/`StorageDepKey` split into `StorageQueryPort` (`download`, `list`) / `StorageCommandPort` (`upload`, `delete`); resolve via `ctx.storage.query(spec)` / `.command(spec)`. S3/GCS factory renames.
- **Breaking — coordinators → adapters:** `DocumentCoordinator`→`DocumentAdapter`, `DocumentCacheCoordinator`→`DocumentCache`, `OutboxStagingCoordinator`→`OutboxStaging`, `DistributedLockCoordinator`→`DistributedLockScope`; `forze.application.coordinators` removed.
- **Breaking — codecs unified on `ModelCodec`:** document/search/analytics paths materialize through spec-owned codecs; document kernel gateways require explicit codecs (build via `read_gw`/`doc_write_gw`).
- **Breaking — frozen `attrs` integration configs:** all integration wiring configs are frozen `attrs` (no dict/`TypedDict`); module-level `validate_*_conf` removed (validation at construction / `.validate()`); some timeout fields move to `timedelta`.
- **Breaking — `ensure_bucket` is create-if-missing on both backends (S3):** both now create idempotently and race-safe (was `not_found`). Use `bucket_exists()` for existence assertions.
- **Breaking — `nack(requeue=...)` semantics aligned (SQS):** `requeue=False` no longer deletes the message — it leaves it for the redrive policy; `requeue=True` = immediate redelivery. Apps relying on nack-to-drop must `ack`.
- **Breaking — `workflow_id_template` → `workflow_id_base`:** the schedule field is passed verbatim (Temporal appends the fire timestamp); renamed across contract/adapter/mock, no alias.
- **Idempotency reshaped to engine-level result idempotency:** `IdempotencySnapshot` replaced by interface-agnostic `IdempotencyRecord(result: bytes)`; new `IdempotencyWrap` hook returns the stored typed result early. FastAPI middleware reads `Idempotency-Key`.
- **OCC retry routed through the resilience pipeline:** Postgres/Mongo/Firestore write gateways drop their own `tenacity` for the shared `occ_retry` (`"occ"` policy). Attempt counts unchanged.
- **Write gateways — unified OCC/history validation:** Postgres/Mongo share one `HistoryOccMixin`; a missing history snapshot now raises retryable `exc.precondition` on both.
- **Async contract protocols standardized on `def … -> Awaitable[X]`:** remaining `async def` Protocol ports converted (type-only; call sites unaffected). Async-generator methods unchanged.
- **Transaction nesting contract:** nested scopes are savepoints; isolation/`read_only` honored only at root; a conflicting nested `read_only` raises `tx_nested_read_only_conflict`. `TransactionHandle.id` removed; gained `read_only`.
- **Unbounded-read protection unified on the implicit cap:** Mongo/Firestore gain `find_many_implicit_limit` (default 10 000, `None` disables); the hard "filters or limit required" precondition dropped.
- **Analytics SQL pagination wraps in a subquery:** `apply_limit_offset` wraps Postgres/ClickHouse too; negative limit/offset now raise.
- **`forze_mock` adapters are stricter (potentially breaking for tests):** password verifier actually compares; `MockAuthzDecisionPort`/scope deny-by-default; `MockDocumentAdapter.create` raises `conflict` on duplicate id.
- **Graph contracts (evolving, pre-1.0):** dual-addressing `EdgeRef.by_key`/`by_endpoints`; `shortest_path` single path + new `k_shortest_paths`; `validate_graph_module_spec` raises `configuration`.
- **Execution-context lifecycle tripwire + import-linter + kernel consolidation:** constructing an `ExecutionContext` mid-operation warns; plane layering now `lint-imports`-enforced (14 contracts); kernel-client boilerplate onto `GuardedLifecycle`/`ContextScopedResource`.
- **Internal package layout:** integration `kernel`→`kernel.client`, `execution`→`lifecycle/` + `execution.deps.{configs,factories}`; registry/planning/facade/run move under `forze.application.execution.operations`. Package-root imports unchanged; direct internal-module imports must update.
- **Performance:** hookless operations skip body-stage scaffolding (~30%); per-scope caches reuse gateways/adapters/codecs; JSON logs render via `orjson`.
- **Misc:** Postgres streaming uses a server-side named cursor; outbox bulk `INSERT … ON CONFLICT DO NOTHING` + `claim_pending`/stale-`processing` reclaim (`reclaim_stale_after`, default 5 min) + `requeue_failed`; `forze[oidc]` now bundles `httpx`.

### Deprecated

- **`forze_identity.oidc`:** `OidcTokenVerifier.enforce_issuer_and_audience` now defaults to `True` — construction requires both `issuer` and `audience` unless explicitly opted out.

### Removed

- **Dead public surface (pre-release cleanup, all verified unreferenced):** the `forze[arango]` extra; `AccessTokenService.try_decode_token`; `ISSUER_FORZE_JWT`; `EffectiveGrantsAdapter`; `GCSHead`/`GCSListedObject` aliases; `PostgresQualifiedName.from_string`; the `forze_postgres.kernel.client.fingerprint` module; the never-honored `batch_size` of `MongoClientPort.delete_many`.
- **`python-dateutil` core dependency:** dropped; `datetime_to_uuid7` parses ISO-8601 via stdlib `datetime.fromisoformat` (trailing `Z` accepted).
- **`forze[casbin]` extra:** dropped (no integration shipped).
- **`forze_identity.local` (breaking):** use `forze_identity.builtin.local`; local verifiers/factories no longer exported from `forze_identity.authn`/`.tenancy`.
- **`forze_identity.builtin.telegram`:** Telegram Mini App `initData` HMAC preset, superseded by Telegram Login OIDC under `forze_identity.builtin.idp.telegram`.
- **Execution:** `forze.application.coordinators`; `forze.application.execution.{registry,planning,facade,running}`; `OperationRunner`; `lifecycle_graph_from_sequence` (use `steps_graph_from_sequence`).
- **Validation helpers from public APIs:** Postgres (`validate_pg_search_conf`, …) and integrations (`validate_mongo_search_conf`, …); validation now lives on the config types. Also dict/mapping coercion for `ConfigurablePostgresDocument`/`…ReadOnlyDocument`.
- **Codecs:** `RecordMappingCodec`/`Pydantic*`/`Msgspec*`, `codec_for_model`, public `pydantic_*`/`msgspec_*` helpers (use `ModelCodec`/`default_model_codec`); `SearchSpec.row_codec`/`resolved_row_codec` and `DocumentReadGatewayPort.effective_row_codec` (use `read_codec`).
- **Relocated to `forze_kits` (breaking):** former `forze_patterns`, `forze.application.{composition,kit,handlers.*,mapping,dto}`, and `forze_secrets` now live under `forze_kits`. `Mapper`/`MapperFactory` stay on `forze.application.contracts.mapping`. `OutboxDestination(queue_route=…, queue=…)` replaced by discriminated `OutboxDestination.queue(route=…, channel=…)` (also `.stream`, `.pubsub`).

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

- **Package error mappers were dead code in 12 integrations:** `ChainExceptionMapper` now flattens nested chains, so Postgres `SerializationFailure`/`DeadlockDetected` (+ Mongo/Neo4j conflicts) map to `CONCURRENCY` and OCC retry fires on real serialization conflicts.
- **Firestore transactions:** `Aborted` → `CONCURRENCY`; rollback on `BaseException`; `count_documents` joins the ambient tx; mismatched `database` raises configuration.
- **ClickHouse `run_query_all_pages` is one streaming execution** — consistent snapshot, no growing-OFFSET duplicates.
- **Redis pipelines fail loud on reads:** value-returning methods inside `pipeline()` raise `redis_read_in_pipeline`.
- **RabbitMQ robustness:** `close()` nacks/requeues pending unacked; poison messages dead-lettered; one delay queue per distinct delay. Same poison handling on SQS.
- **Outbox relay failure model (transient retry / poison / drain):** codec-decode (poison) fails immediately; publish failures reschedule with backoff until `max_attempts` (default 5). New durable `attempts`/`available_at` columns (**migration:** `ALTER TABLE … ADD COLUMN attempts INT NOT NULL DEFAULT 0, ADD COLUMN available_at TIMESTAMPTZ`), `mark_retry(...)` (breaking for port implementers), `requeue_failed` resets the counter. At-least-once, ordering not preserved across retries — key on `event_id`.
- **Outbox staging is per-route and per-task:** fixes a process-global `flushed` flag dropping events and shared buffers. New `buffer_for(route)`/`flushed_for(route)`/`peek(route?)`.
- **`GuardedLruRegistry` use-after-dispose race:** refcount transitions and eviction reads happen under the registry lock; a dispose error during drain deregisters and propagates.
- **After-commit callbacks run to completion:** a failing post-commit callback no longer skips the rest — failures aggregate into one `after_commit_failed`.
- **Lifecycle steps are shut down exactly once:** per-scope started-state tracking ends double-shutdown on failed startup.
- **`finally` hooks observe before-hook denials:** before hooks run inside the try/finally; `on_failure` stays handler-only.
- **OCC history validation hardened:** records re-keyed by `(id, rev)`; comparisons run in canonical python-mode space so no-op resends don't falsely conflict.
- **`Document.update()` re-validates the patched state:** merges into a python-mode dump and `model_validate`s — semantic no-ops yield empty diff, partial nested dicts/ISO datetimes no longer raw, `@computed_field` keys excluded, `@model_validator` now runs on update.
- **Concurrent graph waves report all failures** — `ExceptionGroup` for 2+; a single failure raises directly.
- **Per-scope port cache works for per-call specs** — value equality, identity fast-path first.
- **`kill()`/`kill_many()` verify row counts on every path** — all paths raise `not_found` on missing rows.
- **SQS message identity fixed (was breaking inbox dedup):** `QueueMessage.id` is now the broker `MessageId`; the ReceiptHandle moved to `SQSQueueMessage.receipt_handle`.
- **Postgres transaction options no longer leak across pooled connections:** read-only/isolation emitted as `SET TRANSACTION …` inside the root tx.
- **Mongo write conflicts retry under OCC:** WriteConflict (112) and `TransientTransactionError` → `CONCURRENCY`.
- **`forze_fastapi` middleware errors return proper status codes:** `CoreException`s in forze middlewares render the standard JSON error (via `build_core_exception_response`) instead of 500s.
- **RabbitMQ/SQS receive & consume defaults:** bounded `receive` windows; uniform idle-timeout `consume` (`None` = forever, finite = clean stop).
- **`DistributedLockScope` no longer loses the lock silently:** a lost heartbeat is raised (`CONCURRENCY`) at scope exit without masking the body's exception.
- **Notify consumer dedup:** event id derived deterministically from the broker message identity (was a random UUID).
- **All integration kernel clients:** `initialize()`/`close()` serialize on an internal lock; partial-failure assignment hardened (BigQuery/Postgres/Redis).
- **Analytics adapters:** `run_chunked`/`select_run_chunked` reject non-positive `fetch_batch_size` (shared `validate_fetch_batch_size`).
- **Misc fixes:** Postgres `ON CONFLICT` from `conflict_target`/inferred PKs; PGroonga `index_first` cap + `search_count=exact`; Mongo bulk-upsert miss → `mongo_ensure_bulk_miss`; Meilisearch federated finalization; identity duplicate/ambiguous-login detection; `connection_string_fingerprint` includes sorted query params.
- **`forze_temporal` + `forze[mcp]` workflow sandbox:** `sandboxed_workflow_runner()` / `default_sandbox_restrictions()` pass `beartype` and `coverage` through the sandbox, fixing circular-import failures and a coverage-induced test hang.

### Security

- **Password change revokes existing sessions (breaking by default):** `change_password` revokes all sessions (refresh families + `sid`-bound access JWTs); `revoke_sessions_on_password_change=True`, opt-out explicit, missing session ports fail at startup.
- **Rehash-on-login (opt-in):** `Argon2PasswordVerifier` persists parameter-upgraded hashes after login (`password_rehash_on_login=True`), OCC- and fire-safe.
- **`sensitive=True` spec marker keeps credentials off generated surfaces:** `attach_*_routes` / `register_tools` / `register_schema_resources` / `register_resource_templates` refuse sensitive specs at attach time; shipped authn specs are marked.
- **Owner-override permission keys configurable + documented:** the `"admin"` / `"{resource_type}.admin"` bypass moves to `AuthzKernelConfig.owner_override_permissions` (defaults unchanged, empty set disables).
- **`tenancy_mode="global"` warns over tenant-partitioned stores** — grants shared across tenants — set `require_invocation_tenant` for isolation.
- **OIDC nonce value binding:** `verify_id_token_nonce` (constant-time) + `generate_nonce()`/`generate_state()`; VK/Telegram exchange accept `expected_nonce`.
- **Secret values masked in reprs framework-wide:** credential value objects become `repr=False`; `ClickHouseConfig.password`/routing creds and `InngestConfig` keys → `SecretStr`. Direct readers must call `.get_secret_value()`.
- **Outbound HTTP does not follow redirects by default** (`HttpConfig.follow_redirects=False`) — prevents custom credential headers following a malicious 30x to an attacker host.
- **`AuthnDepsModule` rejects a token-verifier override without a resolver override** — principal-collision hazard; fails at startup naming the route.
- **Tenancy adapters enforce the cache/history guard** — a cached principal→tenant binding could keep a detached principal resolving after revocation.
- **Cursor pagination tokens validated as client input:** malformed/stale/tampered tokens raise 4xx (was `INTERNAL` 500); values restricted to JSON scalars.
- **Log message text is scrubbed:** string scrub rules apply to the rendered message after interpolation, not just structured extras.
- **Postgres sort direction whitelisted** (`asc`/`desc` only). **S3 object tags URL-encoded.** **`OidcClaimMapper` rejects empty `iss`/`sub`.**
- **5xx responses no longer leak internal diagnostics:** generic detail for status ≥ 500, sanitized `context` restricted to < 500; `CONFIGURATION`-kind details no longer sent to clients.
- **Authz document-scope filters fail closed:** a scope port returning row filters with no DTO attribute to carry them raises `CONFIGURATION` (was silently dropped → unscoped query).
- **Raw-query tenancy hardening:** `ctx.graph.raw(spec)` (`forze_neo4j`) fails closed in a tenant-aware module and binds `$tenant`; new `ctx.tenancy.current()`/`require_current_id()`.
- **Missing authentication surfaces as `AUTHENTICATION` (401), not `AUTHORIZATION` (403).**
- **`builtin.local` API-key verification no longer 500s on non-ASCII input** (UTF-8 bytes comparison).
- **`asyncio.CancelledError` passes through exception interceptors** — was converted to `CoreException`, breaking timeouts/structured concurrency/graceful shutdown.
- **`forze_identity.authn` session enforcement (breaking):** access JWTs carry a `sid` claim cross-checked against the session store. Pre-upgrade tokens without `sid` fail until re-login (or register a stateless verifier override).
- **`forze_identity.authn` `change_password` requires the current password (breaking):** re-authenticates first, so a hijacked session can't escalate to account takeover.
- **`forze_identity.authn` principal eligibility (breaking):** authn/credential lifecycle gated on `is_active` via `PrincipalEligibilityPort`; `PrincipalDeactivationPort` cascades deactivation; API keys enforce `expires_at`.
- **`forze_identity.authn` login hardening:** generic 401 for all failures, always runs Argon2 verify (anti-enumeration/timing).
- **`forze_identity.authz` fail-closed tenant isolation:** grant-resolution adapters refuse to construct when a tenant-scoped route has a non-tenant-aware binding/catalog port.
- **`forze_identity.oidc`** resolves JWKS signing keys in a worker thread (no event-loop block on cache miss).
- **Secret-field redaction:** JWT signing keys / HMAC peppers become `repr=False`; `VaultConfig.token`, `S3RoutingCredentials.secret_access_key`, `GCSRoutingCredentials.service_account_json` → `SecretStr`; `HttpRoutingCredentials.headers` redacted.
- **`forze_fastapi` — `X-Tenant-Id`/`X-Forwarded-Host` not trusted by default (breaking):** a raw `X-Tenant-Id` is ignored unless `trust_tenant_header=True`; forwarded host gated on `trust_forwarded_host=True`; Scalar docs default `persist_auth=False`.
- **Input/identifier hardening:** Meilisearch filter attribute names validated; Postgres PGroonga terms quoted as literal phrases; SQS rejects absolute-URL queue names on tenant-aware adapters; object-storage keys validated (no `..`/absolute); `forze_identity.tenancy` rejects invalid hints and inactive tenants.
- **Misc:** BigQuery/GCS routed clients unlink temp service-account JSON on close; `configure_logging(sanitize_logs=True)` scrubs `error.message`/`error.stack`, and `include_exception_stack=False` omits stacks from JSON logs.

## [0.2.0] - 2026-05-28

### Added

- **Execution:** `OperationRegistry`/`Handler` with stage hooks, `OperationRegistry.patch()`, and `run_operation`; `ResolvedOperationPlan` drives hooks, tx scopes, and after-commit dispatch.
- **Execution context:** nested resolvers `ctx.document`, `ctx.deps`, `ctx.tx_ctx`, `ctx.authz`.
- **Tracing:** `ResolutionTracer`/`RuntimeTracer` with `DepsPlan.with_tracing()`; dev runtime tracing (`FORZE_RUNTIME_TRACE`).
- **Composition catalogs:** `DOCUMENT_OPERATIONS` (and search/storage/authn) under `forze_kits.*.catalog`; plan hooks `forze.application.hooks.*`.
- **Query DSL:** literal `$values`/field `$fields` filters, `$not`, array quantifiers, text patterns, aggregate `$groups`/`$trunc`; `QueryFilterLimits`.
- **Document & search:** `DocumentCoordinator`, `update_matching`/`ensure`, method-specific ports (`find_page`/`find_cursor`/…); federated search, `RowLockMode`, stream methods, `default_sort`.
- **Durable functions:** contracts under `forze.application.contracts.durable.function`; `run_durable_function`.
- **`forze_inngest` (`inngest` extra):** Inngest adapter with registry-backed runs and FastAPI `serve`.
- **Workflow schedules:** schedule contracts and `forze_temporal` Temporal Schedules via declarative `schedule_bootstraps`.
- **Queue delayed delivery:** `enqueue`/`enqueue_many` accept `delay`/`not_before`.
- **`forze_identity` (+ `oidc` extra):** consolidated authn/authz/tenancy/OIDC with `AuthnOrchestrator`, `AuthzPolicyService`.
- **Analytics:** `AnalyticsSpec`/`AnalyticsQueryPort` with Postgres / ClickHouse (`clickhouse`) / BigQuery (`bigquery`) adapters.
- **`forze_firestore` (`firestore`), `forze_gcs` (`gcs`), `forze_secrets`, `forze_vault` (`vault`):** document, object-storage, and secrets integrations.
- **Postgres startup validation:** Pydantic↔column compatibility and tenancy-wiring checks on `PostgresDepsModule`.
- **Scrubbing & logging:** `forze.base.scrubbing` (`sanitize`, `configure_logging(sanitize_logs=True)`).
- **Integrations:** Redis distributed locks; `PydanticModelCodec`/`MsgspecModelCodec`; optional `forze_kits` domain mixins.

### Changed

- **Breaking — execution & composition:** `Usecase`/`UsecaseRegistry` replaced by `Handler` + `OperationRegistry`. Register with `set_handler`, compose via `.patch()`/`.bind*()`, `.freeze()`, then `registry.resolve(operation, ctx)`.
- **Breaking — `ExecutionContext`:** `ctx.doc_query`/`ctx.doc_command` → `ctx.document.query`/`.command`; `ctx.dep(...)` → `ctx.deps.provide`; `ctx.transaction(...)` → `ctx.tx_ctx.scope(...)`; `CallContext` → `InvocationMetadata` via `ctx.inv_ctx`.
- **Breaking — document & search ports:** result shape/pagination chosen by method name (`find_page` vs `find_cursor`); `find_many_with_cursor` removed.
- **Breaking — query DSL:** filter literals use `"$values"` (was `"$fields"`); field compares use `"$fields"` (was `"$compare"`); grouping uses `"$groups"`/`"$trunc"` (top-level `"$time_bucket"` removed).
- **Breaking — identity:** legacy `forze_authnz` consolidated into `forze_identity`. `AuthnIdentity` is principal-only; `AuthnPort` returns `AuthnResult`; tenant hints validated via `TenantResolverPort`.
- **Breaking — authorization:** `AuthzPort.permits(...)` removed; use `AuthzDecisionPort.authorize(AuthzRequest)`. Import plan helpers from `forze.application.hooks.authz`.
- **Breaking — durable workflows:** contracts under `forze.application.contracts.durable.workflow` with `DurableWorkflow*` types and renamed dep keys.
- **Breaking — errors:** `forze.base.errors` removed in favor of `forze.base.exceptions`; HTTP `X-Error-Code` defaults to `core.<kind>`.
- **Breaking — tracing:** runtime tracing renamed to `forze.application.execution.tracing`; `Deps.merge()` no longer propagates tracer flags (use `DepsPlan.with_tracing()`).
- **Breaking — FastAPI:** `forze_fastapi.endpoints/` and `transport.http/` removed; package now ships middleware, exception handlers, OpenAPI helpers, and security resolvers only.
- **Breaking — Mongo:** `MongoClient.db`/`collection` and `MongoGateway.coll` are async.
- **Document/search pagination:** omitting `sorts` no longer emits `ORDER BY id` when the read model has no `id` field; configure `default_sort` or pass explicit `sorts`.
- **Messaging contracts:** `QueueMessage`/`PubSubMessage`/`StreamMessage` are frozen attrs value objects; specs require a `ModelCodec`.
- **`forze_gcs`:** native async `gcloud-aio-storage`. **Postgres PGroonga:** match/`weights` follow index order; indexed columns must appear in `SearchSpec.fields`. **Postgres & Redis:** safer batched writes, `get`/`mget` → `bytes | None`, atomic `mset` with `NX`/`XX`.
- **Scrubbing/console:** log scrub uses `**********`; traceback frames 8 → 20. **Socket.IO:** `ForzeSocketIOAdapter.bind` takes `operation_resolver`. **`forze_fastapi`:** unhandled route exceptions return a generic JSON 500.

### Removed

- **Execution:** `Usecase`, `UsecaseRegistry`, `UsecasePlan`, the `bucket` module, `facade_call`, and registry graph introspection types.
- **FastAPI:** the `endpoints/` package, `transport.http/`, `ForzeAPIRouter`, and attach-based route helpers.
- **Authn & identity:** monolithic `AuthnAdapter`, `HeaderAuthnIdentityResolver`, `OAuth2Tokens`, and principal codec ports.
- **Query/search/domain:** deprecated predicate aliases; legacy `PostgresFTSSearchAdapter`/`PostgresPGroongaSearchAdapter`; `forze.domain.mixins` (use `forze_kits` mixins).

### Fixed

- **`forze_fastapi`:** `register_exception_handlers` CRITICAL-logs tracebacks for unhandled exceptions; deliberate causeless 5xx logs at ERROR.
- **Errors:** `CoreError.details` and FastAPI `context` responses no longer expose raw credentials or Pydantic validation `input`.
- **Postgres:** batched `UPDATE … FROM (VALUES …)` casts nullable cells correctly; `read_only` set before opening transactions. **Postgres search:** empty queries no longer emit invalid rank SQL.
- **Redis:** script result normalization avoids rare `isinstance` failures. **S3:** user-metadata decoding fixed; default keys use a fresh UUID v7. **Authn:** API-key lifecycle unpacks `(prefix, secret)` in the correct order.

## [0.1.14] - 2026-04-08

### Added

- `forze.base.logging`: structlog-based logging (structured records, TRACE level, Rich/JSON renderers, request/context binding, per-namespace levels, optional dual pretty stderr + JSON stdout, global `register_unhandled_exception_handler`). Replaces the previous Loguru stack.
- `forze_fastapi`: ANSI-colored HTTP status in access logs (`format_status_for_log`); optional `forze_unhandled_exception_handler` / `register_exception_handlers` for non-`CoreError` exceptions.
- `forze.application.contracts.workflow`: port protocols and specs for workflow engines (start, signal, update, query, cancel, terminate).
- `forze_temporal`: Temporal integration package—`TemporalDepsModule` and lifecycle; workflow adapter implementing `WorkflowCommandPort`; client/worker interceptors propagating `ExecutionContext` and running payload codecs.
- `forze_fastapi.middlewares.context`: ASGI `ContextBindingMiddleware` to bind call/principal context and emit call-context headers.

### Changed

- `Deps` replaces `DepRouter`: spec-based `DepRouter` and `contracts/deps/router.py` removed. Route selection lives on `Deps`: `plain_deps` vs `routed_deps`, `provide(key, route=..., fallback_to_plain=...)`, `Deps.plain` / `Deps.routed` / `Deps.routed_group`, updated merge / `without` / `without_route`.
- `DepKey` / `DepsPort` imports moved to `forze.application.contracts.base`; the old `forze.application.contracts.deps` package (keys, ports, router) is gone—replace `from forze.application.contracts.deps import …` with `from forze.application.contracts.base import DepKey, DepsPort` (drop router types).
- `DepsModule` wiring: integration packages (`forze_postgres`, `forze_mongo`, `forze_redis`, `forze_s3`, `forze_rabbitmq`, `forze_sqs`, `forze_temporal`, …) now build `Deps` through module callables with routed registration—review each package's `execution/deps/`.
- Contracts: ports, specs, and dep keys updated across domains (document, search, workflow, cache, queue, pubsub, stream, tx), including renames and new overloads; search `internal/` parse helpers removed; `MapperPort` under `forze.application.contracts.mapping`.
- `forze_fastapi`: HTTP integration reorganized under `endpoints/` (`attach_document`, `attach_search`, `attach_http`, route features for idempotency/ETag); `ForzeAPIRouter` and the `forze_fastapi.routing` package removed—compose a standard `APIRouter` and use the `attach_*` helpers.
- `forze.base.logging`: new `Logger` API (`configure`, `getLogger`, message `sub` vs extras); migrate code that relied on Loguru-specific helpers.
- `forze.base.logging`: OpenTelemetry-aware processors, `ExceptionInfoFormatter`, configurable dim keys, level-aware Rich console styling.
- `forze_fastapi`: idempotent routes do not record idempotency when the body is invalid JSON (422), so the key can be reused after fixing the body.
- `forze_fastapi`: `attach_http_endpoints` for batch HTTP route registration; `exclude_none` on `attach_document`, `attach_http`, `attach_search`.
- `forze.application.execution`: `UsecaseRegistry.finalize` supports `inplace=True`.
- `forze.application.contracts.document` and adapters (`forze_postgres`, `forze_mongo`, `forze_mock`): optional `return_new` and `return_diff` on create, update, touch, and batch variants.

### Removed

- `DepRouter` and the `forze.application.contracts.deps` package; use `Deps` routing and `forze.application.contracts.base` for `DepKey` / `DepsPort`.
- `TenantContextPort` and `forze.application.contracts.tenant`.
- `ActorContextPort` and `forze.application.contracts.actor` (caller identity modeled via `ExecutionContext` / `AuthIdentity` and FastAPI `ContextBindingMiddleware`).
- Loguru-based implementation and the `loguru` dependency; removed `configure(prefixes=...)`, `render_message`, `safe_preview` in favor of the structlog `Logger`.

### Fixed

- `forze_postgres` / `forze_mongo`: document deps modules register each `rw_documents` route's read/query port from that route's `read` config (fixes incorrect reuse of `ro_documents`).
- `forze_postgres` / `forze_mongo`: tenant-aware write gateways include `tenant_id` in UPDATE and hard-delete predicates; Postgres still raises `NotFoundError` when no row matches the scoped delete.
- `forze_postgres`: `PostgresFTSSearchAdapter` reads rows from the configured source relation and uses the index only for catalog `tsvector` metadata; empty-query FTS uses a valid `ORDER BY`.

## [0.1.13] - 2026-03-15

### Added

- `hybridmethod` descriptor in `forze.base.descriptors` for class/instance dual methods.
- `Pagination` DTO with `page` and `size` fields for list/search request payloads.
- `DocumentDTOs` with `list` and `raw_list` keys for custom list request DTO types.
- `SearchDTOs` with `read`, `typed`, and `raw` keys for search facade DTO configuration.
- `build_document_list_mapper` and `build_document_raw_list_mapper` in document composition.
- `build_search_typed_mapper` and `build_search_raw_mapper` in search composition.
- `LoggingMiddleware` in `forze_fastapi.middlewares` for request/response logging with scope.
- `Logger.opt` for passing options (depth, exception) to the underlying logger.
- `UVICORN_LOG_CONFIG_TEMPLATE` and `InterceptHandler` in `forze_fastapi.logging` for uvicorn log_config integration.
- Storage application layer: `UploadObject`, `ListObjects`, `DownloadObject`, `DeleteObject` usecases plus `StorageUsecasesFacade`, `StorageDTOs`, `build_storage_registry`.

### Changed

- `OperationPlan.merge`, `UsecasePlan.merge`, `UsecaseRegistry.merge` are now hybridmethods (callable on class or instance).
- `OverrideDocumentEndpointNames` renamed to `OverrideDocumentEndpointPaths`; `name_overrides` renamed to `path_overrides` in document router.
- `OverrideSearchEndpointNames` renamed to `OverrideSearchEndpointPaths`; `name_overrides` renamed to `path_overrides` in search router.
- Document/search facades now use `dtos: DocumentDTOs` / `dtos: SearchDTOs` instead of `read_dto`; `build_document_registry` and `build_search_registry` require `dtos`.
- `DTOMapper` now requires `in_` (source model type) in addition to `out`; update existing mappers.
- `MappingStep` protocol is now generic (`MappingStep[In: BaseModel]`); custom steps should specify the source type.
- `CoreModel` no longer includes `Decimal` in `json_encoders`; custom Decimal serialization must be handled elsewhere.
- `ListRequestDTO` and `SearchRequestDTO` extend `Pagination`; pagination (`page`, `size`) now in request body.
- List/search usecases take request DTO directly instead of TypedDict with body/page/size.
- Postgres and Mongo document adapters: write operations now return results via read gateway.
- Logging: scope-based contextualization; `logger.section()` for structured spans; `safe_preview` replaces `_args_safe_for_logging`.

### Fixed

- Document list endpoints now correctly pass pagination to the usecase.
- Logging format: escape extra dict to avoid loguru KeyError; exclude redundant `logger_name`.

### Removed

- `Pagination` and `pagination` from `forze_fastapi.routing.params`; use request body instead.
- `Usecase.log_parameters` and `Usecase._args_safe_for_logging`; use `safe_preview` from `forze.base.logging`.
- `register_uvicorn_logging_interceptor`; use `UVICORN_LOG_CONFIG_TEMPLATE` in uvicorn `log_config`.

## [0.1.12] - 2026-03-11

### Added

- Paginated list documents endpoint in `forze_fastapi` with typed (`list`) and raw (`raw-list`) variants, `ListRequestDTO`, `RawListRequestDTO`, `ListDocument` usecase.
- `name_overrides` on document/search routers: `OverrideDocumentEndpointNames` and `OverrideSearchEndpointNames` for customizing operation IDs and paths.
- `attach_document_routes` and `attach_search_routes` for attaching routes to existing routers.

### Changed

- `attach_search_router` renamed to `attach_search_routes` in `forze_fastapi.routers.search`. Update imports.

### Fixed

- Postgres bulk update: correct table alias in RETURNING clause; English error messages for consistency errors.

## [0.1.11] - 2026-03-11

### Added

- Route-level HTTP ETag support in `forze_fastapi` with `ETagProvider` protocol, `ETagRoute`, `make_etag_route_class`.
- `RouteETagConfig` and `RouterETagConfig` for per-route/per-router ETag configuration (enabled, provider, auto_304).
- `DocumentETagProvider` deriving ETag from document `id:rev` without response hashing.
- ETag and `If-None-Match` / 304 support on the document metadata endpoint.
- `get()` override on `ForzeAPIRouter` with `etag` and `etag_config` parameters.
- `RouteFeature` protocol and `compose_route_class` engine in `forze_fastapi.routing.routes.feature` for composable route behaviors.
- `ETagFeature` and `IdempotencyFeature` as standalone `RouteFeature` implementations.
- `route_features` parameter on `ForzeAPIRouter.add_api_route`, `.get()`, `.post()`.
- Document update validators now run even when the update produces an empty diff.
- `pydantic_model_hash` normalizes `Decimal` for stable hashing; `CoreModel` adds `Decimal` to `json_encoders`.

### Changed

- `ForzeAPIRouter` now composes idempotency, ETag, and custom `RouteFeature` instances into a single route class via `compose_route_class`, replacing the single-feature `route_class_override` pattern.
- `pydantic_validate` default `forbid_extra` changed from `True` to `False`; extra keys are now ignored by default.
- `Document.touch()` now returns a new instance via `model_copy` instead of mutating in place.
- Postgres document gateway: revision mismatch now raises `ConflictError` with `code="revision_mismatch"` when history is disabled.
- Postgres query renderer: array operators (`$subset`, `$disjoint`, `$overlaps`) now require array column types via `raise_on_scalar_t`.

### Fixed

- Document metadata endpoint path corrected from `/medatada` to `/metadata`.
- Cache operations in Postgres/Mongo document adapters are now non-fatal; failures suppressed so primary operations succeed.

## [0.1.10] - 2026-03-11

### Added

- Error handler for `forze_mongo` (`mongo_handled`) mapping PyMongo exceptions to `CoreError` subtypes.
- Optimistic retry with tenacity on `MongoWriteGateway` write operations (`create`, `create_many`, `_patch`, `_patch_many`) for `ConcurrencyError`.
- Default adaptive retry configuration (3 attempts) for S3 client when none provided.

### Changed

- Replaced `DeepDiff`-based dict diff with a lightweight recursive implementation (50–250× speedup on `calculate_dict_difference`, 10–150× on `apply_dict_patch`).
- Removed `deepdiff` and `mergedeep` runtime dependencies from the core package.
- Cached middleware chain in `Usecase.__call__` to avoid rebuilding closures per invocation.
- Cached `inspect.signature` lookups in error-handling decorators via `lru_cache`.
- Cached `inspect.getmodule` lookups in introspection helpers via `lru_cache`.
- Cached `TypeAdapter` instances per payload type in `SocketIOEventEmitter`.
- Pre-computed `MappingStep.produces()` results in `DTOMapper`.
- `Document._apply_update` now uses `model_copy(deep=False)` for scalar-only diffs.
- S3 storage adapter `list` now fetches object metadata concurrently via `asyncio.gather`.
- Used `list.extend` over `+=` for middleware chain construction in `UsecasesPlanRegistry`.
- Added `slots=True` to `_CmWrapper` and `_AsyncCmWrapper` in error utilities.
- Eliminated per-call `inspect.signature().bind_partial()` overhead; operation name resolved once at decoration time.
- Postgres `fetch_one` with dict row factory uses a dedicated `_row_to_dict` method.
- SQS queue name sanitization uses pre-compiled regex patterns.
- RabbitMQ `ack`/`nack` now acquire the pending-messages lock once per batch.
- Cached `pydantic_field_names` via `lru_cache`; return type narrowed to `frozenset[str]`.
- Cached `normalize_pg_type` in Postgres introspection utilities via `lru_cache`.
- Pre-computed query operator sets as module-level `frozenset` constants in the filter parser.
- S3 `list_objects` now exits pagination early when the limit window is fully collected.

## [0.1.9] - 2026-03-10

### Added

- Socket.IO integration package `forze_socketio` with typed command-event routing, usecase dispatch through `ExecutionContext`, typed server-event emitter, ASGI/server builders, optional `forze[socketio]` extra.

### Changed

- Contracts refactor: removed conformity protocols (`DocumentConformity`, `PubSubConformity`, `QueueConformity`, `SearchConformity`, `StreamConformity` and their dep variants). Port protocols remain the source of truth.
- Removed `forze.base.typing`; type checking now enforced via mypy strict mode.

## [0.1.8] - 2026-03-10

### Added

- `strict_content_type` parameter (default True) to `ForzeAPIRouter` and route methods.
- Tenant context support in S3 storage adapter (`forze_s3`).
- `S3Config` TypedDict for abstracting botocore configuration in `forze_s3`.
- Socket and connect timeouts to `RedisConfig` in `forze_redis`.
- Prefix validation to `S3StorageAdapter`.
- Mongo document adapter with dependency factories and CRUD/query support in `forze_mongo`.
- PubSub contracts (`PubSubSpec`, conformity protocols, dep keys/ports) and Redis pubsub adapter/execution wiring.
- RabbitMQ integration package `forze_rabbitmq` with queue contracts wiring, client/adapters, execution module/lifecycle, and test coverage.
- In-memory integration package `forze_mock` with shared-state adapters/deps for document, search, counter, cache, idempotency, storage, queue, pubsub, stream, tx manager.
- SQS integration package `forze_sqs` with async aioboto3 client/adapters, execution module/lifecycle, optional `forze[sqs]` extras, LocalStack coverage.

### Changed

- Search router: split building and attachment.
- Response body chunk processing in idempotent route (performance).
- Postgres `__patch_many` loop now uses `asyncio.gather` (performance).
- Postgres document write operations avoid redundant reads (performance).
- Mongo integration now mirrors Postgres composition with dedicated read/write/history gateways and configurable rev/history strategies (application-managed).
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

Execution and mapping refactor, middleware-first usecases, split search/cache/document contracts.

### Added

- `forze.application.mapping` module with `DTOMapper`, `MappingStep`, `NumberIdStep`, `CreatorIdStep`, `MappingPolicy` for composable async DTO mapping.
- `build_document_plan`, `build_document_create_mapper`, `replace_create_mapper` in `build_document_registry`.
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
- `CreateDocument` and `UpdateDocument` use async `DTOMapper` instead of sync `Callable` mappers. `CreateNumberedDocument` removed; use `build_document_create_mapper(spec, numbered=True)` with `replace_create_mapper`.
- Search spec: public TypedDict specs vs internal attrs; per-index `source`; `SearchGroups` from dict to list for ordering.
- `DepRouter` subclasses: `dep_key` must be set as class attribute when using `@attrs.define`.

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
- Exports in `forze_postgres`, `forze_redis`, `forze_s3`: `PostgresDepsModule`, `RedisDepsModule`, `S3DepsModule`, client dep keys, lifecycle steps.
- `IdempotencyDepKey` in `forze.application.contracts.idempotency`.
- `forze_fastapi.routing.routes` with `IdempotentRoute` and `make_idempotent_route_class` for route-level idempotency.
- `DepsModule`, `DepsPlan` in `forze.application.execution.deps`.
- `DepsPlan.from_modules` and `LifecyclePlan.from_steps`, `with_steps` factory methods.
- `LifecyclePlan` and `LifecycleStep` in `forze.application.execution.lifecycle`.
- `ExecutionRuntime` in `forze.application.execution.runtime` combining deps plan, lifecycle, context scope.

### Changed

- `Deps` moved from `forze.application.contracts.deps` to `forze.application.execution`. Update imports.
- Postgres, Redis, S3 restructure: `dependencies/` removed; modules moved to `execution/` with `PostgresDepsModule`, `RedisDepsModule`, `S3DepsModule` (attrs-based) and lifecycle steps (`postgres_lifecycle_step`, `redis_lifecycle_step`, `s3_lifecycle_step`). Replace `postgres_module(client)` with `PostgresDepsModule(client=client)()`, similarly redis/s3.
- `DepRouter.from_deps` now accepts `DepsPort` and returns optional remainder.
- Port resolvers `doc`, `counter`, `txmanager`, `storage` consolidated into `PortResolver` namespace class. Replace `doc(ctx, spec)` with `PortResolver.doc(ctx, spec)`, similarly for the others.
- `DTOSpec` renamed to `DocumentDTOSpec` in `forze_kits.aggregates.document`. Update imports.
- Document router: request body params now use `Body(...)` with `override_annotations` for correct OpenAPI schema.
- `ForzeAPIRouter` and `build_document_router` no longer accept idempotency parameters; idempotency applied via custom route class and resolved from `ExecutionContext` via `IdempotencyDepKey`. Register your `IdempotencyDepPort` with the key.

## [0.1.4] - 2026-02-27

### Added

- Configurable revision bump strategy in `forze_postgres`: `PostgresRevBumpStrategy` enum (DATABASE vs APPLICATION) and `postgres_document_configurable` factory with `rev_bump_strategy`.
- Middleware protocol and chain composition in `forze.application.execution.usecase.Usecase`.
- `forze.application.features.outbox` module with buffer middleware and flush effect.
- `MiddlewareFactory` and middleware support in `UsecasePlan`.

### Changed

- `TxContextScopedPort` renamed to `TxScopedPort` (removed `ctx` requirement). Update imports from `TxContextScopedPort` to `TxScopedPort`.
- `require_tx_scope_match` decorator removed; tx scope validation now handled by `ExecutionContext` when resolving dependencies.
- `PostgresDocumentAdapter` no longer requires `ctx`; uses `TxScopedPort` instead.

### Fixed

- Duplicate guards, middlewares, and effects are now deduplicated by priority when merging `UsecasePlan` operations.

## [0.1.3] - 2026-02-27

### Added

- Filter query DSL in `forze.application.dsl.query`: AST nodes, parser, value coercion.
- Mongo query renderer in `forze_mongo.kernel.query` for compiling filter expressions to MongoDB queries.
- `forze.base.primitives.buffer` for buffer handling.

### Changed

- Application layer restructure: `forze.application.kernel` split into `forze.application.contracts` (ports, specs, deps, schemas) and `forze.application.execution` (context, usecase, plan, registry, resolvers). Update imports.
- Contracts flattening: top-level re-exports (`contracts.document`, `contracts.deps`, etc.); internal modules moved to `_ports`, `_deps`, `_schemas`, `_specs`.
- Tx contracts rename: `TxManagerPort` and related contracts moved from `contracts.txmanager` to `contracts.tx`. Update imports from `forze.application.contracts.txmanager` to `forze.application.contracts.tx`.
- Postgres filter builder: replaced `forze_postgres.kernel.builder` with DSL-based `forze_postgres.kernel.query` renderer. Old builder (coerce, filters, sorts) removed.

## [0.1.2] - 2026-02-26

### Added

- `forze.base.typing` with protocol conformance helpers.
- Domain document support in `forze.domain` built from `forze.domain.models.Document` with name/number/soft-deletion mixins and update-validator infrastructure.
- Document kernel in `forze.application.kernel`: pluggable usecase plans, `DocumentUsecasesFacade` factory, `DocumentPort` with explicit `DocumentSearchPort` and `DocumentReadPort`/`DocumentWritePort`, `DocumentOperation` enum.
- Optional FastAPI integration package `forze_fastapi`: routing helpers, idempotent POST support, prebuilt document router.
- Optional provider packages: `forze_postgres`, `forze_redis`, `forze_s3`, `forze_temporal`, `forze_mongo` with platform clients, gateways/adapters, dependency keys.

### Changed

- Kernel: transaction handling and dependency resolution refactored around `ExecutionContext` and `forze.application.kernel.deps.*`; `TxManagerPort`/`AppRuntimePort` removed from `forze.application.kernel.ports`.
- Postgres filter builder (in `forze_postgres.kernel.builder`): filter input accepts only canonical operator names (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `is_null`, `or`, plus array/ltree ops). Aliases such as `==`, `ge`, `not in`, `in_`, `or_` are no longer accepted and raise `ValidationError`. Use `in` and `or`.
- Infrastructure previously under `forze.infra` moved into optional packages; core `forze` no longer ships Postgres, Redis, S3, or Temporal implementations.

### Fixed

- Correct UUIDv7 datetime conversion in `forze.base.primitives.uuid` so round-trips preserve timestamp semantics.

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
