# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

**Reliability & durability**

- **Self-hosted durable execution (Postgres)** — crash-resumable durable functions and sagas: exactly-once step replay, lease-based recovery with heartbeat renewal, delayed and cron runs, multi-worker fencing, and an opt-in read-only admin port. Wired via `forze_kits`; adds a croniter dependency.

- **HLC durable high-water mark** — the hybrid logical clock resumes from a checkpoint so a restart cannot re-issue a stamp; the outbox flush advances the mark in-transaction. Opt-in, node-global.

- **Postgres co-located idempotency store** — the dedup record commits inside the business transaction (exactly-once across a crash) when the store reports transactional commit; mock counterpart included.

- **CPU-offload seam** — `run_cpu` / `run_cpu_map` run blocking work off the event loop via a context-bound executor (bounded pool in production, inline and deterministic under simulation), honoring the invocation deadline.

- **Fencing-token capability for distributed locks** — a lock spec requiring fencing tokens fails closed at resolve against a backend that cannot mint them (Redis and the mock can).

- **Wiring check** — opt-in `check_wiring` dry-runs every registered operation against a throwaway context so a missing or misrouted dependency surfaces at startup. Diagnostic only.

- **Neo4j transaction manager** — enlists graph writes into the framework transaction scope (READ COMMITTED); not co-transactional with other backends.

**Querying & read models**

- **Nested-field projection & sorting** — dotted paths into nested models and string-keyed mappings work in projections and sort keys on all document backends. The mock now nests a dotted projection key instead of emitting it flat.

- **Materialized derived fields** — `DocumentSpec(materialized=…)` persists selected computed fields as filterable/sortable columns; in-place search accepts them too (Postgres, Mongo).

- **Lenient read & write-omit fields** — a read-model field may have no backing column and a write may omit columns, for expand/contract migrations; documents on all backends plus Postgres/Mongo search.

- **Streaming reads on Mongo & Firestore** — bounded-memory batched reads bring all three document backends to parity.

- **Procedures port** — `ctx.procedure.command(spec).run(params)` runs a spec-named parametrized statement (Postgres + programmable mock); a tenant-aware route fails closed at wiring unless the SQL binds the tenant.

- **Query parameters** — a typed contract binds query-scoped session settings (Postgres + mock); capability-gated, fail-closed.

- **Aggregate capability gate** — a backend that cannot compile aggregates (Firestore) rejects aggregate queries with a precondition error instead of failing at the driver.

- **Graph parity, schema provisioning & k-shortest paths** — `forze_neo4j` covers the full graph query/command ports with the mock verified equivalent by differential conformance tests; idempotent schema provisioning; native k-shortest paths, weighted opt-in via GDS and fail-closed without it.

**Search**

- **Bounded-memory result streaming** — search streams iterate the ranked set in keyset chunks, capability-gated per backend; unsupported backends fail closed.

- **Vector & hybrid search as a declared capability** — unsupported retrieval features fail closed via `SearchCapabilities`; pages gain an optional scores sidecar; fusion choice and filtered-ANN recall are declared per backend.

- **Facets & highlights** — declared on the spec, returned as optional page sidecars across mock, Meilisearch, Postgres and federated search; unsupported fields or topologies fail closed.

- **Thin federated merge** — `FederatedSearchSpec(thin_merge=True)` late-materializes the RRF merge (Postgres, Meilisearch); opt-in, identical results.

- **Search result caps** — `max_results` bounds an unbounded offset search and `highlight_scan_limit` bounds the PGroonga highlight scan; both opt-in.

- **Hub member from a standalone config** — `PostgresHubSearchMemberConfig.from_search_config` derives a hub leg from a standalone search config.

**Execution & handlers**

- **Two-phase prepare/apply handlers** — `prepare` runs read-only outside the transaction, `apply` inside it; a transaction route is required.

- **Transaction isolation as a fail-closed contract** — an operation declares an isolation level verified against the route's manager, never silently weaker; the current level is introspectable.

- **Cross-aggregate (system) invariants** — declarative `SystemInvariant` laws with detective and preventive (in-transaction rollback) enforcement in kits, compilable into a DST oracle.

- **Resilience control plane (`ResilienceAdminPort`)** — `inspect()` snapshots live per-policy/route state; `force_open` is a manual kill-switch where omitting the route covers the whole policy, and a clear also resets breaker state so recovery is immediate; `retune` hot-swaps a policy, re-validated by the wiring gate.

**Observability & encryption**

- **KEK replacement with a migration overlap** — a key directory may name a previous key: reads accept current and previous while writes use current, re-encryption sweeps move the data, and dropping the previous key restores the strict guard. Key-version rotation still needs no action.

- **Blob re-encryption sweep (`reencrypt_objects`)** — streams every object of a route down and back under a fresh data key in bounded memory, in place; metadata survives, an object deleted mid-sweep is counted and skipped, and the sweep returns a `ReencryptReport` (rewritten and skipped counts).

- **Cloud KMS backends (`forze_kms`)** — AWS, GCP and Yandex Cloud envelope-key backends behind the shared `KeyManagementPort` (extras kms-aws / kms-gcp / kms-yc), with transparent key-version rotation and per-tenant KEK provisioning through the same provisioner port as schemas and buckets.

- **One-call logging setup** — `bootstrap_logging` wires framework, integration and third-party loggers plus the uncaught-exception hook; opt-in sampling and dedup volume controls.

- **Unified integration logging** — previously-silent integrations now log under `forze_<pkg>` namespaces with typed logger-name enums; client connect/close logs at trace, off by default.

- **Sampled access logs** — FastAPI and MCP request logging is quiet by default: successes sampled one-in-N, errors always logged, health paths excluded. Successful requests are no longer all logged at INFO; a full mode restores that.

- **Per-port OTel spans & logging** — opt-in per-call CLIENT spans and per-call logging for every resolved port.

- **Signed / encrypted / context-bound cursor tokens** — an opt-in signer HMAC-signs every keyset cursor (or a cipher AEAD-encrypts it) and binds it to spec, tenant and filter fingerprint, rejecting tampered or replayed tokens. Off by default; enabling is a hard cutover — pre-existing cursors 400 once.

- **Per-item stream interception** — an around-stream interceptor capability wraps async-generator port calls: cooperative yield points, whole-stream logging, opt-in mid-stream fault injection.

- **W3C trace-context propagation** — a published event carries its span outbox→broker→inbox (opt-in; relational backends need a traceparent column); outbound HTTP injects it.

- **`EncryptionReach` ladder** — names the messaging encryption reach (none < at_rest < end_to_end); a required reach refuses a weaker route at resolve.

- **Bounded-memory object-storage streaming** — `download_stream` / `upload_stream` move large objects through fixed memory, including client-side-encrypted blobs via a chunked-AEAD format with ranged reads; legacy whole-payload envelopes still read.

**Realtime**

- **Server push + offline store-and-forward** — handlers publish realtime signals through messaging ports; the Socket.IO gateway bridges to tenant-scoped rooms, and durable signals are mailboxed and replayed per device on reconnect.

- **Tenant-aware & multi-node hardening** — per-tenant consume loops, a tenant-sharded outbox relay, TTL-backed presence with heartbeat, credential-expiry eviction, per-emit timeout.

- **BREAKING — realtime delivery envelope** — every Socket.IO frame is the uniform `{id, data}` envelope; clients read data and dedup by id.

**Transports & DX**

- **Offset-log stream consumption** — a fourth delivery model for partitioned offset-committed logs: commit-stream query/admin ports with capability gates, and a kits consumer that commits after inbox dedup (exactly-once effect). Mock adapters and a conformance battery included.

- **`forze_kafka`** — the first offset-log backend (kafka extra): produce, consume and admin (replay, lag), end-to-end encryption, tenancy.

- **Redis stream & pub-sub transports** — production Redis backends for stream and pub-sub specs with optional end-to-end encryption and tenant prefixes; the consumer-group adapter splits data and admin planes.

- **Streaming download route** — the generated download route streams by default with real ranged requests, conditional 304s and a new HEAD route, backed by three read-only storage kit ops. The ETag is now the backend etag; a buffered mode stays opt-in.

- **Top-level front door** — the most-used names re-export lazily from forze and forze_kits; the core never imports kits.

- **Less CRUD boilerplate** — `build_document_registry` derives DTOs when omitted; `document_facade` returns a typed per-call factory.

- **AggregateKit** — one declaration composes a governed aggregate's wiring: document CRUD, soft delete, search-index sync, invariant enforcement, in-transaction outbox flush, route projection, and DST-verifiable invariants. Its four primitives (outbox emit, search sync, soft delete, invariants) are usable standalone.

- **Shared error helpers** — one client-safe error envelope and kind-to-HTTP-status mapping shared by the FastAPI and Socket.IO edges.

- **Mock document adapter tenant scoping on every write** — the in-memory mock injects the tenant column on ensure/upsert/update/touch, matching Postgres.

- **Telegram Login Widget verifier** — verifies Login Widget callback data via Telegram's HMAC scheme with a freshness bound, emitting the canonical assertion.

**Deterministic Simulation Testing (`forze_dst`)** — new package

- **Point-at-a-real-app simulation** — one master seed reproduces a whole run (schedule, faults, latency, inputs, crashes, partitions) over real registries and runtimes, single-process or N-node, with no app changes; a violation minimizes to a reproducible counterexample.

- **Deterministic runtime & seams** — a simulation event loop and time source give byte-identical replay; time and entropy flow through bindable seams, and a determinism gate bans raw time/entropy outside them.

- **Faults, latency, crash & partitions** — declarative fault rules, latency profiles, crash-restart recovery over persisted mock state, and lossy or asymmetric network partitions.

- **Workloads, schedulers & coverage** — scenario rules and a fuzzer generate workloads; PCT and systematic schedulers explore interleavings; coverage-guided exploration and parallel seed sweeps scale the search.

- **Oracles** — built-in invariants (duplicate effects, mutual exclusion, linearizability, consistency models), transactional-isolation oracles up to serializability cycles, commutativity checks, reachability targets, and opt-in value-level checks.

- **Reporting & regression corpus** — causal-graph violation reports and timelines, failure bundles with honest replay semantics, a regression corpus, and a CLI (`forze dst run` / replay / coverage / topology / derive).

- **Mock substrate** *(behavior change)* — journalled transactions with MVCC isolation are now the mock default; in-memory outbound HTTP added.

- **Adapter conformance** — a backend-agnostic isolation-anomaly battery asserts the mock matches real Postgres and Mongo, including lock races, outbox→inbox delivery under crash, resilience stores under partition, and identity rotation/revocation under fault.

### Changed

**Breaking — search**

- **Search pages split from the base pagination contract** — search page types now carry facets, highlights and the snapshot handle; the facet, highlight and snapshot value types move to the search contract.

- **`SearchFuzzySpec` is a frozen value object** — was a dict; edit-distance ratio defaults to 0.34 (validated 0.0–1.0), prefix-length removed. No shim.

- **Search options de-leaked** — the raw-Groonga override is removed, the PGroonga plan is adapter config only, candidate caps renamed, and hub/federated member keys move to a multi-source options type.

- **Search index provisioning moves to `SearchManagementPort`** — ensure-index and delete-all move off the command port onto the management port (Meilisearch).

- **Typed value-object configs** — search engine is a tagged union, federated merge takes a shared Rrf, warehouse analytics take a shared IngestSpec (Postgres drops the legacy schema field).

**Breaking — imports & DSL**

- **Application contracts surface consolidation** *(no runtime change)* — codecs move to base serialization, conformity and lock types move to their contracts homes, aggregate laws rename to `SumOf` / `CountAll`, and a `TenantSecretResolver` replaces the per-kind resolver callables.

- **Contract value types import from their contracts home** — resilience store types, lifecycle types, deps maps and outbox staging types import from `contracts.*`, not the execution layer.

- **`update_many` takes `Sequence[KeyedUpdate]`** — not id/rev/dto tuples; single-item update unchanged.

- **`GroupRef` (query grouping) → `GroupField`** — resolves the clash with the authz GroupRef.

- **PEL stream ports renamed** — `StreamGroup*` becomes `AckStreamGroup*` (ports and dep keys), pairing with the new commit-stream ports; behavior unchanged. Stream messages gain optional partition/offset fields.

- **Package restructures** — forze_dst splits into a facade over engines/oracle/artifacts; forze_mock moves adapters and factories (top-level imports unchanged); the notify router becomes a register-then-freeze builder.

**Behavior**

- **Lazy transaction acquisition** — default for Postgres/Mongo/Firestore: a scope defers connection checkout to the first operation; opt out per route.

- **Runtime owns its CPU-offload pool** — a runtime scope binds and closes a scope-lifetime thread pool; with nothing bound, `run_cpu` runs inline.

- **Search snapshots stream their pool** and expose an expiry; hub search defers heavy projection to per-page hydration.

- **Empty filter/sort maps are no-ops** on list/search requests; a structured-but-empty envelope is still rejected.

- **Sizing bounds clamp or reject instead of silently resetting** — an out-of-range batch size is rejected at wiring; a per-call chunk size clamps to the nearest bound.

- **Integration logger namespaces unified** — integrations log under `forze_<pkg>`; update log filters keyed on the old bare prefixes.

- **Hot-path micro-optimizations** — byte-identical output: faster normalization, cursor canonicalization, bulk decode and span construction.

- **Generated FastAPI routes omit null response fields by default** — opt out per attach call to restore explicit nulls; raw-Response routes unaffected.

- **`reencrypt_documents` returns a `ReencryptReport`** *(breaking)* — rewritten and skipped counts instead of a bare int; a row deleted between listing and its write-back is now skipped instead of aborting the pass.

### Removed

- **`msgspec` dropped; the codec layer is Pydantic-only** *(breaking)* — the msgspec codec and serialization module are removed; record models must be Pydantic. Storage value objects become frozen keyword-only attrs classes.

### Fixed

**Durable execution & broker failure paths**

- **Inngest event-supplied identity is not trusted by default** — the envelope's principal and tenant are attacker-controllable, so they no longer bind as identity unless opted in via bind_identity_from_event; payload decryption still uses the envelope tenant (self-authenticating).

- **Temporal saga failures fail the workflow** — a saga CoreException becomes a non-retryable ApplicationError; the deterministic clock survives a plain import through the workflow sandbox.

- **Inngest deterministic failures stop retrying** — malformed events, failed decrypts and non-retryable errors raise NonRetriableError; retryable kinds still propagate.

- **SQS: one poison message no longer poisons the receive batch** — per-message decode failures are isolated (left for redrive; deleted on FIFO so a message group cannot deadlock); FIFO per-message delay and over-length queue names fail closed.

- **RabbitMQ: opt-in dead-letter sink and redelivery counting** — a configured DLX makes a poison nack dead-letter instead of drop, and opt-in redelivery counting lets poison-parking fire past the broker's redelivered ceiling (requires publisher confirms; enabling the DLX on an existing queue requires recreating it).

- **Kafka admin reads no longer auto-create topics** — existence is checked against the all-topics listing, so querying lag for a mistyped topic returns empty instead of minting the topic.

- **Inngest function-level config** — `InngestFunctionConfig` forwards Inngest's native controls (retries, concurrency, rate limits, batching and more).

- **An unregistered durable run name cannot livelock recovery** — the run lands FAILED with the reason recorded instead of stranding every co-claimed run each sweep; the scheduler likewise isolates a corrupt cron expression.

- **A transient KMS failure during consume is retried, not poison** — decrypt failures classify through the egress policy: retryable kinds requeue (queue consumer) or crash for a supervised rewind-and-restart (commit-stream) instead of being dropped; tampering still parks.

**Reliability — durability, shutdown, resilience**

- **Deadlines enforced at the database driver** — a bound deadline sets Postgres statement_timeout in-transaction and wraps Mongo ops in client-side timeouts, tighten-only, with an asyncio backstop.

- **Cancellation or a deadline tearing a commit is non-retryable** — both surface `commit_ambiguous` instead of a retryable error or a raw CancelledError, scoped to operation-owned tasks so a shutdown cancel still reaches consumer loops; during the body both still roll back and stay retryable.

- **Shutdown reliability** — the drain timeout cancels still-running ops and awaits their unwind before teardown; detached refresh tasks are cancelled; each shutdown hook gets its own timeout.

- **Spawned operations no longer escape drain; engine hops stay inside the admitted op** — nesting is decided by task identity, so a handler-spawned op is admitted, counted and drained, while the engine's own hops (prepare, hedges, post-commit, fan-out) adopt the operation and cannot be rejected mid-drain.

- **A failed after-commit effect no longer discards a committed result** — it is reported out-of-band to an after-commit error handler, now installable via build_runtime; fatal callbacks still re-raise.

- **Inbox dedup no longer treats the ordering key as an event identity** — the fallback is now header then message id (also for causation binding and notify event ids), so distinct headerless events sharing an ordering key both process. Pre-0.4 headerless relay messages degrade to at-least-once instead of silent drops.

- **Inbox exactly-once fails closed on a cross-client misconfiguration** — inbox processing asserts the store commits in the transaction, raising a configuration error instead of silently non-atomic dedup.

- **Resilience hardening** — the breaker classifies by downstream health (429/OCC do not trip it; timeouts count); state maps are LRU-bounded; a blanket policy retrying ambiguous failures is refused at build; a store outage fails open by default.

- **Streams under a port policy now feed the circuit breaker** — acquisition is breaker-gated (including force-open) and mid-stream infrastructure failures record with unary parity, so a stream-only-failing downstream trips the breaker for its unary siblings. Streams get no retry, hedge, timeout or bulkhead slot.

- **Resilience bookkeeping never fails a completed call** — hedge metrics count correctly, and limit, breaker and digest bookkeeping errors surface as metrics only.

- **Hedging requires an explicit safety basis** *(breaking, freeze-time)* — an idempotency wrap alone no longer passes the gate; every hedge must declare a safety basis or fail closed at freeze.

- **Bulkhead no longer over-admits after a cancelled shed** — a shed-then-cancelled waiter no longer releases a slot it never held.

- **Operation OTel spans stop painting expected 4xx failures red** — client-class domain errors record as failed on a clean span, so error-rate alerts track genuine faults.

- **Default resilience executor is per-event-loop** — a bulkhead never resolves a waiter parked on a foreign or closed loop.

- **Idempotency** — the dedup TTL default rises from 30 seconds to 24 hours to cover the redelivery horizon; a store failure after the business commit no longer fails the succeeded op.

- **Batch field decryption no longer stalls the event loop** — large encrypted result sets decrypt off-loop against a thread-safe snapshot, byte-identical output.

- **`require_tenant_id` raises authentication, not internal** — a missing bound tenant is caller-caused.

- **Hard-delete cache-invalidation failures surface at error level** — a deleted document served from cache is a correctness hazard; still best-effort so a cache outage cannot block a delete.

- **Opt-in guard against outbox dual-writes** — an outbox spec can require flush-inside-a-transaction as a checked precondition.

- **Outbox relay no longer dead-letters the backlog on a missing keyring** — the pass aborts with rows left pending; genuine decode poison still parks the row.

**Bounded memory**

- **Unbounded in-process caches gain bounded defaults** — the Postgres estimate lane, Redis breaker local cache, document-cache refresh fan-out and L1 registry sweep; all with escape hatches.

- **More read/list paths stream** — GCS listing pages, mailbox trim projects only ids, Postgres hub parallel search late-materializes, and update_matching batches by keyset instead of one unbounded UPDATE.

- **Streamed offline-mailbox replay** — replay emits page-by-page instead of loading a device's whole backlog.

- **Mongo ranked search late-materialization** — thin rank rows are sorted and paged before hydrating the page by id.

- **Analytics `run_chunked` truly streams** — DuckDB, ClickHouse and BigQuery consume the streamed query one window at a time.

- **`HttpConfig(max_response_bytes=…)`** — caps the in-memory response body; default off.

**Error taxonomy**

- **Client-caused errors no longer masquerade as 500s** — unsupported features raise precondition and malformed values raise validation, uniform across mock, Postgres, Mongo and Firestore; genuine server faults stay internal.

- **Bad query fields are a client error** — a sort or filter naming an absent field raises precondition; a spec's own bad default sort stays configuration.

- **Malformed `$and`/`$or` is a clean 400** — a combinator whose operand is not a list raises precondition instead of an AttributeError.

- **Cursor limits and token fields are validated at decode** — a non-integer page size is rejected and clamped to the 10,000 cap; a crafted token version or non-finite decimal/float raises validation instead of an uncaught 500.

- **Encryption fail-closed** — filtering a randomized-encrypted field raises precondition; encrypted-sort rejection covers every search backend.

- **Consistent adapter errors** — the mock rev-conflict matches the real adapters; a missing dependency names what is registered; database errors classify on codes, not message text.

**Correctness & consistency**

- **Reverse cursor pages return the window adjacent to the cursor** — the document assembler, Postgres hub parallel cursor and Mongo search cursor kept the wrong over-fetch sentinel; all now delegate to one shared keyset windowing. Mongo search first-page prev cursor is now null like every backend.

- **Firestore cursor pagination works past page 1** — the anchor snapshot was passed in a list (a TypeError on any real token) and the before direction was inverted; a deleted anchor now fails closed as a stale cursor. The dead start_before_id client-port parameter is removed.

- **`$neq` / `$nin` / `$disjoint` include NULL rows on Postgres** — the renderer uses IS DISTINCT FROM semantics, matching mock and Mongo.

- **Decimal cursor keys order numerically** — keyset comparison coerces numeric types to Decimal (was string-comparing) and a Decimal sort key round-trips exactly.

- **Intercepted streams close deterministically** — the proxy wrap and both builtin stream interceptors now chain aclose to the backend cursor at close time instead of leaving release to garbage collection; abandonment logs nothing and mid-stream errors keep their classification.

- **Concurrency primitives hardened** — the in-flight lane shields the shared task from one caller's timeout, and the LRU registries no longer double-dispose or dispose under the lock.

- **Mock isolation matches Postgres at the default level** — READ COMMITTED conflict detection anchors on the version actually read, a duplicate-id create race raises conflict, and FOR UPDATE is honoured; verified against real Postgres.

- **DST systematic search is complete again** — the DPOR frontier zero-pads the choice prefix, so previously-unreachable schedules are explored; violation reports print a faithful reproduce line.

- **Regression bundles handle non-self-contained strategies honestly** — a bundle that cannot self-replay reports as a clear failure, and one bad bundle cannot crash the batch.

- **Mock outbox/inbox write-through is a catalogued DST divergence** — confirm a premature-visibility finding against a real broker or store.

- **Kafka commit-stream consumer is loss-free under poison and rebalance** — malformed payloads pause instead of raising, every pause or abort re-seeks to committed, a rebalance listener drops stale routing, and the supervised lifecycle restarts crash-loss-free.

- **Firestore write path is OCC- and tenant-safe** — patch does real rev-CAS in a transaction, deletes are tenant-verified, unsupported operators fail closed, creates fail closed on an existing id, and Firestore joins the cross-backend DSL parity harness.

- **CQRS read-only guard covers eager (factory-time) write-port acquisition** — an eager command acquisition in a QUERY factory hits the same guard as a call-time one.

- **Notifications route through the queue consumer** — redelivery dedups on the event id and poison messages park.

- **Outbox relay tenancy** — each claim's tenant is bound before publishing; a tenant-aware outbox on the plain relay fails closed.

**Field encryption & KMS**

- **Durable-secret entropy is a distinct type a seeded source cannot satisfy** — a replayable EntropySource splits from a CSPRNG-only SecretEntropy for nonces, tokens and keys, making a predictable secret unrepresentable; the permit_insecure_entropy flag is removed. A simulation seeds only the replayable seam.

- **`SystemEntropySource.random()` is CSPRNG-backed** — it drew from the global Mersenne Twister while advertising the system CSPRNG.

- **Strict mode for encrypted fields (`reject_plaintext`)** — opt-in rejection of non-ciphertext values in encrypted and searchable fields after backfill, reachable declaratively: the spec flag flows through every codec resolver (documents, search, graph, analytics, procedures), previously hardcoded off.

- **Plaintext data keys no longer reachable via repr** — the keyring's caches and frozen decryptor suppress raw DEK bytes.

- **Cached data keys honor a TTL** — a KEK rotation or revocation takes effect within the configured window; the crypto module now forwards the TTL and cache bounds to the keyring it builds.

- **Confused-deputy guard on decrypt** — with a tenant supplied, the keyring authorizes an envelope's key id against the tenant's own key before any KMS unwrap. The guard holds on decrypt-cache hits too — the sync pre-pass previously skipped it when the key was already cached.

- **Vault Transit signer picks up key rotation** — the cached public key re-fetches after a TTL, so a rotated key verifies without a restart.

**Identity & authorization**

- **OIDC verifier no longer re-fetches JWKS per request** — the verifier and its key provider are built once and reused, so the JWKS cache actually spans requests.

- **Nonce enforcement reachable through presets** — presets forward require_nonce to the token verifier; default off.

- **`ForzeJwtTokenVerifier` guards its session spec** — the same no-cache/no-history construction check as every sibling verifier, so a revoked session cannot be served from cache.

- **Authz grant resolution cross-checks the tenant** — a caller-supplied scope naming a different tenant is refused.

- **OIDC assertion records the validated audience** — the matched audience, not the first list entry.

- **`trust_tenant_header` no longer binds a tenant for anonymous requests on a resolver-gated app** — the raw header fallback applies only without a resolver, or for authenticated requests the resolver did not bind.

**Transport & agent surfaces**

- **MCP no longer leaks internal error details** — boundary errors render through the same egress-masked envelope as HTTP; caller-caused errors keep message and code.

- **MCP stops advertising idempotency it cannot honor** — the boundary binds no idempotency key, so the retry-replay claim is gone from tool descriptions.

- **MCP tool defaults run their default_factory per call** — uuid and timestamp defaults regenerate per call instead of freezing at registration.

- **Generated FastAPI routes render one 422 shape** — request-validation errors use the shared error envelope; raw ctx/input dropped.

- **`forze dst replay` survives a bad corpus target** — one unloadable target counts as a failure while the rest of the corpus replays.

**PostgreSQL**

- **Query parameters no longer leak across reads in a caller transaction** — each param-bound read resets its session settings after the fetch.

- **`find_many` warns when its implicit 10,000-row cap truncates** — pass an explicit limit or paginate to read past it.

- **`update_matching` bounds its primary-key snapshot** — capped at one million rows by default and fail-closed beyond it; opt back into unbounded with None.

**Adapters & security**

- **Tenant-aware Mongo writes and Firestore queries no longer crash at the driver** — both gateways passed a raw UUID the drivers reject; the tenant is now stamped and filtered as the canonical string everywhere, with tenant-isolation integration tests on both backends.

- **Firestore updates no longer strip the tenant tag** — every gateway write re-stamps the tenant (patch previously full-set the domain image, hiding the row from all tenant-filtered reads); history snapshots are tenant-stamped and a cross-tenant get reads as not-found.

- **Temporal tenant scoping covers listing and handle ops** — signal, cancel, terminate, describe and every schedule op resolve ids through the create-time tenant prefix and refuse a foreign tenant's id; schedule listing filters to the tenant's prefix. Non-tenant-aware wiring is unchanged.

- **Object storage tenant isolation covers reads, not just writes** — every key-taking read, delete, copy and presign path now requires the key to lie within the active tenant's prefix.

- **A missing S3/GCS object classifies as not_found, not retryable infrastructure** — a caller miss is no longer retried or counted against the breaker, generated download routes return 404 on real backends as on the mock, and the re-encryption sweep can skip a deleted object instead of aborting.

- **Meilisearch write path** — a failed task raises instead of reporting success, task waits are bounded, tenant-tagged writes and deletes are scoped, and windows crossing maxTotalHits fail closed with the index provisioned to match.

- **Neo4j keyed-edge identity & quantifier coercion** — a keyed-edge ensure matches on the edge key so distinct keyed edges stay separate; hop quantifiers are int-coerced before inlining.

- **`forze_redis` imports on redis-py 7 again** — version-specific typing aliases are self-owned; client-side caching fails closed below redis-py 8.

- **Redis idempotency store cannot be corrupted via the idempotency key** — the untrusted key is hashed, results live in a disjoint scope, and commit/fail are fenced compare-and-set. The key format change resets the in-flight dedup window once on upgrade.

- **Temporal default workflow id is a real UUID** — the factory stringified the function instead of calling it, so every unnamed start collided.

- **VK login** — the untrusted introspection envelope is no longer copied into claims.

- **Mongo** — the query renderer rejects dollar-prefixed field names; index introspection keeps string index directions verbatim.

- **FastAPI `X-API-Key`** — splits prefix:secret on the first colon; bare keys still authenticate.

- **Per-tenant routed clients** — fingerprint the full host list, so multi-host DSNs no longer raise.

- **Postgres** — schema validation accepts parameterized column types; search index-definition parsing is delimiter-aware.

- **Log scrubbing closes three leaks** — exceptions render scrubbed under sanitize_logs; assignment scrubbing covers credential-suffix keys, whole Authorization headers and user:pass DSNs; a non-string dict key no longer raises into the log site.

- **Scrubbing fragment lists reconciled** — pwd, passphrase, private_key and six more fragments that existed only as key heuristics now also mask in assignment form (`pwd=…`, `db_pwd=…`, `private_key=…`); a parity test keeps the value-form and key lists from drifting apart again.

- **Ranged reads over encrypted objects detect tail truncation** — the range path now verifies the terminal frame's authenticated final flag (riding an already-required fetch, no extra I/O), raising the same chunked-truncated error as streaming instead of serving truncated bytes as authentic; a spliced early final frame is refused too.

- **`configure_logging()` configures the root logger by default** — with no logger names it previously attached nothing and INFO logs vanished; an explicit list is still an allowlist.

- **Misc** — BigQuery empty-array params typed from annotations; timezone offsets validated; S3 multipart-ETag normalization; If-None-Match parsed per RFC 7232; outbound HTTP suppresses its default bearer when an Authorization header is set; GCS rejects reserved metadata keys.

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
