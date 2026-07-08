# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

**Reliability & durability**

- **Self-hosted durable execution (Postgres)** — crash-resumable durable functions and sagas on the Postgres you already run, no Temporal/Inngest. A `DurableFunctionStepPort` memo journal (`durable_step`) gives exactly-once steps on replay; a `DurableRunStorePort` (`durable_run`) persists runs for lease-based recovery (`FOR UPDATE SKIP LOCKED`). `forze_kits` adds a provider-agnostic `DurableFunctionRunner` (enqueue / run-now / recover), `DurableFunctionRegistry`, recovery + scheduler lifecycle steps, and the gap-closer `DurableSagaExecutor` (`SagaExecutorPort` via `SagaDepsModule`) that journals each step + compensation so a crash mid-saga resumes rather than leaving committed steps un-compensated. The runner and scheduler resolve from the container — `DurableKitsDepsModule` / `durable_kits_deps(registry=…)` register them under `DurableRunnerDepKey` / `DurableSchedulerDepKey`, reached from a request handler via `resolve_durable_runner` / `resolve_durable_scheduler`; `cron_schedule_id(spec, index)` names the schedule a cron trigger registers under (shared by the registrar and any enable/disable control plane). A read-only `DurableRunAdminPort` (`list_runs`, newest-first keyset paging with an opaque cursor, `status` / `name` filters) lists persisted runs for an ops surface — opt-in via `PostgresDurableRunConfig(admin=True)` and resolved with `resolve_durable_run_admin`; kept separate from the claim/write store so a CQRS `QUERY` handler can read it (`DurableRunRecord` now carries `created_at`). Multi-worker-safe (concurrent `SKIP LOCKED` claims + a **fenced** terminal write), with `max_concurrency`, delayed runs (`enqueue(run_at=…)`), recurring **cron** schedules (`durable_schedule` + `DurableScheduler`; fire-once/skip-missed; `DurableFunctionCronTrigger` auto-registration), and per-tenant recovery (tagged shared table or namespace `tenants=` shard). Opt-in `DurableTelemetry` (OTel span + `forze.durable.*` metrics); exactly-once-across-crash verified by a DST crash scenario (`forze_dst.invariants.no_duplicate_trace_effect`). `PostgresDepsModule(durable_step=…, durable_run=…, durable_schedule=…)` + mock stores; saga context must be a serializable `pydantic.BaseModel`; tables are app-provided (schema in the adapter docstrings). Adds `croniter` as a core dependency.

- **HLC durable high-water mark** — `HybridLogicalClock.resume(mark)` + a co-located `HlcCheckpointPort` so a restart no longer resets the clock to `(0, 0)` and re-issues a stamp it already emitted; the outbox flush advances the mark inside the business transaction. `PostgresDepsModule(hlc_checkpoint=PostgresHlcCheckpointConfig(...))` + `hlc_checkpoint_recovery_lifecycle_step()`; mock `MockDepsModule(hlc_checkpoint=True)`. Opt-in, node-global.

- **Postgres co-located idempotency store** — `PostgresDepsModule(idempotencies={route: PostgresIdempotencyConfig(...)})` commits the dedup record *inside* the business transaction (exactly-once across a crash), driven from an auto-injected `on_success` hook when a store reports `commits_in_transaction`; falls back to out-of-tx commit otherwise. Mock `MockIdempotencyAdapter(transactional=True)`.

- **CPU-offload seam** — `run_cpu` / `run_cpu_map` run blocking or CPU-bound work off the event loop via a context-bound `CpuExecutor` (bounded thread pool in production; inline and deterministic under simulation), honoring the invocation deadline via a cooperative `checkpoint()`.

- **Fencing-token capability for distributed locks** — `DistributedLockSpec(requires_fencing_token=True)` fails closed at resolve against a backend not reporting `FencingAware` / `fencing_tokens` (Redis and mock support it; default `False`).

- **Wiring check (`check_wiring`)** — opt-in dry-run resolving every registered operation against a throwaway context, so a missing/misrouted dependency surfaces at test/startup instead of first live call; one resolve covers the handler, hooks, tx-scope stages and saga targets (and the QUERY read-only bind). `WiringReport.raise_if_failed()`; `check_facade_factory_wiring` for facade factories. Diagnostic only (lifecycle steps are validated by opening a scope).

- **Neo4j transaction manager** — `Neo4jTxManagerAdapter` (`IsolationAware`, declares `READ_COMMITTED`) enlists the client transaction into the framework transaction scope, so a handler's graph writes commit or roll back as a unit; bind it via `Neo4jDepsModule(tx={…})`. Not co-transactional with the outbox or other backends (no cross-database two-phase commit) — it makes only the graph statements atomic among themselves.

**Querying & read models**

- **Nested-field projection & sorting** — projection `fields` and sort keys may be dotted paths into nested Pydantic sub-models and `str`-keyed mappings (`project(filters, ["contract.reg_number"])`, `sorts={"addr.city": "asc"}`); a path crossing a list maps the selection over each element. Resolved across `project_*` / `project_search_*` and offset/keyset reads on every backend (mock, Postgres, Mongo, Firestore). *Behavior change:* the mock now nests a dotted projection key instead of emitting it flat. Sorting on a nested path whose root column is field-encrypted is rejected.

- **Materialized derived fields** — `DocumentSpec(materialized=…)` persists selected `@computed_field`s as filterable/sortable columns (create/update collisions rejected, startup column check); `SearchSpec` / `HubSearchSpec` accept it for in-place search (Postgres, Mongo; not startup-validated there).

- **Lenient read & write-omit fields** — `DocumentSpec(lenient_read_fields=…)` (or `read_conformity="lenient"` to auto-derive) lets a read-model field have no backing column; `write_omit_fields=…` is the write-side counterpart. Honored on Postgres, Mongo, Firestore documents (and Postgres/Mongo search). For expand/contract migrations.

- **Streaming reads on Mongo & Firestore (`find_many_chunked`)** — bounded-memory validated batches of ≤`fetch_batch_size` via `find_many_streamed` (Mongo, supports `offset`) and `query_stream_batched` (Firestore, cursor-only); all three document backends now at parity.

- **Procedures port** — `ctx.procedure.command(spec).run(params)` runs a spec-named parametrized statement (function / `CALL`, set-based recompute, `REFRESH MATERIALIZED VIEW`); one `ProcedureSpec[In, Out]`, command-only, Postgres plus a programmable mock. Tenant-aware routes fail closed at wiring unless the SQL binds `%(tenant)s`.

- **Query parameters** — `ctx.document.query(spec).with_parameters(P(...))` binds a typed `query_params` contract as query-scoped session settings (Postgres documents plus a programmable mock); capability-gated and fail-closed.

- **`QueryCapabilities.supports_aggregates`** — a backend that can't compile aggregates (the Firestore MVP) rejects `find_many(aggregates=…)` / `find_many_aggregates` / `count_aggregates` with `precondition` (`query_feature_unsupported`) instead of an opaque `internal`.

- **Graph schema provisioning + k-shortest paths (Neo4j)** — a new `GraphManagementPort` (`ctx.graph.management(spec)`) with idempotent `ensure_schema()` / `drop_schema()` provisions node key-uniqueness constraints (composite with the tenant property under tagged tenancy), keyed-edge key-uniqueness constraints (so a concurrent `ensure_edge` can no longer create duplicate keyed edges — the in-query `MERGE` alone races), and tenant-property indexes; Community-edition uniqueness (NODE KEY stays an Enterprise upgrade). `k_shortest_paths` is implemented on Neo4j via the native Cypher `SHORTEST k` selector (unweighted / hop-bounded; tenant-scoped like `shortest_path`) — no plugin required. **Weighted** paths are opt-in via `ShortestPathParams.weight_property` (an edge property to minimize): on Neo4j they run over GDS (`gds.shortestPath.yens`) against a per-call tenant-filtered projection, gated by `Neo4jGraphConfig(graph_algorithms=True)` and failing closed (`graph_algorithm_unavailable`) when off or when GDS isn't installed; `max_hops` applies as a post-filter (weighted paths are selected by cost). Analytics beyond paths (centrality/community/similarity) stay behind the raw-query hatch by design.

- **Graph read-introspection on Neo4j *and* the mock** — `get_vertices` / `get_edges` (batched, found-only, input order), `edge_exists`, `count_vertices` / `count_edges`, `vertex_degree` / `count_neighbors` / `incident_edges`, plus endpoints-mode `get_edge`, are now implemented on both `forze_neo4j` and `forze_mock` (previously each raised on a different subset). `count_*` accept an equality `property_filter` that fails closed on an encrypted (sealed) property; a differential conformance test asserts the mock ≡ Neo4j across the whole read surface, tenant isolation included. `find_vertices` / `find_edges` (equality `property_filter`, offset/limit, key-ordered for stable pagination) are likewise implemented on both planes and covered by the same differential test. The **write set** — `update_edge` / `delete_edge` (keyed *and* endpoints mode), `ensure_vertex` (create-if-missing, existing returned unchanged), and the bulk `create_vertices` / `create_edges` / `delete_vertices` / `delete_edges` — completes both adapters, with a differential test asserting mock ≡ Neo4j write *effects*. **Multi-endpoint edge kinds** (a spec declaring more than one `(from, to)` pair) are supported on both planes: the create/ensure command names the pair via `from_kind` / `to_kind` (transient routing fields, validated against the spec, never stored) — closing the mock's latent bug where such edges were mislabelled and could collapse under merge. `forze_neo4j` now covers the **entire** `GraphQueryPort` / `GraphCommandPort` (no `graph_not_implemented` left). The **mock** gained multi-hop traversal too — `expand`, `shortest_path` (unweighted BFS or weighted Dijkstra), and `scoped_walk` via a small in-memory traversal core; since Neo4j's traversal ordering/tie-breaking is only partly specified, the differential conformance test compares invariants (step multisets, path lengths/costs, reachable target sets) rather than byte-identical output. Only `k_shortest_paths` (and the raw hatch) remain mock-only-via-`forze_neo4j`.

**Search**

- **Bounded-memory result streaming** — `search_stream` / `project_search_stream` / `select_search_stream` on `SearchQueryPort` iterate the whole ranked set in keyset chunks (`chunk_size=500`, no total count) for large exports; peak memory is one chunk. Capability-gated by `SearchCapabilities.supports_stream` (`validate_stream_supported`): Postgres FTS/PGroonga and Mongo text/Atlas stream; Meilisearch, vector (top-k), and hub (`per_leg_limit`-bounded) fail closed. Filter-only exports use the document port's `find_stream`; a frozen order uses a snapshot.

- **Vector & hybrid search as a declared concept** — `SearchCapabilities` (`supports_vector` / `hybrid_fusion` / `filtered_ann` / `auto_embed`) on `SearchQueryPort.search_capabilities` mirrors `QueryCapabilities`: an unsupported retrieval feature fails closed (`validate_vector_supported` / `validate_fusion_supported`, `query_feature_unsupported`) instead of an empty page. Pages gain an optional index-aligned `scores` sidecar (fused RRF score on federated; engine relevance/similarity on single-index and hub — previously discarded; `None` for a filter-only browse). `MultiSourceSearchOptions.fusion` (`rrf` default / `weighted`) is capability-gated (mock serves both; Postgres/Meilisearch reject `weighted`). `filtered_ann` declares recall (`postfilter` pgvector, `prefilter` Atlas); `max_candidates` is the portable recall dial.

- **Facets & highlights** — term facet distributions and per-hit highlights via search options, declared on the spec, returned as optional page sidecars; mock, Meilisearch, Postgres single-index (PGroonga/FTS) and hub over offset/cursor (`sql`/`parallel`), plus federated highlights; generated routes carry them. Unsupported fields/topologies fail closed. In-process highlighting (mock, hub) resolves nested (dotted) fields; flat-field engines fail closed on a dotted field.

- **`FederatedSearchSpec(thin_merge=True)`** — late-materialized RRF merge (fetch `id` per leg, fuse on `(member, id)`, hydrate one page); Postgres and Meilisearch, opt-in, results identical to the full path. A member-shared secondary `sort` (incl. dotted paths) stays on the thin path; a sort whose root field is absent on a member falls back to full-fetch.

- **`SearchSpec(max_results=…)` / `SearchSpec(highlight_scan_limit=…)`** — cap an unbounded offset search (Postgres/Mongo/Meilisearch) and bound the PGroonga highlight text scan; both opt-in, `None` keeps prior behaviour.

- **`PostgresHubSearchMemberConfig.from_search_config(config, *, hub_fk, …)`** — derive a hub leg from a standalone `PostgresSearchConfig`, carrying every field over and adding the hub wiring (`hub_fk` / `heap_pk` / `same_heap_as_hub`).

**Execution & handlers**

- **Two-phase prepare/apply handlers** — `TwoPhaseHandler` (plus a kit base): `prepare(args)` runs outside the transaction (read-only) and `apply(args, payload)` inside it. A tx route is required.

- **Transaction isolation as a fail-closed contract** — operations declare an isolation level, verified against the route's manager (`exc.configuration`, never silently weaker); declaring isolation without a tx route is rejected at freeze. `TransactionContext.current_isolation()`; the `tx` `exit` trace event carries `outcome` (`commit`/`rollback`).

- **Cross-aggregate (system) invariants** — `SystemInvariant` (`ReadSet`, `SumOf`, `CountAll`) in `forze.application.contracts`; `forze_kits.invariants` adds `evaluate` / `enforce` (detective) / `enforce_preventive` (in-tx rollback) / `propose`; `forze_dst.compile_oracle(*laws[, per_commit=True])`.

**Observability & encryption**

- **One-call logging setup** — `bootstrap_logging(...)` wires the framework loggers, named integration loggers, third-party stdlib loggers, and the uncaught-exception hook in one call; `Logger`, `get_logger`, and `configure_logging` are now re-exported from the top-level `forze`. `configure_logging(enable_sampling=…)` adds per-event volume controls: `_sample=N` (keep 1-in-N) and `_dedup_key=` (emit once per window) collapse high-volume events, stripped before rendering.

- **Integration logger naming** — shared adapter/port machinery logs under `forze.integrations.<domain>` (overridable per adapter via `resolve_logger` / the `LoggerAware` mixin); `forze_kits` now logs under `forze_kits.*` instead of borrowing `forze.application`; `forze_sqs`, `forze_rabbitmq`, and `forze_identity` gained typed `Forze*Logger` name enums.

- **Native logging for the previously-silent integrations** — `forze_s3`, `forze_gcs`, `forze_neo4j`, `forze_temporal`, `forze_clickhouse`, `forze_bigquery`, `forze_duckdb`, and `forze_inngest` now emit their own logs: previously-swallowed `health()` failures at `debug`, read-retry attempts (ClickHouse/BigQuery) at `debug`, Temporal saga compensation-step failures at `warning`, and Inngest durable-function invocations at `debug`. ClickHouse/BigQuery/DuckDB/Inngest gained their `Forze*Logger` name enums; `forze_gcs` gained `FORZE_GCS_LOGGER_NAMES`.

- **Coherent connect/close logging** — every client-based integration (Postgres, Mongo, Firestore, Meilisearch, RabbitMQ, SQS, HTTP, Vault, S3, GCS, Neo4j, Temporal, ClickHouse, BigQuery, DuckDB) now logs client connect and close at `trace`, matching the existing Redis convention — uniform and off by default in production.

- **Sampled access logs** — the FastAPI and MCP request-logging middlewares are quiet by default via `AccessLogSampler`: successful requests are sampled 1-in-N and error responses are always logged. FastAPI additionally excludes health/readiness probe **paths** (`DEFAULT_HEALTH_PATHS`) via its default sampler; MCP messages have no such path, so its default sampler applies no path exclusion (pass `exclude=` to skip specific method names). *Behavior change:* successful requests are no longer all logged at INFO; pass `access_log=AccessLogSampler(mode="full")` to restore, or `mode="off"` to silence.

- **Per-port OpenTelemetry client spans** — `DepsRegistry.with_otel_port_spans()` opts every resolved port into a per-call `CLIENT` span inside the resilience policy; opt-in, zero-cost off.

- **Per-port logging** — `DepsRegistry.with_port_logging()` registers a `LoggingInterceptor` on the interception seam that logs every resolved port call `(surface, route, op, duration_ms)` under `forze.integrations.<domain>`: `trace` on success (zero-cost off), `debug` on a domain failure, `warning` with a traceback on an unexpected one. Opt-in.

- **Opt-in signed, encrypted, and context-bound cursor tokens** — `ExecutionRuntime(cursor_token_signer=CursorTokenSigner(secret=…))` (or `configure_cursor_signer` / `bind_cursor_signer`) HMAC-SHA256-signs every keyset cursor token and makes verification reject any unsigned or tampered token (constant-time), so a client can't forge keyset values. For confidentiality, `ExecutionRuntime(cursor_token_cipher=CursorTokenCipher(secret=…))` (or `configure_cursor_cipher` / `bind_cursor_cipher`) AEAD-encrypts the whole token instead — the boundary sort-key values (which may not be in the row projection) and the cursor internals are hidden, and the AEAD tag authenticates, so a cipher supersedes the signer. Under either, each token also embeds a **context binding** — the search-spec name, tenant, and a deterministic (PYTHONHASHSEED-independent) fingerprint of the query filter — and verification rejects a cursor replayed against a different spec, tenant, or filter (sort was already bound). All off by default (tokens byte-unchanged); enabling either is a hard cutover — cursors minted before it 400 once and the client restarts pagination. Signer and cipher are context-scoped (bound per runtime scope), so two runtimes in one process protect with their own key; a single binding covers every mint/verify (document + search, all backends: mock/Postgres/Mongo/Firestore) with no per-path wiring. The cipher is a static-secret AES-256-GCM (via `forze.base.crypto`, nonce from the entropy seam so it's deterministic under simulation); a KMS/BYOK-backed cursor key is a possible future step.

- **Per-item interception for streaming port methods** — a new `StreamPortInterceptor` capability (`around_stream`) lets an interceptor wrap the *iteration* of an async-generator port call (`find_cursor`, `search_stream`, `consume`, `run_chunked`, …), not just obtaining the generator. `CooperativeInterceptor` now yields **per item**, so concurrent stream consumption is a per-item interleaving point the DST scheduler explores (interleaving bugs in streamed reads no longer hide); `LoggingInterceptor` times the whole stream and logs a mid-stream failure with its item count (instead of timing the near-instant open and reporting success for a stream that later fails); and `FaultRule(stream_faults=True)` injects a mid-stream `error`/`timeout`/`crash` after any item — off by default, so it never perturbs the seeded fault stream of existing runs. An `around`-only interceptor keeps its acquisition-only behavior unchanged.

- **W3C trace-context propagation** — a published event carries its span outbox→broker→inbox; opt in with `OutboxIntegrationConfig.propagate_trace` (add a nullable `traceparent` column on relational backends first), `forze_http` injects outbound. Trace-parenting only.

- **`EncryptionReach` ladder (`none < at_rest < end_to_end`)** — names the outbox/messaging reach; `OutboxEncryptionTier` is now a back-compat alias and `MessageEncryptionTier` its transport subset. `CryptoDepsModule(required_reach="end_to_end"|"at_rest")` refuses a weaker outbox/transport route at resolve (opt-in).

- **Bounded-memory streaming for object storage (incl. client-side-encrypted blobs)** — `StorageQueryPort.download_stream(key)` and `StorageCommandPort.upload_stream(chunks, …)` transfer a large object through fixed memory instead of buffering it whole. Upload streams the bytes to the backend as a multipart upload (a new client `upload_multipart_part` — app-provided part bytes — on S3 and GCS); download reads it back in ranged GETs. On a client-side-encrypting route the stream is sealed/opened chunk-by-chunk via a new chunked-AEAD wire format (`forze.base.crypto`: magic `FZEc` vs the whole-payload `FZEv`; `ChunkedHeader` / `ChunkedStreamReader` / `seal_chunk` / `open_chunk` / `is_chunked_envelope`) — each chunk bound to its position and a terminator flag for reordering/truncation resistance, one KMS-wrapped data key per stream, reusing the tenant key-id confused-deputy guard. This lifts the multipart-under-encryption restriction for the app-mediated path (presigned uploads remain incompatible with client-side encryption). The keyring exposes it as `StreamingBytesCipherPort` (`encrypt_stream` / `decrypt_stream`); the ports are general (a non-encrypting route streams plaintext straight through), and `download_stream` still reads legacy whole-payload (`FZEv`) objects (buffered decrypt) and plaintext objects. `ObjectStorageAdapter(stream_part_size=…)` tunes the transfer part size. **`download_range` now works on client-side-encrypted objects too:** a chunked (`FZEc`) object is served by fetching and decrypting only the chunks the byte range covers (`StreamingBytesCipherPort.open_chunked_stream` + a random-access opener) and trimming to the exact bytes — bounded to the covering chunks; a legacy whole-payload object stays refused (a single AEAD blob can't be sliced), and a plaintext object passes through.

**Realtime**

- **Server push (egress) + offline store-and-forward** — a handler publishes a `RealtimeSignal` to a principal/topic through messaging ports; the Socket.IO gateway bridges to a tenant-scoped room (ephemeral at-most-once or durable exactly-once), and a durable principal-addressed signal is mailboxed for an offline recipient and replayed per-device on reconnect. Read-only operations cannot publish.

- **Tenant-aware & multi-node hardening** — `TenantShardedSignalSource` / `RealtimeShard` run one consume loop per tenant, scoped by a trusted tenant from the stream (not the header); pass `tenants=` to the background relay step for a tenant-sharded outbox relay. TTL-backed presence with heartbeat, credential-expiry eviction, and a per-emit timeout.

- **BREAKING — realtime delivery envelope** — every Socket.IO frame is now the uniform `{id, data}` envelope (durable carries the event id, ephemeral null); no transitional dual-emit. Clients read `data` and dedup by `id`.

**Transports & DX**

- **Offset-log stream consumption** — a fourth delivery model for Kafka-class partitioned, offset-committed logs, backend-neutral under `contracts/stream/`: `CommitStreamGroupQueryPort` (`read` / `tail` / `commit`) + `CommitStreamGroupAdminPort` (`ensure_topic` / `ensure_group` / `reset_offsets` / `lag`), `StreamPosition` / `OffsetReset` / `ConsumerLag`, and a `CommitStreamGroupCapabilities` (`supports_replay` / `supports_transactions`) fail-closed at the admin-call / resolve boundary (via `ctx.stream` / `StreamSpec.requires_transactions`). A `CommitStreamGroupConsumer` kits runner commits after `process_with_inbox` (at-least-once transport + inbox dedup = exactly-once effect; DLQ-and-advance or pause-and-alert on poison). Mock reference adapters + a conformance battery pin the semantics.

- **`forze_kafka` offset-log backend** — the first real offset-log transport (`kafka` extra, over `aiokafka`): `KafkaDepsModule(streams=…, commit_groups=…)` wires produce (`StreamCommandPort`) + consume (`CommitStreamGroupQueryPort`) + admin (`CommitStreamGroupAdminPort`); `key`→partition, native record headers, `commit` translates to Kafka's next-offset, replay/lag over the admin client, `end_to_end` encryption sealed through the broker, `namespace` / dedicated (routed) tenancy. `kafka_lifecycle_step(...)` / `routed_kafka_lifecycle_step(...)`. Verified against a live broker (testcontainers) as the offset-log conformance differential.

- **Redis stream & pub-sub transports** — `RedisDepsModule` wires `StreamSpec` / `PubSubSpec` via `RedisStreamConfig` / `RedisStreamGroupConfig` / `RedisPubSubConfig`; `encryption="end_to_end"` seals through the broker, `tenant_aware` adds a key prefix. The stream consumer-group adapter splits into a data-plane query adapter and a control-plane `*StreamGroupAdminAdapter` (`ensure_group`), for Redis and the mock.

- **Bounded-memory streaming download route** — the generated FastAPI `GET` download route now **streams** the object (`StreamingResponse`) instead of buffering it whole, so one large object can no longer OOM the process. A plain download runs a single governed op (`download_stream`, whose result carries the `ETag` / `Last-Modified` cache validators); a `Range` request does a real backend-ranged fetch (`206`, capped at `max_range_bytes` = 16 MiB, wider windows served as an RFC-7233 truncated partial), `416` for an unsatisfiable range, and `304` via backend `ETag` (`If-None-Match`) / `Last-Modified` (`If-Modified-Since`). An `HTTP HEAD /{key}` route answers object metadata (size / etag / content-type / last-modified) as headers with no body. Backed by three new read-only storage kit ops — `head` / `download_stream` / `download_range` (`StorageKernelOp`, `build_storage_registry`); `StreamedDownload` now carries `etag` / `last_modified`, and `RangedDownload` carries `filename` so a `Range` and a full download advertise the same `Content-Disposition`. *Behavior change:* the download route streams by default (`attach_storage_routes(stream=False)` keeps the legacy fully-buffered route); the `ETag` is now the backend etag (not a body MD5); an encrypted object streams with no `Content-Length` (chunked transfer, plaintext size unknown). Reuses the existing adapter streaming/ranged machinery (S3/GCS/mock; plaintext + chunked-AEAD), verified against MinIO and fake-gcs.

- **Top-level front door** — the most-used names re-export lazily (PEP 562) from `forze` / `forze_kits` (`from forze import DocumentSpec, build_runtime`; `from forze_kits import DocumentFacade, build_document_registry`); deep paths keep working, the core never imports kits.

- **Less CRUD boilerplate** — `build_document_registry(spec)` derives `DocumentDTOs` when `dtos` is omitted (`create=None` / `update=None` to disable an op); `document_facade(runtime, registry, spec)` returns a per-call typed factory.

- **Shared error helpers** — `error_envelope()` and `guard_frame()` give one client-safe `CoreException` projection and a shared guarded boundary; `http_status_for_kind(kind)` maps an `ExceptionKind` to its HTTP status. FastAPI and Socket.IO render through them.

- **Mock document adapter — tenant scoping on every write** — the in-memory mock injects the tenant column on ensure/upsert/update/touch (not only create), matching Postgres.

- **Telegram Login Widget verifier** — `TelegramWidgetVerifier` (in the Telegram builtin preset) verifies Login Widget callback data via Telegram's HMAC-SHA256 scheme — the data-check-string authenticated by `HMAC(SHA256(bot_token), …)`, compared constant-time, with an `auth_date` freshness (replay) bound — and emits the canonical `VerifiedAssertion` (Telegram user id as subject). `verify(data)` for a parsed field map, or the `TokenVerifierPort` `verify_token` for the widget query string; pure-stdlib crypto (no JWT). Complements the existing Telegram Login *OIDC* flow.

**Deterministic Simulation Testing (`forze_dst`)** — new package

- **Point-at-a-real-app simulation** — `Simulation` / `SimulationConfig`, `Simulation.run(config, ...)`: one master seed reproduces a whole run (schedule, faults, latency, inputs, crashes, partitions) over real registries and runtimes, single-process or N-node, with no app changes; a violation minimizes to a reproducible counterexample.

- **Deterministic runtime + ambient seams** — `SimulationEventLoop` / `SimulationTimeSource` / `run_simulation(...)` (wall-ms, byte-identical replay; `RealIOForbidden`, `SimulationDeadlock`); new seams `EntropySource` / `bind_entropy_source`, `TimeSource.monotonic()` / free `monotonic()`, and `DepsRegistry.with_interceptors(...)`. A determinism gate bans raw time/entropy outside the seams.

- **Faults, latency, crash & partitions** — `FaultPolicy` / `FaultRule` and `LatencyProfile` (`Constant` / `Uniform` / `Exponential` / `LogNormal` / `Pareto`) on `SimulationConfig`; `SimulationConfig.crash` (crash-restart-recovery over persisted `MockState`), `SimulationConfig.runtime=True` (real `ExecutionRuntime`), `Cluster` / `Partition` / `PartitionSchedule` (lossy/asymmetric links).

- **Workload generation** — `Scenario` / `Rule` / `ModelState` / `derive_scenario`; a fuzzer (`OpSpec`, `generate_workload`, `simulate_workload`); `PCTScheduler` / `SystematicScheduler`; coverage-guided exploration (`behavioral_coverage`, `Simulation.coverage`, `Simulation.coverage_guided`).

- **Oracle & invariants** — `Recorder` / `record_event` / `History`; built-ins `no_duplicate_effect` / `mutual_exclusion` / `linearizable` / `RegisterSpec`; `explore` + `minimize` → `ViolationReport`; reachability (`reached`, `sometimes`, `reachability_targets`); value-level behind `capture_values=True` (`read_your_writes`, `expect_value`).

- **Transactional-isolation oracles** — `snapshot_isolation()` / `serializable(complete=True)` (and kernel `find_snapshot_isolation_violations` / `find_serializable_violations` / `find_serializability_cycle` over `TxRecord` / `VersionedTxRecord`) detect lost update, write skew, and ≥3-transaction anti-dependency cycles; `had_isolation_conflict`, `isolation_oracle_for(level)`. Shared `evaluate_filter` / `compile_filter` in `forze.application.contracts.querying`.

- **Commutativity** — `OperationDescriptor.commutative` + `commutative_convergence(build, *, final_state, schedule_seeds)`.

- **Trace, reporting & sweeps** — `RuntimeTracer`, `operation_fingerprint` / `FrozenOperationRegistry.fingerprint()`; `forze_dst.report` (`CausalGraph`, `format_report`, `ViolationReport.format()`), `timeline()` / `build_timeline` / `render_timeline`; regression corpus (`RegressionEntry`, `append_regression`, `load_regressions`, `behavioral_fingerprint` / `strict_behavior`); `FailureBundle` / `replay_bundle`; `parallel_sweep(run, seeds, workers=…)` / `SimulationSeedRunner` → `SweepResult` (`SeedOutcome.reached`, `SweepResult.reachability`).

- **Mock substrate** *(behavior change)* — `MockDepsModule(transactions="journal")` is now the default (undo journal + MVCC isolation; `exc.concurrency` / `serialization_failure`; `none` / `strict` opt-in); in-memory outbound HTTP (`MockHttpServicePort` / `MockHttpServiceAdapter` / `MockHttpRegistry`, `MockDepsModule(http=…)`).

- **CLI** (`forze[cli]`) — `forze dst run module:sim` (exit 1 on a violation) plus `replay`, `coverage`, `topology`, `derive`.

- **Adapter conformance** — `forze_dst.conformance`: a backend-agnostic isolation-anomaly battery over the `Conductor` (a known verdict per `IsolationLevel`; `CONTRACT_STRENGTHENINGS` / `MECHANISM_DIVERGENCES`), run against the mock and real Postgres (every level) and a real Mongo replica set (`SNAPSHOT`) over testcontainers, asserting `mock ≡ real`. The two lock-race cases (`duplicate_key_insert`, `for_update_lost_update`), which BLOCK the contender on a real engine (unique index / row lock) rather than abort it and so ran against the abort-based mock only, now run against real Postgres too via a block-aware driver (`Gate.arrive_blocking` / `_drive_lock_race`) that converts a lock wait into the same explicit signal the mock produces by aborting — pinning block-vs-abort. The FOR UPDATE verdict is by final value (was an update lost?), so it holds where Postgres READ COMMITTED commits *both* writers and loses nothing (the locked re-read sees the committed value) as well as where SNAPSHOT/SERIALIZABLE serialization-abort. The harness also covers a second family — **outbox→inbox delivery semantics under a crash** (`run_crash_recovery_delivery`): stage + flush → claim → publish → crash before `mark_published` → reclaim → re-publish → consume, asserting at-least-once delivery + exactly-once effect (inbox dedup) as `mock ≡ real Postgres`, with a paired `dedup=False` run proving the redelivery is real. The `outbox-inbox-write-through` divergence is now a *checked* disagreement rather than a forward-looking note: `observe_uncommitted_outbox_visibility` asserts, from both ends, that the mock's write-through outbox lets a concurrent relay claim a producer's uncommitted row (a phantom event) while real Postgres READ COMMITTED does not.

- **Resilience stores under partition** — the divergences a distributed (Redis) resilience store adds over the in-memory reference are now pinned as tests against real Redis: (1) **fleet-rate collapse to per-replica** — a shared `RedisRateLimitStore` enforces one fleet budget until a replica loses Redis, at which point it fails open to its own in-memory bucket and the fleet-effective rate jumps back to `permits × replicas`; (2) **breaker fast-path cache staleness** — `RedisCircuitBreakerStore`'s `local_cache_ttl` (0.25s) closed-phase cache serves `admit` without a Redis read, so a fleet trip elsewhere is invisible until the window lapses (driven deterministically via the store's injectable clock); (3) **LRU evicts a hot open breaker** — the in-memory `InMemoryCircuitBreakerStore` silently resets an OPEN breaker to closed when a high-cardinality `route` evicts it (a burst then passes until it re-trips), where a distributed store pays only an extra read. A partition is simulated by a client wrapper whose `run_script` raises, exercising the stores' fail-open-to-local degrade path.

### Changed

**Breaking — search**

- **Search pages split from the base pagination contract** — `SearchPage` / `SearchCountlessPage` / `SearchCursorPage` now carry facets, highlights, and the snapshot handle (off `Page` / `CountlessPage` / `CursorPage`); `FacetBucket`, `FacetResults`, `HitHighlights`, `SearchSnapshotHandle` move to the search contract.

- **`SearchFuzzySpec` is a frozen value object** — was a dict; edit-distance ratio defaults to 0.34 (validated 0.0–1.0), prefix-length removed. No shim.

- **Search options de-leaked** — raw-Groonga override removed, the PGroonga plan is adapter-config only, candidate caps renamed `max_candidates` / `merge_candidates`; hub/federated member keys move to a multi-source options type single-index search rejects at type-check.

- **Search index provisioning → `SearchManagementPort`** (`forze_meilisearch`) — `ensure_index` / `delete_all` move off `SearchCommandPort` onto `ctx.search.management(spec)` (`SearchManagementDepKey`).

- **Typed value-object configs** — search `engine` is a tagged union (`forze_postgres`, `forze_mongo`; bare strings still shorthand); federated merge takes a shared `Rrf`; warehouse analytics take a shared `IngestSpec` (Postgres drops legacy `schema`) — `forze_postgres` / `forze_bigquery` / `forze_clickhouse`.

**Breaking — imports & DSL**

- **Application contracts surface consolidation** *(no runtime change)* — removed `contracts.codecs` (→ `forze.base.serialization`); `contracts.lenient_read` / `contracts.materialized` → `contracts.conformity`; `RowLockMode` / `row_lock_requires_transaction` → `contracts.document.value_objects`; `Sum` / `Count` → `SumOf` / `CountAll`; new `TenantSecretResolver` (`contracts.secrets`) replaces `resolve_dsn_for_tenant` / `resolve_structured_for_tenant`, `ensure_dsn_fingerprint(resolver=)`.

- **Contract value types import from their contracts home, not the execution layer** — `BreakerKey` / `CircuitBreakerStore` / `RateLimitStore` / `RateLimitKey` / `LatencyDigestStore` / `LatencyDigestKey` / `Transition` from `contracts.resilience`; `LifecycleModule` / `LifecycleStep` from `contracts.execution`; `RoutedDeps` / `PlainDepsMap` from `contracts.deps`; `OutboxStagingContext` from `contracts.outbox`.

- **`update_many` takes `Sequence[KeyedUpdate[U]]`** (document command port) — not `Sequence[tuple[UUID, int, U]]`; `KeyedUpdate` (`id`, `rev`, `dto`) from `contracts.document`. Single-item `update` unchanged.

- **`GroupRef` (query grouping) → `GroupField`** — resolves the clash with the authz `GroupRef` (unchanged).

- **PEL stream ports renamed for the two-sub-model split** — `StreamGroupQueryPort` / `StreamGroupAdminPort` → `AckStreamGroupQueryPort` / `AckStreamGroupAdminPort` (and dep keys `StreamGroup*DepKey` → `AckStreamGroup*DepKey`), pairing symmetrically with the new `CommitStreamGroup*` offset-log ports; behavior unchanged (find-and-replace `StreamGroup*` → `AckStreamGroup*`). `StreamMessage` gains optional `partition` / `offset` fields (`None` for the ack sub-model).

- **Package restructures** — `forze_dst` splits into a thin `Simulation` facade over `engines/` / `oracle/` / `artifacts/` (`SchedulerKind` removed); `forze_mock` root modules → `adapters/`, factories → `execution.factories` (top-level imports unchanged); notify kit's `NotificationRouter` is now a mutable builder (`register()` → `freeze()`) with resolution on `FrozenNotificationRouter`, reorganized into `routing` / `events` / `consumer` / `lifecycle`.

**Behavior**

- **Lazy transaction acquisition, default for Postgres/Mongo/Firestore** — a tx scope defers connection checkout to the first operation (no idle-in-transaction); a connect failure surfaces there. Opt out `lazy_transaction=False`.

- **Runtime owns its CPU-offload pool; no import-time global** — an `ExecutionRuntime` scope binds and closes a scope-lifetime `ThreadPoolCpuExecutor` (sized via `cpu_workers`) instead of a process-global; an injected `cpu_executor` stays caller-owned, an already-bound executor (e.g. a simulation's) is respected, and with nothing bound `run_cpu` runs inline (`InlineCpuExecutor`).

- **Search snapshots stream their pool** (peak = one chunk) and expose `expires_at`; Postgres hub (and similar single-index) search defers its heavy projection to per-page hydration.

- **Empty filter/sort maps are no-ops** on list/search requests; a structured-but-empty envelope is still rejected.

- **Sizing bounds clamp/reject instead of silently resetting** — an out-of-range document-adapter `batch_size` (outside `[10, 20000]`) is now rejected with `exc.configuration` at wiring instead of being replaced with `200` on first use; a per-call stream `chunk_size` is clamped to the nearest bound (`[10, 20000]`) instead of reset to `500`, so an over-large request runs at the ceiling rather than shrinking. A BigQuery `max_poll_attempts < 1` is likewise rejected at wiring. Bounds are enforced through a shared `clamp` primitive.

- **Integration logger namespaces unified to `forze_<pkg>.*`** — `forze_redis` / `forze_postgres` / `forze_http` / `forze_firestore` / `forze_temporal` no longer log under bare prefixes; update log filters keyed on the old ones.

- **Hot-path micro-optimizations** (byte-identical output) — faster `normalize_string`, keyset sort-value canonicalization, once-per-struct msgspec exclude-flag resolution, allocation-free trusted bulk decode, compile-once in-memory scans, `forze.base.crypto.ENVELOPE_B64_PREFIX`, and per-wrapped-method (not per-call) construction of the OpenTelemetry port-span name/attributes and the port-interceptor terminal.

- **Generated FastAPI routes omit null response fields by default** — every `attach_*_routes` helper now sets `response_model_exclude_none=True`, so a JSON response drops fields whose value is `None` (smaller payload); the OpenAPI schema is unchanged (fields stay optional). Pass `exclude_none=False` to any `attach_*_routes` (or `attach_operation_routes`) to restore explicit `null`s. Raw-`Response` routes (download/head bytes) are unaffected.

### Removed

- **`msgspec` dropped; the codec layer is Pydantic-only** *(breaking: serialization)* — `MsgspecModelCodec` and `forze.base.serialization.msgspec` are removed; record models (read models, create/update commands, idempotency results) must be `pydantic.BaseModel` subclasses. The storage value objects (`UploadedObject`, `DownloadedObject`, `ObjectMetadata`, `StoredObject`) are now frozen, keyword-only `attrs`. Migration: model record/payload shapes as Pydantic.

### Fixed

**Durable-execution & broker failure paths**

- **Inngest — event-supplied identity is no longer trusted by default (impersonation fix)** — the `_forze` envelope's `principal_id` / `tenant_id` are plaintext, attacker-controllable event data, so a durable-function invocation no longer binds them as its identity unless `register_functions(..., bind_identity_from_event=True)` (or `serve(...)`) opts in — for deployments where every event producer is trusted, mirroring the inbox consumer's `bind_tenant_from_headers`. Previously any producer could impersonate any principal in any tenant. Tracing metadata (correlation/execution ids) is still restored; end-to-end payload decryption still uses the envelope tenant for AAD (self-authenticating — a forged tenant fails the AEAD open). *Migration:* if you relied on producer-carried identity, set `bind_identity_from_event=True` on registration.
- **Temporal — saga failures fail the workflow; deterministic clock survives a plain import** — `TemporalSaga` now converts a saga `CoreException` (`saga.step_failed` / `saga.forward_incomplete`) into a `temporalio.ApplicationError` (`non_retryable` from the framework's per-kind retryability policy), so an *uncaught* saga failure reaches `FAILED` instead of failing the workflow *task* and retrying forever. The replay-deterministic clock is fixed for a workflow that imports forze without `workflow.unsafe.imports_passed_through()`: `forze.base.primitives` is passed through the sandbox so `utcnow()` / `uuid7()` read the interceptor-bound source (were silently the wall clock, or hung the task on a restricted re-import).
- **Inngest — deterministic failures stop retrying** — a malformed event (`ValidationError`), a failed end-to-end payload decrypt (e.g. a forged-tenant AEAD open), or a non-retryable `CoreException` from a durable-function handler now raises `inngest.NonRetriableError` (retryability from the same per-kind policy), so Inngest stops retrying a failure that can never converge; retryable kinds (infrastructure / throttled / concurrency) still propagate for Inngest's own retry.
- **SQS — one poison message no longer poisons the receive batch; louder, fail-closed edges** — `receive()` isolates a per-message base64-decode failure (skip + log, left in-flight for the queue's redrive → DLQ) instead of aborting the whole batch and stranding the good messages; `consume()` now logs a receive failure before backing off; a per-message delay on a FIFO queue (`sqs.fifo_per_message_delay`) and an over-length queue name (`sqs.queue_name_too_long`) fail closed instead of a silent SQS rejection or name-truncation aliasing distinct queues. On a **FIFO** queue an undecodable message is deleted (error log records its size and the decode error, never the raw body — which may carry production data) rather than skipped, so it can no longer sit at the head of its message group and deadlock every later message behind it; an already-resolved queue URL is served from cache before the length check, so `ack`/`nack` of in-flight deliveries never strand on that guard.
- **RabbitMQ — opt-in dead-letter sink + working redelivery counting** — `RabbitMQConfig(dead_letter_exchange="…")` declares a fanout DLX plus a bound durable `<dlx>.dlq` (once per client — the topology is broker-global and durable) and stamps `x-dead-letter-exchange` on work queues, so a `nack(requeue=False)` (an undecodable / schema-drift message) dead-letters there instead of being silently dropped on the DLX-less topology the client itself declared. `RabbitMQConfig(redelivery_counting=True)` makes `nack(requeue=True)` republish the message with an incremented `x-forze-delivery` header and ack the original (message id preserved for inbox dedup), so the delivery count advances past the broker's `redelivered`-flag ceiling of 2 and `max_deliveries >= 2` poison-parking actually fires (was structurally inert → infinite hot redelivery). The republished copy carries the full AMQP property set (expiration/priority/correlation-id/reply-to/…), so TTLs and RPC reply routing survive a counted retry, and the same counted path applies to deliveries requeued at client shutdown (so a poison message left pending across a worker restart keeps advancing its count). `redelivery_counting=True` requires `publisher_confirms` (rejected at config time — the republish-then-ack would otherwise ack a message a fire-and-forget publish never delivered), and a per-message republish failure is isolated (only originals whose copy reached the broker are acked; a failed one stays unacked for redelivery, never dropped). Both opt-in, default off — *migration:* enabling the DLX on a pre-existing queue requires recreating it (AMQP queue arguments are immutable).
- **Inngest — function-level config** — `InngestFunctionBinding(config=InngestFunctionConfig(retries=…, concurrency=…, rate_limit=…, throttle=…, idempotency=…, priority=…, debounce=…, batch_events=…, timeouts=…, singleton=…, cancel=…))` forwards Inngest's native `create_function` controls (previously none were exposed at the function level); `InngestFunctionBinding.for_registry_operation(..., config=…)` carries the same config for registry-operation bindings.

**Reliability hardening — durability, shutdown, resilience**

- **Deadlines enforced at the database driver** — a bound deadline sets Postgres `SET LOCAL statement_timeout` (in-tx) and wraps each Mongo op in `pymongo.timeout` (CSOT), remaining-budget + grace, tighten-only; a loose `asyncio.timeout` backstop still fires first so the server cancels the query and returns the connection clean. `push_invocation_deadline` kill switch. (PG autocommit stays asyncio-only.)

- **A deadline that tears a transaction commit is non-retryable** — surfaces `internal` (`commit_ambiguous`) rather than retryable `deadline_exceeded`, so an at-least-once caller can't retry into a duplicate; a deadline during the body still rolls back and stays retryable.

- **Shutdown reliability** — the drain-timeout now cancels still-running ops and awaits their unwind before teardown closes the clients they hold; detached document-cache early-refresh tasks are cancelled via a per-runtime background-owner registry; each shutdown hook gets `ExecutionRuntime.shutdown_step_timeout` (default 10s) so a wedged hook can't hang process exit.

- **Spawned operations no longer escape drain** — nesting is decided by `asyncio.Task` identity, not the mere presence of the active-operation marker (a ContextVar copied into every `create_task`). An operation a handler spawns (`asyncio.create_task(facade.run(...))`) is now admitted, counted in flight, and drained/cancelled at shutdown instead of running on against clients teardown is closing. Genuine same-task in-await nesting still rides the outer slot; concurrent sub-dispatch on separate tasks (e.g. `asyncio.gather(facade.run(a), facade.run(b))`) is now gated, so it can receive `THROTTLED` (`draining`) during a drain rather than silently riding the parent slot.

- **A failed after-commit effect no longer discards a committed result** — a best-effort post-commit callback (cache invalidation, event dispatch, an idempotency-record write) that fails is now logged and reported to an optional `ExecutionContext.after_commit_error_handler` (an `AfterCommitError`) out-of-band, instead of surfacing as an `internal` (500) that threw away the committed operation's result. A *deliberate* post-commit domain check keeps failing loud: `run_or_defer(..., fatal=True)` (used by detective invariant `enforce`) re-raises after every callback has run. Default post-commit callbacks are now `fatal=False`.

- **Inbox exactly-once fails closed on a cross-client misconfiguration** — `process_with_inbox` now asserts (via `tx_ctx.assert_enlisted`) that the inbox store commits in the transaction opened on `tx_route`. An inbox wired to a different client/pool than the handler's writes is silently non-atomic (the dedup mark commits on its own connection); it now raises `configuration` (`core.tx.not_enlisted`) at the first message rather than breaking exactly-once silently. Adds the `TransactionallyEnlistable` contract; the Postgres inbox reports enlistment from `client.is_in_transaction()`.

- **Durable runs renew their lease under a long body** — new `DurableRunStorePort.renew(run_id, *, lease_for, fence)` (fenced `UPDATE`; Postgres + mock stores) plus a `DurableFunctionRunner` heartbeat (every `lease_for / heartbeat_divisor`, default 3) keep a still-executing run's lease alive so the recovery scanner can't reclaim it mid-flight and double-execute its side effects. A lost fence (a newer claim advanced `attempts`) cancels the body rather than continuing; covers durable functions and sagas.

- **Resilience hardening** — the breaker classifies by downstream *health* (a `429` / OCC conflict no longer trips it; a timeout now counts as a failure); per-`(policy, route)` state maps are LRU-bounded (`max_state_entries` / `max_entries`, default 4096); a blanket policy retrying an *ambiguous* failure on every method is refused at build (`resilience.blanket_write_retry`); a distributed resilience-store outage fails open by default (`ResiliencePolicy.fail_open_on_store_error`), and a `record` failure is swallowed.

- **Adaptive-bulkhead latency digest fails open like the breaker** — a distributed latency-digest store error while feeding the AIMD controller (on both the success and failure paths) is now swallowed and surfaced as a `latency_digest_store_error` metric, instead of turning an already-successful call into a failure or replacing the in-flight business exception on the failure path.

- **Hedging now requires an explicit safety basis** *(breaking, freeze-time)* — an operation carrying a `HedgeWrap` no longer passes the freeze-time safety gate just because it also has an `IdempotencyWrap`. The idempotency key is claimed once *outside* the hedge, so the hedge's concurrent duplicate attempts each open their own transaction and can both commit — a boundary dedup can't make that safe. Every `HedgeWrap` must declare `safety=` (read-only / OCC / naturally idempotent); a hedged op without one fails closed at freeze with `configuration`.

- **Bulkhead no longer over-admits after a cancelled shed** — a queued waiter that was shed (queue displacement / CoDel) and whose awaiting task was then cancelled no longer decrements `in_use` for a slot it never held. The prior underflow drove `in_use` negative and permanently admitted past the limit; the guard now decrements only a genuinely granted slot (and retrieves the shed exception, silencing the "never retrieved" warning).

- **Operation OTel spans stop painting expected 4xx failures red** — a client-class domain `CoreException` (validation / not-found / conflict / precondition — a 4xx kind the caller may handle) is now recorded as `forze.outcome="failed"` on a clean span, matching the port-span policy, so error-rate alerting on `forze.outcome="error"` tracks genuine faults instead of firing on every validation error. The `hedge_won` metric now counts only the calls a hedge actually rescued (the primary winning emits `hedge_primary_won`), restoring hedge-effectiveness as a usable ratio.

- **Default resilience executor is per-event-loop** — the process-wide default (used when no `ResilienceDepsModule` is registered) is now keyed per running loop, so a bulkhead never `set_result`s a waiter parked on a foreign/closed loop (a multi-loop app, or sequential pytest loops). Off-loop construction still gets a shared fallback.

- **Idempotency** — dedup TTL default 30s → 24h to cover the redelivery horizon (materially raises a Redis store's footprint at scale); a store failure recording the result *after* the business commit no longer fails the succeeded op. At-least-once-with-dedup, not exactly-once.

- **Batch field decryption no longer stalls the event loop** — a ≥64-row encrypted result set offloads to `run_cpu_map` against a thread-safe snapshot (`EncryptingModelCodec.freeze_for_decrypt`); small batches stay inline, output byte-identical.

- **`require_tenant_id` raises `authentication`, not `internal`** — a missing bound tenant is caller-caused.

- **Hard-delete cache-invalidation failures surface at error level** — a deleted document served from the distributed cache until its TTL is a correctness hazard; stays best-effort so a cache outage can't block a delete.

- **Opt-in guard against outbox dual-writes** — `OutboxSpec(require_transaction=True)` makes flush-inside-a-transaction a checked precondition (`exc.configuration`, `core.outbox.flush_outside_transaction`); default off (stage-then-relay flushes outside a tx by design).

- **Outbox relay no longer dead-letters the backlog on a missing keyring** — a `core.crypto.payload_cipher_missing` raised when an encrypted row meets a `None` keyring is a deployment fault, not row poison: the relay now aborts the pass (leaving rows pending for redelivery) instead of marking the whole encrypted backlog terminally `failed`, matching what both consumer runners already do. Genuine decode poison (keyring present) still parks the row.

**Bounded memory**

- **Unbounded in-process caches gain bounded defaults** — Postgres introspector filtered-estimate lane (`max_filtered_estimate_entries=2048`), Redis breaker local cache (`max_cache_entries=4096`), document-cache refresh fan-out (`max_inflight_refresh=64`), L1 live-store registry weak-ref sweep. All with escape hatches.

- **More read/list paths stream** — GCS `list_objects` page-by-page (exact total kept); realtime mailbox `trim` projects only `id`; Postgres hub `execution="parallel"` late-materializes (thin id/sort/key rows, page hydrated by id) and bounds per-leg concurrency with the pool semaphore; Postgres `update_matching` honours `batch_size` (keyset pages, not one unbounded UPDATE).

- **Streamed offline-mailbox replay** — `RealtimeMailbox.replay_since` (HLC keyset, `replay_page_size=100`) emits page-by-page instead of loading a device's whole backlog.

- **Mongo ranked search late-materialization** — projects to thin `{_id, sort-key, rank}` before `$sort`/`$skip`/`$limit` and hydrates the page by `_id`.

- **Analytics `run_chunked` truly streams** — `run_chunked` / `select_run_chunked` / `project_run_chunked` (DuckDB, ClickHouse, BigQuery) consume `run_query_streamed` through a shared `stream_shaped_chunks`, one window at a time.

- **`HttpConfig(max_response_bytes=…)`** caps the in-memory response body (streams, refuses an over-`Content-Length` response, aborts an oversized chunked one); default `None`.

**Error taxonomy**

- **Client-caused errors no longer masquerade as 500s** — across query / cursor / aggregate / storage / search, an unsupported feature → `precondition` (400) and a malformed/out-of-range value → `validation` (422), uniform across mock ≡ Postgres ≡ Mongo ≡ Firestore; genuine server faults stay `internal`.

- **Bad query fields are a client error** — a sort/filter/direction naming an absent field → `precondition` (`field_not_on_read_model` / `invalid_sort_value`); a spec's own bad `default_sort` stays `configuration` (author misconfiguration).

- **Malformed `$and`/`$or` is a clean 400** — a combinator whose operand is not a list of filter objects (`{"$or": "abc"}` iterated characters; `{"$and": ["x"]}` crashed on `.keys()`) now raises `precondition` instead of an `AttributeError` (500), matching the existing `$not` object check.

- **Cursor page size is coerced and clamped** — `resolved_cursor_limit` (shared by the mock and the Postgres gateway) rejects a non-integer `limit` with `validation` (not a 500 from `int('abc')`) and clamps to `MAX_CURSOR_LIMIT` (10,000) so a huge value can't materialize an unbounded `LIMIT`.

- **Encryption fail-closed** — filtering a randomized-encrypted field raises `precondition` (`core.crypto.encrypted_field_not_filterable`); encrypted-sort rejection now covers every search backend (`core.search.encrypted_sort_field`).

- **Consistent adapter errors** — the mock rev-conflict raises `exc.precondition(..., code="revision_mismatch")` like the real adapters; a missing dependency reports a legible `configuration` error naming what *is* registered; database error classification keys on codes (Postgres SQLSTATE, ClickHouse numeric, Mongo op code, Redis RESP token), not message text.

**Correctness & consistency**

- **`$neq` / `$nin` / `$disjoint` include NULL / missing rows on Postgres** — the in-memory evaluator (the DST oracle) and Mongo (`null_matches_missing`) treat an absent/null field as *matching* a negative operator; the Postgres field-vs-value renderer used a three-valued `<>` / `NOT (… = ANY …)` that excluded NULL rows — diverging from the oracle and even from its *own* field-to-field `$neq` (`IS DISTINCT FROM`). It now renders `IS DISTINCT FROM` / `… IS NULL OR NOT …`, so mock ≡ Postgres ≡ Mongo. New parity-corpus cases (`neq_nullable`, `nin_nullable`) pin it, verified against real Postgres and Mongo.

- **Decimal cursor keys order numerically** — keyset comparison coerces `int` / `float` / `Decimal` to `Decimal` instead of comparing their string form (which ordered `'9' > '10'`, skipping/duplicating rows on money-shaped sort keys), and a `Decimal` sort key now round-trips through the cursor token exactly (a tagged wire value) so the seek compares it numerically. mock ≡ Postgres restored for Decimal-keyed pagination.

- **Concurrency primitives hardened** — `InflightLane` now `shield`s the shared task so one caller's timeout (or cancellation) can no longer cancel the in-flight computation for the other followers sharing it, and it deregisters on *task completion* (a done-callback that also retrieves the result) rather than on any waiter leaving, preserving single-flight. `SimpleLruRegistry.get_or_create` disposes a lost-create-race value *outside* the lock (a slow or re-entrant dispose no longer stalls or deadlocks the registry), and `GuardedLruRegistry.evict` no longer double-disposes an entry the idle-drain path is already disposing (a new `disposing` claim).

- **Mock isolation matches Postgres at the default level (both directions)** — READ COMMITTED conflict detection now anchors on the version at which each row was actually read/rev-checked, not on transaction begin, so a legal fresh-read-then-update no longer false-aborts (writes are still buffered and published at commit — no dirty reads). A concurrent duplicate-id `create` race now raises `exc.conflict` (matching Postgres 23505) instead of silently merging; `ensure`/`upsert` stay `ON CONFLICT DO NOTHING`. `FOR UPDATE` is now honoured (conflict-on-read); `SKIP LOCKED` is a declared mechanism divergence, not a silent no-op. Serializability soundness is unchanged. New conformance-battery cases (`fresh_read_update`, `duplicate_key_insert`, `for_update_lost_update`) catch each divergence, verified mock ≡ real Postgres. Removes a class of DST false positives *and* a false-negative; production adapters unchanged.

- **DST systematic search is complete again** — the DPOR frontier now zero-pads the choice prefix instead of truncating it, so schedules that hold a branch point FIFO before deviating (e.g. `(0, 1)`) are reachable; previously the search covered only an exponentially small contiguous-deviation corner and the completeness guarantee was false. Violation reports now carry the actual config / DPOR choices / Hypothesis plan and print a faithful `reproduce(...)` line (the old one reset to library defaults and usually would not reproduce under `thorough()`).

- **Regression bundles handle non-self-contained strategies honestly** — a bundle reproduces from seed + config via the *auto-derived* scenario, so an `OP_CASE` bundle (whose workload is the caller's `cases=`, never stored) can't self-replay. `assert_no_regressions` now reports such a bundle as a clear failure and wraps each replay so one bad bundle can't crash the batch (was an uncaught `ValueError` aborting the whole regression check); `replay_bundle` raises an actionable error instead of the raw dispatch one. The catalogued limitation (a bug found under a custom `scenario=` re-derives a different scenario on replay) is now documented on both entry points.

- **Mock outbox/inbox write-through is a catalogued DST divergence** — added the `outbox-inbox-write-through` entry to `MECHANISM_DIVERGENCES`: only the document store gets MVCC isolation, while the journalled outbox/inbox are write-through (atomic on rollback — no double-publish-from-abort — but a concurrent in-flight transaction can dirty-read their not-yet-committed rows). A premature-visibility / phantom-event finding on the outbox→relay→inbox path should be confirmed against a real broker/store.

- **Kafka commit-stream consumer is loss-free under poison and rebalance** — a malformed payload is surfaced as an `UndecodableStreamPayload` marker (paused, not raised out of `read()`); every pause/abort path calls the new `seek_to_committed` so an aborted batch is re-fetched, not skipped; a `KafkaCommitRebalanceListener` drops stale partition routing on revoke and seeks on assign; and the new supervised `commit_stream_consumer_background_lifecycle_step` restarts crash-loss-free. Adds `seek_to_committed` to `CommitStreamGroupQueryPort` and `UndecodableStreamPayload` to the stream contracts.

- **Firestore write path is OCC- and tenant-safe** — `_patch` wraps read-check-write in a Firestore transaction for real rev-CAS (works with or without a caller transaction; a concurrent write aborts → `@occ_retry` re-applies), replacing an unconditional full-document `set()` that lost even non-overlapping concurrent field changes. `kill`/`kill_many` do a tenant-verified delete and raise `not_found` on miss/cross-tenant. `$neq`/`$nin`/`$null` are removed from advertised capabilities (Firestore cannot match absent/null the way the agnostic oracle does, so they now fail closed with `query_feature_unsupported` instead of returning silently-wrong rows); `get_many` chunks ids at Firestore's 30-value cap. `create`/`create_many` now fail closed (`conflict`) on an existing id via Firestore's create-only write instead of silently overwriting, matching Postgres/Mongo; `ensure`/`upsert` catch that conflict and re-read so a lost create-race returns/updates the existing row rather than erroring. Firestore joins the cross-backend DSL parity harness.

- **CQRS read-only guard covers eager (factory-time) write-port acquisition** — an eager `ctx.document.command(spec)` in a QUERY factory now hits the same guard as a call-time one.

- **Notifications route through `QueueConsumer`** — `notification_consumer_lifecycle_step` / `notification_queue_consumer_handler` dedup redelivery on the event id and park poison messages; the consumer warns once per run when `max_deliveries` is set but the backend can't report a delivery count.

- **Outbox relay tenancy** — binds each claim's tenant before publishing; a tenant-aware outbox on the plain relay fails closed (`outbox_relay_tenant_unbound`).

- **Keyring fill-lock stripe uses a stable hash** — PYTHONHASHSEED-independent, for deterministic-simulation replay; a guard bans `hash(x) % n`.

**Field encryption & KMS hardening**

- **Entropy seam fails closed for secrets** — AEAD nonces, refresh/invite tokens, API keys, and OAuth `state`/PKCE now draw through `secure_random_bytes` / `secure_token_urlsafe`, which refuse a non-CSPRNG entropy source (`core.crypto.insecure_entropy`) unless a deterministic simulation explicitly permits it (`permit_insecure_entropy`, bound by `run_simulation`). A seeded source leaking into a context that mints a real secret can no longer produce a predictable nonce/token. Production and simulation output are unchanged.

- **`SystemEntropySource.random()` is now CSPRNG-backed** — it drew floats from the process-global Mersenne Twister while the source advertised the system CSPRNG; it now reads `os.urandom` (a shared `random.SystemRandom`), so every read from the default source is truly CSPRNG-backed. Only jitter/backoff use this float today, but the mismatch was a latent trap for a future secret-shaped float.

- **Opt-in strict mode for encrypted fields (`reject_plaintext`)** — the field codec's read-path plaintext tolerance is a permanent fail-open hole once a migration is done: chosen plaintext written to an encrypted column was accepted as authentic. `EncryptingModelCodec(reject_plaintext=True)` / `encrypting_document_codecs(reject_plaintext=…)` flips it after backfill — a non-ciphertext value in an encrypted or searchable field is rejected (`core.crypto.plaintext_rejected`), and record-id AAD binding stops falling back to the legacy id-less AAD (a pre-binding ciphertext no longer reads). Default `False` keeps zero-downtime rollout behavior.

- **Plaintext data keys no longer reachable via `repr`** — the keyring's active-key entry, its encrypt/decrypt caches, and the frozen decryptor suppress `repr` of the raw DEK bytes (matching `DataKey.plaintext`), so a log line, DST trace, or debugger dump can't print them.

- **Cached data keys honor a TTL** — `Keyring(dek_ttl_seconds=…)` bounds how long a plaintext DEK is served from cache on both the encrypt and decrypt paths, so a KEK rotation or revocation takes effect within the window instead of only after a restart (default `None` = unchanged, cache-until-eviction).

- **Confused-deputy guard on decrypt** — when a tenant is supplied, the keyring authorizes an envelope's `key_id` against the tenant's own key *before* any KMS unwrap and rejects a mismatch (`core.crypto.key_id_unauthorized`) with no backend call, so a caller can't drive a cross-tenant unwrap under a key id it names but does not own. The field codec threads the tenant through its decrypt pre-pass; `BytesCipherPort.decrypt` / `FieldCipherPort.ensure_unwrapped` gain an optional `tenant` (single-key `None` unchanged).

- **Vault Transit signer picks up key rotation** — `VaultTransitSigner(public_key_ttl_seconds=…, default 300s)` re-fetches the cached public key after the TTL, so a rotated Transit key is honored for verification and the published JWKS without a process restart.

**Identity & authorization hardening**

- **OIDC verifier no longer re-fetches JWKS per request** — `ConfigurableOidcIdpVerifier` (and the Google/Telegram builtin factories) built a fresh `JwksKeyProvider` on every call, so the 300s JWKS cache never spanned requests and every token-authenticated request hit the IdP's JWKS endpoint — an amplifier a spray of garbage bearer tokens could turn into a DoS / egress rate-limit. The verifier and its key provider are now built once and reused, so the cache works as intended.

- **Nonce enforcement reachable through presets** — `OidcIdpPreset(require_nonce=…)` (and the Google/Telegram configs) now forward `require_nonce` to `OidcTokenVerifier`, so a deployment can reject an `id_token` carrying no `nonce` claim (presence check; value binding stays `verify_id_token_nonce`'s job). Default `False` (unchanged).

- **`ForzeJwtTokenVerifier` guards its session spec** — it now applies the same `forbid_cache_and_history` construction-time check every sibling credential verifier uses, so a cached or history-enabled session query can't serve a revoked/rotated session row and defeat logout/refresh revocation.

- **Authz grant resolution cross-checks the tenant** — `AuthzGrantResolver` now takes the invocation tenant and refuses a caller-supplied `AuthzScope` naming a different tenant (`authz.scope_tenant_mismatch`) instead of silently resolving grants against the ambient tenant's bindings — defense-in-depth beside the storage layer's auto-scoping. No-op when no tenant is bound.

- **OIDC assertion records the validated audience** — for a multi-audience `id_token`, the mapper recorded `aud[0]`, which may be a different party than the one the verifier validated against its configured audience; it now records the matched (validated) audience.

- **`trust_tenant_header` no longer binds an arbitrary tenant for anonymous requests on a resolver-gated app** — the raw `X-Tenant-Id` fallback is honored when there is *no* tenancy resolver, or when the request is *authenticated* and the resolver returned no binding (a gateway authenticated it and set the header; a genuine tenant mismatch still raises). An *anonymous* request on a resolver-gated app gets no tenant rather than an attacker-settable one. Verified-credential issuer hints and the authenticated resolver path (when it does bind a tenant) are unchanged; `trust_tenant_header` still defaults `False`.

**Transport & agent surfaces**

- **MCP no longer leaks internal error details to agents** — `build_mcp_server` sets FastMCP `mask_error_details=True`, and a boundary `CoreException` is translated to a client-safe `ToolError` using the same egress-masked `error_envelope` the HTTP edge renders. A caller-caused error (validation/precondition/…) keeps its actionable message + code; an internal/infrastructure error is masked to a generic detail; any other exception is masked by FastMCP. Previously a tool call surfaced the raw exception message verbatim.

- **MCP stops advertising idempotency it can't honor** — the tool description no longer claims write operations support idempotent-retry replay: the MCP boundary binds no idempotency key (there is no per-call key channel like the HTTP `Idempotency-Key` header), so the wrap is a no-op and a retry would re-execute the write. Telling an agent retries are safe actively invited duplicate writes.

- **MCP tool defaults run their `default_factory` per call** — a flat tool argument backed by a `default_factory` (e.g. a `uuid`/timestamp) was materialized once at registration and reused for every call that omitted it; it is now left unset (a sentinel the handler strips) so the DTO regenerates it per call.

- **Generated FastAPI routes render one 422 shape** — a `RequestValidationError` (FastAPI's own body/query/path parse failure) is now rendered in the shared Forze error envelope + `X-Error-Code` (`request_validation_error`), matching operation-raised validation errors instead of FastAPI's default `{"detail": [...]}`; per-error `loc`/`msg`/`type` are kept, raw `ctx`/`input` dropped.

- **`forze dst replay` survives a bad corpus target** — one unloadable `module:attr` (renamed/moved app) is reported and counted as a failure while the rest of the corpus still replays, instead of aborting the whole run with a raw traceback.

- **Storage download route documents its buffering bound** — the generated `download` route fully buffers the object in memory (a `Range` slices the buffer, not a ranged backend fetch); its docstring now says so and points to `PRESIGN_DOWNLOAD` for large/untrusted-size objects. (A streaming, backend-ranged download route remains a follow-up.)

**PostgreSQL hardening**

- **Query parameters no longer leak across reads in a caller transaction** — the bound-parameter GUCs are set with `SET LOCAL` (transaction-scoped), but a read inside a caller transaction runs in a *savepoint*, and on its release the settings merged into the outer transaction — leaking one read's parameters into later statements. Each param-bound read now resets its GUCs after the fetch, confining them to that read.

- **`find_many` signals when its implicit row cap truncates** — an uncapped `find_many` (and `find_many_aggregates`) silently returned the default 10 000-row limit with no indication more existed. It now probes one row past the cap and logs a warning when it actually truncates, so the reconcile-everything footgun is visible; pass an explicit `limit` or paginate to read past it.

- **`update_matching` bounds its primary-key snapshot** — it snapshots the matching PKs to keep the set stable across the chunked update, but the snapshot `SELECT` was unbounded (a broad filter pulled millions of keys into memory). It now caps at `PostgresWriteGateway(update_matching_max_rows=…)` (default 1 M) and fails closed (`core.document.update_matching_too_broad`) when a filter matches more; `None` opts back into unbounded.

- **Recovery/claim schemas document their indexes** — the durable-run, durable-schedule, and outbox stores' documented table schemas now include the recommended (partial) `CREATE INDEX` for their `FOR UPDATE SKIP LOCKED` claim scans, which otherwise seq-scan and sort under lock as the table grows. The outbox docstring also spells out its delivery model: at-least-once (no fence — the inbox dedup makes the effect exactly-once), and per-`ordering_key` FIFO is not guaranteed with concurrent relays.

**Adapters & security**

- **Temporal default workflow id is a real UUID** — the default `workflow_id_factory` called `str(uuid4)` (the function's repr), so every unnamed `start()`/`schedule()` shared one garbage id and collided; now `str(uuid4())`.

- **Redis idempotency store can't be corrupted via the idempotency key** — the untrusted `Idempotency-Key` is hashed and the result body moved to a disjoint key scope, so a key containing the codec separator (e.g. one ending in `:body`) can no longer alias and overwrite another key's stored result; `commit`/`fail` became Lua compare-and-set/delete fenced on the caller's own PENDING claim. `RedisKeyCodec.join` no longer silently aliases distinct inputs (empty parts rejected, edge separators preserved). *Compat:* the idempotency key format changed, so the in-flight dedup window resets once on upgrade (old records expire by TTL — no corruption; a safe re-execute).

- **VK login** no longer copies the untrusted introspection envelope into claims (keeps only the masked `user`).

- **Object storage tenant isolation now covers reads, not just writes** — for a tenant-aware adapter, `download` / `head` / `download_range` / `download_if_changed` / `delete` / `copy` / `move` / `presign_download` / `presign_upload` / `put_object_tags` took a caller-supplied key verbatim, so under the `tagged` isolation tier (one shared bucket) a key like `tenant_<other>/…` could read/delete/presign another tenant's object. The key check now also requires the key to lie within the active tenant's prefix (`core.storage.key_outside_tenant`); keys minted by `upload`/`construct_key` already carry it, so legitimate round-trips are unaffected. No-op for non-tenant-aware adapters.

- **Meilisearch write path — failed tasks, unbounded waits, tenant leaks** — a completed-but-*failed* Meilisearch task is now raised (`infrastructure`) instead of reported as success; the task wait is bounded by a configurable `task_wait_timeout` (default 60s) rather than hanging forever. Under tagged tenancy, writes now stamp the tenant discriminator, `delete_all` deletes only the current tenant's documents (a filtered delete, never wiping the whole shared index), and `delete(ids)` is tenant-scoped — closing a cross-tenant write/delete hole. Deep offset / snapshot windows that would cross the index's `maxTotalHits` (default 1000) now fail closed (`core.search.max_total_hits_exceeded`, mirrored by a `max_total_hits` config) instead of silently truncating; the RRF fusion pool is clamped to that ceiling. Meilisearch page totals are estimated by default (`SearchCapabilities.exact_total_count=False`); `ensure_index` now provisions the index's own `maxTotalHits` to match `max_total_hits`, and an opt-in `MeilisearchSearchConfig(exact_total_count=True)` reports an **exact** total (Meilisearch page-mode `totalHits`, bounded by `maxTotalHits`) via one extra count query, flipping the capability to `True`.

- **Neo4j — keyed-edge identity, hop-quantifier coercion** — a keyed-edge `ensure_edge` (`MERGE`) now matches on the edge key (`MERGE (a)-[r:T {key: $edge_key}]->(b)`) so distinct keyed edges between the same pair stay separate instead of collapsing; the variable-length `*..n` quantifier is `int()`-coerced before inlining (defense-in-depth). Graph writes now enlist in the framework transaction scope via `Neo4jTxManagerAdapter` (wired with `Neo4jDepsModule(tx={…})`), so a handler's writes commit or roll back as a unit; `client.transaction()` remains the explicit lower-level scope for multi-statement atomicity, and graph writes stay non-co-transactional with the outbox (no cross-database two-phase commit).

- **`forze_redis` imports on redis-py 7 again** — the stream client imported `XReadResponse` / `XReadGroupResponse` / `StreamEntry` / `StreamRangeResponse` / `XReadGroupStreamResponse` from `redis.typing`, which only exist in redis-py 8 (the typed-response overhaul), so the whole package raised `ImportError` on redis-py 7.x despite the `redis>=7.3.0` pin. Those aliases are now self-owned (they typed nothing behavioural — pure annotations over the RESP2-list / RESP3-dict wire shapes, which `utils` inspects at runtime). Client-side caching invalidation genuinely needs redis-py 8's RESP3 push API, so it now fails closed on redis-py < 8 with `configuration` (`redis.client_side_caching_unsupported`) and asserts a RESP3 connection; **everything else in `forze_redis` works on redis-py 7.3+**. A guard test forbids re-importing `redis.typing`.

- **Mongo** query renderer rejects `$`-prefixed field names (injection); index introspection keeps string index directions verbatim (text/2dsphere/2d/hashed/vector).

- **FastAPI `X-API-Key`** splits `prefix:secret` on the first colon (matching `forze_mcp`); bare keys still authenticate.

- **Per-tenant routed clients** fingerprint the full host list — multi-host DSNs (Mongo replica set, Redis Sentinel, AMQP cluster) no longer raise.

- **Postgres** schema validation accepts parameterized column types (`NUMERIC(10,2)`, `TIMESTAMP(3) WITH TIME ZONE`); search index-definition parsing hardened (balanced-delimiter, dollar-quote-aware).

- **Log scrubbing closes three leaks** — console render mode (the default) no longer hands the raw exception to Rich when `sanitize_logs=True`, so a credential in an exception message (e.g. a DSN) is scrubbed in both the message and the rendered traceback (pretty tracebacks stay when scrubbing is off); assignment scrubbing now covers a bounded suffix (`secret_key=`, `aws_secret_access_key=`, `token_value=`), the whole `Authorization:` header, and a scheme-agnostic `scheme://user:pass@` DSN (ClickHouse/Mongo/HTTP, not just the four SQL schemes); and a non-str dict key no longer raises `TypeError` into the caller's log site.

- **`configure_logging()` no longer silently drops unlisted loggers** — with no `logger_names` it attached a handler to *nothing*, so every INFO log hit Python's WARNING-level last-resort handler and vanished; it now configures the **root** logger by default (an explicit `logger_names` list is still honored verbatim as an allowlist), so a caller who omits or forgets a name keeps seeing their logs.

- **Misc** — BigQuery empty-array/null params typed from annotations; Meilisearch strips embedded quotes; numeric timezone offsets validated; `forze dst --seeds` parsing fails loud; S3 multipart-ETag normalization and unknown-total range downloads; `If-None-Match` parsed per RFC 7232; `forze_http` suppresses its default bearer when an `Authorization` header is set; GCS rejects reserved `forze-tag-` metadata keys.

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
