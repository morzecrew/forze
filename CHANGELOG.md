# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

**Inference seam** — typed model invocation behind one port; whether the model is a local artifact, a served endpoint or a cloud runtime is a wiring fact.

- Handlers call `ctx.inference.model(spec)` for single, batch (all-or-nothing) or streaming prediction. It is a read-plane port, so a CQRS query can hold it; a backend response that does not match the declared output type is refused at the boundary rather than handed on.
- Backends: a local adapter whose loader runs off the event loop and is warmed fail-closed at boot, a programmable mock, and remote adapters for KServe-V2/MLflow over HTTP and for SageMaker (extras `inference-http`, `inference-sagemaker`). Capabilities are declared per backend and unsupported features fail closed.
- Upstream error bodies are withheld from raised errors and from logs (only status, size, and the container's log pointer are recorded); an upstream 401/403 and a failing model container classify as infrastructure, not caller errors. SageMaker pins botocore retries to a single attempt unless configured explicitly (`invoke_endpoint` is metered and non-idempotent); the `config` knob is on the client, the lifecycle hook, and `sagemaker_inference_lifecycle_step`.
- The local adapter's `serialize_calls` lock is awaited on the loop before dispatch, so a waiting prediction holds no slot in the shared CPU pool and cancels cleanly. The mock accepts per-route `capabilities=` mirroring a real backend's declared surface and enforces them (batch cap, stream refusal), so capability gates fail against the oracle where production would; the full-surface default is unchanged.
- Remote wiring refuses to build until data egress is explicitly acknowledged, and honours all four tenant-isolation tiers — `dedicated` requires a routed client with per-tenant credentials.
- Simulation value capture masks inference inputs by default; any spec type can now declare its own sensitive capture fields.

**Portability** (`forze_kits.integrations.portability`) — carry an application's system-of-record state to any other wired backend.

- `export_archive` / `import_archive` move the document, blob, graph and counter planes through a backend-agnostic archive. Ids are preserved and revisions reset; import fails closed on version, fingerprint or checksum mismatch.
- `migrate` fuses the same pipelines port-to-port: no artifact, and the target re-seals fields under its own keys.
- Scope is declared, never inferred — one tenant or the whole system. Claiming a quiesced export requires an attested quiesce report, and per-tenant exports leave identity and credential specs out unless asked for.
- Archives are plaintext by construction; passing a sealer encrypts one under a KMS-wrapped per-archive key. Gzip by default, zstd available via the `zstd` extra.

**Export foundations** — every plane an application binds is enumerable, quiescible and streamable; a portable export refuses anything undeclared.

- **Spec inventory** — applications register their specs, and `build_runtime` reconciles that inventory against the wired deps so an unbound or unregistered plane surfaces at startup. Identity and aggregate kits contribute their own specs. A registry fingerprint hashes the app's portable shape as a drift signal, not a gate.
- **Quiesce** — `quiesce()` stops admission, waits for in-flight work, then polls the outbox, durable and stream-group planes to rest, returning settled and attested verdicts. Run without closing the gate it is a health check that cannot attest.
- **Outbox and counter observability** — read-only admin ports report outbox depth, undrained state and oldest pending age, and enumerate counter partitions; both are safe for a CQRS query to hold.
- **Counter backends** — Postgres, Mongo and Firestore join Redis. Counter operations never join the caller's transaction, which is **breaking** for custom implementations of the three client ports; Postgres additionally needs an app-migrated counter table.
- **Graph streaming reads** — capability-gated keyset streaming of vertices and edges (Neo4j, mock), failing closed where unsupported. **Breaking** for custom graph query ports: two new protocol methods.
- **Search-index rebuild** — an idempotent keyset-paged backfill that upserts live rows and removes soft-deleted ones, available standalone or on an aggregate kit.
- **Relay drain and provenance** — the outbox relay can opt into draining on shutdown, and analytics specs declare whether they are projected or system-of-record so an export can refuse to carry a plane that cannot be reproduced.
- **Tenant enumeration** — tenant management pages every tenant with a total, independent of membership.

**Realtime operational hardening** — the egress plane (stream → gateway → Socket.IO) reaches the durable plane's operational bar.

- **Supervised loops** — a shared background-loop runner with jittered restart backoff and a consecutive-crash ceiling now underpins the realtime loops. **Breaking** for imports of `BackgroundLoopControl`, which moved out of `forze_kits.lifecycle` (unreleased).
- **Retention and trim** — realtime streams can be capped, and a supervised trim removes only entries every group both delivered and acked.
- **Delivery guards and depth** — a poison ceiling bounds redelivery of a stuck signal (default 5), and group depth is readable and quiescible.
- **Mailbox durability** — an age sweep bounds mailbox growth and prunes idle device cursors, stored signal bodies can be sealed at rest (sealing the replay index is refused), and the in-memory mailbox caps entries per principal.
- **Perimeter and tenancy** — the WebSocket route takes an Origin allowlist and enforces credential expiry continuously, a built-in reauth command refreshes it for the same principal and tenant, and an untenanted signal is dropped rather than broadcast to the global room.
- **Observability and crash conformance** — gateway and backplane stats with OTel instrumentation, bridging under the producer's trace context, plus a DST conformance run for gateway crash delivery against the mock or a real Redis stream.

**Realtime transports & client contract** — three transports (Socket.IO, SSE, raw WebSocket), one versioned wire contract.

- **Versioned wire protocol** — the envelope, cumulative ack and handshake are now normatively documented, and every transport negotiates a protocol version at connect, refusing one it does not support.
- **Transport-neutral kernel** — mailbox and cursor seams, replay/ack helpers, the client-key ladder and presence move into the core realtime integration; `forze_socketio` re-exports every established name unchanged.
- **SSE and raw WebSocket egress** — both offer replay plus live tail; SSE resumes from `Last-Event-ID` and acks over POST, while the WebSocket route adds governed command dispatch with per-frame idempotency, deadline budgets, and in-flight and frame-size bounds. Topic subscriptions are authorized fail-closed.
- **Fail-closed WebSocket scopes** — the middlewares refuse raw WebSocket scopes unless the exact mounted path is allowlisted, reconciled at startup. **Breaking** for apps mounting raw WebSocket routes behind them.
- **AsyncAPI export** — the realtime catalog renders as an AsyncAPI 3 document, servable like the OpenAPI one for client codegen.

**Mock field-encryption conformance** — `forze_mock` runs the real field-encryption path on every field plane.

- **Synchronous key seam** — opt-in synchronous twins of the key-management and directory ports, for computation-only key backends. Implemented by the mock and the shipped directories only, never a real KMS, and they keep the same key-ownership guard as the async path.
- **Every mock field plane seals** — document, graph, search (including hub/federated and snapshots), analytics and procedures resolve the same fail-closed encrypting codecs as real backends, with decrypt-on-read, searchable-equality rewriting and cross-tenant key refusal. **Behavior change** for mock suites asserting raw stored ciphertext; text queries no longer match sealed content.

### Changed

**Breaking — graph**

- **An edge kind identified by its endpoints now enforces that identity** — at most one edge per endpoint pair, so creating a second one conflicts instead of quietly laying a parallel edge. **Migration:** kinds that legitimately allow parallel edges declare a key field and identify by it. Ensure, update and keyed kinds are unaffected.

- **Graph module specs are validated at construction** — a duplicate kind name, an endpoint naming an unknown node kind, or a key field missing from its read model now fails at build rather than at first use.

**Behavior**

- **Background loops stop gracefully instead of being cancelled** — kits loops register per scope and shutdown stops each between units of work; consumers accept a stop signal, and a commit-stream consumer commits processed offsets even when cancelled mid-batch.

- **The realtime loops join them, supervised** — the gateway, presence heartbeat and identity-expiry sweep restart on crash with jittered backoff and register as drainable, and each tenant's loop is supervised independently. **Breaking** for custom signal sources, which must accept a stop signal.

- **Realtime gateway defaults harden** — emits time out after 5 seconds instead of hanging unbounded, and a realtime stream route declaring an encryption tier is refused at start rather than silently unsupported.

- **Redis pub/sub subscribers reconnect by default** — after a transport error a subscriber resubscribes instead of silently stopping; opt out per client config.

- **`python-socketio` capped below 6** — the gateway and server builder depend on 5.x constructor surfaces.

- **An outbox relay with no destination is rejected** — naming a transport spec that was never provided now raises at construction instead of quietly dropping that route from the inventory.

### Fixed

**Persistence tenancy & fidelity**

- **Counter tenancy (Postgres, Mongo, Firestore)** — counters resolve through the bound tenant, so namespace-tier isolation holds, and the spec route is folded into the stored key so two specs sharing a relation no longer merge sequences. An existing pre-route sequence is carried forward rather than restarting at zero.
- **A contended Firestore counter no longer surfaces a concurrency error** — allocation retries under a budget sized for counter contention instead of the shared optimistic-concurrency default, which gave up while concurrent callers were still queued. Postgres and Mongo allocate atomically and were unaffected.
- **Mongo history reads scope by tenant** — snapshots are stamped and reads filter strictly on it, matching Firestore and closing a tagged-tier cross-tenant read. A pre-upgrade snapshot carries no tenant, so it is unowned and invisible to every tenant; backfill it on legacy history rows for strict pre-upgrade concurrency continuity.
- **Listing a missing bucket can read as empty** — opt-in, so the object-list route and a blob-less export no longer fail outright; the default still raises. Object listing also bounds its per-object HEAD fan-out.
- **Decimal filter values** — the query caster no longer locale-guesses a comma, and a JSON string is accepted as an exact Decimal or datetime range bound, cast per field against the read model; non-finite bounds (`"NaN"`/`"Infinity"`, string or native) are refused on every backend.
- **Meilisearch Decimal reads are exact** — a shadow field restores the precision the f64 index number rounds away.
- **Mock graph matches Neo4j on four write-path guards** — deleting a vertex detaches its edges, edges require existing endpoints, creating a duplicate key conflicts, and unknown kinds raise.

**Portability, quiesce & inventory**

- **A full-system scope must declare its tenant dimension** (**breaking**) — there is no default; a full export or migrate walks each declared tenant bound and writes one archive section per tenant (archive format 2), counters included, so no tenant's sequences restart at zero.
- **Import confirms the target tenant** (**breaking**) — a per-tenant archive must be imported against an explicit tenant. The manifest is cross-checked rather than trusted, and sealed frames bind the tenant into their authenticated data, so an edited manifest fails authentication instead of re-homing the payload.
- **The artifact is cross-checked against the manifest and the target's plan** — an unlisted data file, or a plane the target expects that the manifest never lists, refuses the import; a missing plane no longer imports as an empty one.
- **Unsealed credential-adjacent exports are refused** — carrying identity or field-encrypted specs requires either a sealer or an explicit plaintext acknowledgement.
- **Quiesce attests only what it observed** — planes it could not read are now recorded as unobserved and block attestation: unreachable admin ports, catalogued queues and locks with no probe, streams with no named group, and a runtime with no spec inventory.
- **Inventory registration refuses conflicting metadata** — a re-registration that disagrees with the first now raises instead of silently winning, an empty registry over bound planes fails reconciliation, and planning an export against an empty inventory is refused.

**Realtime**

- **An untenanted signal no longer stops the gateway for every tenant** — it is now a per-signal error, parked and bounded by the poison ceiling, rather than a process-terminal configuration verdict.
- **Cumulative ack could skip — then trim — undelivered mailbox entries** — the replay cap is now a newest-first retention window, always a complete suffix rather than a truncated prefix, and acks clamp to the replay's delivered floor mid-drain.
- **Node-local presence on a multi-node backplane suppressed every live emit** — the gateway now refuses node-local presence under a pub/sub Socket.IO manager instead of silently dropping emits.
- **One hostile frame could tear down a WebSocket connection** — a binary frame closes cleanly, and a command result that cannot be serialized becomes an error ack instead of cancelling every in-flight command.
- **Redis counter reset returned the previous value, not the new one** (**behavior change** for callers relying on the old return); batch allocation now uniformly accepts a size of one and rejects anything smaller on every backend.
- **A principal in two orgs could wedge the ack path forever** — the per-device cursor id now includes the tenant (it collided across tenants on the shared-table shape, looping find-miss/create-conflict unboundedly), and the compare-and-advance loop is bounded, raising `realtime_cursor_advance_stalled` instead of spinning.
- **A cap-filled replay no longer lifts the ack clamp** — a replay that stopped at the mailbox cap could let a live-frame ack advance the cursor over the undelivered middle, which the trim then hard-deleted. A one-entry probe settles the ambiguity: an exactly-drained replay proceeds normally, while a truncated one keeps the Socket.IO delivered-floor clamp and ends the SSE stream (`Last-Event-ID` resumes it).
- **SSE acks require `?device_id=...`** (**behavior change**) — the device-less fallback cursor is shared by every tab of a principal, so one tab's cumulative ack could trim another tab's undelivered backlog. Device-less streams still work via `Last-Event-ID` resume.

**A saga step interrupted at its own commit no longer reports a consistent rollback** — a drain-timeout cancel at a step's transaction commit (`commit_ambiguous`) was read as a step failure: earlier steps were compensated around a possibly-committed step and DOMAIN `saga.step_failed` falsely certified consistency. All three drivers (in-process, durable, Temporal) now compensate nothing and raise infrastructure-kind `saga.step_ambiguous` for operator reconciliation (pinned non-retryable in the Temporal mapping); the durable journal keeps the classification across crash recovery and replay.

**`Decimal` is a first-class filter and sort value across the query DSL** — the scalar union omitted it, so the parser rejected an explicit `Decimal` operand and a bare shortcut misrouted to a membership test.

- **Postgres** — nested JSON Decimal leaves compared as text, numeric columns round-tripped filter values through `float`, and writing a Decimal into `jsonb` raised; all fixed, and Decimal-annotated array quantifiers now compare numerically for integer and float operands too.
- **Mongo** — a Decimal filter value was stringified before coercion and so matched nothing (the read-side sibling of the 0.5.0 write fix).
- **Firestore** — a Decimal field could not be written at all, and UUID and Decimal filter values reached the driver raw; writes and filters now share one coercion.
- **Mock** — aggregates refused Decimal fields; they now fold in float space.
- **Meilisearch** — Decimal fields index as JSON numbers, so filters and sorts are numeric rather than lexical (the document plane keeps the exact value). Sealed roots are never converted. Rebuild the index if Decimal fields were indexed from an unreleased build.
- **Meilisearch filter literals now match the indexed representation** — aware datetimes normalize to UTC on both sides, where a UTC equality filter previously never matched, and an enum operand renders its indexed value.

**Streamed object uploads dropped their tags** — multipart completion carries no tagging, so a streamed upload never wrote its tag map while the returned object reported it; tags are now applied after completion.

**Sealed fields are refused as filter and sort keys on every backend, including the mock** (**breaking**) — decided from the spec's declaration: filtering a randomized field is refused (searchable fields keep equality), and sorting any sealed field is refused, including a default sort. Such filters and sorts now raise where they previously returned wrong answers.

**Mock storage refused to create a missing object on unconditional overwrite** — it raised not-found for any absent key, contradicting the port contract and the real S3/GCS adapters. It now creates-or-replaces, and still answers not-found for a *conditional* overwrite of a vanished object.

**Mongo could not store a document with a UUID or Decimal field** — such a document could be updated but never created; the write coercion now applies on insert too, and reads convert back exactly.

**JSON-boundary encoding** — four adapters handed Python-mode maps, holding live UUID, datetime and Decimal objects, to JSON serializers and raised on ordinary payloads. The rule is now documented at the seam; Postgres and ClickHouse keep the Python encode on purpose.

- **Transactional outbox** — staging such a payload raised; it now encodes to JSON before any backend sees it.
- **Meilisearch** — every committed write on a search-synced aggregate raised against a real index, since a standard read model carries a UUID and a datetime.
- **BigQuery analytics ingest** and **Inngest events** — raised or rejected on any of the three.

**Cross-process fingerprints: sets hashed in iteration order** — **idempotency was live-broken**, as a command model with a set field hashed differently per replica, so a byte-identical retry was rejected as a payload-hash mismatch. Sets now hash order-independently; search-snapshot and federated-cursor fingerprints are fixed with it.

**Kafka**

- **A failed rewind could silently skip records** — every rewind failure was treated as a benign rebalance, so a coordinator error with partitions still held left the position past unprocessed records and then committed past them. The two cases are now told apart and an unrestorable consumer is discarded.
- **A poison marker no longer drops the record's headers** — it now carries the decoded headers and message type, so a forwarded sealed envelope keeps the ids its authenticated data binds to and stays decryptable for dead-letter triage.

**The log scrubber masks camelCase and PascalCase names** — a credential masked as `db_pwd` leaked as `dbPwd`, and `secret_key=` masked while `secretKey=` leaked. The key heuristic and the value rule both recognize a case hump now; mid-token runs like `backupwd` stay unmasked as before, and compound-suffix matching is linear-time on hostile text.

**A backward keyset page no longer dead-ends navigation** (**behavior change**) — a `before` page landing flush on the start of the set returned no cursor in either direction, stranding the client on a full page with rows still ahead. It now always carries a forward cursor, and `has_more` reports that same forward answer instead of the backward fetch, so the flag and the cursor cannot disagree. Applies to every keyset-paging backend.

**A rejected MCP tool argument no longer echoes the value back** — invalid arguments raise a masked validation error (`mcp_invalid_arguments`) with field-level errors and the raw input stripped, no longer depending on the host server's `mask_error_details` (which a caller-owned FastMCP does not set).

**The search-sync outbox route declares `require_transaction`** — a marker flushed outside a transaction is refused (`core.outbox.flush_outside_transaction`) instead of silently degrading to a dual-write. The kit already stages in-transaction; hand-rolled wiring that attaches the staging hook without `bind_tx()` now fails loudly.

**Durable search sync works multi-tenant out of the box** (**behavior change**) — `OutboxSearchSync.bind_tenant_from_headers` defaults to `True`, so the consumer binds the tenant its own relay stamped rather than nacking forever; set `False` for the strict posture.

**Permanent dependency faults are no longer retried forever**

- **A deleted or disabled KMS key is classified permanent** (**behavior change**) — AWS, GCP and Yandex map a key state they name outright (not-found, disabled, destroyed) to `CONFIGURATION`, so a commit-stream consumer pauses-and-alerts with `failed > 0` instead of crash-restarting forever, and a queue consumer requeues the affected messages uncounted and keeps consuming (throttled while nothing decrypts), instead of parking the encrypted backlog as poison. Anything ambiguous stays retryable: access-denied, throttling, and any state naming no terminal condition.
- **The commit-stream supervisor escalates instead of giving up** — a `CONFIGURATION`-kind crash is terminal, while a retryable one is retried indefinitely; new `crash_alert_after` (default 5 min, `None` to never escalate) raises one critical log per incident once it has crashed on every restart for that long. Healthy uptime opens a fresh incident.

**Broker delivery integrity (RabbitMQ, draining)**

- **The RabbitMQ pending map leaked on partial ack and after channel recovery** — a channel reopen purges the stale delivery tags, deliveries read on a channel replaced mid-drain are discarded rather than registered against it, and only confirmed acks and nacks are counted and settled.
- **Draining no longer parks in-flight messages as poison** — a message refused by the drain gate mid-quiesce is requeued without counting as a delivery attempt and the loop stops, so a rolling deploy cannot drive it to the poison ceiling with no handler defect. Terminal nacks can now opt out of delivery counting where the backend tracks it.
- **A terminal nack no longer wedges an SQS FIFO message group** (**behavior change**) — on a FIFO queue it now retains a copy on the configured poison queue and deletes, rather than blocking the group forever where no redrive policy would ever trim it. Standard queues are unchanged.
- **SQS honors an uncounted requeue** — `nack(requeue=True, count=False)` replaces the message with a byte-identical copy whose receive count restarts, keeping drain refusals and key-outage redeliveries away from the redrive DLQ. A FIFO queue keeps the order-preserving reset until the count nears its redrive threshold, then copies back under the same message group; without a redrive policy it always resets.
- **A FIFO poison message is never deleted without being retained** (**behavior change**) — where a retention queue is configured but the copy cannot be sent, the original is kept rather than destroyed and its message group stays blocked until the retention queue recovers. Unconfigured retention still deletes, as before.

**Graph**

- **A kind could seal its own key field, making its vertices unreachable** — a key field named in its own kind's encryption policy is now refused at spec construction; encrypting an ordinary property is unaffected.
- **Neo4j: concurrent statements in one transaction no longer race the lazy open** — two statements under one `asyncio.gather` could each begin a transaction, the second orphaning the first (never committed; its session leaked); a scope now opens exactly one transaction.
- **Neo4j: a routed transaction refuses a tenant change mid-scope** — later statements would run auto-committed on the other tenant's client while the scope committed only the first tenant's work; now fails closed (`neo4j_tx_tenant_conflict`), matching the direct client's database guard.

**Meilisearch federation returned ciphertext for sealed fields** — `merge="federation"` now decrypts sealed fields through the same seam as every other read path (the same query returned plaintext under `merge="rrf"`), refuses sealed sort keys, and fails closed when the fused window exceeds the smallest member's `maxTotalHits`.

## [0.5.0] - 2026-07-13

### Added

**Reliability & durability**

- **Self-hosted durable execution (Postgres)** — crash-resumable durable functions and sagas: exactly-once step replay, lease-based recovery with heartbeat renewal, an execution deadline (`max_run_duration`, default 1h — a hung body is cancelled and FAILED instead of renewing its lease forever), multi-worker fencing, delayed and cron runs, and an opt-in read-only admin port. Wired via `forze_kits`; adds croniter.

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

- **Blob re-encryption sweep** — streams every object of a route down and back under a fresh data key, in place and in bounded memory, preserving metadata and reporting rewritten and skipped counts. The rewrite is conditional on the object being unchanged, so a concurrent delete stays deleted rather than being resurrected and a concurrent change is retried once from fresh bytes.

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

- **AggregateKit durable search sync** — opt-in `search_delivery=OutboxSearchSync()` routes index maintenance through the transactional outbox: an identity-only marker stages in the write's transaction and a consumer re-reads the committed row (idempotent, reorder-safe, no payload in the event), so a transient index failure converges instead of drifting. With soft delete composed, kit search reads exclude soft-deleted rows; the index must declare `is_deleted` facetable, checked fail-closed.

- **Shared error helpers** — one client-safe error envelope and kind-to-HTTP-status mapping shared by the FastAPI and Socket.IO edges.

- **Mock document adapter tenant scoping on every write** — the in-memory mock injects the tenant column on ensure/upsert/update/touch, matching Postgres.

- **Telegram Login Widget verifier** — verifies Login Widget callback data via Telegram's HMAC scheme with a freshness bound, emitting the canonical assertion.

**Deterministic Simulation Testing (`forze_dst`)** — new package

- **Point-at-a-real-app simulation** — one master seed reproduces a whole run (schedule, faults, latency, inputs, crashes, partitions) over real registries and runtimes, single-process or N-node, with no app changes; a violation minimizes to a reproducible counterexample.

- **Deterministic runtime & seams** — a simulation event loop and time source give byte-identical replay; time and entropy flow through bindable seams, and a determinism gate bans raw time/entropy outside them.

- **Faults, latency, crash & partitions** — declarative fault rules, latency profiles, crash-restart recovery over persisted mock state, and lossy or asymmetric network partitions.

- **Workloads, schedulers & coverage** — scenario rules and a fuzzer generate workloads; PCT and systematic schedulers explore interleavings; coverage-guided exploration and parallel seed sweeps scale the search.

- **Oracles** — built-in invariants (duplicate effects, mutual exclusion, linearizability, consistency models), transactional-isolation oracles up to serializability cycles, commutativity checks, reachability targets, and opt-in value-level checks.

- **Reporting & regression corpus** — causal-graph violation reports and timelines, failure bundles with honest replay semantics, a regression corpus, and a CLI (`forze dst run` / replay / coverage / topology / derive). Bundle serialization round-trips every config field (an introspective guard fails on any future field a serializer misses — it caught stream-fault rules and the pruning flag being dropped).

- **Mock substrate** *(behavior change)* — journalled transactions with MVCC isolation are now the mock default; in-memory outbound HTTP added.

- **Adapter conformance** — a backend-agnostic isolation-anomaly battery asserts the mock matches real Postgres and Mongo, including lock races, outbox→inbox delivery under crash, resilience stores under partition, and identity rotation/revocation under fault. The Mongo leg runs the full battery (abort-vs-block catalogued as a mechanism divergence); contract strengthenings can be engine-scoped, so a backend-specific one never masks a mock deviation.

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

- **SQS: one poison message no longer poisons the receive batch** — per-message decode failures are isolated (left for redrive; deleted on FIFO so a message group cannot deadlock); FIFO per-message delay and over-length queue names fail closed. Opt-in `poison_queue_url` retains a raw copy (with provenance attributes) before the FIFO delete — the framework's one poison-destroying path; unconfigured destruction now logs a warning naming the knob.

- **RabbitMQ: opt-in dead-letter sink and redelivery counting** — a configured DLX makes a poison nack dead-letter instead of drop, and opt-in redelivery counting lets poison-parking fire past the broker's redelivered ceiling (requires publisher confirms; enabling the DLX on an existing queue requires recreating it). A consumer starting on a queue with no DLX now warns once per queue that poison messages would be destroyed.

- **Kafka admin reads no longer auto-create topics** — existence is checked against the all-topics listing, so querying lag for a mistyped topic returns empty instead of minting the topic.

- **Inngest function-level config** — `InngestFunctionConfig` forwards Inngest's native controls (retries, concurrency, rate limits, batching and more).

- **An unregistered durable run name cannot livelock recovery** — the run lands FAILED with the reason recorded instead of stranding every co-claimed run each sweep; the scheduler likewise isolates a corrupt cron expression. Externally cancelling a run's awaiter also tears down the body and heartbeat tasks instead of leaving them running detached.

- **A transient saga step error no longer triggers irreversible compensation** — a retryable kind (infrastructure, throttled, concurrency) is retried in place with backoff before anything is journaled, so a one-off blip cannot undo committed steps; compensation fires only on exhausted retries or a genuinely non-retryable failure, and a crash mid-blip replays completed steps from the journal. The same retry now guards transient errors during compensation itself.

- **A transient KMS failure during consume is retried, not poison** — decrypt failures classify through the egress policy: retryable kinds requeue (queue consumer) or crash for a supervised rewind-and-restart (commit-stream) instead of being dropped; tampering still parks.

- **The e2e-encrypted DLQ copy is decryptable and keeps its identity** — the dead-letter path re-sealed the decrypted payload tenant-unbound under a fresh id while keeping the old tenant header, so a DLQ consumer could not authenticate it and correlation was severed. The diverted copy is now the original sealed envelope byte-identical, and sealing without a bound tenant drops a stale tenant header rather than contradicting the envelope.

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

- **Encryption fail-closed** — filtering a randomized-encrypted field raises precondition; encrypted-sort rejection covers every search backend. Declaring an encrypted field as an indexed search content field (randomized or deterministic-searchable — ciphertext never tokenizes) is now refused at spec construction with a configuration error, instead of sealing ciphertext into the index and silently matching nothing.

- **Consistent adapter errors** — the mock rev-conflict matches the real adapters; a missing dependency names what is registered; database errors classify on codes, not message text.

**Correctness & consistency**

- **Reverse cursor pages return the window adjacent to the cursor** — the document assembler, Postgres hub parallel cursor and Mongo search cursor kept the wrong over-fetch sentinel; all now delegate to one shared keyset windowing. Mongo search first-page prev cursor is now null like every backend.

- **Firestore cursor pagination works past page 1** — the anchor snapshot was passed in a list (a TypeError on any real token) and the before direction was inverted; a deleted anchor now fails closed as a stale cursor. The dead start_before_id client-port parameter is removed.

- **`$neq` / `$nin` / `$disjoint` include NULL rows on Postgres** — the renderer uses IS DISTINCT FROM semantics, matching mock and Mongo.

- **Tenant-aware Mongo text search works** — the prefilter (tenant tag, caller filters) was emitted as its own leading match stage, pushing the required first-stage `$text` to stage two, which the server rejects — every prefiltered text search failed. The first match now carries the text query and the prefilter together, across the offset, cursor, count and thin paths.

- **Decimal cursor keys order numerically** — keyset comparison coerces numeric types to Decimal (was string-comparing) and a Decimal sort key round-trips exactly.

- **Intercepted streams close deterministically** — the proxy wrap and both builtin stream interceptors now chain aclose to the backend cursor at close time instead of leaving release to garbage collection; abandonment logs nothing and mid-stream errors keep their classification.

- **Concurrency primitives hardened** — the in-flight lane shields the shared task from one caller's timeout, and the LRU registries no longer double-dispose or dispose under the lock.

- **Mock isolation matches Postgres at the default level** — READ COMMITTED conflict detection anchors on the version actually read, a duplicate-id create race raises conflict, and FOR UPDATE is honoured; verified against real Postgres.

- **DST systematic search is complete again — and honest about pruning** — the DPOR frontier zero-pads the choice prefix, so previously-unreachable schedules are explored; violation reports print a faithful reproduce line. Signature-pruning (a heuristic that can miss a violation behind an effect-equivalent prefix) is now switchable: `dpor_prune=False` walks the full choice tree, and the docs state the actual guarantee.

- **Regression bundles handle non-self-contained strategies honestly** — a bundle that cannot self-replay reports as a clear failure, and one bad bundle cannot crash the batch.

- **Mock outbox/inbox write-through is a catalogued DST divergence** — confirm a premature-visibility finding against a real broker or store. In-transaction outbox status transitions (claim, mark, retry, requeue, reclaim) now journal and revert on rollback, matching Postgres — the catalogued divergence covers visibility only.

- **Kafka commit-stream consumer is loss-free under poison and rebalance** — malformed payloads pause instead of raising, every pause or abort re-seeks to committed, a rebalance listener drops stale routing, and the supervised lifecycle restarts crash-loss-free.

- **Mongo datetimes decode timezone-aware (UTC)** — the client now reads BSON dates as aware UTC like Postgres and the mock (was: naive UTC, caller-visible). The naive reads also made history OCC's three-way check flag a stale-rev update that echoed an unchanged datetime as a false `historical_consistency_violation`; Mongo document history is now covered by integration tests end-to-end.

- **GCS conditional multipart completion covers the metadata stamp** — user metadata binds inside the same precondition boundary as the final write: the single-part rewrite carries it in the conditional write itself, and the compose paths pin the follow-up patch to the composed object's generation — a concurrent overwrite answers conflict instead of inheriting the completing caller's metadata.

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

- **A sealed field cannot reach the external search index in clear** — index sync re-seals the aggregate's decrypted read model under the search spec's own policy, so a search spec that omitted a field the document spec sealed published that field's plaintext to Meilisearch. The "same policy on both specs" rule was documented but unenforced; it is now checked wherever the two specs meet, failing closed.

**Identity & authorization**

- **OIDC verifier no longer re-fetches JWKS per request** — the verifier and its key provider are built once and reused, so the JWKS cache actually spans requests.

- **Nonce enforcement reachable through presets** — presets forward require_nonce to the token verifier; default off.

- **`ForzeJwtTokenVerifier` guards its session spec** — the same no-cache/no-history construction check as every sibling verifier, so a revoked session cannot be served from cache.

- **Authz grant resolution cross-checks the tenant** — a caller-supplied scope naming a different tenant is refused.

- **OIDC assertion records the validated audience** — the matched audience, not the first list entry.

- **`trust_tenant_header` no longer binds a tenant for anonymous requests on a resolver-gated app** — the raw header fallback applies only without a resolver, or for authenticated requests the resolver did not bind.

**Transport & agent surfaces**

- **MCP no longer leaks internal error details** — boundary errors render through the same egress-masked envelope as HTTP; caller-caused errors keep message and code. Resource templates now mask through the same shared helper as tools — a raw internal error no longer reaches the agent on a caller-owned server.

- **Pre-built pagination DTO is bounded** — `page` and `size` validate at the boundary (`size` capped at 10,000, matching the cursor clamp and the implicit find-many limit), rendering as the standard 422 envelope instead of admitting unbounded materialization.

- **MCP stops advertising idempotency it cannot honor** — the boundary binds no idempotency key, so the retry-replay claim is gone from tool descriptions.

- **MCP tool defaults run their default_factory per call** — uuid and timestamp defaults regenerate per call instead of freezing at registration.

- **Generated FastAPI routes render one 422 shape** — request-validation errors use the shared error envelope; raw ctx/input dropped.

- **A range request over a client-side-encrypted object serves the right bytes** — ranges resolved against the stored ciphertext size, so a suffix range (what media and zip readers ask for first) returned a window shifted by the envelope overhead under a plausible-looking content range, and a range past the plaintext end surfaced as a 400 rather than a 416. Ranges now resolve against the plaintext total.

- **`forze dst replay` survives a bad corpus target** — one unloadable target counts as a failure while the rest of the corpus replays.

**PostgreSQL**

- **Query parameters no longer leak across reads in a caller transaction** — each param-bound read now restores its session settings after the fetch, and analytics and procedure timeout and search-path settings do the same. Previously a procedure's per-tenant search path and statement timeout applied to every statement the caller ran afterwards. A failed restore never masks the original query error.

- **An OCC retry inside a caller transaction can no longer die on the aborted transaction** — on Mongo a write conflict aborts the whole server transaction, so the in-place retry failed with NoSuchTransaction; on Postgres a serialization failure or deadlock did the same, surfacing as masked infrastructure. Both now surface the original clean concurrency error for a whole-scope re-run; Postgres still heals a healthy zero-rows rev conflict in place.

- **`find_many` warns when its implicit 10,000-row cap truncates** — pass an explicit limit or paginate to read past it.

- **`update_matching` bounds its primary-key snapshot** — capped at one million rows by default and fail-closed beyond it; opt back into unbounded with None.

**Adapters & security**

- **Tenant-aware Mongo writes and Firestore queries no longer crash at the driver** — both gateways passed a raw UUID the drivers reject; the tenant is now stamped and filtered as the canonical string everywhere, with tenant-isolation integration tests on both backends.

- **Firestore updates no longer strip the tenant tag** — every gateway write re-stamps the tenant (patch previously full-set the domain image, hiding the row from all tenant-filtered reads); history snapshots are tenant-stamped and a cross-tenant get reads as not-found.

- **Temporal tenant scoping covers listing and handle ops** — signal, cancel, terminate, describe and every schedule op resolve ids through the create-time tenant prefix and refuse a foreign tenant's id; schedule listing filters to the tenant's prefix. Non-tenant-aware wiring is unchanged.

- **Object storage tenant isolation covers reads, not just writes** — every key-taking read, delete, copy and presign path now requires the key to lie within the active tenant's prefix.

- **A missing S3/GCS object classifies as not_found, not retryable infrastructure** — a caller miss is no longer retried or counted against the breaker, and download routes 404 on real backends as on the mock. Bucket-level 404s stay infrastructure, and the re-encryption sweep confirms the container still lists before counting a skip.

- **Object-storage reads no longer create the bucket** *(behavior change)* — listing was the only read path that created on demand, so listing a deleted bucket re-created it and answered "empty", making an absent container indistinguishable from an empty one and letting the re-encryption sweep report a "complete" rotation it never performed. Reads now raise on a missing bucket; only the write paths create on demand.

- **Meilisearch write path** — a failed task raises instead of reporting success, task waits are bounded, tenant-tagged writes and deletes are scoped, and windows crossing maxTotalHits fail closed with the index provisioned to match.

- **Neo4j keyed-edge identity & quantifier coercion** — a keyed-edge ensure now matches on the edge key so distinct keyed edges stay separate, hop quantifiers are integer-coerced before inlining, and walk and path parameters validate their bounds at construction. A filter key must be a plain identifier, since a crafted key could otherwise reach query text through the parameter name; the mock enforces the identical rule from a shared helper.

- **Neo4j transactions honour the per-tenant database** — an enlisted transaction bound to the client's static default and ignored the per-statement database, so under the namespace tier a tenant's transactional graph writes landed in the shared database and reads cross-contaminated. The transaction now binds to the tenant-resolved database of its first statement, and a second, different database inside one transaction is refused rather than silently redirected.

- **Credentials in a logged JSON or dict-repr body are masked** — the log scrubber matched only unquoted key/value forms, so in a serialized body every secret term but one went out verbatim wherever a request, response or webhook body was logged. Quoted keys are now derived from the same term vocabulary as the plain form, and the mask stops at the value so neighbouring fields survive.

- **Logger-name constants are importable as documented** — `FORZE_POSTGRES_LOGGER_NAMES` / `FORZE_REDIS_LOGGER_NAMES` (and their logger enums) are now re-exported from the package roots for `bootstrap_logging` wiring, matching `forze_http`.

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

- **Console renderer no longer shares mutable defaults across instances** — the renderer's class-level dict and list defaults were shared by every instance, so per-instance customization leaked between renderers; they are now per-instance factories.

- **Misc** — BigQuery empty-array params typed from annotations; timezone offsets validated; S3 multipart-ETag normalization; If-None-Match parsed per RFC 7232; outbound HTTP suppresses its default bearer when an Authorization header is set; GCS rejects reserved metadata keys.

## [0.4.1] - 2026-06-17

### Added

- **Mergeable quantile sketch** — a relative-error sketch answering any quantile and mergeable across streams, for fleet-wide multi-quantile latency, in windowed and unwindowed forms.

- **Hybrid logical clock** — a skew-tolerant causal clock over the ambient time source, with an optional drift guard.

- **Causal outbox ordering** — opt-in on the Postgres and Mongo outbox configs: events are stamped with a hybrid logical clock and claimed in causal order across replicas. Off by default; Postgres needs a new column, and legacy rows fall back to their creation time.

- **Fleet-wide adaptive-bulkhead congestion signal** — the AIMD latency signal flows through a pluggable digest store, defaulting to the existing in-process one, and Redis-backed storage lets the limit react to the fleet's p95 rather than one replica's. Opt-in.

- **Prioritized load shedding** — opt-in, making the bulkhead wait queue criticality-aware through a task-scoped criticality binding. A no-op until enabled, and requires a bounded queue.

- **Delay-based bulkhead** — a third bulkhead kind tuning concurrency from the latency gradient, needing no latency threshold. Mutually exclusive with the other kinds.

### Changed

- **Quantile estimators relocated** — the P² estimators moved from the resilience module to the shared primitives, alongside the new sketch, and are now public. The old module path is removed; internal resilience wiring is unaffected.

### Fixed

- **Typing annotations** — type-only imports moved under `TYPE_CHECKING` with forward references (including the runtime-optional OpenTelemetry types), so affected modules import cleanly without those optional dependencies installed and skip needless runtime imports.

## [0.4.0] - 2026-06-17

### Added

- **Envelope-encryption core** — an encrypted-envelope primitive, a bring-your-own-key management seam that keeps the key-encryption key in the backend, a field-encryption policy, and a fail-closed minimum-encryption floor ordered none < field < envelope. Adds `cryptography` to core dependencies. Opt-in, off by default.

- **Per-tenant keyring and wiring** — a directory port resolves a tenant to its key, a deps module composes the stack, and the mock ships a dev-only key backend.

- **At-rest sealing across persistence and transport planes** — each plane takes an encryption policy on its spec, fail-closed at wiring and tolerant of legacy plaintext.

- **Object-storage encryption** — opt-in per storage route on S3 and GCS; presigned URLs are refused on an encrypting route.

- **Document-field encryption** — declared per spec. Binding the record id into the authenticated data refuses bulk updates of a bound field, and a re-encrypt pass upgrades legacy ciphertext.

- **Searchable deterministic-field encryption** — AES-SIV, needing no KMS round-trip, so equality and membership filters rewrite to ciphertext; root rotation matches the previous root and re-encrypts. Trade-off: it leaks equality and frequency within a tenant.

- **Encrypted search reads** — the same policy object as the document spec, decrypting out of results across every read path.

- **Analytics and graph encryption** — sealed on write and decrypted out of every read and traversal. Encrypted columns are neither analyzable nor matchable; analytics rejects record-id binding, and graph binds the kind's key field.

- **Outbox and direct-messaging encryption** — at-rest or end-to-end for the outbox, end-to-end for queue, stream and pubsub specs, with tenant and event id bound into the authenticated data.

- **Durable-payload encryption** — opt-in on Temporal and Inngest with per-tenant keys. A Temporal worker must be built from the same encrypting client to decode.

- **Cache, search-snapshot, and idempotency encryption** — sealed when the underlying route encrypts. The in-process L1 cache stays plaintext in memory.

- **Vault Transit KMS** (`forze_vault`) — key management backed by Vault Transit, plus an idempotent provisioner that creates a tenant's Transit key.

- **BYOK access-token signing and JWKS** — a pluggable signer (HMAC by default, plus local-asymmetric and Vault Transit) and a route publishing JWKS. **Breaking:** the access-token service now takes a signer, issuing and verifying became awaitable, and the algorithm config field is gone.

- **Crypto and signing observability** — always-on instrumentation for both.

- **Declared-minimum tenant isolation, fail-closed at wiring** — every deps module accepts `required_tenant_isolation` over none < tagged < namespace < dedicated, enforced per route, and each integration declares its supported ceiling. Additive, with the `None` default unchanged.

- **Neo4j reaches namespace and dedicated isolation** — the database accepts a per-tenant resolver, and a routed client resolves per-tenant Bolt URI and credentials, failing closed on partial auth.

- **Tenant infrastructure provisioning** — idempotent provision and deprovision behind a port, with reference object-storage and Postgres-schema implementations. Opt-in.

- **Analytics per-tenant namespace routing and advisory binding** — query operations route into the tenant's namespace, and `tenant_aware` routes bind the tenant id and fail closed if unbound. Off by default.

- **Tenant-safe structured graph walk and raw gating** — a scoped walk runs an adapter-owned traversal that is tenant-scoped along the full path, and the raw query hatch is now disabled by default. **Breaking:** deployments using raw graph queries must opt back in.

- **Fluent query builder** — `Q.field("age").gt(18) & Q.field("name").like("a%")` lowers to the same filter AST as the mapping form. Additive.

- **Hierarchy operators** — `$descendant_of` and `$ancestor_of` over a tree-path field, using Postgres `ltree` where available and a text-prefix fallback otherwise, gated by a capability flag.

- **Aggregation operators** — `$count_distinct`, `$stddev`, `$var`, `$percentile`, and post-group `$having` on Postgres and Mongo (`$first`/`$last` deferred).

- **Full and array-of-arrays nested quantifiers** on every document backend; the previous capability gate is dropped. `validate_query_field_types` now runs in the gateway and the mock, rejecting mismatches.

- **Mixed-direction keyset pagination with per-key null ordering** — coherent null ordering across backends; old cursor tokens stay valid, and Mongo opts in via `computed_null_ordering`.

- **Query discovery metadata** — `build_query_discovery` projects a read model's filterable, sortable, and aggregatable surface as an OpenAPI extension plus an MCP line.

- **Tenant selector self-service** — `GET /tenants`, an activate endpoint that re-mints a tenant-scoped token pair, and a leave endpoint, via the new `attach_tenancy_routes`.

- **Tenant admin** (`forze_kits.aggregates.tenancy_admin`) — create, list members, invite, remove and deactivate. Ships unguarded, so bind authn and authz per op. **Breaking** for tenant-management implementers: two new listing methods.

- **Self-service API-key management** — issue, list and revoke under `/api-keys`, with the secret returned once. **Breaking** for API-key lifecycle implementers; the account table gains hint and label columns.

- **Delegation-aware API keys (user to agent)** — a key can bind a delegation actor, carried as an RFC 8693 `act` claim. **Breaking** for API-key lifecycle implementers; the account table gains an actor-principal column.

- **MCP boundary API-key auth** — an API-key verifier and identity resolver protect a FastMCP server with the identity plane and no OAuth flow, reads-only by default.

- **OpenAPI security from configured authn** — security schemes are derived from the configured authn requirement, and principal-requiring operations are flagged.

- **Authn plane** — an orchestrator with a full mock identity plane and routes for login, refresh, logout, change-password, deactivate and self-service password reset. Principal deactivation ships unguarded.

- **In-process L1 document cache** — sits ahead of the distributed cache, tenant-scoped with pluggable eviction and Redis invalidation push. Off by default.

- **Stampede protection and adaptive freshness** — singleflight on read-through misses, probabilistic early refresh, per-entry age and sliding TTLs, and a keyword `ttl=` on every setter.

- **New resilience strategies** — adaptive bulkhead over AIMD concurrency, adaptive throttle, tail-based hedging, and a token-bucket rate limit, all configured as per-port policies.

- **Invocation deadlines** — per-operation budgets; expiry raises a timeout, surfacing as 504.

- **Distributed rate limits** — a pluggable store, with a fail-open Redis implementation, lets replicas share one rate. Bulkheads and budgets stay process-local.

- **App assembly and deployment** — a runtime builder and ASGI lifespan, graceful drain defaulting to 10s, and fleet and serverless deployment profiles, the latter rejecting long-running operations.

- **Envelope headers and correlation propagation** — messages gain headers and a delivery count, the relay forwards the full envelope, and the inbox rebinds correlation and causation across broker hops.

- **Outbox ordering key** — per-aggregate ordering, mapping to an SQS FIFO message group or a stream partition key. Requires a new column.

- **Kits queue-consumer runner** — a consumer plus background lifecycle step giving inbox exactly-once, requeue, poison parking and envelope rebinding.

- **Stream pending-entry recovery** — claim and pending inspection over consumer groups. **Breaking** for stream port implementers.

- **Presigned object-storage URLs** — download and upload URLs on S3, GCS and the mock. **Breaking** for port implementers, since minting an upload URL is a CQRS write.

- **Object-storage metadata and access ops** — head, ranged and conditional download, copy and move, and object tags; generated routes honour range and conditional request headers. **Breaking** for the storage and client ports.

- **Resumable multipart uploads** — begin, presign-part, complete and abort behind a session port, guarded as a CQRS write and refused on object-encrypting routes.

- **Storage HTTP edge** — kit ops and generated routes for presigned download and upload and the full multipart session. Minting an upload URL is a command op, so bind authn and authz.

- **Server-side encryption at rest (SSE/CMEK)** — configured per storage route. A separate axis from client-side encryption, which it does not satisfy for an encryption floor. Off by default.

- **Catalog and registry ergonomics** — catalog entries carry idempotency-key and required-permission facts, duplicate merge keys raise with an override hatch, and registration is one step.

- **Generated-route mount ergonomics** — every route-attach helper accepts a resource name (mutually exclusive with a namespace) and per-path overrides. Additive.

- **Scoped, materialized patch authoring** — patches can be scoped to a namespace and folded into per-op plans. Merging now raises when a patch from one registry matches another's ops, which is **breaking** only in that case and can be opted out of.

### Changed

- **Queue consumer and outbox relay are now configurable classes** — the old module-level runner functions become classes with a run method; lifecycle steps keep flat parameters. **Breaking** for direct callers of the old functions.

- **Tenant-isolation tier model made coherent** — the ladder is none < tagged < namespace < dedicated, dropping the old relation rung; each integration owns its supported ceiling and namespace resolution is unified. Key and path formats are unchanged.

- **Argon2 hashing off the event loop** — hashing, verification and the timing dummy are now async on a bounded pool; the synchronous variants remain.

- **Performance (measured)** — the engine hot path roughly halved, document updates copy only changed subtrees, the Postgres and Mongo write paths cut round-trips, and lazy error context with opt-in tracing cut overhead.

- **RPC-style FastAPI routes use REST verbs and query params** — e.g. `GET /notes.get?id=`. **Breaking:** RPC clients must switch from posting to the operation path; REST and MCP are unchanged.

- **The singleton lifecycle step takes a lock spec, not a live port** — **breaking** for callers passing a resolved port.

- **Release-coherence sweep** — the relay logs the at-least-once to fire-and-forget downgrade, Temporal query/update/result deserialize into declared types, the API-key prefix is validated, and saga `step_failed` stays a domain error.

### Fixed

- **Tenant-isolation correctness and parity** — Postgres outbox and inbox enforce the declared isolation floor, a missing bound tenant fails closed consistently as an authentication error, and mock durable, graph, and document adapters now tenant-partition their stores.

- **Post-commit work survives task cancellation** — the after-commit drain runs as a cancellation-protected critical section and then re-raises; cancellation during the body still rolls back.

- **PGroonga search honors tenant isolation regardless of plan** — a tenant-aware search now always uses `filter_first`, overriding index-first plans that scanned cross-tenant rows and could truncate results.

## [0.3.0] - 2026-06-11

### Added

- **Generated FastAPI routes** — project a frozen registry's operations onto the app's router in either a REST or an RPC style, which must be chosen explicitly. Idempotency moves to the engine.

- **`forze_mcp`** — expose operations as MCP tools, read-only by default; commands must be opted in.

- **`forze_duckdb`** — in-process, query-only analytics over a Parquet, CSV, Iceberg or Delta lake on S3, GCS or local disk, with no standing warehouse.

- **Delegated identity (on-behalf-of, RFC 8693)** — the authenticated identity carries an acting principal, authorization enforces a least-privilege intersection, and explicit authority is checked through a delegation port.

- **Operation-level CQRS** — an operation declared as a query runs read-only: command ports cannot be acquired and the transaction opens read-only, enforced by the database. Untagged operations remain commands.

- **Operation catalog descriptors** — interface-agnostic request and response schema metadata, joined with the operation kind, for projecting operations onto MCP or HTTP.

- **Queryable-field policy** — per-aggregate filterable, sortable and aggregatable allow-sets, powering schema discovery and boundary enforcement. Direct port calls stay unrestricted.

- **OpenTelemetry traces and metrics** — every operation gets a span plus a counter and duration histogram. Opt-in, additive.

- **Declarative domain invariants** — an always-true rule enforced on both create and update, closing the merge-patch bypass that model validators allowed.

- **Saga / process orchestration** — declarative multi-step processes across aggregates with typed steps and reverse compensation before the pivot, run by an in-process executor outside any enclosing transaction.

- **DDD domain events and aggregate roots to outbox** — aggregates buffer domain events, and persisting one drains and dispatches them inside the operation's transaction.

- **End-to-end worked example** (`examples/recipes/order_fulfillment/`) — the first runnable, test-backed example: checkout saga to outbox to relay to inbox to downstream, plus compensation, on `forze_mock`.

- **Deterministic time and ids** — the clock and id generator read a context-active time source, so binding a frozen one makes every read deterministic with no call-site changes.

- **Resilience policy pipeline** — composable strategies validated into one policy and run through the context. Adds hedging and a distributed circuit breaker that fails open.

- **Inbox / consumer-side dedup** — a mark-if-unseen port, with a helper that marks and runs the handler in one transaction for an exactly-once effect. Adds a Postgres store plus mock.

- **Graph contracts plus `forze_neo4j`** — query, command and raw graph ports, a Neo4j async Bolt adapter covering CRUD, neighbours, expand, shortest path and a raw Cypher hatch, and an in-memory mock.

- **`forze_kits` — consolidated kit package** — kits, aggregates, mapping, DTOs, outbox/notify, secrets and scopes, absorbing the former patterns and secrets packages and several application modules.

- **`forze_http`** — outbound HTTP services declared by spec and resolved by name from the context, backed by httpx, with per-tenant routed clients.

- **`forze_meilisearch`** — async Meilisearch with offset search, index maintenance, and federated search using native or weighted reciprocal-rank fusion.

- **Transactional outbox, notify, and search-command** — outbox contracts with Postgres, Mongo and mock stores plus relay helpers, a notify kit, and a core search-command port for external-index maintenance.

- **Tenant routing** — declarative per-request backend targets across all integrations, with per-tenant client variants, routed lifecycle steps, LRU pool dedup, and a client registry.

- **Identity — IdP presets** — OIDC presets for Google, VK ID and Telegram Login, bootstrap wiring and PKCE helpers. Authn adds API-key rotation and single-use password invites.

- **Execution — freeze/resolve pipeline** — an authoring registry freezes, then resolves per scope, separating registration from resolution, with a matching lifecycle plan. Per-scope caches default on.

- **Codecs** — a default model codec, per-spec document codecs, optional read and ingest codecs, and trusted-row read validation.

- **Postgres / Mongo search** — Postgres strict and trusted read validation, PGroonga plan modes and hub parallel legs; Mongo text, Atlas and vector search over both offset and cursor paging.

- **Document adapters** — page-count ceilings on scan, stream and chunked commands (default 100 000, unlimited on request) with cursor-stall detection.

- **Durable workflow** — run status and description types, plus describing a run through the durable workflow query port.

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

- **Shared base primitives** — bounded LRU registries, single-flight and cache lanes, a once-initialized cell, frozen mappings, and fingerprint helpers, all now public.

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

[unreleased]: https://github.com/morzecrew/forze/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/morzecrew/forze/compare/v0.4.1...v0.5.0
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
