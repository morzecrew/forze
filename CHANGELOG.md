# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`forze_duckdb` (`forze[duckdb]`) — in-process DuckDB analytics over object storage (query-only).** A new integration implementing `AnalyticsQueryPort` with an embedded DuckDB engine, for "hard analytics" that read a data lake (Parquet/CSV and, via extensions, Iceberg/Delta) on S3/GCS/local files without a standing warehouse — the cheap compute-without-a-server complement to the ClickHouse/BigQuery adapters. Handlers stay infra-agnostic (they name only a `query_key` + output type per the hexagonal boundary); the lake source binds **below the line** in `DuckDbAnalyticsConfig` (per-query SQL) and the `duckdb_lifecycle_step` (extensions, `CREATE SECRET` credentials, and startup views), so both inline `read_parquet('s3://…')` and registered-view source styles are supported. The synchronous, embedded engine is bridged to asyncio safely: queries run on a **dedicated bounded executor** (DuckDB releases the GIL during execution, so the event loop stays responsive and analytics load can't starve other `to_thread` users), each query uses its **own cursor** so concurrent reads don't serialize, and the `AnalyticsRunOptions` timeout is enforced via native **cursor interrupt** (`asyncio.to_thread` isn't cancellable). Results are fetched as **Arrow** internally and materialized to rows lazily at the shaping edge. Query-only by design: no ingest/table-management contract (lakehouse maintenance is ops, not a domain port). Wire it with `DuckDbDepsModule` + `duckdb_lifecycle_step`. Being in-process and Docker-free, it also serves as a real-engine test double for the analytics port.
  - **Typed lake / lakehouse sources + credentials (declarative, credential-safe).** Beyond raw scan strings, the lifecycle step now accepts a sealed `DuckDbSource` union — `ParquetSource` / `CsvSource` / `JsonSource` (lake) and `IcebergSource` (metadata-path scan) / `DeltaSource` (lakehouse) — that compiles to the right scan expression and reports the extensions it needs, so `extensions=` is **auto-derived** (an `s3://` source pulls `httpfs`, an `IcebergSource` pulls `iceberg`, etc.) instead of being hand-maintained. `sources=` takes `Mapping[str, str | DuckDbSource]`, so a raw scan string still works as an escape hatch. Object-storage credentials become typed `ObjectStoreCredentials` (`S3Credentials` — incl. S3-compatible MinIO/R2 via `endpoint` — and `GcsCredentials`) compiled to `CREATE SECRET`, **adapter-preferred**: pass a `SecretRef` and the payload is resolved at startup through the wired `SecretsPort` (Vault/env/directory/…), or an `inline` payload for tests/local runs (exactly one of the two; secret values are `SecretStr`, never logged). `object_stores=` on `duckdb_lifecycle_step` carries them; the raw `secrets=` list remains as an escape hatch. Catalog-managed Iceberg (REST/Glue `ATTACH`), warehouse `ATTACH` (Postgres/MySQL) + MotherDuck are intentionally deferred — every future source is just another variant of the same union.
- **Delegated identity (on-behalf-of) across the identity plane.** `AuthnIdentity` gains an optional, chainable `actor: AuthnIdentity | None`: `principal_id` is the effective **subject** (e.g. the user) and `actor` is the principal *performing* the action (e.g. an AI agent acting for the user) — the RFC 8693 `act` model. `AuthnIdentity.is_delegated` reflects whether an actor is attached. `AuthzSubject` mirrors it (`actor: AuthzSubject | None`) and `subject_from_authn` carries the chain through. The standard authz before-hook (`AuthzBeforeAuthorize`) now enforces **least-privilege intersection**: a delegated call is allowed only if *both* the subject and the actor are independently permitted the action — so an agent can never exceed `intersect(subject grants, actor grants)` (the confused-deputy defense), raising `delegate_denied` otherwise. The bound actor id is added to the invocation context's logging fields (`actor_id`) for audit/trace. All fields are optional and default to `None`, so direct (non-delegated) calls and existing `AuthzDecisionPort` implementations are unchanged. `forze_mcp` ships a `DelegatedIdentityResolver` that resolves the subject (user) from the MCP session and attaches the server's service principal as the actor, so MCP tool calls run as governed on-behalf-of operations.
  - **Token-derived delegation (`act` claim).** `AuthnOrchestrator` gains an optional `actor_claim` (e.g. `"act"`, wired via `AuthnDepsModule(actor_claim=...)`): on token authentication it reads that claim as the RFC 8693 delegation assertion, resolves the actor through the *same* `PrincipalResolverPort`, runs the principal-eligibility gate on it, and attaches it as `AuthnIdentity.actor` — recursing on a nested `act` to build multi-hop chains. `None` (default) ignores the claim. Only the token path honours it (password/API-key assertions carry no actor); a malformed actor claim (no string `sub`) raises `invalid_actor_claim`.
  - **`may_act` delegation grant (explicit pairwise authority).** New `DelegationPort.may_act(actor_id, subject_id, *, scope)` and `DelegationGrantPort` (`grant_delegation`/`revoke_delegation`/`list_delegators`) with `DelegationDepKey`/`DelegationGrantDepKey` and `AuthzDeps.delegation()`/`delegation_grant()` accessors. `AuthzSpec.enforce_delegation_grant` (default `False`) opts a route into requiring an explicit grant pairing actor→subject *on top of* the least-privilege intersection; the before-hook now **walks the full actor chain** (each actor independently permitted — `delegate_denied`; and, when enforced, holding a `may_act` grant — `delegation_not_granted`). Enforcement is fail-loud: enabling the flag without a wired `DelegationPort` errors at hook build rather than silently skipping the check. Document-backed adapters ship in `forze_identity` (`DelegationGrant` junction document + `DelegationQueryAdapter`/`DelegationGrantAdapter`, registered via `AuthzDepsModule(delegation=..., delegation_grant=...)`), with in-memory `MockDelegationPort`/`MockDelegationGrantPort` (deny-unless-granted) in `forze_mock`.
- **`forze_mcp` (`forze[mcp]`) — a toolkit for exposing operations to AI frameworks as MCP tools (read-only MVP):** a driving-adapter *integration kit* (in the spirit of `forze_fastapi`) that plugs Forze operations into an MCP server you own, rather than owning the server itself. `register_tools(server, registry, ctx_factory, *, identity=None, include_writes=False)` adds the registry's operations as tools onto your own `FastMCP` instance (bring your own auth, transport, and hand-written tools) — `add_tool` is additive, so Forze tools coexist with yours. Each tool projects `FrozenOperationRegistry.catalog()`: a **flat** argument signature is synthesized from the operation's input-DTO fields (top-level args, the natural MCP-client contract — not a nested object), `OperationKind` drives `readOnlyHint`/`destructiveHint`, and a call validates arguments into the input DTO, establishes a boundary context (`InvocationContext.bind` with per-call metadata + a resolved identity), runs the operation via `run_operation`, and lets FastMCP serialize the (Pydantic) result as structured content. The adapter holds no business logic and enforces no authorization — governance stays in the engine, upstream of the boundary. `register_dsl_query_prompts(server, *, prefix="forze")` additionally attaches framework-level (aggregate-agnostic) MCP **prompts** — `{prefix}.querying` (filter/sort/pagination grammar) and `{prefix}.aggregates` (group-by/metrics grammar), each with an optional `goal` argument — so an LLM can pull the querying DSL reference on demand and drive the `list`/`search` tools correctly. `register_schema_resources(server, *specs, prefix="schema")` publishes one MCP **resource** per `DocumentSpec` (`{prefix}://{name}`) carrying the read model's JSON schema plus the resolved filterable / sortable field sets and default sort — the per-aggregate grounding that tells the model *which* fields it may query and how, complementing the DSL prompts (resources are client-pulled context, the natural primitive for static grounding data). `register_resource_templates(server, registry, ctx_factory, [ResourceTemplateSpec(op=…, scheme="notes")], *, identity=None)` exposes get-by-id operations as MCP **resource templates** (`notes://{id}`): reading a concrete URI dispatches the read-only operation through the same governed pipeline as a tool call (identity binding, read-only enforcement, audit), returning the read model as JSON. Registration fails loud if the operation is missing, not read-only, lacks a descriptor with an input type, or `id_param` is not a field of it. (`list`/`search` stay tools — their filter/sort/pagination arguments don't map onto a URI.) `LoggingMiddleware` (a FastMCP middleware, the MCP analogue of `forze_fastapi`'s request logger) emits a structured access line per MCP message via Forze's structlog-backed `Logger` — method, target (tool/resource/prompt), direction, duration, and outcome (`ok` / `error`, with classified `CoreException`s logged at warning and unexpected errors via `critical_exception`); add it with `server.add_middleware(LoggingMiddleware())` and pair with `configure_logging` / `attach_foreign_loggers(["fastmcp", "uvicorn", "uvicorn.error", "uvicorn.access"])` so the server's own, FastMCP's, and uvicorn's logs share one format instead of the default plaintext (when serving over HTTP also pass `uvicorn_config={"log_config": None}` to `server.run(...)` so uvicorn doesn't re-apply its own logging config and overwrite the attached handlers — the example shows the full setup). `build_mcp_server(...)` is an optional batteries-included convenience that constructs a `FastMCP` and registers everything. Built on standalone **FastMCP 3.x** (`fastmcp`) — chosen for its additive tool registration plus its modern auth providers, middleware, and streamable-HTTP transport, which the *host* server owns (the adapter is unaware of them). **Default scope is read-only:** only `QUERY` operations are exposed unless `include_writes=True`, which also exposes command operations tagged `readOnlyHint=False`/`destructiveHint=True` so MCP clients can gate them; identity is a pluggable `MCPIdentityResolver` (`StaticIdentityResolver` for a fixed/no identity, `DelegatedIdentityResolver` for on-behalf-of calls — see the delegated-identity entry). **Deferred (next phases):** token-derived identity extraction (mapping a verified MCP-session token through `PrincipalResolverPort`/`TenantResolverPort` behind `resolve_subject`), `outputSchema` projection, and optional capability modules for the rarer primitives (sampling — the inbound mirror of the outbound `CompletionPort`; elicitations). Tests (incl. read-only and write round-trips through an in-memory client↔server and an additive-registration check) require the `mcp` extra and skip without it. A runnable, test-backed example (`examples/mcp_server.py`) serves a mock "Notes" aggregate over Streamable HTTP for poking with the MCP Inspector (`npx -y @modelcontextprotocol/inspector`).
- **Queryable-field policy on `DocumentSpec` (`QueryFieldPolicy`).** A new optional `query_policy: QueryFieldPolicy | None` declares per-aggregate allow-sets — `filterable`, `sortable`, and `aggregatable` (each `frozenset[str] | None`, `None` = all read-model fields, the default unrestricted behavior) — validated against the read model at spec construction (an unknown field name fails fast). `DocumentSpec.filterable_fields()` / `sortable_fields()` / `aggregatable_fields()` resolve the effective sets. It is a *static per-aggregate contract*, distinct from (and composable with) authorization row-scoping (a dynamic per-subject policy). Two halves ship together: **discovery** — it powers `forze_mcp`'s `register_schema_resources` so an LLM learns which fields it may filter/sort by; and **enforcement** — the kit `list`/`search`/aggregate handlers now carry a `QueryFieldGuard` that rejects caller filters/sorts/aggregate fields referencing a non-allowed field (`field_not_filterable` / `field_not_sortable` / `field_not_aggregatable`, raised at the governed-operation boundary). The aggregate path is fully covered: group-by dimensions (plain and `$trunc`) and computed-metric source fields are checked against `aggregatable`, while each computed metric's per-metric `filter` sub-expression is checked against `filterable`. Enforcement is **boundary-only**: it runs in the governed operations, *not* the query port — so internal code calling `ctx.doc.query(spec).find_*` / `aggregate_page` directly stays unrestricted (the operation-vs-port trust gradient is the "user request" signal; no origin flag needed). New core helpers (in `forze.application.contracts.querying`) — `collect_filter_field_roots` / `collect_aggregate_field_roots` / `collect_aggregate_filter_expressions`, `validate_filterable_fields` / `validate_sortable_fields` / `validate_aggregatable_fields`, and `QueryFieldGuard` — reuse the filter/aggregate parsers (which also structurally validate the expression at the boundary) and reduce dotted paths to their top-level field. Behavior-preserving: specs without a `query_policy` build no guard and are unchanged.
- **Operation catalog descriptors (`OperationDescriptor` + `FrozenOperationRegistry.catalog()`):** the operation registry can now carry interface-agnostic catalog metadata, the foundation for projecting operations onto external surfaces (MCP tools, an auto-generated HTTP router, …) without re-deriving schemas. An `OperationDescriptor` (`forze.application.execution`) records the request/response DTO types and a human description — the facts that survive neither the handler *factory* (which erases `Handler[Args, R]` type arguments) nor the docstring — and exposes `input_schema()` / `output_schema()` via Pydantic. Descriptors are attached with `registry.set_descriptor(op, …)` / `set_descriptors(...)` (namespace-aware, override-guarded, merged and frozen alongside handlers/plans), and `FrozenOperationRegistry.catalog()` returns one `OperationCatalogEntry` per operation that **joins** the descriptor with the plan's `OperationKind` (so `entry.is_read_only` reflects the existing read/write classification). Read/write stays on the plan (execution-semantic) and *exposure* stays an interface decision — a descriptor means an operation is *describable*, not that any surface exposes it. All `forze_kits` aggregate builders are populated: read operations are marked `as_query()` (so they run read-only **and** surface as read-only in the catalog) and every operation carries a descriptor with its request/response schema — document (`get`/`list`/`raw_list`/`list_cursor`/`raw_list_cursor`/`agg_list` read), search (all four typed/raw × offset/cursor ops read; hub identical; federated typed/cursor read, heterogeneous so request-schema only), storage (`list`/`download` read, `upload`/`delete` write), stored_file (`get`/`list`/`download`/`search` read, `upload`/`delete` write), soft_deletion (`delete`/`restore`, both write), and authn (`password_login`/`refresh_tokens`/`logout`/`change_password`, all write). Additive and behavior-preserving — operations without a descriptor still appear in the catalog with `descriptor=None`. **Deferred:** required-authz introspection in the catalog (left to the consuming interface, which can read the plan's authz step).
- **OpenTelemetry traces + metrics export (`instrument_operations`):** Forze was observability-rich internally but externally closed. `instrument_operations(registry)` (from `forze.application.execution`, call before `.freeze()`) wraps **every** operation in an OTel span named by its key (attributes: kind, execution/correlation/causation ids, tenant, principal; failures set `ERROR` + record the exception and re-raise) plus two metrics labelled by operation/kind/outcome — `forze.operations` (counter) and `forze.operation.duration` (histogram, ms). It uses the existing per-op `wrap` middleware seam (no engine changes — only a small `OperationRegistry.operation_keys()` accessor was added) and emits via the **global** OTel providers, so the app owns the exporter choice. OpenTelemetry is already a **core dependency** (the logging layer uses it), so this is built in — no extra to install. Pairs with `configure_logging(otel_config=…)` for free log↔trace correlation. Opt-in and additive — uninstrumented apps are unchanged. **Deferred:** transaction-scope child spans (needs a tx-tracer seam) and per-port-op spans (high cardinality).
- **Operation-level CQRS — `QUERY`/`COMMAND` kind + forbid-writes-in-a-query by construction:** operations were CQRS at the *port* level but untyped at the *operation* level. An operation now carries an `OperationKind` (`COMMAND`, the default, or `QUERY`), tagged with `registry.bind(op).as_query()` / `.as_command()`. A `QUERY` operation runs under a read-only flag for its duration (a `ContextVar` on `InvocationContext`, bound by the engine in `ResolvedOperation.__call__`), and **a command (write) port cannot be acquired — by construction**: the six `*.command(...)` accessors (document, outbox, search, graph, dlock, storage) route through a single `ConvenientDeps._resolve_command` guard that raises `precondition` in a read-only op; read/`query` accessors are unaffected. This makes the read/write split structural (the `reads_before_writes_in_tx` *intent* existed only as post-hoc trace validation) and subsumes the CQRS-consistency audit. Untagged operations default to `COMMAND`, so existing code is unchanged (behavior-preserving). **Now also enforced at the database (Phase 2):** a `QUERY` operation opens its transaction **read-only** — `TransactionManagerPort.transaction(read_only=…)`, threaded from `plan.kind` through the engine into `ctx.tx_ctx.scope(route, read_only=…)`, honored by Postgres as `BEGIN ... READ ONLY` (asyncpg `set_read_only`) so the DB rejects writes, **including via the raw-query escape hatch** that the port guard can't see. The flag is keyword-default-`False`, so the many direct `scope()` callers (saga, inbox, example) are unchanged; Mongo/Firestore accept it (no native read-only tx), the mock records it for tests. Explicit replica routing (read-only-enforced on the replica) works via `as_query().bind_tx().set_route("pg_replica")`. **The guard is uniform across adapters (Phase 3):** beyond the original 6 `*.command` accessors, the read-only guard now also covers every other first-class state-write accessor — `analytics.ingest`, `authz.principal_registry`/`role_assignment`, and the `authn.*_lifecycle` ports (token/password/api-key/provisioning/deactivation) — so a query op can't mutate domain *or* auth state through any backend. The line is **pragmatic**: writes that legitimately happen during a read stay allowed (`cache` read-through, `counter` metrics, `search.snapshot` result-caching), and framework-internal marks (`inbox`, `idempotency`, which only run in command ops) are unguarded. The full read/write classification of every `ctx.*` accessor is now documented. **Still deferred:** read-replica *auto*-routing from kind, and per-kind default plans.
- **`@invariant` — declarative domain invariants (closes a footgun):** an `@invariant` is an always-true `(self) -> None` rule on an aggregate's state, declared once and enforced on **both** create and update. It closes a real footgun: Forze's merge-patch update applies changes via `model_copy`, which **bypasses Pydantic `@model_validator`s** — so a `@model_validator` used as an invariant runs on create but silently *not* on update, letting an update drive the aggregate into an invalid state. `@invariant` runs on construction (a model validator) and again on the post-update state (`Document.update`), so the rule always holds. Collection mirrors `@update_validator` (gathered across the MRO in `__init_subclass__`); it's a no-op for models that declare none (behavior-preserving). Positioning: `@invariant` for state rules, `@update_validator(before, after, diff)` for transition rules, raw `@model_validator` only as an escape hatch (now documented as not running on updates). Importable from `forze.domain.models` / `forze.domain.validation`.
- **End-to-end worked example (`examples/order_fulfillment.py`):** the first runnable, **test-backed** example, proving the DDD + orchestration pieces compose in-process (`forze_mock`, no Docker). A checkout saga (`reserve` → `confirm` pivot) confirms an `Order` aggregate whose `@event_emitter` dispatches `OrderConfirmed` **inside the saga step's transaction**; the outbox bridge stages it, a relay (standing in for a broker + relay worker) claims it, and the consumer processes it **exactly-once via the inbox** to create a `Shipment` — plus the compensation path (pivot failure releases reserved inventory and stages nothing). Executed by `tests/unit/test_examples/test_order_fulfillment.py` (happy path / idempotent redelivery / compensation), establishing a "tested examples" convention.
- **Saga / process orchestration (`SagaDefinition` + in-process executor):** a lightweight, declarative saga contract for multi-step processes across aggregates where the steps can't share one transaction. A `SagaDefinition` is a tuple of typed `SagaStep`s (each `action` + optional `compensation`, threading a working context), with an optional per-step `tx_route` (commit the step in its own transaction) and `retry_policy` (a named resilience policy retried before compensating). Steps carry a `SagaStepKind` (`COMPENSATABLE` / `PIVOT` / `RETRYABLE`, ordered `compensatable* pivot? retryable*` and validated) so the saga models a **point of no return**: a failure *before* the pivot compensates the completed steps in reverse (each in its own transaction, retried under an optional `compensation_policy`) and raises `saga.step_failed` (`DOMAIN`, consistent) or `saga.compensation_failed` (`INFRASTRUCTURE`, carrying the originals) if a compensation itself fails; a failure *after* the pivot does **not** compensate (the saga is committed) and raises `saga.forward_incomplete` (`INFRASTRUCTURE`) — it must be completed forward, not rolled back. `run_saga(ctx, definition, initial)` runs the steps via the resolved `SagaExecutorPort` (the in-process `InProcessSagaExecutor` by default; register a custom/durable one via `SagaDepsModule`). Each step commits independently — so the saga **must run outside an enclosing transaction** (enforced by a `ctx.tx_ctx.depth()` guard). Composes with the rest: a step that persists an aggregate dispatches its `@event_emitter` events in-transaction, and per-step retry reuses the resilience executor (fresh transaction per attempt). Emits `domain="saga"` tracer events. **Durability — one brain, two drivers:** the pivot/compensation decision logic is a backend-agnostic `SagaProgress` coordinator (pure, ctx-free; tracks completed steps + the pivot, decides compensate-vs-forward, builds the `saga.*` errors) that every driver shares, so the semantics never fork. `InProcessSagaExecutor` drives it (synchronous, not crash-resumable). `forze_temporal.TemporalSaga` drives the *same* coordinator from inside a Temporal workflow with steps as activities, so **Temporal owns durability** — persistence, resume, retries, and timeouts (per-activity `RetryPolicy`/timeouts + the workflow history) — and Forze contributes only the saga semantics. The shared asset is the coordinator, not the definition (a Temporal saga's steps are registered, serializable activities), so the Temporal path is a workflow helper started via `DurableWorkflowCommandPort`, not a `SagaExecutorPort`. An in-core durable saga store was deliberately not built — it would reinvent Temporal. The `SagaProgress` coordinator is **incremental** (`register(name, kind)`, sharing one `validate_saga_order`), so `TemporalSaga` declares each step's name + kind **at the call site** (`await saga.step("charge", run, kind=PIVOT)`) instead of parallel `kinds`/`step_names` lists + manual indices — the desync footgun is gone and the helper reads like the in-process definition. `SagaStep.idempotent` (validated) makes a retried/`RETRYABLE` step affirm re-run safety; the executor emits a `saga_completed` event. `forward_incomplete` resume and in-flight visibility remain durability features (use the Temporal driver); parallel branches / saga-level deadline are deferred.
- **Domain events flow on persistence + functional-decider `AggregateRepository`:** the `@event_emitter` / `AggregateRoot` / dispatcher machinery is now wired into the document command flow. Persisting an aggregate (`ctx.document.command(spec).create/update/...`) drains its `collect_events()` and dispatches them **in the operation's transaction** to the registered handlers (→ the outbox bridge) — so emitter reactions reach their handlers atomically with the write, with no manual `dispatch`/outbox-staging in handlers. The dispatch is guarded (`isinstance(domain, AggregateRoot)` with pending events), so plain documents are untouched and behavior is unchanged; an aggregate that emits without a `DomainEventsDepsModule` registered raises rather than dropping events. Wired via a `dispatcher_provider` injected into the document adapter by each writable integration factory (postgres / mongo / firestore / mock) — the adapter stays `ctx`-free. For behavior-rich aggregates, `forze_kits.aggregates.AggregateRepository` (`load` / `add` / `apply`) supports the functional-decider pattern: load the domain aggregate, call a pure decision method that returns a merge-patch (raising on invalid), and persist it under the aggregate's revision (OCC) — decisions live on the aggregate while persistence stays command-shaped. This is the chosen "make aggregates own decisions + events" direction; a classic snapshot Repository / Unit-of-Work was deliberately rejected as a regression against Forze's command-shaped CQRS grain. Invariants reuse the existing pre-persist mechanisms (Pydantic `@model_validator` for create, `@update_validator` for update).
- **Deterministic time & ids (ambient `TimeSource` seam):** time and id reads are now controllable for deterministic tests and durable replay. `forze.base.primitives.utcnow()` and the no-argument `uuid7()` read a context-active `TimeSource` (the system clock by default), so a `bind_time_source(FrozenTimeSource(...))` scope makes every read deterministic — including domain self-stamping (`DomainEvent.occurred_at`/`event_id`, `Document.id`/`created_at`) — with **no call-site changes** and without leaking `ctx` into the domain. Time is treated as an ambient, context-scoped source (not a routed dependency), so domain and application code read it the same way (`utcnow()` / `uuid7()`) — one source of "now". This also closes a durable-replay determinism hole: the Temporal worker binds a deterministic source (`workflow.now()`/`workflow.uuid4()`) for the workflow scope so workflow time/id reads reproduce across replays (inside a workflow `uuid7()` yields a runtime-deterministic id — determinism over time-ordering). Behavior is unchanged when no source is bound.
- **Hedging (`HedgeWrap` + `HedgeStrategy`):** concurrent-redundant-attempt resilience for tail latency — fire the request, and if it hasn't answered by `HedgeStrategy.delay`, fire another copy, race them, take the first success, cancel the losers (`ResilienceExecutorPort.run_hedged`). A `HedgeStrategy` (`delay`, `max_attempts`, optional `RetryBudget` cap reused as the hedge budget) is declared on a named `ResiliencePolicy` (`hedge=`); `forze.application.hooks.resilience.HedgeWrap` attaches it per operation at priority 15 — outer to `ResilienceWrap` (each attempt re-runs the resilience pipeline) and inner to `IdempotencyWrap` (a replayed result skips hedging). **Safety gate:** hedging sends concurrent duplicates, so the operation-registry freeze **hard-refuses** a hedged op unless it carries an `IdempotencyWrap` (auto-detected) or the `HedgeWrap` declares an explicit `HedgeSafety` (`READ_ONLY` / `IDEMPOTENT`) — no blanket override. The gate stays layering-clean via structural marker protocols (`ProvidesIdempotency` / `DeclaresHedge` in `contracts.execution`), so the freeze validator detects the hooks without importing them. The `forze_mock` passthrough executor runs a single attempt.
- **Distributed circuit-breaker (`CircuitBreakerStore` seam + Redis adapter):** the resilience breaker is no longer hard-wired to process-local state. `InProcessResilienceExecutor` now delegates breaker admit/record to a pluggable `CircuitBreakerStore` (`forze.application.execution.resilience`) — `InMemoryCircuitBreakerStore` (the default, identical to prior behavior) or a shared store so a multi-replica fleet trips and recovers together instead of each pod probing a dead downstream independently. **`RedisCircuitBreakerStore`** (`forze_redis`, built via `redis_circuit_breaker_store(client)` and wired through `ResilienceDepsModule(breaker_store=...)`) keeps counters/phase in a Redis hash per `(policy, route)`, mutated atomically by Lua using the **server clock** (no replica skew). It is **two-tier** (a short local cache fast-paths the closed admit so a healthy breaker pays no per-call Redis read; every outcome is recorded to share counts), **fails open** to a process-local fallback on any Redis error (emitting a `breaker_store_degraded` trace event — the breaker never becomes a per-call SPOF), and is keyed globally per `(policy, route)` (not per tenant). Bulkhead/retry-budget remain process-local. Behavior-preserving refactor: the existing in-process resilience semantics are unchanged.
- **Inbox / consumer-side dedup (`forze.application.contracts.inbox`):** the symmetric half of the transactional outbox — exactly-once *effect* for at-least-once message delivery. `InboxPort.mark_if_unseen(inbox, message_id)` is a single atomic primitive (True = newly recorded / process, False = already seen / skip). The `forze_kits.integrations.inbox.process_with_inbox` consumer helper opens a transaction on `tx_route`, marks the message, and runs the handler in the **same transaction** — so the dedup mark and the handler's writes commit atomically (or roll back together); the dedup id defaults to `message.key or message.id` (outbox relay sets `key` to the integration `event_id`), with a caller extractor override. Backed by `PostgresInboxStore` (`INSERT ... ON CONFLICT DO NOTHING` on the tx-bound connection; app-provided table) wired via `PostgresDepsModule(inboxes=...)`, plus a mock adapter (`ctx.inbox`). Distinct from idempotency (operation-level result replay) — the inbox is message-level seen/not-seen.
- **DDD domain events + aggregate roots + in-process dispatcher → outbox:** the domain layer gains tactical building blocks. `DomainEvent` (frozen value object) and `AggregateRoot` (`forze.domain.models`) let aggregates raise events into a transient, non-persisted buffer (`record_event` / `collect_events`); `AggregateRoot` overrides `model_copy` so events stay independent across `Document.update` copies (no aliasing / double-dispatch). A declarative **`@event_emitter`** decorator (mirroring `update_validator`) raises an event from an `(before, after, diff)` transition automatically on `Document.update` — a pure `(before, after, diff) -> DomainEvent | None` collected across the class hierarchy, enforced (at class creation) to live only on an `AggregateRoot`, with an optional `fields=` filter — so common events need no bespoke behavior method. A new `DomainEventDispatcherPort` (`forze.application.contracts.domain`, resolved via `ctx.domain()`) with an in-process `InProcessDomainEventDispatcher` + `DomainEventRegistry` runs handlers within the operation's transaction. Handlers are registered as **factories** `(ctx) -> (event) -> None`: the factory resolves narrow capabilities from `ctx`, the running handler is context-free (the execution context never leaks into per-event handlers). The `outbox_event_handler` bridge factory maps a domain event to an `IntegrationEvent` staged in the transactional outbox, so domain-event side effects flush atomically with the aggregate write. Registered via `DomainEventsDepsModule(registry)`; `forze_mock` registers a no-op dispatcher (`MockDepsModule(domain_events=...)` to wire handlers in tests).
- **Operation-level resilience wrap (`forze.application.hooks.resilience`):** `ResilienceWrap` is an operation-plan middleware that applies a named resilience policy (timeout / circuit-breaker / bulkhead / retry) around a whole operation, attached per-op via `registry.bind(op).bind_outer().wrap(ResilienceWrap(policy="...", route=...).to_step())`. It resolves the executor via `resolve_resilience_executor`, falling back to the shared default so the builtin `occ` / `transient` policies work without registering `ResilienceDepsModule`. Retry re-executes the operation with a fresh transaction per attempt (transactional side effects roll back between attempts); the docstring documents attaching retry-bearing policies only to read-only / fully-transactional / idempotency-guarded operations. Default priority places it just inside `IdempotencyWrap` so a replayed result skips retries.
- **Resilience policy pipeline (`forze.application.contracts.resilience`):** a Polly/resilience4j-style cross-cutting contract for declaring resilience once and applying it uniformly. Composable strategy value objects — `BulkheadStrategy`, `CircuitBreakerStrategy`, `RetryStrategy` (with `BackoffStrategy` jitter modes incl. decorrelated, and an optional `RetryBudget` token-bucket cap), `TimeoutStrategy`, and a call-site `FallbackStrategy` marker — compose into a `ResiliencePolicy` (validated outer→inner order: bulkhead → breaker → retry → timeout). Retry classification reuses the existing `ExceptionKind` retryable taxonomy. Named policies are declared via `ResilienceSpec` and run through `ctx.resilience().run(fn, policy=..., route=...)` (`ResilienceExecutorPort`). Ships a built-in in-process executor (`InProcessResilienceExecutor`, registered via `ResilienceDepsModule`; process-local breaker/bulkhead/budget state keyed by `(policy, route)`) with default `"occ"` and `"transient"` policies, and emits retry/breaker/timeout/bulkhead events into the runtime tracer (`domain="resilience"`). `forze_mock` registers a no-op `PassthroughResilienceExecutor` by default (`MockDepsModule(resilience="real")` opts into the real one).
- **Graph contracts wired into the framework + `forze_neo4j`:** the graph module contracts (`forze.application.contracts.graph`) are now resolvable via `ctx.graph.query(spec)` / `.command(spec)` / `.raw(spec)` (new `GraphDeps`), plus an opt-in `GraphRawQueryPort` escape hatch for engine-specific Cypher. New **`forze_neo4j`** (`forze[neo4j]`) package implements the graph ports on Neo4j via the async Bolt driver — `Neo4jDepsModule`, `Neo4jClient`, `neo4j_lifecycle_step`, and a `Neo4jGraphAdapter` covering vertex/edge CRUD, both edge-identity modes, `neighbors` / `expand` / `shortest_path`, tenant-property isolation, and the raw hatch (remaining traversal/bulk methods raise a clear `NotImplementedError` pending follow-ups). Cypher generation lives in a reusable `forze_neo4j.kernel.cypher` module for future openCypher siblings. `forze_mock` gains an in-memory `MockGraphAdapter` so handlers can use graph without a real engine.
- **`forze_kits`:** consolidated package for domain kits, aggregate registries/facades, mapping, DTOs, outbox/notify integrations, secrets adapters, and runtime scopes (`DistributedLockScope`). Absorbs the former `forze_patterns`, `forze.application.composition`, `forze.application.handlers`, `forze.application.mapping`, `forze.application.dto`, `forze.application.kit`, and `forze_secrets` (see migration table under **Removed**).
- **`forze_kits` stored-file kit:** closed-schema, document-backed object storage (`StoredFileKitSpec`, fixed domain/read/cmd models, orchestration handlers, `freeze_stored_file_registry`); record-then-upload create and soft-delete with post-commit blob purge; optional search merge and outbox events.
- **`forze[http]` / `forze_http`:** outbound HTTP integration — `HttpServiceSpec`, `HttpServicePort`, `HttpxClient` / `RoutedHttpxClient` (tenant routing), `HttpxDepsModule`, and a declarative `BaseHttpIntegration` + `async_http_op` toolkit (`forze.application.integrations.http`); `ExecutionContext.http` resolves configured services by spec name.
- **`forze_meilisearch` (`forze[meilisearch]`):** async Meilisearch integration with offset `SearchQueryPort`, `SearchCommandPort`, and federated search (native federation or weighted RRF). Requires `meilisearch-python-sdk>=7.2.1`.
- **Outbox:** `forze.application.contracts.outbox` (`OutboxSpec`, `IntegrationEvent`, `OutboxCommandPort`, `OutboxQueryPort`), request-scoped `OutboxStaging`, and persistence stores `PostgresOutboxStore` / `MongoOutboxStore` / `MockOutboxStore` (wired via `Configurable*OutboxCommand` / `*OutboxQuery`). Relay helpers (`relay_outbox_to_queue` / `_to_stream` / `_to_pubsub`, dispatcher, and `outbox_relay_background_lifecycle_step`) live in `forze_kits.integrations.outbox`, with at-least-once claim/reclaim semantics.
- **Notify:** `forze_kits.integrations.notify` — typed notification commands, routing, dispatch, and a queue-consumer helper.
- **Tenant routing — `RelationSpec` / `NamedResourceSpec`:** declarative per-request resolution of backend targets (relations, collections, indexes, buckets, namespaces, queues, ingest targets) for static or tenant-scoped routing, with `coerce_*` / `require_static_*` helpers (`forze.application.contracts.resolution`). Adopted across Postgres, Mongo, Firestore, Meilisearch, Redis, SQS, RabbitMQ, Temporal, S3, GCS, BigQuery, and ClickHouse, with public `*Spec` / `coerce_*` / resolver exports per package. Deps modules warn when a dynamic resolver is combined with `tenant_aware=True`.
- **Tenant routing — `Routed*Client`:** all integration clients gain per-tenant routed variants with `*RoutingCredentials`, `routed_*_lifecycle_step`, and LRU pool dedup by connection fingerprint, backed by a shared `TenantClientRegistry` and tenancy/secret-resolution helpers in `forze.application.contracts.tenancy` (`require_tenant_id`, `parse_tenant_hint`, `coalesce_tenant_request_hints`, `TENANT_ID_HEADER`, `secret_ref_for_tenant`, `resolve_str_for_tenant`, `TenantAwareIntegrationConfig`).
- **Identity — IdP presets (`forze_identity.builtin.idp`):** OIDC presets for Google Sign-In (`google_identity_deps`), VK ID (`vk_identity_deps`, code exchange), and Telegram Login (`telegram_login_identity_deps`); `oidc_bootstrap_identity_deps` for routes accepting external `id_token` JWTs. `OidcIdpPreset` / `ConfigurableOidcIdpVerifier` wire issuer/JWKS/audience without vendor URLs.
- **Identity — OAuth/OIDC:** PKCE helpers (`generate_pkce`, `PkcePair`) for OAuth 2.1 authorization-code flows; `OidcTokenVerifier.require_nonce` (default `False`).
- **Identity — authn:** `ApiKeyLifecycleAdapter.refresh_api_key` now rotates the key (validate, re-issue, retire) instead of raising `NotImplementedError`; single-use password invites (`issue_password_invite` / `accept_invite_with_password`, backed by `authn_password_invites` storing only an HMAC digest, wired via `kernel.invite_token_pepper` / `InviteTokenConfig`); `AuthnDepsModule` skips `kernel.access_token_secret` validation on routes with a custom `token_verifiers` override.
- **Postgres search / hub:** `read_validation` (`"strict"` | `"trusted"`) on document, search, and hub configs; PGroonga plan modes (`filter_first` / `index_first` / `auto`) with candidate-row caps and `EXPLAIN`-based estimates; hub `per_leg_limit` / `combo_limit` / `combo_top`, optional `execution: parallel` legs (with cursor and exact totals), and `SearchOptions` overrides (`pgroonga_plan`, `candidate_limit`, `groonga_query`, `search_count`, `combo_limit`).
- **Mongo search:** `MongoDepsModule.searches` with `SearchQueryPort` adapters (text / Atlas / vector), offset + cursor, optional Redis result snapshots, and an optional index-validation lifecycle step that warns on unsafe secondary unique indexes used with `ensure` / `upsert`.
- **Core search:** `SearchCommandPort` (`ensure_index`, `upsert`, `upsert_many`, `delete`, `delete_all`) for external search-index maintenance.
- **Codecs:** `default_model_codec` and `stored_field_names_for` (`forze.base.serialization`); `DocumentCodecs` / `document_codecs_for_spec` / `DocumentSpec.resolved_codecs`; optional `read_codec` / `ingest_codec` on `SearchSpec`, `HubSearchSpec`, and `AnalyticsSpec`; `PostgresReadOnlyDocumentConfig.read_validation` for faster materialization from trusted SQL rows.
- **Execution — freeze/resolve pipeline:** authoring `DepsRegistry` (`freeze()` → `FrozenDepsRegistry.resolve()` → `FrozenDeps`) separates registration from per-scope resolution; matching `LifecyclePlan` → `FrozenLifecyclePlan` → `ResolvedLifecyclePlan` with `LifecycleModule`, `LifecyclePlan.from_modules` / `with_modules` / `with_concurrent()`, topological ordering, and `routed_client_lifecycle_step` / `RoutedClientLifecycle`. `PostgresLifecycleModule` covers pool, optional catalog warmup, and optional schema validation.
- **Execution — per-scope caches:** `ExecutionRuntime.cache_resolved_operations` and `cache_resolved_ports` (both default `True`) memoize resolved operations and configurable ports per scope; tenant-scoped resolvers stay per-call so a shared adapter never pins one tenant's target.
- **Document adapters:** `max_scan_pages`, `max_stream_pages`, and `max_chunked_command_pages` (default 100_000; `None` for unlimited) with cursor-stall detection; `forze_identity.authz.fetch_all_document_hits` accepts `max_pages`.
- **Durable workflow:** `DurableWorkflowRunStatus`, `DurableWorkflowRunDescription`, and `describe()` on `DurableWorkflowQueryPort` (`forze_temporal` maps Temporal `WorkflowHandle.describe()`).
- **`forze.base` primitives:** `CacheLane` (TTL/FIFO cache), `SimpleLruRegistry` / `GuardedLruRegistry` (async LRU resource caches with optional in-use-guarded eviction, `dedup_key`, fail-fast reentrancy detection, and optional `timeout`), `InflightLane` (asyncio singleflight), `OnceCell` (frozen-field memo), `frozen_mapping`, and fingerprint helpers (`stable_json_bytes`, `stable_payload_fingerprint`, `stable_fingerprint`, `connection_string_fingerprint`).
- **`forze_mock`:** tenancy helpers (`partition_namespace`, `resolve_mock_namespace`, `MockTenancyMixin`, `MockRoutedStateRegistry`), extended `MockState` buckets, and new adapters (distributed lock, search command/snapshot/hub/federated, durable workflow/schedule/function, identity stubs) registered on `MockDepsModule`.

### Changed

- **Document writes: identity is now an explicit argument, not a field on the create payload (breaking).** `CreateDocumentCmd` no longer carries `id` / `created_at` — those leaked server-managed fields into every create payload and tool/route schema. The create payload type bound is now plain `BaseDTO`, and `CreateDocumentCmd` is a deprecated empty `BaseDTO` subclass kept only for back-compat. The `DocumentCommandPort` write surface changed to: `create(payload, *, id=None)` (server-assigns the key unless `id` is given — a caller-chosen "put"), `ensure(id, payload)`, `upsert(id, create, update)`, with bulk variants `ensure_many(items)` / `upsert_many(items)` taking `KeyedCreate(id, payload)` / `UpsertItem(id, create, update)` value objects (both exported from `forze.application.contracts.document`). `update` / `update_many` / `touch` / `kill` are unchanged (already id-explicit). The low-level `DocumentWriteGatewayPort` mirrors this with parallel sequences — `create(payload, *, id=None)`, `ensure(id, payload)`, `ensure_many(ids, payloads)`, `upsert(id, create, update)`, `upsert_many(ids, creates, updates)` — and each adapter (Postgres/Mongo/Firestore/mock) injects the explicit `id` when building the domain. The runtime `require_create_id` / `require_create_id_for_many` guards are gone (id is now a typed argument; duplicate-id rejection moved into the bulk methods). **Import / restore:** to preserve `created_at` / `last_update_at` on a faithful restore, mix the new `forze_kits.dto.ImportTimestamps` into a create payload and use `ensure` — those fields flow through the existing codec transform; `rev` is intentionally not preserved. **Migration:** move `id`/`created_at` out of create-command constructors into the new arguments; replace `ensure(cmd)`/`upsert(c, u)` with `ensure(id, payload)`/`upsert(id, create, update)`; replace bulk lists/tuples with the value objects (port) or parallel sequences (gateway).
- **Idempotency reshaped from HTTP snapshot to engine-level result idempotency:** `IdempotencySnapshot` (HTTP `code`/`content_type`/`body`/`headers`) is replaced by an interface-agnostic `IdempotencyRecord(result: bytes)`, and idempotency is now wired into the execution engine. A new `IdempotencyWrap` operation-plan hook (`forze.application.hooks.idempotency`) reads an `idempotency_key` bound on the `InvocationContext` (`bind_idempotency` / `get_idempotency_key`), computes a payload hash from the operation args (`stable_payload_fingerprint`), and on a duplicate returns the stored **typed** result early — skipping the handler and its transaction — using the operation's declared `result_type` codec (`default_model_codec`). It is a no-op when no key is bound. `IdempotencyPort.begin/commit` now exchange `IdempotencyRecord`; `ctx.idempotency.get(spec)` resolves the store; Redis and Mock adapters store the serialized result (claim/state-machine/TTL unchanged). The boundary supplies only the key; engine-level early-return is decoupled from any specific transport. The FastAPI `InvocationMetadataMiddleware` reads the canonical `Idempotency-Key` header (configurable via `idem_header`) into the invocation context, making it usable end-to-end over HTTP.
- **Async contract protocols standardized on `def ... -> Awaitable[X]`:** the remaining Protocol ports still declared with `async def ... -> X` (document gateway ports, `HttpServicePort`, `ResilienceExecutorPort`, routed-client/lifecycle/outbox/notify/typing-host protocols across core and the Postgres/Mongo/Firestore/Inngest packages) now use `def ... -> Awaitable[X]`, matching the convention already used by the other ~21 contract `ports.py`. This is a type-only change — `async def` implementations and `await` call sites are unaffected — and it makes the contracts decorator-friendly (e.g. `occ_retry` returns `Awaitable`). Async-generator methods (`subscribe`, `consume`, `tail`, streaming reads) are unchanged.
- **Optimistic-concurrency retry routed through the resilience pipeline:** the Postgres, Mongo, and Firestore write gateways no longer carry their own `tenacity` `optimistic_retry` decorators — mutating methods are now wrapped with the shared `occ_retry` decorator backed by the `"occ"` resilience policy (retry on `ExceptionKind.CONCURRENCY`, decorrelated backoff, 3 attempts). The executor is resolved per scope (`resolve_resilience_executor`) and falls back to a shared process default when no `ResilienceDepsModule` is registered, so existing apps keep OCC retries with no wiring change; registering the module lets an app override the `"occ"` policy. Retry behavior and attempt counts are unchanged; only the backoff timing is unified across the three engines.
- **Graph contracts (evolving, pre-1.0):** refined toward genuine dual-engine (Neo4j/openCypher + ArangoDB) support. `EdgeRef` now has two addressing modes — `EdgeRef.by_key(kind, key)` and `EdgeRef.by_endpoints(kind, from_ref, to_ref)` — selected per edge kind via `GraphEdgeSpec.identity` (`"key"` | `"endpoints"`); `GraphNodeSpec.key_field` (default `"id"`) and `GraphEdgeSpec.key_field` name the ref-key property. `shortest_path` no longer carries `max_paths` (single path); a separate `k_shortest_paths(..., k)` returns multiple. New `resolve_query_directions` helper pins direction defaults. `validate_graph_module_spec` now raises a `configuration` `CoreException` (was `ValueError`) and checks `key_field` existence.
- **Frozen `attrs` integration configs (breaking):** all integration wiring configs — Postgres, Mongo, Firestore, Meilisearch, Redis, S3/GCS storage, SQS, RabbitMQ, Temporal, ClickHouse, BigQuery, Inngest — are now frozen `attrs` classes; dict / `TypedDict` literals are no longer accepted (use constructors). `tenant_aware` is inherited from `TenantAwareIntegrationConfig`, and module-level `validate_*_conf` functions are removed (validation runs at construction or via `.validate()` / `validate_against_spec`). Several timeout fields move to `timedelta` (Inngest `request_timeout`, BigQuery/ClickHouse client timeouts, `ClickHouseConfig.keepalive_timeout`, OIDC `JwksKeyProvider.timeout` / `cache_ttl`); `S3Head`, `GCSHead`, `GCSListedObject`, `SQSQueueMessage`, and `RabbitMQQueueMessage` are frozen value types.
- **Coordinators → adapters (breaking):** `DocumentCoordinator` → `DocumentAdapter`, `DocumentCacheCoordinator` → `DocumentCache`, `SearchResultSnapshotCoordinator` → `SearchResultSnapshot`, `OutboxStagingCoordinator` → `OutboxStaging`, `DistributedLockCoordinator` → `DistributedLockScope`; field renames `cache_coord` → `document_cache` and `snapshot_coord` → `result_snapshot`. Adapter-side helpers now live under `forze.application.integrations` (`document`, `search`, `outbox`); `forze.application.coordinators` is removed.
- **Storage — CQRS split (breaking):** the single `StoragePort` / `StorageDepKey` is split into `StorageQueryPort` (`download`, `list`) and `StorageCommandPort` (`upload`, `delete`) with separate `StorageQueryDepKey` / `StorageCommandDepKey`, matching the document/outbox/dlock port taxonomy. Resolve via `ctx.storage.query(spec)` / `ctx.storage.command(spec)` instead of the removed callable `ctx.storage(spec)`. S3/GCS/Mock register both keys from the same `storages` config (one adapter still satisfies both ports); the split enables wiring a read-only query side (e.g. read-only credentials or a read-replica/CDN bucket) independently. S3/GCS factory renames: `ConfigurableS3Storage` → `ConfigurableS3StorageQuery` / `ConfigurableS3StorageCommand` (and GCS equivalents).
- **Codecs unified on `ModelCodec` (breaking for low-level callers):** document, search, and analytics read/write paths materialize through spec-owned codecs (`DocumentSpec.resolved_codecs`, `SearchSpec.read_codec`, `AnalyticsSpec.read_codec` / `ingest_codec`); document kernel gateways (Postgres/Mongo/Firestore) require `codec` / `create_codec` / `update_codec` / `history_codec` at construction (build via `read_gw` / `doc_write_gw` — no silent `PydanticModelCodec`). `read_validation="trusted"` decode is available on Postgres/Mongo/Firestore. The versioned document cache stores compact JSON bytes (legacy dict entries remain readable until TTL).
- **Write gateways — unified OCC/history validation:** Postgres and Mongo share one `HistoryOccMixin`; a missing history snapshot during OCC validation now raises `exc.precondition` (code `history_not_found_retry`) on **both** backends (Mongo previously raised `exc.not_found`) — treat it as a retryable stale-revision precondition.
- **Search snapshot fingerprints (one-time re-baseline):** computed via the shared `orjson`-based `stable_payload_fingerprint`; the digest bytes differ from the previous encoding, so persisted snapshot fingerprints are re-baselined once (self-healing).
- **Postgres streaming reads:** `find_many_chunked` / `fetch_all_batched` use a server-side (named) cursor, so rows stream in batches and the full result set is never buffered client-side — client memory stays bounded regardless of result size (same rows, order, and batch sizes).
- **Performance:** hook-less operations skip body-stage scaffolding and fold the middleware chain iteratively (~30% faster in the in-process benchmark); per-scope operation/port caches reuse resolved gateways, adapters, and codecs; trusted bulk decode hoists the field set and construct loop for large pages; JSON logs render via `orjson`. Behavior is unchanged.
- **Outbox (Postgres/Mongo):** bulk `INSERT … ON CONFLICT DO NOTHING` flush, `claim_pending` setting `processing_at`, stale-`processing` reclaim before claim (`reclaim_stale_after`, default 5 minutes, reported as `OutboxRelayResult.reclaimed`), and `requeue_failed`; `MongoClientPort.find_one_and_update` for atomic claim.
- **Storage / analytics internals:** the shared storage-adapter base, object-storage client port, and warehouse-analytics helpers move from `forze.application.contracts` to `forze.application.integrations` (contracts retain ports, specs, and value objects). MIME sniffing via `python-magic` stays in `S3StorageAdapter` / `GCSStorageAdapter` (optional extras); the shared base uses stdlib `mimetypes`. Public adapter and port APIs are unchanged.
- **Mongo:** `MongoSearchConfig` uses a single `index_name` (semantics depend on `engine`) instead of separate `atlas_index_name` / `vector_index_name` / `text_index_name`.
- **`forze[oidc]` extra:** now includes `httpx` (used by the VK and Telegram authorization-code-exchange helpers).
- **Internal package layout:** integration `kernel` packages are reorganized into `kernel.client`, and `execution` into `lifecycle/` plus `execution.deps.{configs,factories}` subpackages, across all `forze_<integration>` packages; the operation registry, planning, facade, and run modules move under `forze.application.execution.operations`. Public package-root imports are unchanged; direct imports of internal modules (`kernel.platform`, `execution.deps.deps`, flat `execution.lifecycle`, etc.) must use `kernel.client`, `execution.deps`, `execution.lifecycle`, or the package root.

### Deprecated

- **`forze_identity.oidc`:** `OidcTokenVerifier.enforce_issuer_and_audience` now defaults to `True` — construction requires both `issuer` and `audience` unless explicitly opted out.

### Removed

- **`python-dateutil` core dependency:** dropped; `datetime_to_uuid7` now parses ISO-8601 strings via stdlib `datetime.fromisoformat` (trailing `Z` accepted).
- **`forze[casbin]` extra:** dropped (no integration package or adapter shipped against it).
- **`forze_identity.local` (breaking):** use `forze_identity.builtin.local` (`LocalIdentityConfig`, `local_identity_deps`, `LocalApiKeyVerifier` / `ConfigurableLocalApiKeyVerifier`, `LocalTenantResolver` / `ConfigurableLocalTenantResolver`); local verifiers/factories are no longer exported from `forze_identity.authn` / `forze_identity.tenancy`.
- **`forze_identity.builtin.telegram`:** Telegram Mini App `initData` HMAC preset, superseded by Telegram Login OIDC under `forze_identity.builtin.idp.telegram`.
- **Execution:** `forze.application.coordinators`; `forze.application.execution.registry` / `planning` / `facade` / `running`; `OperationRunner`; `lifecycle_graph_from_sequence` (use `steps_graph_from_sequence`).
- **Postgres:** `validate_pg_search_conf`, `validate_postgres_hub_search_conf`, `validate_postgres_federated_search_conf`, and `is_postgres_federated_embedded_hub_config` from the public API; dict/mapping coercion for `ConfigurablePostgresDocument` / `ConfigurablePostgresReadOnlyDocument` (use the config constructors and instance validation).
- **Integrations:** `validate_mongo_search_conf`, `validate_meilisearch_search_conf`, `validate_meilisearch_federated_search_conf`, `validate_clickhouse_analytics_config`, and `validate_bigquery_analytics_config` from public exports (validation now lives on the config types).
- **Codecs:** `RecordMappingCodec` / `PydanticRecordMappingCodec` / `MsgspecRecordMappingCodec`, `codec_for_model`, `pydantic_cache_dump` / `pydantic_cache_dump_many`, and the public `pydantic_*` / `msgspec_*` helpers in `forze.base.serialization` (use `ModelCodec` / `default_model_codec`, or import low-level helpers from `forze.base.serialization.pydantic` / `.msgspec`); `SearchSpec.row_codec` / `resolved_row_codec` and `DocumentReadGatewayPort.effective_row_codec` (use `read_codec`).
- **Relocated to `forze_kits` (breaking):** the former `forze_patterns`, `forze.application.composition`, `forze.application.kit`, `forze_secrets`, `forze.application.handlers.*`, `forze.application.mapping`, and `forze.application.dto` modules now live under `forze_kits`. `Mapper` / `MapperFactory` protocols stay on `forze.application.contracts.mapping`. `OutboxDestination(queue_route=..., queue=...)` is replaced by the discriminated `OutboxDestination.queue(route=..., channel=...)` (also `.stream`, `.pubsub`).

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

See [Kits reference](pages/docs/reference/kits.md).

### Fixed

- **Postgres:** `ensure` / `ensure_many` / `upsert` / `upsert_many` build `ON CONFLICT` from `PostgresDocumentConfig.conflict_target` or inferred primary-key columns (fixes composite PKs and tables with additional UNIQUE indexes).
- **Postgres search:** PGroonga `index_first` no longer silently applies a 5000-row cap when `pgroonga_candidate_limit` is disabled (falls back to `filter_first`); `search_count=exact` with a candidate cap no longer under-counts; FTS/vector coalesced read==heap paths apply non-trivial filters on the heap.
- **Postgres hub (parallel):** exact page totals use uncapped leg CTEs instead of `len(merged)`; trusted reads strip internal hub keys (e.g. `_hub_rank`) to match SQL-path semantics; cursor compares UUID/encoded sort keys via wire canonicalization and aligns in-memory row order with ranked cursor specs (including `DESC` sorts); `combo_top` offset ordering matches cursor.
- **Mongo:** `ensure_many` / `upsert_many` classify bulk `$setOnInsert` upserts safely; missing rows after a bulk upsert raise `mongo_ensure_bulk_miss` instead of a generic not-found.
- **Meilisearch:** federated search awaits snapshot finalization; `ensure_index` and multi-search use `meilisearch-python-sdk` models.
- **Identity (authn):** password provisioning rejects duplicate logins (`password_account_exists`); login detects ambiguous duplicate accounts (`password_account_ambiguous`); `MappingTableResolver` re-reads mappings after create conflicts when `provision_on_first_sight=True`.
- **`forze_fastapi`:** tenant resolution honors JWT/OIDC `issuer_tenant_hint` and `X-Tenant-Id` via `TenantResolverPort.requested_tenant_id`, with a hint-only fallback when no tenant resolver is registered.
- **`forze.base`:** `connection_string_fingerprint` includes sorted URI query parameters so routed-client LRU dedup distinguishes targets that differ only by query string.
- **`forze_temporal` + `forze[mcp]`:** Temporal workflow validation no longer fails with `RuntimeError: Failed validating workflow <name>` when the MCP stack is imported in the same process. `fastmcp`'s transitive `py-key-value-aio` dependency installs a process-wide `beartype.claw` import hook at import time; the Temporal workflow sandbox re-imports each workflow module through that hook and hits a circular import. New `forze_temporal.sandboxed_workflow_runner()` / `default_sandbox_restrictions()` (and `PASSTHROUGH_MODULES`) pass `beartype` through the sandbox — use the runner as your `Worker(workflow_runner=...)` when running workers alongside the MCP integration.
- **`forze_temporal` (workflow sandbox under coverage):** the same `sandboxed_workflow_runner()` now also passes `coverage` through the sandbox. Under `coverage` on Python 3.14 (`sys.monitoring` tracing), a branch callback firing inside sandboxed workflow code lazily imports `coverage.env`, whose module-load `platform.python_implementation()` call is restricted in the sandbox — raising `RestrictedWorkflowAccessError`, failing the workflow task, and (because Temporal retries workflow-task failures indefinitely) **hanging** the test run rather than failing it.

### Security

- **Raw-query tenancy hardening (the escape-hatch seam):** the structured path injects tenant isolation automatically, but the raw hatches bypassed it silently. `ctx.graph.raw(spec).run(...)` (`forze_neo4j`) now **fails closed** in a tenant-aware module — it raises if no tenant is bound (was: ran *unscoped across all tenants*) and binds the current tenant as `$tenant` so the query can `MATCH (... {tenant_id: $tenant})`; non-tenant-aware modules are unchanged. For the universal kernel client ports (`PostgresClientPort`/`Neo4jClientPort`, where you own scoping), a new ergonomic `ctx.tenancy.current()` / `ctx.tenancy.require_current_id()` returns the bound tenant (raising if none) instead of reaching into `inv_ctx`. The per-hatch trust model is now documented. (Deeper enforcement — scoped-fragment raw API, Postgres RLS — remains future work; the exposure is small and the seam is now fail-closed.)
- **`forze_identity.tenancy`:** `TenantResolverAdapter` rejects invalid tenant hints (`tenant_mismatch`) and inactive tenants (`tenant_inactive`) instead of silently returning `None`.
- **`forze_identity.authn` — session enforcement:** first-party access JWTs carry a `sid` session claim, and the default `ForzeJwtTokenVerifier` wiring cross-checks session `principal_id` / `tenant_id` against token `sub` / `tid` and rejects bearer tokens when the session is revoked or rotated, so logout and refresh-rotation invalidate access before JWT `exp`. **Breaking:** pre-upgrade tokens without `sid` fail until clients re-login, or apps must register a stateless verifier override (`session_qry=None`).
- **`forze_identity.authn` — `change_password` requires the current password (breaking):** the change re-authenticates with the current password first, so a hijacked session cannot escalate to account takeover (the `forze_kits` change-password DTO/handler gained `current_password`).
- **`forze_identity.authn` — principal eligibility (breaking):** authentication and credential lifecycle are gated on `authz_policy_principals.is_active` via `PrincipalEligibilityPort` (required by `AuthnOrchestrator`; the advisory `authn_principals` store is removed); `PrincipalDeactivationPort` cascades policy deactivation, session revocation, and credential deactivation; API keys persist and enforce `expires_at`; `revoke_api_key` / `revoke_many_api_keys` and `issue_api_key` take `identity: AuthnIdentity` for ownership checks (and no longer require a pre-existing key row).
- **`forze_identity.authn` — login hardening:** `Argon2PasswordVerifier` returns a generic `401` (`invalid_credentials`) for all failures and always runs Argon2 verify (including unknown accounts) to reduce enumeration and timing leakage.
- **`forze_identity.authz` — fail-closed tenant isolation:** `BaseDocumentPort` / `DocumentReadGatewayPort` expose a `tenant_aware` property, and grant-resolution adapters (`GrantQueryAdapter`, `AuthzDecisionAdapter`, `RoleAssignmentAdapter`) refuse to construct when an `AuthzSpec` route is tenant-scoped but any binding/catalog query port is not tenant-aware — surfacing the misconfiguration at startup instead of leaking grants across tenants (`"global"` routes unaffected).
- **`forze_identity.oidc`:** `OidcTokenVerifier` resolves JWKS signing keys in a worker thread so cache misses do not block the event loop.
- **Secret-field redaction:** JWT signing keys and HMAC peppers (`AccessTokenService.secret_key`, `ApiKeyService` / `RefreshTokenService` / `InviteTokenService` peppers, `AuthnKernelConfig` secrets) are `repr=False`; `VaultConfig.token`, `S3RoutingCredentials.secret_access_key`, and `GCSRoutingCredentials.service_account_json` become `SecretStr`; `HttpRoutingCredentials.headers` is redacted from `repr` and routed through the one-way secret KDF in routing fingerprints.
- **`forze_fastapi` — `X-Tenant-Id` not trusted by default (breaking):** when no tenancy resolver validates the request, a tenant from the raw `X-Tenant-Id` header is ignored unless `SecurityContextMiddleware.trust_tenant_header` / `resolve_tenant_identity(..., trust_tenant_header=True)` is set (default `False`); a tenant derived from a verified credential is still honored. `register_scalar_docs` / `scalar_docs` no longer honor `X-Forwarded-Host` unless `trust_forwarded_host=True`, and Scalar docs default to `persist_auth=False`.
- **`forze_meilisearch`:** filter attribute names are validated against a safe identifier pattern before rendering, so a user-controlled filter key can no longer inject filter-expression fragments (e.g. bypass the tenant filter); the tenant filter value is escaped via the shared literal formatter.
- **`forze_postgres` PGroonga (breaking search behavior):** user-supplied full-text terms are quoted as literal Groonga phrases instead of being passed through the `&@~` query-syntax operator, so operator characters can no longer alter match scope or amplify query cost; trusted advanced queries still use the `groonga_query` `SearchOptions` override.
- **`forze_sqs`:** absolute-URL queue names (`http(s)://…`) are rejected on a tenant-aware adapter, since they would skip namespace + tenant prefixing and bypass tenant isolation.
- **Object storage:** `ObjectStorageAdapter.download` / `delete` validate the object key (safe charset, no `..` traversal, no absolute path) before forwarding it to the store.
- **BigQuery / GCS:** routed clients unlink Forze-created temporary service-account JSON files on inner-client `close()` (tenant eviction and pool shutdown).
- **`forze.base`:** `configure_logging(sanitize_logs=True)` scrubs `error.message` and `error.stack` with log string rules; `include_exception_stack=False` omits stacks from JSON logs.

## [0.2.0] - 2026-05-28

### Added

- **Execution:** `OperationRegistry`, `FrozenOperationRegistry`, `Handler`, stage hooks, `OperationRegistry.patch()` / `PlanPatch`, `make_registry_operation_resolver`, `run_operation`, and `facade_op` on document/search/storage/authn facades. `ResolvedOperationPlan` drives runtime hooks, transaction scopes, and after-commit dispatch.
- **Execution context:** Nested resolvers — `ctx.document`, `ctx.search`, `ctx.deps`, `ctx.tx_ctx`, `ctx.inv_ctx`, `ctx.authz`, and `ctx.analytics`.
- **Dependency tracing:** `ResolutionTracer`, `RuntimeTracer`, `DepsPlan.with_tracing()`, and `DepsResolutionTrace.to_key_dag()` / `canonical_edges()`.
- **Runtime tracing (development):** `forze.application.execution.tracing` with `RuntimeTrace`, `FORZE_RUNTIME_TRACE`, `validate_runtime_trace`, and port recording via `Deps.resolve_configurable`.
- **TxTracer:** Optional transaction scope observer on `TransactionContext`.
- **Composition catalogs:** `DOCUMENT_OPERATIONS`, `SEARCH_OPERATIONS`, `STORAGE_OPERATIONS`, and `AUTHN_OPERATIONS` under `forze_kits.*.catalog`.
- **Hooks:** `forze.application.hooks.authz`, `hooks.authn`, and `hooks.tenancy` for operation-plan authz, principal, and tenant enforcement.
- **Query DSL:** Literal `$values` / field `$fields` filters, `$not`, array quantifiers (`$any`, `$all`, `$none`), text patterns (`$like`, `$ilike`, `$regex`), aggregate `$computed` / `$groups` / `$trunc`, configurable `QueryFilterLimits`, and pre-parsed `QueryExpr` support on gateways.
- **Document & search:** `DocumentCoordinator`, `DocumentCacheCoordinator`, and `SearchResultSnapshotCoordinator`; `update_matching` / `ensure`; method-specific ports (`find_page`, `find_cursor`, `search_page`, `project_*`, `select_*`, …); hub and federated search (FTS/PGroonga v2, weighted RRF).
- **Document contracts:** `RowLockMode` on `for_update`; `select_cursor`; stream methods `find_stream`, `project_stream`, and `select_stream`; post-write `hydrate_from_write` when read/write sources align.
- **Sort defaults:** `DocumentSpec.default_sort`, `SearchSpec.default_sort`, and `HubSearchSpec.default_sort`; shared helpers `resolve_effective_sorts`, `normalize_sorts_for_keyset`, and `validate_sort_fields` in `forze.application.contracts.querying`.
- **Durable functions:** contracts under `forze.application.contracts.durable.function`; optional `DurableFunctionSpec.operation`; `handler_for_registry_operation` and `run_durable_function` in `forze.application.execution.operations.run`.
- **`forze_inngest` (`inngest` extra):** Inngest adapter (`InngestClient`, `InngestDepsModule`, `register_functions`, `InngestFunctionBinding`, `inngest_lifecycle_step`, FastAPI `serve`) with registry-backed cron/event runs via `register_functions(..., registry=)`.
- **Workflow schedules:** schedule contracts and `forze_temporal` Temporal Schedules support (create/upsert/update/delete/pause/unpause/trigger/describe/list) with declarative bootstrap via `TemporalDepsModule.schedule_bootstraps`.
- **Queue delayed delivery:** `QueueCommandPort.enqueue` / `enqueue_many` accept `delay` and `not_before`; SQS `DelaySeconds`, Mock `visible_at` filtering, and RabbitMQ DLX delay queues when `delayed_delivery=True`.
- **`forze_identity`:** consolidated authn, authz, tenancy, and OIDC (`oidc` extra) with verify-then-resolve ports, `AuthnOrchestrator`, and `AuthzPolicyService`.
- **`forze_identity.local` (demo / MVP):** file/env API-key identity (`LocalIdentityConfig`, `LocalApiKeyVerifier`, `LocalTenantResolver`, `local_identity_deps()`).
- **Analytics:** `AnalyticsSpec`, `AnalyticsQueryPort`, optional `AnalyticsIngestPort`, and adapters for Postgres, ClickHouse (`clickhouse` extra), and BigQuery (`bigquery` extra).
- **`forze_firestore` (`firestore` extra), `forze_gcs` (`gcs` extra), `forze_secrets`, and `forze_vault` (`vault` extra):** document, object storage, and secrets integrations with routed clients and lifecycle steps.
- **Postgres startup validation:** Pydantic↔column compatibility, bookkeeping triggers, and tenancy wiring checks on `PostgresDepsModule`.
- **Scrubbing:** `forze.base.scrubbing` with `sanitize(value, context="egress"|"log")` and default structlog field scrubbing via `configure_logging(sanitize_logs=True)`.
- **Console logging:** `ForzeConsoleRenderer.max_traceback_frames` for Rich traceback frame collapsing (default `20`; `0` shows all frames).
- **Integrations:** Redis distributed locks; `PydanticModelCodec` and `MsgspecModelCodec`; `StrKeySelector` and `StrKeyNamespace`; optional domain mixins in `forze_kits`.

### Changed

- **Breaking — execution & composition:** `Usecase` / `UsecaseRegistry` replaced by `Handler` + `OperationRegistry`. Register with `set_handler`, compose plans via `.patch()` / `.bind()` / `.bind_outer()` / `.bind_tx()`, then `.freeze()`; resolve with `registry.resolve(operation, ctx)`.
- **Breaking — `ExecutionContext`:** `ctx.doc_query` / `ctx.doc_command` → `ctx.document.query` / `ctx.document.command`; `ctx.dep(...)` → `ctx.deps.provide` or `ctx.deps.resolve_configurable`; `ctx.transaction(...)` → `ctx.tx_ctx.scope(...)`; `CallContext` → `InvocationMetadata` via `ctx.inv_ctx`.
- **Breaking — document & search ports:** result shape and pagination mode are chosen by method name (`find_page` vs `find_cursor`, `search_page` vs `search_cursor`, …); `find_many_with_cursor` removed.
- **Breaking — query DSL:** filter literals use `"$values"` (was `"$fields"`); field compares use `"$fields"` (was `"$compare"`); grouping uses `"$groups"` / `"$trunc"` (top-level `"$time_bucket"` removed).
- **Breaking — identity:** legacy `forze_authnz` consolidated into `forze_identity` (`authn`, `authz`, `tenancy`, `oidc` subpackages). `AuthnIdentity` is principal-only; `AuthnPort` returns `AuthnResult`; tenant hints are validated via `TenantResolverPort`.
- **Breaking — authorization:** `AuthzPort.permits(...)` removed; use `AuthzDecisionPort.authorize(AuthzRequest)` with `Authz*` types (`AuthzSubject`, `AuthzScope`, `AuthzDecision`, …). Import operation-plan helpers from `forze.application.hooks.authz`.
- **Breaking — durable workflows:** contracts moved to `forze.application.contracts.durable.workflow` with `DurableWorkflow*` public types and renamed dep keys (`durable_workflow_command`, …).
- **Breaking — errors:** `forze.base.errors` removed in favor of `forze.base.exceptions`; HTTP `X-Error-Code` defaults are `core.<kind>`.
- **Breaking — runtime tracing:** package renamed to `forze.application.execution.tracing`; `ExecutionTrace` → `RuntimeTrace`, `trace_execution` → `trace_runtime`, `validate_trace` → `validate_runtime_trace`.
- **Breaking — dependency tracing:** `Deps.merge()` no longer propagates tracer flags; pass `resolution_tracer` / `runtime_tracer` or use `DepsPlan.with_tracing()`.
- **Breaking — FastAPI:** `forze_fastapi.endpoints/` and `forze_fastapi.transport.http/` removed; package now ships middleware, exception handlers, OpenAPI helpers, and security resolvers only.
- **Breaking — Mongo:** `MongoClient.db` / `collection` and `MongoGateway.coll` are async.
- **Document/search pagination:** omitting `sorts` no longer emits `ORDER BY id` when the read model has no `id` field; configure `default_sort` or pass explicit `sorts`.
- **Document gateways (Postgres/Mongo/Firestore/Mock):** Pydantic `@computed_field` names excluded from persistence; `ensure` / `upsert` skip redundant read round-trips on insert.
- **Messaging contracts:** `QueueMessage`, `PubSubMessage`, and `StreamMessage` are frozen attrs value objects; queue/pubsub/stream specs require a `ModelCodec`.
- **`forze_gcs`:** native async `gcloud-aio-storage` instead of threaded `google-cloud-storage`.
- **Postgres PGroonga search:** match and `weights` follow index declaration order; every indexed column must appear in `SearchSpec.fields`.
- **Postgres & Redis:** safer batched writes, implicit read limits, routed pool locking, `get`/`mget` returning `bytes | None`, atomic `mset` with `NX`/`XX`, and concurrent cache adapter I/O.
- **Scrubbing:** log-context scrub uses `**********` and Logfire-aligned substring rules instead of scrubadub placeholders.
- **Console logging:** default Rich traceback visibility increased from 8 to 20 frames before middle collapse.
- **Socket.IO:** `ForzeSocketIOAdapter.bind` takes `operation_resolver`; use `make_registry_operation_resolver`.
- **`forze_fastapi`:** unhandled route exceptions return Forze generic JSON 500 (`{"detail":"Internal server error"}`) when `register_exception_handlers(app)` is used.

### Removed

- **Execution:** `Usecase`, `UsecaseRegistry`, `UsecasePlan`, `bucket` module, `facade_call`, `FacadeOpRef`, `OpKeySpace`, `GuardSkip`, and registry graph introspection types.
- **FastAPI:** `endpoints/` package, `transport.http/`, `ForzeAPIRouter`, `facade_dependency`, and attach-based route helpers.
- **Authn & identity:** monolithic `AuthnAdapter`, `HeaderAuthnIdentityResolver`, `OAuth2Tokens`, `PrincipalContext`, and principal codec ports.
- **Query:** deprecated predicate aliases (`QueryPredicate`, `is_query_predicate`, …).
- **Postgres search:** legacy `PostgresFTSSearchAdapter` / `PostgresPGroongaSearchAdapter` and `hub_pgroonga` module.
- **Domain:** `forze.domain.mixins` — use `forze_kits` mixins instead.

### Fixed

- **`forze_fastapi`:** `register_exception_handlers` CRITICAL-logs tracebacks for unhandled exceptions and for 5xx `CoreException` with a chained cause; deliberate 5xx without a cause logs at ERROR with structured fields only.
- **Errors:** `CoreError.details` and FastAPI `context` responses no longer expose raw credentials or Pydantic validation `input`.
- **Postgres:** batched `UPDATE … FROM (VALUES …)` casts nullable cells correctly; write gateway no longer duplicates `rev` in `VALUES`; `read_only` set before opening transactions; array coercion for `text[]` columns.
- **Postgres search:** hub/PGroonga empty queries no longer emit invalid rank SQL; offset snapshot pages reuse validated rows when possible.
- **Redis:** script result normalization avoids rare `isinstance` failures on union types.
- **S3:** user metadata decoding on download/list; upload persists optional `description`; default object keys use fresh UUID v7 per call.
- **Authn:** API key lifecycle unpacks `(prefix, secret)` from key generation in the correct order.

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

[unreleased]: https://github.com/morzecrew/forze/compare/v0.2.0...HEAD
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
