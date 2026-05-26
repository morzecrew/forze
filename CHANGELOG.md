# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Document contracts:** `RowLockMode` (`False`, `True`, `"nowait"`, `"skip_locked"`) on query `for_update`; `select_cursor`; keyset stream methods `find_stream`, `project_stream`, and `select_stream`; coordinator post-write hydration (`hydrate_from_write`) when read and write share the same physical source and read fields are a subset of the domain model.
- **Postgres document schema validation (tier 1):** startup checks for Pydantic↔column type compatibility, required-field nullability, tenant column presence/type/nullability when `tenant_aware`, and a warning when read-model fields are not on the write relation (read/write split).
- **Postgres bookkeeping validation:** startup checks that `bookkeeping_strategy="database"` documents have UPDATE triggers on the write table; warns when `application` strategy coexists with bump triggers or when `history_enabled` relies on DB triggers.
- **Postgres tenancy wiring validation:** `PostgresDepsModule` fails when `RoutedPostgresClient` is used without `introspector_cache_partition_key`; warns on redundant `tenant_aware=True` with routed clients; schema validation warns when `tenant_id` exists on the write table but row isolation is disabled.
- **Analytics glue hardening:** `AnalyticsAppendResult` now includes optional `rejected` and `errors`; BigQuery and ClickHouse clients expose `health()`, configurable read retries, and insert batching; query config supports `skip_total` (cheaper `run_page`) and ClickHouse `cursor_column` (keyset cursors with `{forze_after:Type}` in SQL); shared adapter helpers in `forze.application.contracts.analytics._adapter_common`; ClickHouse per-request `database` via query settings (no shared-client mutation); `ClickHouseConfig` HTTP pool and `SecretStr` password.
- **Runtime tracing (development):** Opt-in per-task sequence of transaction boundaries and configurable port calls under `forze.application.execution.tracing` (`RuntimeTrace`, `TracingEvent`, `FORZE_RUNTIME_TRACE`, `Deps.trace_runtime` / `deps.runtime_trace()`); `validate_runtime_trace(..., validator=...)`, `assert_runtime_trace_valid`, `run_traced_operation` harness; `TraceExpectation` / `assert_trace_contains`; violation reports; sync+async port proxy; `resolve_simple` resolve events; trace buffer max 10k events; `FORZE_RUNTIME_TRACE_LOG`; Firestore validator `validate_reads_before_writes_in_tx`; pytest helpers in `tests.support.runtime_tracing`.
- **`forze.application.hooks.authz`:** Operation-plan helpers (`authorize_before_step`, `document_scope_wrap_step`, `policy_scope_from_invocation`, …) moved out of `forze.application.contracts.authz` so contracts stay port-only.
- **`forze.application.hooks.authn` / `hooks.tenancy`:** `authn_required_before_step` and `tenant_required_before_step` (and `AuthnBeforeRequired` / `TenancyBeforeRequired` factories) enforce bound `ctx.inv` principal and tenant on operation plans.
- **Query DSL:** Text pattern filter operators `$like`, `$ilike`, and `$regex` on `$values` (and inside array element quantifiers). Operands accept a single pattern string or a sequence (OR at parse time). Parse-time limits `max_pattern_length` and `max_pattern_or_branches`; `$regex` rejects known catastrophic patterns. Postgres and Mongo renderers support all three; Firestore MVP raises `CoreError`.

### Changed

- **Errors (breaking):** `forze.base.errors` removed in favor of `forze.base.exceptions` (`CoreException`, `exc.*` factories, `ExceptionKind`, `ExceptionInterceptor`, `map_pydantic`). HTTP `X-Error-Code` defaults are now `core.<kind>` (for example `core.not_found`). FastAPI integration no longer ships `endpoints/` or `transport.http/`; tests for those modules were dropped and remaining FastAPI tests target middleware, OpenAPI, and exception handlers only.
- **Document gateways (Postgres/Mongo):** `ensure` / `upsert` write paths skip redundant read-gateway round-trips on insert (Mongo hydrates from write payload; Postgres conflict paths use in-transaction `SELECT` on the write table). **`update_matching`** contract docs clarify bulk-patch semantics vs `update_matching_strict`; Postgres `RETURNING` rows hydrate to read models via the coordinator when sources align.
- **Runtime tracing API (breaking):** Package `forze.application.execution.trace` renamed to `tracing`; `ExecutionEvent`/`ExecutionTrace` → `TracingEvent`/`RuntimeTrace`; `trace_execution`/`FORZE_EXEC_TRACE` → `trace_runtime`/`FORZE_RUNTIME_TRACE`; `execution_trace()` → `runtime_trace()`; `validate_trace` → `validate_runtime_trace`; events are contract-agnostic (`domain`, `surface`, `route`, `phase`); trace buffer uses per-`Deps` `ContextVar` (not a module-global buffer); port recording centralized in `Deps.resolve_configurable`.
- **Authorization layering:** `contracts.authz` no longer imports `contracts.execution` or exports registry wiring helpers; import hooks from `forze.application.hooks.authz` instead.
- **Authorization naming:** Contract types use the `Authz*` prefix (`AuthzSubject`, `AuthzScope`, `AuthzRequest`, `AuthzDecision`, …). Ports: `AuthzDecisionPort`, `AuthzScopePort`. Value objects grouped under `contracts.authz.value_objects` (`catalog`, `decision`, `scoping`, `grants`). `ctx.authz.decision(spec)` is the only decision accessor; dep key `authz_decision` (`AuthzDecisionDepKey`). `AuthzDepsModule` registers `decision=` routes (no `runtime=` / `authz=` kwargs). Hooks: typed `AuthzBeforeAuthorize` / `AuthzDocumentScopeWrap` factories with `to_before_step()` / `to_middleware_step()`. Removed backward-compatible aliases (`Authorization*`, `PolicyScope`, `ProtectedResource`, `AuthorizationRuntimePort`, …).

### Added

- **Authorization (vNext):** Split authz contracts into decision (`AuthorizationDecisionPort.authorize`, `AuthorizationRequest` / `AuthorizationDecision`), data scoping (`AuthorizationScopePort`), and grant management ports. `AuthzDeps` on `ExecutionContext` (`ctx.authz`). Dedicated [authorization reference](pages/docs/reference/authorization.md). `forze_authz` adapters implement the new ports; owner-aware ABAC hints on `ProtectedResource.attributes`. Integration tests under `tests/integration/test_forze_authz/`.
- **Analytics contracts:** `AnalyticsSpec`, `AnalyticsQueryDefinition`, `AnalyticsQueryPort` (named `run*` / `project_run*` / `select_run*` / cursor / chunked), optional `AnalyticsIngestPort`, `AnalyticsDeps` on `ExecutionContext` (`ctx.analytics`), and `MockAnalyticsAdapter` with seeded query hits and ingest log.
- **`forze_clickhouse` (`clickhouse` extra):** ClickHouse analytics integration (`ClickHouseDepsModule`, `ClickHouseClient`, `ClickHouseAnalyticsAdapter` implementing `AnalyticsQueryPort` / `AnalyticsIngestPort`, `clickhouse_lifecycle_step`, COUNT wrapper for `run_page`, offset cursor tokens, chunked reads, insert ingest, integration tests with Docker ClickHouse) backed by `clickhouse-connect[async]`.
- **`forze_bigquery` (`bigquery` extra):** Google BigQuery analytics integration (`BigQueryDepsModule`, `BigQueryClient`, `BigQueryAnalyticsAdapter` implementing `AnalyticsQueryPort` / `AnalyticsIngestPort`, `bigquery_lifecycle_step`, COUNT wrapper for `run_page`, cursor `pageToken` encoding, chunked reads, streaming insert ingest, integration tests with goccy/bigquery-emulator) backed by `gcloud-aio-bigquery`.
- **`forze_gcs` (`gcs` extra):** Google Cloud Storage integration (`GCSDepsModule`, `GCSClient`, `GCSStorageAdapter` implementing `StoragePort`, `gcs_lifecycle_step`, tenant-aware key prefixes, integration tests with fake-gcs-server) backed by `gcloud-aio-storage`.
- **`forze_firestore` (`firestore` extra):** Cloud Firestore document integration (`FirestoreDepsModule`, `FirestoreClient`, optimistic `rev`, optional history collection, MVP query renderer, transactions via `FirestoreTxManagerAdapter`, Docker-based emulator integration tests).
- **`forze_secrets`:** canonical `SecretsPort` adapters for in-memory mappings, environment variables, and directory-backed secret files; `SecretsDepsModule` registers backends under `SecretsDepKey`.
- **`forze_vault` (`vault` extra):** HashiCorp Vault KV v2 + token-auth integration (`VaultClient`, `VaultKvSecrets`, `VaultDepsModule`, `vault_lifecycle_step`) for runtime secret resolution via `SecretsPort`.
- **`forze_fastapi.transport.http` (experimental):** function-first HTTP transport with `run_operation`, `make_facade_dep`, `ForzeRouter`, `forze_route`, `RequirePrincipal`, and `AuthnRequirement`; `attach_document_routes`, `attach_search_routes`, `attach_storage_routes`, and `attach_authn_routes` on plain `APIRouter`; `register_route` / `RouteRegistration`; `IdempotentPolicy`, `ETagPolicy`, `run_idempotent`, and `document_etag` (rewritten from legacy `endpoints.http.features`, no imports from `forze_fastapi.endpoints`).
- **Composition operation catalogs:** `DOCUMENT_OPERATIONS`, `SEARCH_OPERATIONS`, `STORAGE_OPERATIONS`, and `AUTHN_OPERATIONS` with presets and capability checks under `forze.application.composition.*.catalog` for protocol-agnostic attach; `forze.application.execution.run_operation` for registry invocation.
- **Deprecation (soft):** prefer `forze_fastapi.transport.http.attach_*_routes` over legacy `forze_fastapi.endpoints.*.attach_*_endpoints`; legacy `endpoints.http.features` idempotency/ETag remain until a later removal PR.
- **Query DSL:** Filter combinator `$not` (single child expression). Array element quantifiers `$any`, `$all`, and `$none` under `$values` with equality/ordering inner predicates (scalar shortcuts, operator maps, or element-relative `$values` for object arrays). Vacuous `$all` / `$none` on missing or empty arrays.
- **Scrubbing:** `forze.base.scrubbing` with `sanitize(value, context="egress"|"log")`, Pydantic error helpers, and default structlog field scrubbing via `configure_logging(sanitize_logs=True)`.
- **Execution:** `OperationRegistry.freeze()` rejects orphan plan patches, equal-specificity patch merge conflicts, and operations with transaction-scoped stages or dispatch but no `set_route`.
- **Query DSL:** Configurable filter abuse limits (`QueryFilterLimits`: `max_depth`, `max_clauses`, `max_in_size`) enforced at parse time; `QueryFilterExpressionParser` is an attrs instance (`parse_filter`) with a classmethod `parse` shim; gateways expose `compile_filters` and accept pre-parsed `QueryExpr` on `where_clause` / `render_filters`; aggregate computed fields store `parsed_filter` to avoid re-parsing; `DocumentCoordinator` offset pages compile filters once and pass `parsed` through `count` / `find_many` gateway calls.
- **Execution:** Registry-centered composition with `OperationRegistry`, `FrozenOperationRegistry`, `Handler` implementations, and stage hooks as `BeforeStep` / `OnSuccessStep` on `bind_outer()` / `bind_tx()`; `make_registry_operation_resolver` for FastAPI and Socket.IO; `facade_op` descriptors on `DocumentFacade`, `SearchFacade`, `StorageFacade`, and `AuthnFacade`; `StrKeyNamespace` on specs (`default_namespace`); optional `DepsResolutionTrace` (`FORZE_DEPS_TRACE`); cyclic dependency detection on `Deps`.
- **Execution context:** Nested resolvers — `ctx.document` / `ctx.doc`, `ctx.search` (including hub and federated), `ctx.deps`, `ctx.tx`, `ctx.inv` (`InvocationMetadata`, authn, tenant binding).
- **Query DSL:** Literal filters `{"$values": ...}`, field compares `{"$fields": ...}`, aggregate `$computed` and calendar grouping via `"$groups"` / `"$trunc"`; UUID operands on range operators; keyset helpers in `forze.pagination`.
- **Document & search:** `DocumentCoordinator`, `DocumentCacheCoordinator`, and `SearchResultSnapshotCoordinator`; `update_matching` / `ensure` on document commands; Postgres/Mongo hub and federated search (FTS/PGroonga v2, weighted RRF); method-specific search/document ports (`find_page`, `find_cursor`, `search_page`, `project_*`, `select_*`, …).
- **Authn & authz:** Packages `forze_authn`, `forze_authz`, and `forze_oidc` (`oidc` extra); verify-then-resolve ports (`*VerifierPort`, `PrincipalResolverPort`, `VerifiedAssertion`); `AuthnOrchestrator` and configurable `AuthnDepsModule`; document-backed grant catalogs and `AuthzPolicyService`; FastAPI `attach_authn_endpoints`, `AuthnRequirement`, token transport features, and single-source identity resolvers; optional `forze_casbin` (`authnz-casbin` extra).
- **Tenancy & secrets:** `forze_tenancy` (`TenancyDepsModule`, tenant resolver/management adapters); `AsyncSecretsPort` and routed DSN/URI/credential clients for Postgres, Mongo, Redis, S3, RabbitMQ, SQS, and Temporal.
- **Integrations:** `attach_storage_endpoints`; Redis distributed locks; record-mapping codecs (`PydanticRecordMappingCodec`, `MsgspecRecordMappingCodec`); `StrKeySelector` in `forze.base.primitives`; `OperationRegistry.patch(selector)` and `PlanPatch` entries resolved at `freeze()` via `str_key_selector`; optional domain mixins in `forze_contrib` (soft deletion, metadata, number id, creator id).

### Changed

- **`forze_gcs`:** `GCSClient` now uses native async [`gcloud-aio-storage`](https://pypi.org/project/gcloud-aio-storage/) instead of `google-cloud-storage` with `asyncio.to_thread`; optional `service_file` on `gcs_lifecycle_step`.
- **Transport HTTP clarity:** Documented catalog → bindings → options → attach layers in FastAPI integration docs and module docstrings; honest `forze_fastapi.transport` package boundary (HTTP under `transport.http`, Socket.IO under `forze_socketio`). Shared attach helpers in `transport.http.attach._loop` (`resolve_route_path`, `resolve_include_in_schema`, `iter_catalog_operations`). Document bindings aligned with search (`document_binding_for`, `make_facade_body_endpoint`). No public API breaks on `attach_*_routes`.
- **Breaking — authn & tenancy split:** `AuthnIdentity` is now principal-only, `AuthnPort` returns `AuthnResult`, and FastAPI tenant resolution validates issuer/request tenant hints against `TenantResolverPort` instead of treating them as canonical tenant context.
- **Postgres PGroonga search:** Match and `weights` arrays follow index declaration order from catalog metadata; `SearchSpec.fields` order is ignored. Every indexed column must appear in `SearchSpec.fields` (via `field_map` when heap names differ); extra spec fields are ignored. Unparseable index expressions fail at query time with `CoreError`.
- **Transport HTTP layout:** `transport.http.specs` replaced by `transport.http.options` (config + `RouteOpts`); route tables split into application catalogs and `transport.http.bindings` (HTTP method/path/response). Presets (`DocumentPreset`, etc.) are defined in composition catalogs and re-exported from `transport.http`. Removed unused `*EndpointsSpec` from the transport public API (legacy `endpoints.*` specs unchanged).
- **Documentation:** Site docs and agent skills aligned with current terminology (`Handler`, `OperationRegistry`, stage hooks, `ctx.deps.*`); removed references to deleted `Usecase` / `ctx.dep` / plan-bucket APIs; renamed operation composition page to `operation-composition.md`; removed legacy `core-concepts/` redirect stubs; distilled Socket.IO integration guide.
- **Socket.IO:** `ForzeSocketIOAdapter` and `SocketIONamespaceRouter.bind` take `operation_resolver` (not `usecase_resolver`); `make_registry_usecase_resolver` removed — import `make_registry_operation_resolver` from `forze_socketio` or `forze.application.execution`.
- **Record mapping:** `RecordMappingCodec` adds `encode_json_bytes` / `decode_json_bytes` with fast-path Pydantic (`model_dump_json` / `model_validate_json`) and msgspec (`msgspec.json.encode` / `decode`) implementations.
- **Messaging contracts:** `QueueMessage`, `PubSubMessage`, and `StreamMessage` are frozen attrs value objects (replacing `TypedDict`). `QueueSpec`, `PubSubSpec`, and `StreamSpec` require a `codec: RecordMappingCodec[...]` (for example `PydanticRecordMappingCodec(model)`). Integration queue/pubsub/stream adapters take `payload_codec` from the spec.
- **Scrubbing:** log-context string scrub uses `**********` and Logfire-aligned substring rules (email, Bearer, `key=value` assignments, secrets in free text) instead of scrubadub `{{TYPE}}` placeholders.
- **Postgres search:** Internal refactor of FTS, PGroonga, vector, and hub search adapters — shared projection-index CTE builders (`_pipeline_sql`), leg scorers (`_leg_*`), and offset/snapshot execution (`_offset_run`); no public API change.
- **Execution:** `ResolvedOperationPlan` now drives operation runtime: stage hooks (`before`, `wrap`, `on_success`, `on_failure`, `finally_`, `dispatch`), transaction scopes, and `after_commit` / `dispatch_after_commit` deferral run in documented order when calling `FrozenOperationRegistry.resolve(...)(args)` (previously only the bare handler ran).
- **Breaking — execution & composition:** `Usecase` / `UsecaseRegistry` replaced by `Handler` + `OperationRegistry`. Register handlers with `set_handler`, author shared plans with `.patch(str_key_selector.all_keys())` (or other selectors) and per-operation overlays with `.bind(...)`, then `.bind_outer() / .bind_tx().finish(deep=True).freeze()`; resolve with `registry.resolve(operation, ctx)`. Stage hooks use explicit step types; handler results are owned by `__call__`, not replaced by hooks. Capability keys on graph steps are plain `str` (`requires` / `provides` on `BeforeStep`, etc.). Inter-operation dispatch is declared on the registry plan only.
- **Breaking — `ExecutionContext`:** `ctx.doc_query` / `ctx.doc_command` → `ctx.document.query` / `ctx.document.command`; `ctx.dep(...)` → `ctx.deps.provide` or `ctx.deps.resolve_configurable`; `ctx.transaction(...)` → `ctx.tx.scope(...)`; `ctx.txmanager(...)` → `ctx.tx.resolver(...)`; `CallContext` / `bind_call` / `get_authn_identity()` → `InvocationMetadata` / `ctx.inv.bind` / `ctx.inv.get_authn()` (and `get_tenant()`). Document dep factories are `factory(ctx, spec)`; resolve cache inside factories via `ctx.cache(spec.cache)` when `DocumentSpec.cache` is set.
- **Breaking — document & search ports:** Result shape and pagination mode are chosen by **method name**, not `return_type` / `return_fields` / `return_count` flags. Examples: `find_page` vs `find_many` vs `find_cursor`; `search_page` vs `search` vs `search_cursor`; `project_*` and `select_*` variants mirror the same pattern. `find_many_with_cursor` is removed (use `find_cursor`).
- **Breaking — query DSL:** Filter literals use `"$values"` (old `"$fields"`). Field-to-field compare uses `"$fields"` (old `"$compare"`). Aggregate grouping uses `"$groups"`; calendar buckets use `"$groups": { "<alias>": { "$trunc": … } }` (top-level `"$time_bucket"` removed).
- **Breaking — authn & authz:** `forze_authnz` split into `forze_authn` and `forze_authz` (`authn` / `authz` extras; `authnz` extra kept for install compatibility). `AuthnPort` orchestrates verifiers + `PrincipalResolverPort`; credential types split (`AccessTokenCredentials`, `RefreshTokenCredentials`, `IssuedTokens`, …); `OAuth2Tokens` removed. `ContextBindingMiddleware` takes ordered `authn_identity_resolvers` and `when_multiple_credentials`. Grants use `RoleRef` / `PermissionRef` keys.
- **Breaking — authorization vNext:** `AuthzPort.permits(principal, action, tenant_id=…, resource=…, context=…)` removed in favor of `AuthorizationRuntimePort.authorize(AuthorizationRequest)` with `AuthorizationSubject`, `PolicyScope`, and `ProtectedResource`. `PrincipalRef` no longer includes `tenant_id`. Dep keys renamed (`AuthorizationRuntimeDepKey`, `AuthorizationScopeDepKey`, `GrantQueryDepKey`; legacy aliases retained). `AuthzSpec` now carries `tenancy_mode` and `enforce_principal_active`. Use `ctx.authz` instead of resolving authz only via `ctx.deps`.
- **Breaking — FastAPI:** `attach_*_endpoints` require a **frozen** registry (`.freeze()` after binding tx routes). `HttpEndpointSpec` uses `operation: StrKey`; handlers resolve via `registry.resolve(operation, ctx)` (no `facade_type` / facade dependencies on routes).
- **Breaking — Mongo:** `MongoClient.db` / `collection` and `MongoGateway.coll` are async.
- **Postgres:** Safer batched writes (single transaction when caller has none), implicit `LIMIT` on unbounded reads (default `10_000`), chunked coordinator reads, stricter array filter rendering, routed pool init locking, and catalog/introspection caching improvements.
- **Redis:** `get` / `mget` return `bytes | None`; atomic `mset` with `NX`/`XX`; optional read retry and pub/sub reconnect hooks; cache adapter writes/deletes are concurrent; search snapshot chunks use Lua CAS.
- **Search snapshots:** Postgres adapters take `SearchResultSnapshotCoordinator`; private snapshot helper modules under `forze_postgres.adapters.search` are removed (use the coordinator API).

### Removed

- **Execution:** `Usecase`, `UsecaseRegistry`, `UsecasePlan`, `bucket` module, `facade_call`, `FacadeOpRef`, `OpKeySpace` / `op_key_space_for`, `GuardSkip`, `SchedulableCapabilitySpec`, `UsecaseDelegate`, `delegated_usecase_hook`, and public `registry.graph` / `RegistryGraph` introspection types.
- **FastAPI:** `facade_dependency`, `HTTP_FACADE_KEY`, `facade_type` / `facade_init_kwargs` on attach helpers; `OperationRef` on `HttpEndpointSpec`.
- **Authn:** `forze_authn` monolithic `AuthnAdapter`; `HeaderAuthnIdentityResolver` (use `HeaderTokenAuthnIdentityResolver` + `HeaderApiKeyAuthnIdentityResolver`); `OAuth2Tokens`; `ACCESS_TOKEN_KIND` / `REFRESH_TOKEN_KIND` constants.
- **Identity:** `PrincipalContext`, `ExecutionContext.get_principal_ctx`, and principal codec ports.
- **Query:** Deprecated predicate aliases (`QueryPredicate`, `is_query_predicate`, …) — use `QueryValuesPredicate`, `QueryFieldsPredicate`, `has_query_values`, etc.
- **Postgres search:** Legacy `PostgresFTSSearchAdapter` / `PostgresPGroongaSearchAdapter` and `hub_pgroonga` module (use v2 or hub adapters).
- **Domain:** `forze.domain.mixins` — use `forze_contrib` mixins instead.

### Fixed

- **Errors:** `CoreError.details` and FastAPI `context` responses no longer expose raw credentials or Pydantic validation `input`; egress uses `forze.base.scrubbing.sanitize`, producers use `sanitize_pydantic_errors`, and `handled()` passes scrubbed bound arguments to error handlers.
- **Postgres:** Batched `UPDATE … FROM (VALUES …)` casts nullable cells correctly; write gateway no longer duplicates `rev` in `VALUES` (fixes ambiguous column); `read_only` set before opening psycopg transactions; document array coercion for `text[]` columns.
- **Postgres search:** Hub/PGroonga empty queries no longer emit invalid rank SQL; offset snapshot pages reuse validated rows when possible.
- **Redis:** Script result normalization avoids rare `isinstance` failures on union types; platform errors map subclass exceptions before generic redis bases.
- **S3:** User metadata decoding on download/list; upload persists optional `description`; default object keys use fresh UUID v7 per call.
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

- `DocumentOperation`, `DocumentUsecasesFacade` moved from `forze.application.facades` to `forze.application.composition.document`. `StorageOperation` moved to `forze.application.usecases.storage`. Facades package removed.
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
- `DTOSpec` renamed to `DocumentDTOSpec` in `forze.application.composition.document`. Update imports accordingly.
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

[unreleased]: https://github.com/morzecrew/forze/compare/v0.1.14...HEAD
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
