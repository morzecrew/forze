# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Postgres PGroonga search:** optional plan modes (`filter_first`, `index_first`, `auto`), candidate row caps, hub `per_leg_limit`, coalesced read/heap fast path, and `SearchOptions` overrides (`pgroonga_plan`, `candidate_limit`, `groonga_query`). `auto` uses cached relation row estimates from the introspector (no extra `COUNT` unless configured).
- **Postgres search (phase 2):** `auto` can choose `index_first` for eligible filters using `EXPLAIN`-based filtered row estimates; index-first heap cap overshoot when projection post-filters apply; hub `combo_top` cap, optional `execution: parallel` per-leg hub queries, `SearchOptions.search_count` (`exact` / `approximate` / `none`), FTS/vector ranked caps via `pgroonga_candidate_limit`, and hub `combo_limit` / `SearchOptions.combo_limit`.
- **Postgres search (phase 3):** shared `_ranked_pipeline` builder for filter-first ranked SQL with uncapped exact-count fragments; hub leg SQL in `hub/_leg_sql.py`; hub snapshot fingerprints include `execution`, `combo_limit`, and `search_count`; `parallel_hub_cte_materialized` on hub config.
- **Postgres search (phase 4):** parallel hub exact totals via SQL `combo` COUNT; hub offset `effective_sorts` parity with cursor; multi-FK parallel merge (`hub/parallel_merge.py`) and sort-aware in-memory merge; hub cursor with `execution: parallel`; federated snapshot reads honor `search_count=none`.

### Security

- **`forze_identity.tenancy`:** `TenantResolverAdapter` rejects invalid tenant hints (`tenant_mismatch`) and inactive tenants (`tenant_inactive`) instead of silently returning `None`.
- **`forze_identity.authn`:** `ForzeJwtTokenVerifier` cross-checks session `principal_id` / `tenant_id` against token `sub` / `tid` when session enforcement is enabled.

### Fixed

- **Postgres PGroonga search:** `index_first` no longer silently applies a 5000-row cap when `pgroonga_candidate_limit` is disabled; the plan falls back to `filter_first` and snapshot metadata matches SQL.
- **Postgres ranked search:** `search_count=exact` with a candidate cap no longer under-counts matches; FTS/vector coalesced read==heap paths apply non-trivial filters on the heap; hub approximate totals respect `combo_limit`.
- **Postgres hub parallel:** exact page totals no longer use `len(merged)` under leg/combo caps; SQL hub exact `COUNT` uses uncapped leg CTEs (data queries still respect `per_leg_limit` / `combo_limit`); offset `combo_top` ordering matches cursor when default/user sorts apply.
- **`forze_identity.authn`:** password provisioning rejects duplicate logins (`password_account_exists`); login lookup detects ambiguous duplicate accounts (`password_account_ambiguous`); `MappingTableResolver` re-reads mappings after create conflicts when `provision_on_first_sight=True`.

### Removed

- **`forze_patterns`:** use `forze_kits.domain.*` (mixins, mapping steps, soft-deletion registry).
- **`forze.application.composition`:** use `forze_kits.aggregates.document`, `forze_kits.aggregates.search`, `forze_kits.aggregates.storage`, `forze_kits.aggregates.authn`, `forze_kits.integrations.outbox`.
- **`forze.application.kit`:** use `forze_kits.scopes` (`DistributedLockScope`).
- **`forze_secrets`:** use `forze_kits.adapters.secrets` (`EnvSecrets`, `DirectorySecrets`, `MappingSecrets`, `SecretsDepsModule`).
- **`forze.application.handlers.*`:** use `forze_kits.aggregates.{document,search,storage,authn}.handlers`.
- **`forze.application.mapping`:** use `forze_kits.mapping` (`PydanticPipelineMapperFactory`, pipeline steps). `Mapper` / `MapperFactory` protocols stay on `forze.application.contracts.mapping`.
- **`forze.application.dto`:** use `forze_kits.dto` (`Pagination`, `Paginated`, `CursorPagination`, `CursorPaginated`, and related types).
- **`OutboxDestination(queue_route, queue)`:** use discriminated `OutboxDestination` with `kind` and `OutboxDestination.queue(route=..., channel=...)` (also `.stream`, `.pubsub`).
- **`RecordMappingCodec` / `PydanticRecordMappingCodec` / `MsgspecRecordMappingCodec`:** use `ModelCodec` / `PydanticModelCodec` / `MsgspecModelCodec` (`forze.base.serialization`).
- **`forze.base.serialization` public `pydantic_*` / `msgspec_*` helpers:** use `ModelCodec` methods or import from `forze.base.serialization.pydantic` / `.msgspec` for low-level access.
- **`pydantic_cache_dump` / `pydantic_cache_dump_many`:** use `PydanticModelCodec(...).encode_json_bytes(..., exclude=CACHE_DUMP_EXCLUDE_OPTS)`.
- **`SearchSpec.row_codec` / `resolved_row_codec`:** use `read_codec` / `resolved_read_codec`.
- **`DocumentReadGatewayPort.effective_row_codec`:** use `read_codec`.
- **`forze_mock._adapters_monolith`:** unused duplicate of `forze_mock.adapters`; production mock wiring uses `forze_mock.adapters.document` only.
- **`codec_for_model`:** use `default_model_codec`.

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
| `PostgresOutboxAdapter` / `MongoOutboxAdapter` / `MockOutboxAdapter` | `PostgresOutboxStore` / `MongoOutboxStore` / `MockOutboxStore` |
| `ConfigurablePostgresOutbox` / `ConfigurableMongoOutbox` | Query-only alias of `ConfigurablePostgresOutboxQuery` / `ConfigurableMongoOutboxQuery`; wire `Configurable*OutboxCommand` on `OutboxCommandDepKey` |
| `ConfigurableMockOutbox` | Removed; use `ConfigurableMockOutboxCommand` / `ConfigurableMockOutboxQuery` |
| `RecordMappingCodec` | `ModelCodec` |
| `PydanticRecordMappingCodec` | `PydanticModelCodec` |
| `MsgspecRecordMappingCodec` | `MsgspecModelCodec` |
| `SearchSpec.row_codec` | `SearchSpec.read_codec` |
| `resolve_row_codec` | `resolve_model_codec` |
| `PostgresReadGateway(...)` without `codec=` | Pass `codec=` or build via `read_gw` |
| `PostgresWriteGateway(...)` without write codecs | Pass `create_codec` / `update_codec` / `codec=` or use `doc_write_gw` |
| `PostgresWriteGateway(..., domain_codec=...)` | Remove `domain_codec`; pass `codec=` (row/domain read codec) plus `create_codec` / `update_codec` |
| `PostgresHistoryGateway(...)` without `history_codec` | Pass `history_codec` and `codec=` or use `_doc_history_gw` / `doc_write_gw` |

See [Kits reference](pages/docs/reference/kits.md).

### Changed

- **`forze_postgres`:** document gateways decode SELECT rows through `ModelCodec` (default `PydanticModelCodec`; behavior unchanged unless `read_validation="trusted"`).
- **`forze_mongo` / `forze_firestore`:** document gateways and factories use spec-owned codecs + optional `read_validation` (same pattern as Postgres).
- **`forze.application.integrations.document`:** versioned document cache stores compact JSON bytes; legacy dict cache entries remain readable until TTL expiry.
- **`forze.application.integrations.document`:** post-write hydration uses `read_gw.read_codec.transform` instead of direct `pydantic_*` dumps.
- **`DocumentSpec`:** codecs are derived from model types via `resolved_codecs` (override with `DocumentSpec.codecs`); Postgres/Mongo/Firestore factories pass them into gateways.
- **`SearchSpec` / `HubSearchSpec`:** optional `read_codec` (auto-derived via `default_model_codec`); search adapters materialize hits through the codec.
- **`AnalyticsSpec`:** optional `read_codec` / `ingest_codec`; warehouse and mock analytics adapters route row encode/decode through codecs.
- **`forze_mock`:** in-memory document adapter uses spec/search codecs for read/write/search paths.
- **`forze_kits.mapping`:** `PydanticPipelineMapper` uses `PydanticModelCodec.transform` when no pipeline steps are configured.
- **`default_model_codec` / `stored_field_names_for`:** live in `forze.base.serialization`; `resolve_model_codec` delegates to the same policy (Pydantic or msgspec).
- **Document kernel gateways (Postgres/Mongo/Firestore):** `codec`, write `domain_codec` / `create_codec`, and `history_codec` are required at construction; use `read_gw` / `doc_write_gw` or pass explicit codecs (no silent `PydanticModelCodec` in `__attrs_post_init__`).
- **`SearchSpec` / `AnalyticsSpec`:** `resolved_read_codec` / `resolved_ingest_codec` resolve without mutating optional override fields.
- **`DocumentCache`:** requires `read_codec` (factories pass it from the read gateway).
- **Document write gateways (Postgres/Mongo/Firestore):** create/ensure/upsert insert legs use `create_codec`; patch/update uses `update_codec` (falls back to `read_codec` when unset).
- **`forze_mock` document adapter:** create/update/read paths use `DocumentSpec.resolved_codecs` (`create`, `update`, `domain`, `read`); optional per-adapter `codec=` override removed.
- **`DocumentSpec.supports_update()`:** uses `stored_field_names_for` (Pydantic and msgspec update commands).
- **Document write gateways (Postgres/Mongo/Firestore):** `domain_codec` constructor argument removed (use `codec` / `create_codec` / `update_codec` via `doc_write_gw`).

### Deprecated

- **`forze_identity.oidc`:** `OidcTokenVerifier.enforce_issuer_and_audience` defaults to `True` (construction requires both `issuer` and `audience` unless explicitly opted out).
- **`forze_kits` layout:** modules live under `domain/`, `aggregates/` (with per-aggregate `handlers/`), `mapping/`, `dto/`, `integrations/`, `adapters/`, and `scopes/` (e.g. `forze_kits.aggregates.document.handlers`, `forze_kits.mapping`, `forze_kits.integrations.outbox`, `forze_kits.adapters.secrets`). Core `forze.application` keeps contracts, execution, hooks, and integrations only.

### Added

- **`forze_identity.builtin.idp`:** OIDC IdP presets for Google Sign-In (`google_identity_deps`), VK ID (`vk_identity_deps`, `exchange_authorization_code`), and Telegram Login (`telegram_login_identity_deps`, code exchange); `oidc_bootstrap_identity_deps` for bootstrap routes that accept external `id_token` JWTs only.
- **`forze_identity.oidc`:** `OidcIdpPreset` and `ConfigurableOidcIdpVerifier` for issuer/JWKS/audience wiring without vendor-specific URLs.
- **`forze_identity.oauth`:** PKCE helpers (`generate_pkce`, `PkcePair`) for OAuth 2.1 authorization-code flows.
- **Authn:** `AuthnDepsModule` skips `kernel.access_token_secret` validation on routes with a custom `token_verifiers` override (same pattern as API-key overrides for external/OIDC/Telegram token verifiers).
- **Outbox (Mongo):** `MongoOutboxAdapter`, `MongoOutboxConfig`, and `outboxes={}` on `MongoDepsModule`; `MongoClientPort.find_one_and_update` for atomic claim.
- **`forze_kits`:** consolidated package for domain kits, aggregate registries/facades, outbox helpers, and runtime ergonomics (see migration table under **Removed**).
- **Outbox (kits):** `outbox_relay_background_lifecycle_step` for optional in-process relay polling.
- **Outbox (kits):** `relay_outbox_to_stream`, `relay_outbox_to_pubsub`, and `relay_outbox` dispatcher; lifecycle step supports `transport` (`queue`, `stream`, `pubsub`).
- **Notify (kits):** `forze_kits.integrations.notify` — typed notification commands, routing, dispatch, and queue consumer helper (no `NotificationPort` in core).
- **`forze_mock`:** Tenancy helpers (`partition_namespace`, `resolve_mock_namespace`, `MockTenancyMixin`, `MockRoutedStateRegistry`), extended `MockState` buckets (dlocks, search snapshots, durable workflow/function, identity), new adapters (distributed lock, search command/snapshot/hub/federated, durable workflow/schedule/function, identity stubs), and `MockDepsModule` registration for all related dep keys. Docs updated under [Mock integration](pages/docs/integrations/mock.md) and [Multi-tenancy](pages/docs/concepts/multi-tenancy.md).
- **`forze_postgres`:** `PostgresReadOnlyDocumentConfig.read_validation` (`"strict"` | `"trusted"`) for faster read-model materialization from trusted SQL rows.
- **`forze.application.contracts.codecs`:** `default_model_codec`, `stored_field_names_for` (Pydantic or msgspec by model type).
- **`forze.application.contracts.document`:** `DocumentCodecs`, `document_codecs_for_spec`, `DocumentSpec.resolved_codecs`.

### Changed

- **`forze[oidc]` extra:** now includes `httpx` (used by `forze_identity.builtin.idp.vk` and `.telegram` authorization-code exchange helpers).
- **Outbox:** `PostgresOutboxAdapter` / `MongoOutboxAdapter` / `MockOutboxAdapter` renamed to `*OutboxStore` (persistence only; no `ExecutionContext` on stores). `OutboxCommandDepKey` resolves `StagingOutboxCommand` via `ConfigurablePostgresOutboxCommand` / `ConfigurableMongoOutboxCommand` / `ConfigurableMockOutboxCommand` (`build_staging_outbox_command_for_store` + enricher + `OutboxStagingContext`); `OutboxQueryDepKey` resolves the store via `Configurable*OutboxQuery`. Legacy `ConfigurablePostgresOutbox` / `ConfigurableMongoOutbox` names remain **query-store aliases only**; `ConfigurableMockOutbox` is removed—do not register a query factory on `OutboxCommandDepKey`.
- **Outbox:** Postgres `flush` uses a single bulk `INSERT … ON CONFLICT DO NOTHING`; `claim_pending` sets `processing_at`; `OutboxQueryPort.reclaim_stale_processing` resets stuck `processing` rows; `relay_outbox_to_queue` reclaims before claim (`reclaim_stale_after`, default 5 minutes) and reports `OutboxRelayResult.reclaimed`; mock adapter matches Postgres idempotency and claim/mark semantics; docs cover `processing_at` DDL, at-least-once relay, and worker patterns; `OutboxQueryPort.requeue_failed`; relay passes `key=str(event_id)` per enqueue; Mongo outbox adapter and docs.
- **Integrations:** Shared storage adapter base, object-storage client port, metadata/path helpers, and warehouse analytics adapter helpers moved from `forze.application.contracts` to `forze.application.integrations` (`integrations.storage`, `integrations.analytics`); contracts retain ports, specs, and value objects only.
- **S3 / GCS:** MIME sniffing via `python-magic` stays in `S3StorageAdapter` / `GCSStorageAdapter` (optional extras); shared `ObjectStorageAdapter` uses stdlib `mimetypes` only. Public `S3StorageAdapter`, `GCSStorageAdapter`, `S3ClientPort`, and `GCSClientPort` unchanged.

### Removed

- **`forze_identity.builtin.telegram`:** Telegram Mini App `initData` HMAC preset (superseded by Telegram Login OIDC under `forze_identity.builtin.idp.telegram`).

### Changed (breaking)

- **`forze_identity.local`:** Removed; use `forze_identity.builtin.local` (`LocalIdentityConfig`, `local_identity_deps`, `LocalApiKeyVerifier`, `ConfigurableLocalApiKeyVerifier`, `LocalTenantResolver`, `ConfigurableLocalTenantResolver`). `LocalApiKeyVerifier` and local configurable factories are no longer exported from `forze_identity.authn` or `forze_identity.tenancy`.
- **Application layer:** Removed `forze.application.coordinators`. Adapter-side helpers live under `forze.application.integrations` (`document`, `search`, `outbox`); app-facing distributed lock ergonomics live under `forze_kits.scopes` (`DistributedLockScope`).
- **Renames:** `DocumentCoordinator` → `DocumentAdapter`, `DocumentCacheCoordinator` → `DocumentCache`, `SearchResultSnapshotCoordinator` → `SearchResultSnapshot`, `OutboxStagingCoordinator` → `OutboxStaging`, `DistributedLockCoordinator` → `DistributedLockScope`.
- **Field renames:** `cache_coord` → `document_cache` on document adapters; `snapshot_coord` → `result_snapshot` on search adapters; outbox adapters use private `_staging` instead of `_coordinator`.

### Security

- **Authn:** `Argon2PasswordVerifier` returns a generic `401` (`Invalid login or password`, code `invalid_credentials`) for all failed logins and always runs Argon2 verify (including unknown accounts) to reduce username enumeration and timing leakage.
- **`forze_fastapi`:** Scalar docs default `persist_auth=False`; `X-Forwarded-Host` / `X-Forwarded-Proto` apply only when `trust_forwarded_host=True` (enable behind a trusted reverse proxy).
- **Repository:** Gitleaks scans `tests/`; `just quality` runs secret detection via pre-commit (no blanket `tests/` allowlist).
- **`forze.base`:** `configure_logging(sanitize_logs=True)` scrubs `error.message` and `error.stack` with log string rules; use `include_exception_stack=False` to omit stacks from JSON logs.
- **BigQuery, GCS:** Routed clients unlink Forze-created temp service-account JSON files on inner client `close()` (tenant eviction and pool shutdown).
- **Authn:** `PrincipalEligibilityPort` gates authentication and credential lifecycle on `authz_policy_principals.is_active` (removed advisory `authn_principals` store).
- **Authn:** API keys persist `expires_at` and enforce expiry at verification; `revoke_api_key` / `revoke_many_api_keys` require `identity` for ownership checks.
- **Authn:** `PrincipalDeactivationPort` cascades policy deactivation, session revocation, and credential deactivation (prefer over `PrincipalRegistryPort.deactivate_principal` alone).
- **Authn:** First-party access JWTs from `TokenLifecycleAdapter` include a `sid` session claim; default `ForzeJwtTokenVerifier` wiring rejects bearer tokens when the session is revoked or rotated (logout and refresh rotation invalidate access before JWT `exp`).
- **OIDC:** `OidcTokenVerifier` resolves JWKS signing keys in a worker thread so cache misses do not block the asyncio event loop.

### Changed

- **`forze_fastapi` (breaking):** `register_scalar_docs` / `scalar_docs` no longer honor `X-Forwarded-Host` unless `trust_forwarded_host=True`; apps behind a reverse proxy must opt in explicitly.
- **Tests / coverage:** Integration `execution/lifecycle/pool.py` modules are included in coverage reports (no longer omitted); `fail_under = 80` applies to `src/forze` and `src/forze/application` in coverage reports.
- **Tests:** Integration tests under `tests/integration/` inherit `integration` and `asyncio` markers from the root conftest.
- **Authn (breaking):** `AuthnDepsModule` wires `ForzeJwtTokenVerifier.session_qry` by default; lifecycle-issued access tokens require `sid`. Pre-upgrade tokens without `sid` fail until clients re-login, or apps register a stateless verifier override (`session_qry=None`).
- **Authn (breaking):** `ApiKeyLifecyclePort.revoke_api_key` and `revoke_many_api_keys` take `identity: AuthnIdentity`.
- **Authn (breaking):** `issue_api_key` no longer requires a pre-existing API key row; requires an active policy principal.
- **Authn (breaking):** `AuthnOrchestrator` requires `PrincipalEligibilityPort`; apps must wire tenant-unaware `authz_policy_principals` document routes alongside authn.

### Added

- **Outbox:** `forze.application.contracts.outbox` (`OutboxSpec`, `IntegrationEvent`, `OutboxCommandPort`, `OutboxQueryPort`, `OutboxDeps`); request-scoped staging via `OutboxStagingContext` and `OutboxStaging` (`forze.application.integrations.outbox`); `outbox_flush_tx_on_success_factory` and `relay_outbox_to_queue` in `forze_kits.integrations.outbox`; `PostgresOutboxAdapter` / `PostgresOutboxConfig` on `PostgresDepsModule`; `MockOutboxAdapter` on `MockDepsModule`.
- **`forze.base`:** `GuardedLruRegistry` / `SimpleLruRegistry` detect reentrant `create` (fail fast instead of deadlocking on `init_lock`); bounded wait when a slot stays in the draining set; optional `timeout` on `InflightLane.run` / `CachedInflightLane`.
- **Document coordinators:** `max_scan_pages`, `max_stream_pages`, and `max_chunked_command_pages` (default 100_000 each; set `None` for unlimited); cursor stream stall detection when `next_cursor` does not change.
- **`forze_identity.authz`:** `fetch_all_document_hits` accepts `max_pages` (default 100_000).
- **Tests:** Lifecycle unit tests for Redis, Mongo, S3, GCS, BigQuery, ClickHouse, Firestore, Inngest, Meilisearch, and Postgres pool hooks (startup/shutdown and routed client steps).
- **Tests:** `tests/README.md`, root `tests/integration/conftest.py`, `tests/support/docker.py`, `tests/support/secrets_fixtures.py`, shared `tests/support/scenarios/document_nested_filters.py`, FastAPI integration smoke test, and split `test_pg_search_hub.py` from hub search cases.
- **Tests:** Hypothesis and parametrized cases for scrubbing, mock query matching, nested query paths, and Redis client errors.
- **Tests:** Unit coverage for authn composition (facades, factories, operations), authn handlers and DTOs, execution graph waves, `DepsResolutionTrace`, dry-run tracing re-exports, `TenancyDeps`, and document default mapper configuration errors.
- **Tests:** Coverage for Postgres DSN fingerprinting, Meilisearch client errors and filter rendering, Mongo document index lifecycle, identity API-key revoke/deactivation adapters, Temporal schedule bootstrap, resolution spec coercion, and operation-registry mutations; integration cases for Meilisearch `$in`/`$or` filters, Postgres API-key revoke, and Mongo index validation startup.
- **Tests:** Integration coverage for routed Firestore, GCS, and BigQuery clients (CRUD, secrets, LRU eviction); extended Meilisearch search/federated/filter integration; Firestore query `$or`/`$neq` filters.
- **Tests:** Unit coverage for Redis base adapter key construction, Mongo search page materialization, Firestore gateway base and query render, Temporal schedule adapter, distributed-lock extend loop, Meilisearch federated zero-weight and member resolution, Postgres analytics cursor dry-run/keyset paths, directed-acyclic-graph helpers, and routed Temporal client delegation; BigQuery analytics `project_run` / backward-cursor integration cases.
- **Durable workflow:** `DurableWorkflowRunStatus`, `DurableWorkflowRunDescription`, and `describe()` on `DurableWorkflowQueryPort` for coarse run lifecycle; `forze_temporal` maps Temporal `WorkflowHandle.describe()`.
- **Application contracts:** `RelationSpec`, `NamedResourceSpec`, coercion and `require_static_*` helpers in `forze.application.contracts.resolution`; `warn_dynamic_relation_with_tenant_aware` and `validate_routed_client_tenancy_wiring` in `forze.application.contracts.tenancy`.
- **Mongo, Firestore:** `RelationSpec` on document (`read` / `write` / `history`) and Mongo search `read` / `index_name`; gateways resolve collections per request; index validation skips dynamic write relations.
- **BigQuery, ClickHouse:** analytics `ingest_relation` with legacy `dataset`/`database` + `ingest_table`; ingest adapters resolve per tenant.
- **Redis, SQS, RabbitMQ, Temporal:** `NamedResourceSpec` on integration configs (`RedisUniversalConfig.namespace`, `SQSQueueConfig.namespace`, `RabbitMQQueueConfig.namespace`, `TemporalWorkflowConfig.queue`) with `coerce_named_resource_spec` (including `StrEnum`), per-package `kernel/relation.py` resolvers, adapter resolution before key/queue naming, and deps-module warnings when combined with `tenant_aware=True`.
- **Core:** `SearchCommandPort` (`ensure_index`, `upsert`, `upsert_many`, `delete`, `delete_all`) for external search index maintenance.
- **Meilisearch:** `forze_meilisearch` package with async client, `SearchQueryPort` (offset), `SearchCommandPort`, and federated search (`merge`: native federation or weighted RRF). Optional extra `forze[meilisearch]`.
- **Search snapshots:** `SearchResultSnapshotCoordinator.federated_fingerprint` accepts optional `extras` for merge-mode-specific cache keys.
- **Mongo:** `MongoDepsModule.searches` and `SearchQueryPort` adapters (`MongoTextSearchAdapter`, `MongoAtlasSearchAdapter`, `MongoVectorSearchAdapter`) with offset, cursor, and optional Redis result snapshots.
- **Mongo:** optional `mongo_document_index_validation_lifecycle_step` warns when write collections define secondary unique indexes used with `ensure` / `upsert`.
- **`forze.base`:** `CacheLane` in `forze.base.primitives.cache` — reusable in-memory TTL/FIFO cache for catalog metadata.
- **`forze.base`:** `SimpleLruRegistry` and `GuardedLruRegistry` in `forze.base.primitives.lru_registry` — async LRU resource caches with optional in-use guarded eviction.
- **`forze.base`:** `InflightLane` in `forze.base.primitives` — asyncio singleflight coalescing for concurrent cache misses.
- **Application contracts:** `require_tenant_id`, `parse_tenant_hint`, `coalesce_tenant_request_hints`, and `TENANT_ID_HEADER` in `forze.application.contracts.tenancy`; `secret_ref_for_tenant` and `resolve_str_for_tenant` in `forze.application.contracts.secrets`.
- **Application execution:** `routed_client_lifecycle_step` and `RoutedClientLifecycle` protocol for tenant-routed integration clients.
- **Application execution:** `LifecycleModule`, `LifecyclePlan.from_modules` / `with_modules` / `freeze()` — merge lifecycle modules and topologically order steps via `requires`, `provides`, and `depends_on` (same graph model as operation hooks; ordering only, no capability skip).
- **Application execution:** `FrozenLifecyclePlan`, `ResolvedLifecyclePlan`, and `LifecyclePlan.with_concurrent()` — lifecycle freeze/resolve/run pipeline aligned with operation plans; optional concurrent execution within the same topological wave.
- **Application execution:** `FrozenDepsRegistry`, `FrozenDeps`, and authoring `DepsRegistry` (`freeze()` / `resolve()`) — dependency registration (`Deps`) is split from per-scope resolution (`FrozenDeps` on `ExecutionContext`).
- **Application execution:** dependency route typing uses `StrKey` directly; `Deps`, `FrozenDeps`, `ProviderStore`, and `DepsModule` are no longer generic over a route type parameter.
- **Postgres:** `PostgresLifecycleModule` (pool, optional catalog warmup, optional document schema validation); lifecycle step factories set `postgres.client` capability metadata for declarative ordering.
- **Application contracts:** `TenantAwareIntegrationConfig` in `forze.application.contracts.tenancy` — shared frozen `tenant_aware` flag for integration wiring configs (distinct from runtime `TenancyMixin`).
- **Application contracts:** `forze.application.contracts.resolution` — `ValueResolver`, `MaybeAwaitable`, and `resolve_value` for static or tenant-scoped async/sync resolution.
- **`forze.base`:** `stable_fingerprint` and `connection_string_fingerprint` in `forze.base.primitives.fingerprint`; optional `dedup_key` on `SimpleLruRegistry` / `GuardedLruRegistry` to share slots across logical keys.
- **Postgres:** `RelationSpec` on document (`read` / `write` / `history`), search (`index` / `read` / `heap`), hub (`hub` plus leg `index` / `read` / `heap`), and analytics (`ingest_relation`) configs; gateways and adapters resolve relations per request via `forze.application.contracts.resolution`.
- **S3, GCS, Meilisearch:** `NamedResourceSpec` on `S3StorageConfig.bucket`, `GCSStorageConfig.bucket`, and `MeilisearchSearchConfig.index_uid` (with `coerce_named_resource_spec`); storage/search adapters resolve per request; deps modules warn when `tenant_aware=True` combines with dynamic resolvers.
- **Postgres:** public exports `RelationSpec`, `coerce_relation_spec`, and `require_static_relation` from `forze_postgres`.
- **Redis:** public exports `NamedResourceSpec`, `coerce_named_resource_spec`, and `resolve_redis_namespace` from `forze_redis`.
- **S3:** public exports `NamedResourceSpec`, `coerce_named_resource_spec`, `is_static_named_resource`, and `resolve_s3_bucket` from `forze_s3`.
- **SQS:** public exports `NamedResourceSpec`, `coerce_named_resource_spec`, and `resolve_sqs_namespace` from `forze_sqs`.
- **Mongo:** public exports `NamedResourceSpec`, `RelationSpec`, `coerce_named_resource_spec`, `coerce_relation_spec`, `is_static_relation`, `relations_match`, `resolve_mongo_collection`, and `resolve_mongo_named_resource` from `forze_mongo`.
- **RabbitMQ:** public exports `NamedResourceSpec`, `coerce_named_resource_spec`, and `resolve_rabbitmq_namespace` from `forze_rabbitmq`.
- **BigQuery:** public exports `RelationSpec`, `coerce_relation_spec`, and `resolve_bigquery_ingest_target` from `forze_bigquery`.
- **ClickHouse:** public exports `RelationSpec`, `coerce_relation_spec`, and `resolve_clickhouse_ingest_target` from `forze_clickhouse`.
- **GCS:** public exports `NamedResourceSpec`, `coerce_named_resource_spec`, `is_static_named_resource`, and `resolve_gcs_bucket` from `forze_gcs`.
- **Firestore:** public exports `RelationSpec`, `coerce_relation_spec`, `is_static_relation`, `relations_match`, and `resolve_firestore_collection` from `forze_firestore`.
- **Meilisearch:** public exports `NamedResourceSpec`, `coerce_named_resource_spec`, `is_static_named_resource`, and `resolve_meilisearch_index_uid` from `forze_meilisearch`.
- **Postgres:** startup warning when a route combines `tenant_aware=True` with dynamic `RelationSpec` resolvers (prefer relation-level isolation without row filters).
- **Postgres:** `require_static_relation` — explicit error when startup document schema validation is wired for a route that uses a dynamic `RelationSpec` resolver.
- **BigQuery, ClickHouse, Meilisearch, GCS, Firestore, Inngest:** `Routed*Client` with per-tenant `*RoutingCredentials`, `routed_*_lifecycle_step`, and LRU pool deduplication by connection fingerprint.
- **OIDC:** `OidcTokenVerifier.enforce_issuer_and_audience` — opt-in construction guard requiring `issuer` and `audience` (recommended for production app factories).

### Changed

- **Application execution:** operation registry, planning, facade, and run modules moved under `forze.application.execution.operations` (`operations.registry`, `operations.planning`, `operations.facade`, `operations.run`). Lifecycle wave execution lives in `forze.application.execution.lifecycle.run`; shared graph wave helpers in `forze.application.execution.graph_run`. `steps_graph_from_sequence` uses registration order as a tie-break when step priorities are equal.
- **Application coordinators:** `DocumentCoordinator` moved to `forze.application.coordinators.document` (query/command/pagination mixins); `DocumentReadGatewayPort` and `DocumentWriteGatewayPort` live in `forze.application.contracts.document`. Public import `from forze.application.coordinators import DocumentCoordinator` is unchanged.
- **Application execution:** `LifecyclePlan.build()` now returns `FrozenLifecyclePlan` (deprecated alias for `freeze()`); `startup` / `shutdown` run on frozen or resolved plans, not on the authoring plan.
- **Application execution:** `DepsPlan` renamed to `DepsRegistry`; `DepsRegistry.build()` is deprecated in favor of `freeze()` then `FrozenDepsRegistry.resolve()`. The former `DepsRegistry` provider map is internal `ProviderStore`. Registration `Deps` no longer supports `provide` / `resolve_*`; use `FrozenDeps` via freeze/resolve.
- **Mongo:** `MongoSearchConfig` uses a single `index_name` (semantics depend on `engine`) instead of separate `atlas_index_name` / `vector_index_name` / `text_index_name` keys.
- **Postgres:** reorganized `forze_postgres.kernel` into `kernel.client`, `kernel.catalog`, and `kernel.sql`; `PostgresIntrospector` uses `CacheLane`. Direct imports of `forze_postgres.kernel.platform`, `forze_postgres.kernel.introspect`, `forze_postgres.kernel.query`, `forze_postgres.pagination`, and related flat kernel modules must be updated to the new paths.
- **Application contracts:** `TenantClientRegistry`, `resolve_dsn_for_tenant`, `ensure_dsn_fingerprint`, `resolve_structured_for_tenant`, and `ensure_structured_fingerprint` in `forze.application.contracts.tenancy` — shared pool and secret-resolution helpers for routed integration clients.
- **Postgres, Redis, Mongo, SQS, S3, GCS, Temporal, RabbitMQ, Firestore, BigQuery, ClickHouse, Meilisearch, Inngest:** all `Routed*Client` implementations use `TenantClientRegistry` and the tenancy helpers above (no public constructor API change; misconfiguration errors may reference `max_entries` or `Tenant client registry is not started`).
- **Postgres:** `PostgresIntrospector` uses `InflightLane` for catalog singleflight (no public API change).
- **Postgres, Redis, Mongo, SQS, S3, Temporal, RabbitMQ:** routed clients and lifecycle steps use shared tenancy/secrets helpers and generic routed lifecycle hooks internally (no public API change).
- **Postgres, Redis, Mongo, SQS, S3, Temporal, RabbitMQ:** routed clients deduplicate LRU pools by connection fingerprint when multiple tenants resolve to the same backend target.
- **Postgres:** internal reorganisation of `forze_postgres.adapters.search` (shared port/cursor/offset base for FTS, vector, and PGroonga; `hub` subpackage). Public imports from `forze_postgres.adapters.search` are unchanged.
- **Postgres:** internal reorganisation of `forze_postgres.adapters.analytics` (package split with shared port/query/cursor/chunked modules); analytics SQL helpers moved from `forze_postgres.kernel.client` to `forze_postgres.kernel.sql`. Public import `forze_postgres.adapters.analytics.PostgresAnalyticsAdapter` is unchanged.
- **Analytics:** shared offset/keyset cursor token helpers in `forze.application.contracts.analytics._adapter_common` (used by Postgres and ClickHouse adapters).
- **Postgres:** internal reorganisation of `forze_postgres.execution` (`lifecycle/` subpackage; `execution.deps.configs` and `execution.deps.factories` subpackages). Public imports from `forze_postgres` are unchanged. Direct imports of removed modules (`forze_postgres.execution.deps.deps`, `forze_postgres.execution.deps.configs` as a single file, top-level `forze_postgres.execution.catalog_warmup`, `forze_postgres.execution.document_schema`, and `forze_postgres.execution.lifecycle` as a module file) must use `forze_postgres.execution.deps`, `forze_postgres.execution.lifecycle`, or the `forze_postgres` package root instead.
- **Redis:** reorganized `forze_redis.kernel` into `kernel.client`; direct imports of `forze_redis.kernel.platform` must use `forze_redis.kernel.client` (or package root).
- **Redis:** internal reorganisation of `forze_redis.execution` (`lifecycle/` subpackage; `execution.deps.configs` and `execution.deps.factories` subpackages). Public imports from `forze_redis` are unchanged. Direct imports of `forze_redis.execution.deps.deps` or `forze_redis.execution.lifecycle` as a module file must use `forze_redis.execution.deps`, `forze_redis.execution.lifecycle`, or the package root.
- **S3 / SQS:** reorganized `kernel` into `kernel.client`; direct imports of `forze_s3.kernel.platform` / `forze_sqs.kernel.platform` must use `kernel.client` (or package root).
- **S3 / SQS:** internal reorganisation of `execution` (`lifecycle/` subpackage; `execution.deps.configs` and `execution.deps.factories`). Public imports from `forze_s3` / `forze_sqs` unchanged. Direct imports of `execution.deps.deps` or top-level `execution.lifecycle` module files must use `execution.deps`, `execution.lifecycle`, or the package root.
- **Mongo / RabbitMQ:** reorganized `kernel` into `kernel.client`; direct imports of `forze_mongo.kernel.platform` / `forze_rabbitmq.kernel.platform` must use `kernel.client` (or package root).
- **Mongo / RabbitMQ:** internal reorganisation of `execution` (`lifecycle/` subpackage; `execution.deps.configs` and `execution.deps.factories`). Public imports from `forze_mongo` / `forze_rabbitmq` unchanged. Direct imports of `execution.deps.deps` or top-level `execution.lifecycle` module files must use `execution.deps`, `execution.lifecycle`, or the package root.
- **BigQuery / ClickHouse:** reorganized `kernel` into `kernel.client`; direct imports of `forze_bigquery.kernel.platform` / `forze_clickhouse.kernel.platform` must use `kernel.client` (or package root).
- **BigQuery / ClickHouse:** internal reorganisation of `execution` (`lifecycle/` subpackage; `execution.deps.configs` and `execution.deps.factories`). Public imports from `forze_bigquery` / `forze_clickhouse` unchanged. Direct imports of `execution.deps.deps` or top-level `execution.lifecycle` module files must use `execution.deps`, `execution.lifecycle`, or the package root.
- **GCS / Firestore:** reorganized `kernel` into `kernel.client`; direct imports of `forze_gcs.kernel.platform` / `forze_firestore.kernel.platform` must use `kernel.client` (or package root).
- **GCS / Firestore:** internal reorganisation of `execution` (`lifecycle/` subpackage; `execution.deps.configs` and `execution.deps.factories`). Public imports from `forze_gcs` / `forze_firestore` unchanged. Direct imports of `execution.deps.deps` or top-level `execution.lifecycle` module files must use `execution.deps`, `execution.lifecycle`, or the package root.
- **Temporal / Inngest:** reorganized `kernel` into `kernel.client`; direct imports of `forze_temporal.kernel.platform` / `forze_inngest.kernel.platform` must use `kernel.client` (or package root).
- **Temporal / Inngest:** internal reorganisation of `execution` (`lifecycle/` subpackage; `execution.deps.configs` and `execution.deps.factories`). Public imports from `forze_temporal` / `forze_inngest` unchanged. Direct imports of `execution.deps.deps` or top-level `execution.lifecycle` module files must use `execution.deps`, `execution.lifecycle`, or the package root.
- **Meilisearch / Vault:** reorganized `kernel` into `kernel.client`; direct imports of `forze_meilisearch.kernel.platform` / `forze_vault.kernel.platform` must use `kernel.client` (or package root).
- **Meilisearch:** internal reorganisation of `execution` (`lifecycle/` subpackage; `execution.deps.configs` and `execution.deps.factories`); expanded package-root exports. Direct imports of `forze_meilisearch.execution.deps.deps` or top-level `execution.lifecycle` module files must use `execution.deps`, `execution.lifecycle`, or the package root.
- **Vault:** internal reorganisation of `execution` (`lifecycle/` subpackage). Public imports from `forze_vault` unchanged. Direct imports of top-level `forze_vault.execution.lifecycle` as a module file must use `execution.lifecycle` or the package root.
- **Inngest:** `InngestConfig` is a frozen attrs class; `request_timeout` is `timedelta | None` (replaces `request_timeout_ms` on config and `InngestRoutingCredentials` JSON).
- **S3 / SQS:** `S3Config` and `SQSConfig` are frozen attrs classes with `to_aio_config()`; `S3Head`, `GCSHead`, `GCSListedObject`, `SQSQueueMessage`, and `RabbitMQQueueMessage` are frozen attrs types (constructors required; dict literals no longer accepted for configs).
- **BigQuery / ClickHouse:** client port `timeout` parameters use `timedelta | None`.
- **ClickHouse:** `ClickHouseConfig.keepalive_timeout` is `timedelta`.
- **Identity OIDC:** `JwksKeyProvider.timeout` and `cache_ttl` use `timedelta`.
- **Postgres:** `Postgres*Config` integration wiring is now frozen `attrs` classes (constructors required; dict literals no longer accepted). `tenant_aware` is inherited from `TenantAwareIntegrationConfig`. Federated members use `PostgresFederatedSearchLegSearch` / `PostgresFederatedSearchLegHub` instead of embedded dict shape detection. Removed module-level `validate_pg_search_conf`, `validate_postgres_hub_search_conf`, and `validate_postgres_federated_search_conf` (validation runs at config construction or via `.validate()` / `validate_against_spec`).
- **Integrations:** `Mongo*Config`, `Firestore*Config`, `Meilisearch*Config`, `ClickHouse*Config`, `BigQuery*Config`, `Redis*Config`, `S3StorageConfig`, `GCSStorageConfig`, `TemporalWorkflowConfig`, `RabbitMQQueueConfig`, `SQSQueueConfig`, and `InngestEventConfig` are frozen `attrs` classes (constructors required; dict literals no longer accepted). `tenant_aware` uses `TenantAwareIntegrationConfig` where applicable. Removed `validate_mongo_search_conf`, `validate_meilisearch_search_conf`, `validate_meilisearch_federated_search_conf`, `validate_clickhouse_analytics_config`, and `validate_bigquery_analytics_config` from public exports (validation on config types).
- **`forze.base`:** `frozen_mapping` in `forze.base.primitives` for immutable nested maps on integration configs.

### Removed

- **Application execution:** `forze.application.execution.registry`, `planning`, `facade`, and `running` subpackages; `OperationRunner`; `lifecycle_graph_from_sequence` (use `steps_graph_from_sequence` from `forze.application.contracts.execution`).
- **Postgres:** `validate_pg_search_conf`, `validate_postgres_hub_search_conf`, `validate_postgres_federated_search_conf`, and `is_postgres_federated_embedded_hub_config` from the public API (use config constructors and instance validation instead).
- **Postgres:** dict/mapping coercion for `ConfigurablePostgresDocument` and `ConfigurablePostgresReadOnlyDocument` (use `PostgresDocumentConfig` / `PostgresReadOnlyDocumentConfig` constructors).
- **Redis:** `forze_redis.execution.deps.deps` module path (use `forze_redis.execution.deps` or `execution.deps.factories`).
- **S3 / SQS:** `forze_s3.execution.deps.deps` and `forze_sqs.execution.deps.deps` module paths (use `execution.deps` or `execution.deps.factories`).
- **Mongo / RabbitMQ:** `forze_mongo.execution.deps.deps` and `forze_rabbitmq.execution.deps.deps` module paths (use `execution.deps` or `execution.deps.factories`).
- **BigQuery / ClickHouse:** `forze_bigquery.execution.deps.deps` and `forze_clickhouse.execution.deps.deps` module paths (use `execution.deps` or `execution.deps.factories`).
- **GCS / Firestore:** `forze_gcs.execution.deps.deps` and `forze_firestore.execution.deps.deps` module paths (use `execution.deps` or `execution.deps.factories`).
- **Temporal / Inngest:** `forze_temporal.execution.deps.deps` and `forze_inngest.execution.deps.deps` module paths (use `execution.deps` or `execution.deps.factories`).
- **Meilisearch:** `forze_meilisearch.execution.deps.deps` module path (use `execution.deps` or `execution.deps.factories`).
- **Meilisearch / Vault:** `forze_meilisearch.kernel.platform` and `forze_vault.kernel.platform` module paths (use `kernel.client` or package root).
- **Inngest:** dict-literal / TypedDict `InngestConfig`; `request_timeout_ms` field names on Inngest types and routing credentials JSON.
- **S3 / SQS:** TypedDict `S3Config` and `SQSConfig`; dict-literal construction for those configs.
- **Integrations:** `validate_mongo_search_conf`, `validate_meilisearch_search_conf`, `validate_meilisearch_federated_search_conf`, `validate_clickhouse_analytics_config`, and `validate_bigquery_analytics_config` from public exports.

### Documentation

- **Multi-tenancy:** document `RelationSpec` exclusions—author-defined analytics query SQL, Inngest/Mock boundaries, and layering routed clients vs relation-level resolvers; analytics query tenancy on Postgres, BigQuery, and ClickHouse integration pages.
- **Concepts and execution reference:** align lifecycle (`LifecycleStep.id`, routed client lifecycle), handler examples (`DocumentIdDTO`, `GetDocument`), `SearchCommandPort` / `ctx.search.command`, document kernel ops table, multi-tenancy helpers, and layered-architecture package examples with current APIs.

### Fixed

- **`forze_fastapi`:** tenant resolution honors JWT/OIDC `issuer_tenant_hint` and `X-Tenant-Id` via `TenantResolverPort.requested_tenant_id`; hint-only fallback when no tenant resolver is registered (`parse_tenant_hint`, `coalesce_tenant_request_hints` in `forze.application.contracts.tenancy`).
- **`forze.base`:** `connection_string_fingerprint` includes sorted URI query parameters so routed-client LRU dedup distinguishes targets such as Temporal gRPC hosts with `?dedup=` test hints.
- **Meilisearch:** federated search awaits snapshot finalization; `ensure_index` and multi-search use `meilisearch-python-sdk` models (`SearchParams.federation_options`, `Federation`). Requires `meilisearch-python-sdk>=7.2.1`.
- **Postgres:** `ensure`, `ensure_many`, `upsert`, and `upsert_many` build `ON CONFLICT` from `PostgresDocumentConfig.conflict_target` or inferred primary-key columns (fixes composite PKs and tables with additional UNIQUE indexes).
- **Mongo:** `ensure_many` and `upsert_many` classify bulk `$setOnInsert` upserts safely; missing rows after bulk upsert raise `mongo_ensure_bulk_miss` conflict instead of a generic not-found.

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
