# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Postgres (`forze_postgres`):** JSON/JSONB dot-path filters and sort keys infer value coercion types from parameterized ``dict[str, V]`` / ``Mapping[str, V]`` on the read model (one path segment per dynamic object key, then types from ``V``, including nested ``BaseModel`` fields and nested string-keyed mappings). Non-string mapping key parameters (for example ``dict[int, …]``) raise a clear error. ``nested_field_hints`` remains for bare ``dict``, ``Any``, ambiguous unions, paths that end on a mapping without a key segment, and per-path overrides.

- **Breaking (`forze.application.execution`):** Middleware placement is modeled by :class:`~forze.application.execution.bucket.Phase`, :class:`~forze.application.execution.bucket.Slot`, and the 11-member :class:`~forze.application.execution.bucket.BucketKey` enum (canonical ``label`` strings such as ``outer_before`` match the former flat ``Bucket`` values). :class:`~forze.application.execution.plan.OperationPlan` stores specs in ``buckets: dict[BucketKey, tuple[MiddlewareSpec, …]]`` with ``add`` / ``build`` / ``specs_for_chain`` taking ``BucketKey`` only. Removed ``Bucket``, ``BucketMeta``, ``BUCKET_REGISTRY``, ``ALL_BUCKETS``, ``CAPABILITY_SCHEDULABLE_BUCKETS``, ``DISPATCH_EDGE_BUCKETS``, ``coerce_bucket``, ``iter_capability_schedulable_buckets``, and ``middleware_specs_for_usecase_tuple``; use ``BucketKey.iter_capability_segments()`` and ``OperationPlan.specs_for_chain`` instead. :class:`~forze.application.execution.plan_kinds.StepExplainKind` and :class:`~forze.application.execution.plan_kinds.ScheduleMode` are ``StrEnum``\ s; synthetic tx rows use :data:`~forze.application.execution.plan_kinds.STEP_EXPLAIN_TX_BUCKET`. :class:`~forze.application.execution.capabilities.CapabilityChainBuilder` / :class:`~forze.application.execution.capabilities.LegacyChainBuilder` ``build(plan)`` take the merged ``OperationPlan`` only. :class:`~forze.application.execution.plan.UsecasePlan` adds :meth:`~forze.application.execution.plan.UsecasePlan.add_step` and :meth:`~forze.application.execution.plan.UsecasePlan.add_pipeline` as canonical writers; ``SchedulableCapabilitySpec`` is a type alias of :class:`~forze.application.execution.plan.MiddlewareSpec`.

- **Redis (`forze_redis`):** `RedisClient.mset` with ``ex`` / ``px`` / ``nx`` / ``xx`` now runs a single atomic Lua script (all keys succeed or none; rollback on partial ``NX``/``XX`` failure) instead of a non-atomic ``MULTI`` pipeline of per-key ``SET`` calls that always returned ``True``.
- **Redis (`forze_redis`):** `RedisClientPort` / `RedisClient` / `RoutedRedisClient` now expose ``get`` / ``mget`` as ``bytes | None`` (pool uses ``decode_responses=False``). Added ``pttl_raw_ms`` for full Redis ``PTTL`` semantics (``-1`` / ``-2`` vs remaining ms). ``RedisSearchResultSnapshotAdapter.append_chunk`` uses a Lua compare-and-swap script for meta + chunk writes. ``RedisConfig`` supports read retries, optional pub/sub auto-reconnect with hooks, and chunked ``mget`` for large key sets. ``pubsub_auto_reconnect`` defaults to ``False`` so ``subscribe`` matches the original single-session behavior unless opted in.

### Fixed

- **Postgres (`forze_postgres`):** `PostgresClient.transaction` applies ``read_only`` via ``set_read_only`` before opening the psycopg transaction context, matching driver rules (avoids ``can't change 'read_only' now`` during ``connection.transaction()``).
- **Redis (`forze_redis`):** `RedisClient.run_script` normalizes script results without ``isinstance`` on primitive unions (uses ``inspect.isawaitable`` in a bounded loop, then ``is`` / ``type(...) is ...`` for ``bool`` / ``int`` / ``bytes`` / ``bytearray``), avoiding rare ``TypeError: isinstance() arg 2 must be a type...`` failures when resolving ``AsyncScript`` / pipeline results.

### Added

- **Execution (`forze.application.execution`):** optional capability-driven ordering for guards and effects via `UsecasePlan(use_capability_engine=True)` / `with_capability_engine()`, `MiddlewareSpec.requires` / `provides` / `step_label`, `schedule_capability_specs`, `CapabilityStore`, `CapabilitySkip` / `GuardSkip`, `UsecasePlan.explain`, `ExecutionPlanReport` / `StepExplainRow`, and stable keys (`AUTHN_PRINCIPAL`, `TENANCY_TENANT`, `authz_permits_capability`). See [Capability execution](reference/capability-execution.md).
- **Execution (`forze.application.execution`):** `GuardStep` / `EffectStep` for capability-aware pipeline composition; `UsecasePlan.merged_operation_plan`; `UsecasePlan.resolve(..., capability_execution_trace=...)` with `CapabilityExecutionEvent` records; expanded `explain` rows (`kind`, `schedule_mode`, `dispatch_edge_count`) in resolve order; `UsecaseRegistry` capability validation at `finalize` when the engine is enabled plus optional `strict_capability_middleware_without_engine`; `Iterable[str | CapabilityKey]` accepted for capability fields on plan builders.
- **Application guards:** `authz_permission_capability_keys` for wiring `AuthzPermissionGuard` with the capability engine.
- **Application guards:** `authn_principal_capability_guard_factory` (`forze.application.guards.authn`) for optional `AUTHN_PRINCIPAL` presence wiring with `CapabilitySkip`.
- **Redis (`forze_redis`):** `RedisConfig.read_retry_attempts`, ``read_retry_base_delay``, ``pubsub_auto_reconnect``, ``pubsub_reconnect_max_delay``, ``on_read_retry``, ``on_pubsub_reconnect`` for resilience and observability. ``RedisBlockingClientDepKey`` is re-exported from ``forze_redis`` when using the package root imports.
- **Authn composition layer:** `forze.application.composition.authn` mirrors the document/search shape with `AuthnOperation` (`PASSWORD_LOGIN`, `REFRESH_TOKENS`, `LOGOUT`, `CHANGE_PASSWORD`), `AuthnUsecasesFacade`, and `build_authn_registry(spec)`. New usecases `AuthnLogout` and `AuthnChangePassword` (in `forze.application.usecases.authn`) pull the bound identity from the execution context and delegate to `TokenLifecyclePort` / `PasswordLifecyclePort`. New `AuthnChangePasswordRequestDTO` (`forze.application.dto.authn`).
- **`forze_fastapi.endpoints.authn`:** pre-built FastAPI endpoints (`attach_authn_endpoints`) for password login, refresh, logout, and change-password. Per-token transports configurable via `HeaderTokenTransportSpec` / `CookieTokenTransportSpec`; password login defaults to `application/x-www-form-urlencoded` body. New HTTP features `TokenTransportInputFeature` (reads refresh token from cookie or header) and `TokenTransportOutputFeature` (issues / clears cookies, strips tokens from body when transport is cookie). `attach_authn_endpoints` auto-applies an `AuthnRequirement` to logout/change-password derived from the configured access transport when the caller does not override it.
- **`AuthnRequirement` value object** (`forze_fastapi.endpoints.http.AuthnRequirement`) declares per-route auth requirements with mutually exclusive transports (`token_header` / `token_cookie` / `api_key_header`). Plumbed into `SimpleHttpEndpointSpec[\"authn\"]`; `apply_authn_requirement(spec, requirement)` in `forze_fastapi.endpoints.http.policy` prepends `RequireAuthnFeature` and merges OpenAPI security scheme + operation-level `security` requirement onto `metadata.openapi_extra`. Document and search attach helpers honor the new field.
- **`DocumentEndpointsSpec` / `SearchEndpointsSpec` base authn:** new top-level `authn: AuthnRequirement` key applies the requirement to every produced endpoint at once. Per-endpoint values in `SimpleHttpEndpointSpec.authn` override the base on the matching route only.
- **`build_authn_requirement_dependency`** (`forze_fastapi.endpoints.http.policy`, re-exported from `forze_fastapi.endpoints.http`): turns an `AuthnRequirement` into a FastAPI `Depends(...)` that enforces a bound `AuthnIdentity` and surfaces the matching `HTTPBearer` / `APIKeyCookie` / `APIKeyHeader` security scheme in OpenAPI. Use it to attach a consistent authn surface to hand-rolled `APIRouter(dependencies=[...])` routes alongside Forze-built endpoints.
- **OpenAPI security helpers:** `openapi_api_key_header_scheme` joins the existing bearer / cookie helpers in `forze_fastapi.openapi.security`.
- **Authn (verify-then-resolve seam):** `VerifiedAssertion` value object (`forze.application.contracts.authn.value_objects.assertion`) carries vendor-flavored proof from a verifier to a resolver. New ports `PasswordVerifierPort` / `TokenVerifierPort` / `ApiKeyVerifierPort` (`ports.verification`) and `PrincipalResolverPort` (`ports.resolution`) with matching dep keys (`PasswordVerifierDepKey`, `TokenVerifierDepKey`, `ApiKeyVerifierDepKey`, `PrincipalResolverDepKey`).
- **`forze_authn` reference verifiers / resolvers / orchestrator:** `Argon2PasswordVerifier`, `ForzeJwtTokenVerifier`, `HmacApiKeyVerifier`; `JwtNativeUuidResolver` (default; trusts subject as UUID), `MappingTableResolver` (document-backed `(issuer, subject) -> principal_id` registry with optional just-in-time provisioning, `IDENTITY_MAPPINGS` document spec), `DeterministicUuidResolver` (stateless `uuid4((issuer, subject))`); `AuthnOrchestrator` enforces `AuthnSpec.enabled_methods` and composes the wired verifiers + resolver. `Configurable*Verifier` / `Configurable*Resolver` factories register them under the new dep keys via `AuthnDepsModule(authn={"main": frozenset({"token", "password"})})`.
- **`forze_oidc` (new package, `oidc` extra):** generic `OidcTokenVerifier` (RS256/ES256/HS256), pluggable `SigningKeyProviderPort` with `JwksKeyProvider` (PyJWKClient cached) and `StaticKeyProvider`, configurable `OidcClaimMapper` (default `iss`/`sub`/`aud`/`iat`/`exp`; tenant claim overridable). Reference for plugging Firebase / Casdoor / Auth0 IdPs into the new verifier-port seam without touching core contracts.
- **FastAPI:** configurable credential order and ambiguity handling on `ContextBindingMiddleware` via a sequence of `AuthnIdentityResolverPort` implementations (`HeaderTokenAuthnIdentityResolver`, `CookieTokenAuthnIdentityResolver`, `HeaderApiKeyAuthnIdentityResolver`) and `when_multiple_credentials`; HTTP security endpoint features (`RequireAuthnFeature`, `RequirePermissionFeature`, `RequireTenantFeature`); `default_http_features` on document/search attach helpers; `merge_http_endpoint_features` / `with_default_http_features` in `forze_fastapi.endpoints.http.policy`; OpenAPI helpers in `forze_fastapi.openapi.security` (including `openapi_operation_security`); optional `attach_oauth2_password_token_template_routes` with `OAuth2TokenJsonResponse` for JSON-serializable token responses; `HttpMetadataSpec` supports `dependencies`, `openapi_extra`, `responses`, and `include_in_schema` on routes registered via `attach_http_endpoint`.
- **Application guards:** `forze.application.guards` — `AuthzPermissionRequirement`, `AuthzOpRequirementMap`, and `authz_permission_guard_factory` for interface-agnostic `AuthzPort.permits` checks in `UsecasePlan` guard pipelines.
- **Tenancy:** `TenancyDepsModule` (`forze_tenancy.execution`) registers `TenantResolverDepKey` / `TenantManagementDepKey` routes with `ConfigurableTenantResolver` / `ConfigurableTenantManagement`; optional `verify_tenant_active` applies to all resolver routes on the module.
- **Docs:** Added D2 diagrams for dependency resolution, FastAPI requests, document CRUD, cache fallback, and adapter boundaries.
- **Redis:** `RedisDepsModule.dlocks` registers `DistributedLockQueryDepKey` / `DistributedLockCommandDepKey` via `ConfigurableRedisDistributedLock` (`RedisDistributedLockConfig` or `RedisUniversalConfig`). Structural `RedisClientPort`, `RoutedRedisClient`, and `routed_redis_lifecycle_step` (per-tenant DSN via `SecretsPort`, LRU pools).
- **Document:** `DocumentCommandPort.update_matching` / `update_matching_strict` (Postgres fast `UPDATE … RETURNING`, Mongo batched updates + `$inc` on `rev`, strict chunked path); `ensure` / `ensure_many`; `get` / `get_many` optional `skip_cache`; read gateways validate `find_many` rows with `return_model` when used by the coordinator (Postgres, Mongo, mock).
- **Query:** UUID operands on `$gt` / `$gte` / `$lt` / `$lte`; document aggregates with `$fields` / `$computed` (`$count`, `$sum`, `$avg`, `$min`, `$max`, `$median`) and optional `filter` on computed fields; `$time_bucket` with `timezone` (mock, Postgres, Mongo `$dateTrunc` 5.0+); keyset helpers `assert_cursor_projection_includes_sort_keys`, `resolved_cursor_limit`, `assemble_keyset_cursor_page`.
- **Coordinators & tx:** `SearchResultSnapshotCoordinator(store=port)` (fingerprints, snapshot I/O, pagination, weighted RRF, federated pages); `DocumentCacheCoordinator` + `document_cache_coord` (read-through, warm, flush, invalidation, after-commit); `DocumentCoordinator` / `DocumentReadGatewayPort` / `DocumentWriteGatewayPort` (backend-agnostic document query/command wiring for future Mongo/Postgres adapter delegations); `AfterCommitPort`; `ExecutionContext.defer_after_commit` / `run_after_commit_or_now`.
- **Execution:** `ConditionalGuard` / `ConditionalEffect` / `WhenGuard` / `WhenEffect`; usecase dispatch graph validation at `finalize` (`add_dispatch_edge`, plan-derived edges, `*` expansion, re-entrancy guard, `UsecaseDelegate` `map_in` / `map_out`).
- **Secrets:** `SecretRef`, `AsyncSecretsPort`, `AsyncSecretsDepKey`, `resolve_structured`; `SecretNotFoundError`.
- **Routed infrastructure:** structural ports + routed clients + lifecycle hooks for Postgres (`AsyncSecretsPort` DSN), Mongo (`SecretsPort` URI, `database_name_for_tenant`), S3 (`S3RoutingCredentials`), RabbitMQ, SQS (`SQSRoutingCredentials`), Temporal. Postgres introspector optional `cache_partition_key`, optional `cache_ttl`, and `PostgresDepsModule.introspector_cache_partition_key` / `introspector_cache_ttl`. Optional `warm_postgres_catalog`, `postgres_catalog_warmup_lifecycle_step`, `PostgresDocumentSchemaSpec`, `validate_postgres_document_schemas`, `postgres_document_schema_spec_for_binding`, and `postgres_document_schema_validation_lifecycle_step` for catalog warmup and startup schema checks against document relations.
- **Auth / authz / graph:** `forze.application.contracts.authz` (registry, roles, authorization ports); `forze.application.contracts.graph` (specs, ports, validators); `forze_authn` / `forze_authz` document-backed integration packages; FastAPI `AuthnIdentityResolverPort` and single-source resolvers (`HeaderTokenAuthnIdentityResolver`, `CookieTokenAuthnIdentityResolver`, `HeaderApiKeyAuthnIdentityResolver`).
- **Authnz:** `forze_authz` — permission/role/group catalog documents, binding junctions, `AuthzGrantResolver` for effective grants, `AuthzPolicyService` for permit checks, adapters for `forze.application.contracts.authz` ports, and `AuthzDepsModule` / `AuthzKernelConfig` execution wiring.
- **Authnz / Casbin:** optional extra `authnz-casbin` (PyCasbin) and integration package `forze_casbin` (`StaticLinesAdapter`, `build_enforcer_with_static_policies`) as a stepping stone toward document-backed Casbin adapters.
- **Authnz:** `forze_authn.execution` — `AuthnKernelConfig` (secrets + token/password/API-key configs), `build_authn_shared_services`, `AuthnRouteCaps`, and route sets for lifecycle/provisioning DepKeys; `AuthnDepsModule` builds shared services once; configurable factories take `AuthnSharedServices`; wiring validation raises `CoreError`; unit tests and Postgres integration tests (merged document deps).
- **Pagination:** `forze.pagination` base64-JSON v1 cursors and `normalize_sorts_with_id`; Postgres `seek_sql`.
- **Document adapters:** Postgres / Mongo `find_many_with_cursor` (Mongo v1: primary-key sorts only); Postgres PGroonga `search_cursor` accepts empty query for filter-only keyset scan (non-empty ranked query remains on offset search methods).
- **Postgres search:** FTS v2 and PGroonga v2 on index heap + projection relation (`WITH filtered … scored …`); shared `_fts_sql` and `_pgroonga_sql`. Hub: `HubSearchSpec`, `ExecutionContext.hub_search_query`, `PostgresHubSearchConfig` / legs / adapter / leg engines (`PgroongaHubLegEngine`, `FtsHubLegEngine`, `hub_leg_engine_for`), optional `same_heap_as_hub`, member weights (`member_weights`, weight `0` skips leg), dot-separated JSON paths + `nested_field_hints`, `validate_fts_groups_for_search_spec`. Federated: `PostgresFederatedSearchAdapter`, config/deps, weighted RRF via `prepare_federated_search_options`. `ConfigurablePostgresSearch` with `engine: "fts"` builds FTS v2.
- **`forze_fastapi.endpoints.storage`:** `attach_storage_endpoints` registers list (POST JSON), multipart upload, binary download (`GET …/download/{key:path}`), and delete (`DELETE …/delete/{key:path}`, 204) for `StorageUsecasesFacade`. Optional `StorageEndpointsSpec.config` enables upload idempotency (`enable_idempotency`, `idempotency_ttl`). Re-exported from `forze_fastapi.endpoints`.
- **FastAPI:** `attach_http_endpoint` `body_mode: "form"` for multipart (`File` / `Form`).
- Integration tests for routed Postgres, Mongo, Redis, S3, SQS, RabbitMQ, and Temporal platform clients (tenant LRU pools, secrets errors, transactions; Mongo/Redis exercise routed ports across CRUD, aggregation, Redis streams/pub/sub/script/pipeline; S3/SQS JSON routing credentials; RabbitMQ DSN strings; Temporal against time-skipping test server).
- Integration tests for ``forze_authn`` authn adapters against Postgres document stores.
- Unit tests for ``forze_authn`` API key, password, refresh token, and access token services.
- **Tenancy:** optional `AuthnIdentity.tenant_id`; access tokens may carry JWT ``tid``; sessions persist optional `tenant_id` for refresh; FastAPI `TenantIdentityResolver` merges credential tenant, optional `HeaderTenantIdentityCodec`, and `TenantResolverPort` with `strict_tenant_sources`; `TenantManagementPort`, `TenantManagementDepKey`, `ExecutionContext.tenant_management()`; optional `TenantIdentity.tenant_key`.
- **Package:** `forze_tenancy` — `tenant_spec`, `principal_tenant_binding_spec`, `DocumentTenantResolver`, `DocumentTenantManagementAdapter`, `ConfigurableDocumentTenantResolver` / `ConfigurableDocumentTenantManagement`.
- **Authnz:** `AUTHN_TENANT_UNAWARE_DOCUMENT_SPEC_NAMES` in `forze_authn.application` for bootstrap document routes.


### Changed

- **Postgres (`forze_postgres`):** multi-step write paths that previously committed per batch now run in one transaction when the caller has not opened one; `gather_db_work` uses a pool-scoped semaphore (including `RoutedPostgresClient`); default pool checkout timeout is 5 seconds; optional `PostgresConfig` session settings (`statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout`, `application_name`) apply on each pooled connection; some transient `OperationalError` messages map to retryable `ConcurrencyError`; `TooManyConnections` maps to `ConcurrencyError`; optimistic write retries use randomized exponential backoff; nested transaction savepoints use unique names; routed clients take a per-tenant lock during cold pool init and document best-effort `is_in_transaction` reads.
- **Postgres read gateway:** `find_many` / `find_many_aggregates` apply a default `LIMIT` when the caller omits `limit` (configurable via `PostgresGateway.find_many_implicit_limit`, default `10_000`; set to `None` for unbounded); `get` / `find` accept `for_update="nowait"` or `"skip_locked"` in addition to boolean locking.
- **Postgres introspector:** concurrent cold loads for relation kind, column types, and index metadata coalesce to a single catalog query; optional `max_cache_entries_per_kind` trims per-kind cache growth.

- **Document (Postgres/Mongo via `DocumentCoordinator`):** offset ``find_*`` / ``project_*`` / ``select_*`` / ``aggregate_*`` calls that omit ``limit`` in pagination are loaded in sequential chunks sized by the document adapter ``batch_size`` (clamped by ``eff_batch_size``, minimum 10) instead of a single unbounded gateway call. Optional ``batch_size`` on ``PostgresReadOnlyDocumentConfig`` / ``MongoReadOnlyDocumentConfig`` matches read-write semantics (bulk writes and these reads). Mongo ``MongoDepsModule`` read-only slices derived from read-write configs now preserve ``batch_size``. When chunking without caller ``sorts``, the coordinator uses primary key ascending for stable ``OFFSET`` paging (undefined engine order no longer applies across chunks).

- **Breaking (`SearchQueryPort` / Postgres search adapters / `MockSearchAdapter` / built-in search usecases):** Result shape and pagination mode are selected by method name instead of ``return_type`` / ``return_fields`` / ``return_count`` flags on ``search`` / ``search_with_cursor``. Use ``search`` / ``search_page`` / ``search_cursor`` for typed read-model hits; ``project_search`` / ``project_search_page`` / ``project_search_cursor`` for ``JsonDict`` projections (``fields`` first); ``select_search`` / ``select_search_page`` / ``select_search_cursor`` for an explicit ``return_type`` (first argument). Counted offset pages use ``*_page``; countless offset pages use the base names; keyset pages use ``*_cursor``. Federated search still rejects field projection (use ``select_search*`` with a ``return_type``); cursor methods remain unsupported there (``CoreError`` mentions ``search_cursor``).

- **Breaking (`DocumentQueryPort` / `DocumentCoordinator` / `MockDocumentAdapter`):** Query result shape is selected by method name instead of flags on overloaded ``find_many`` / ``get`` / ``get_many`` / ``find_many_with_cursor``. Use ``project`` / ``project_many`` / ``project_page`` / ``project_cursor`` for field projections; ``select`` / ``select_many`` / ``select_page`` for an explicit ``return_type``; ``find_page`` / ``project_page`` / ``select_page`` / ``aggregate_page`` / ``select_page_aggregated`` for offset pages that include a total count; ``find_many`` / ``project_many`` / ``select_many`` / ``aggregate_many`` / ``select_many_aggregated`` for countless offset pages; ``find_cursor`` / ``project_cursor`` for keyset pagination (replacing ``find_many_with_cursor``). ``get`` / ``get_many`` return the read model only. ``DocumentReadGatewayPort`` method names and signatures are unchanged.

- **Breaking (Postgres query renderer):** On native array-typed columns, ``$eq`` / ``$neq`` now compare the column to a list/tuple RHS (exact array equality / inequality) instead of rewriting ``$eq`` to ``$superset``. ``$in`` / ``$nin`` use element-wise membership via ``unnest`` + ``ANY`` instead of rewriting to ``$overlaps`` / ``$disjoint``. Scalar ``$eq`` on array columns now raises with guidance to use ``$null``, ``$superset``, ``$overlaps``, or ``$in``.

- **Breaking (authn value objects):** `TokenCredentials` is split into typed `AccessTokenCredentials` (verifier-consumed, carries `scheme` + optional `profile`) and `RefreshTokenCredentials` (lifecycle-consumed). `OAuth2Tokens` (the input wrapper that forced refresh-only flows to send a half-empty bag) is **removed**; `TokenLifecyclePort.refresh_tokens(refresh_token: RefreshTokenCredentials) -> IssuedTokens` now takes the refresh token directly. `OAuth2TokensResponse` is renamed to `IssuedTokens` (with `access: IssuedAccessToken` always present and `refresh: IssuedRefreshToken | None`); `TokenResponse` is split into `IssuedAccessToken` / `IssuedRefreshToken`; `ApiKeyResponse` is renamed to `IssuedApiKey`. `TokenVerifierPort.verify_token(creds: AccessTokenCredentials)` and `AuthnPort.authenticate_with_token(creds: AccessTokenCredentials)` updated accordingly. `PasswordAccountProvisioningPort.accept_invite_with_password` takes a plain `invite_token: str` (no token VO).
- **Breaking (FastAPI middleware):** `ContextBindingMiddleware` accepts a sequence `authn_identity_resolvers: Sequence[AuthnIdentityResolverPort] = ()` plus `when_multiple_credentials: Literal["first_in_order", "reject"] = "first_in_order"` (replaces the single `authn_identity_resolver`). The monolithic `HeaderAuthnIdentityResolver` is replaced by three single-source resolvers: `HeaderTokenAuthnIdentityResolver`, `CookieTokenAuthnIdentityResolver` (renamed from `CookieAuthnIdentityResolver`), and `HeaderApiKeyAuthnIdentityResolver`. Each resolver returns `None` when its source is absent and raises `AuthenticationError` on present-but-invalid credentials.
- **Breaking (authn DTO):** `AuthnTokenResponseDTO.access_token` and `refresh_token` are now `Optional[str]` (set to `None` when the matching transport is cookie); two new optional fields `access_expires_in` / `refresh_expires_in` (seconds) carry the matching lifetimes derived from `IssuedTokens`.
- **Breaking (authn contracts — strategic refactor):** `AuthnPort` is now an orchestration facade only; verification of credentials and resolution of the canonical principal are split across new ports (`PasswordVerifierPort` / `TokenVerifierPort` / `ApiKeyVerifierPort` and `PrincipalResolverPort`). `forze.application.contracts.authn.value_objects` is a package; `forze.application.contracts.authn.ports` is a package containing focused submodules (`authn`, `verification`, `resolution`, `lifecycle`, `provisioning`) — direct submodule imports (e.g. `forze.application.contracts.authn.ports.AuthnPort`) keep working through re-exports. Lifecycle (`PasswordLifecyclePort`, `TokenLifecyclePort`, `ApiKeyLifecyclePort`) and provisioning (`PasswordAccountProvisioningPort`) ports plus their dep keys live under those submodules and remain importable from the `forze.application.contracts.authn` package root. `AuthnSpec` gains `enabled_methods` (default `frozenset({"token"})`) plus `token_profile` / `password_profile` / `api_key_profile` / `resolver_profile` routing hints.
- **Breaking (`forze_authn`):** `AuthnAdapter` removed and decomposed into `verifiers/` (`Argon2PasswordVerifier`, `ForzeJwtTokenVerifier`, `HmacApiKeyVerifier`) and `resolvers/` (`JwtNativeUuidResolver`, `MappingTableResolver`, `DeterministicUuidResolver`) plus a new `AuthnOrchestrator` facade. `AuthnRouteCaps` removed; `AuthnDepsModule` now takes `authn: Mapping[K, frozenset[AuthnMethod]]` and registers verifier + resolver + orchestrator factories under the new dep keys (with optional per-route `password_verifiers` / `token_verifiers` / `api_key_verifiers` / `resolvers` overrides for external IdPs). Strict `scheme == "Bearer" and kind == "access"` token gate dropped — `scheme` and `kind` are routing hints only; the JWT signature/claims are the security boundary.
- **Breaking (packaging):** Split `forze_authnz` into `forze_authn` (authentication) and `forze_authz` (authorization). Imports such as `forze_authnz.authn.*` / `forze_authnz.authz.*` become `forze_authn.*` / `forze_authz.*`. Optional extra `authn` lists JWT/password/crypto dependencies; `authz` is an empty extra (symmetry / future-proofing); `authnz` remains and matches `authn` for backward-compatible installs.
- **Breaking (authz contracts):** `PrincipalRef` includes optional `tenant_id`. `EffectiveGrantsPort.resolve_effective_grants`, `RoleAssignmentPort.assign_role` / `revoke_role` / `list_roles`, and `AuthzPort.permits` take optional keyword-only `tenant_id`. Added `coalesce_authz_tenant_id` to merge explicit `tenant_id` with `PrincipalRef.tenant_id` (mismatch raises `CoreError`). `forze_authz` adapters accept and forward the scope; grant queries remain unchanged until tenant-scoped binding documents exist.

- **Authnz:** token session documents include optional `tenant_id` (persisted access-token tenant for refresh); deployments must add the column / field when using relational document stores.
- **Breaking (Authnz contracts):** `EffectiveGrants` carries `RoleRef` / `PermissionRef`; `AuthzPort.permits(..., permission_key=...)`, `RoleAssignmentPort.assign_role` / `revoke_role(..., role_key=...)`, `list_roles` returns `frozenset[RoleRef]`. `forze_authz` resolves grants from catalog and binding documents (embedded roles on policy principals removed). `AuthzSpec` no longer carries `scope_key` (tenancy and store routing stay on `ExecutionContext` / document deps).
- **Breaking (Authnz execution):** `AuthnDepsModule` uses `AuthnKernelConfig` and capability/route-set registrations instead of per-route service instances (`AuthnRouteConfig`, `TokenLifecycleRouteConfig`, etc.).
- **Docs (authn alignment):** Recipe `pages/docs/recipes/authn-authz-tenancy-fastapi.md` updated to current FastAPI middleware names (`HeaderTokenAuthnIdentityResolver` / `HeaderApiKeyAuthnIdentityResolver` / `CookieTokenAuthnIdentityResolver`) and `AuthnSpec.enabled_methods` shape; added new concept page `pages/docs/concepts/authentication.md` (verify-then-resolve seam, `VerifiedAssertion`, the three first-party resolver flavors, UUID-native rationale, `AuthnSpec` walkthrough); added new reference page `pages/docs/reference/authentication.md` covering the full authn contract surface and the `forze_authn` / `forze_oidc` packages; trimmed `pages/docs/reference/contracts.md` "Context handling" to point at it; added new integration page `pages/docs/integrations/oidc.md` (`OidcTokenVerifier`, `OidcClaimMapper`, `JwksKeyProvider` / `StaticKeyProvider`, install via `forze[oidc]` extra, pairing with Forze resolvers); added new recipe `pages/docs/recipes/external-idp-oidc.md` (end-to-end `AuthnDepsModule` overrides + FastAPI wiring); added D2 sources `pages/diagrams/authn-verify-resolve.d2` and `pages/diagrams/authn-multi-idp-routes.d2` rendered via `just pages diagrams`; nav updated in `pages/mkdocs.yml`. README and `pages/docs/installation.md` extras tables now list `authn`, `authz`, and `oidc`, with notes on bundled integration packages. Skill `skills/forze-auth-tenancy-secrets/SKILL.md` rewritten to teach verify-then-resolve, the new dep keys and `AuthnDepsModule` shape, and external IdP wiring via `forze_oidc`.
- **Docs:** Split core-package contract reference into domain pages with repeatable API-entry tables and a contract selection overview.
- **Breaking (packaging):** optional dependency extra renamed from `contrib` to `authnz` (argon2, PyJWT, email-validator).
- **Authnz:** `Session.refresh_digest` and `RefreshTokenService` token digests use hex strings (not raw bytes).
- **Docs:** Restructured MkDocs navigation around reader intents (Start Here, Concepts, Recipes, Reference, Integrations), moved dense core package inventories to Reference, added recipe pages, and left moved-page stubs for old concept/reference URLs.
- **Docs:** Published Agent Skills under `skills/` updated for current APIs (`doc_query` / `doc_command`, kernel specs, composition plans); added `forze-specs-infrastructure`; README Agent Skills table aligned.

- **Document coordinator:** Empty bulk commands short-circuit without persistence: `kill_many([])` stays a coordinator no-op; for `create_many`, `touch_many`, `delete_many`, `restore_many`, an empty sequence with ``return_new=False`` returns ``None``, with ``return_new=True`` returns ``[]`` (aligned with overloads). ``MockDocumentAdapter`` mirrors this. Postgres FTS/PGroonga/vector/hub/federated adapters take `snapshot_coord: SearchResultSnapshotCoordinator | None` (not `snapshot_store`); deps wire ``SearchResultSnapshotCoordinator(store=port)``. Coordinator methods renamed (`result_record_key_string`, `hydrate_result_record_key`, `federated_record_key_string`, `hydrate_federated_record_key`, `snapshot_pagination`, `put_ordered_snapshot_keys`, `read_federated_snapshot_page_if_requested`); `weighted_rrf_merge_rows` / `federated_merged_hit_field` live on the coordinator (`forze_postgres.adapters.search` still re-exports `weighted_rrf_merge_rows`). Removed `FederatedSearchSnapshotCoordinator` / `search_result_snapshot_coord`. Replace imports from deleted `result_snapshot_ops` / `federated_snapshot` modules with the coordinator.
- **Breaking (private imports):** ranked-search cursor helpers, hub/federated/simple `SearchOptions` normalization, and `calculate_effective_field_weights` moved from `forze_postgres.adapters.search` internals to `forze.application.contracts.search`; log codes for ignored options use neutral `search_options_*` prefixes.
- **Breaking:** Document query/command dep factories are ``factory(ctx, spec)`` without a `cache` argument; resolve cache via ``ctx.cache(spec.cache)`` when set.
- **Breaking:** Aggregate wire shape uses `"$fields"` and `"$computed"` only (not `fields` / `computed_fields`).
- **Breaking:** `MongoClient` `db` / `collection` and `MongoGateway.coll` are async.
- **Breaking:** `ExecutionContext` caller identity is `AuthIdentity` only; FastAPI middleware uses `auth_identity_codec`; Temporal codec adds `Forze-Subject-ID`.
- **Postgres / Mongo:** Gateways and deps use `PostgresClientPort` / `MongoClientPort`; startup hooks still construct concrete clients unless using routed lifecycle + `RoutedPostgresClient` / `RoutedMongoClient`.
- **Postgres search:** `search_with_cursor` on ranked adapters raises `CoreError` instead of `NotImplementedError` where unsupported; hub multi-`hub_fk` legs use dedup CTE + joins (not correlated `LATERAL`); `PostgresGateway.order_by_clause` is async; hub adapter picks leg engine per config (`PostgresHubPGroongaSearchAdapter` alias retained); mock/simple adapters warn and strip hub-only option keys.
- **Document cache:** Postgres/Mongo configurable adapters defer cache warm and read-through writes until after successful commit when `after_commit` is wired; evict-before-repeat-read keeps TX consistency.
- **Redis:** Cache adapter uses concurrent pointer/body writes and deletes; deps/adapters use `RedisClientPort` (see Added).
- **S3 / SQS / RabbitMQ / Temporal:** Deps and adapters use structural client ports; routed variants + `routed_*_lifecycle_step`; async `close` on S3/SQS/Temporal where applicable; Temporal lifecycle awaits shutdown; `RoutedTemporalClient.get_workflow_handle` needs a prior tenant-scoped async call to populate the inner client cache.

### Removed

- **Breaking (authn):** `forze_authn.domain.constants.ACCESS_TOKEN_KIND` / `REFRESH_TOKEN_KIND` (replaced by typed credential value objects). `OAuth2Tokens` (input wrapper) — `TokenLifecyclePort.refresh_tokens` takes `RefreshTokenCredentials` directly. `HeaderAuthnIdentityResolver` (replaced by `HeaderTokenAuthnIdentityResolver` + `HeaderApiKeyAuthnIdentityResolver`); `CookieAuthnIdentityResolver` is renamed to `CookieTokenAuthnIdentityResolver`.
- `forze_postgres.adapters.search.result_snapshot_ops`, `federated_snapshot` (replaced by `SearchResultSnapshotCoordinator` in `forze.application.coordinators`).
- **Breaking:** `PrincipalContext`, `ExecutionContext.get_principal_ctx`, `PrincipalContextCodecPort`, `principal_ctx_codec`.
- `forze_postgres.adapters.search.hub_pgroonga` (merged into `hub.py`).
- Legacy Postgres `PostgresFTSSearchAdapter` / `PostgresPGroongaSearchAdapter`; use v2 adapters or hub search.

### Fixed

- ``forze_authn`` ``ApiKeyLifecycleAdapter.issue_api_key``: unpack ``(prefix, secret)`` tuple from ``ApiKeyService.generate_key`` in the correct order.
- `forze_postgres` `PostgresWriteGateway`: batched updates no longer duplicate `rev` in `VALUES`, fixing PostgreSQL `AmbiguousColumn`.
- `forze_postgres` `PostgresHubPGroongaSearchAdapter`: empty/whitespace queries no longer emit invalid rank SQL.
- `forze_s3` `S3StorageAdapter`: default `key_generator` returns a fresh UUID v7 per call (avoids accidental overwrites).
- `forze_redis` platform errors: subclass exceptions (`AuthenticationError`, `BusyLoadingError`, `ReadOnlyError`) match before generic redis-py bases.
- `RawListDocuments`: passes `return_fields` from the request DTO into `find_many`.
- Docs (`pages/`): Temporal/workflow ports, removed `tx_document_plan`, `UsecasePlan.tx`, `AuthIdentity`, `ctx.transaction` / `ctx.storage` examples, and `contracts.base` imports.

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
