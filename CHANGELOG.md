# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Fluent query builder (`Q`) — a typed, imperative alternative to hand-writing filter dicts:** `Q.field("age").gt(18) & Q.field("name").like("a%")` composes the same `QueryFilterExpression` the dict form produces — `.build()` returns the dict (drop-in anywhere a filter dict is accepted, e.g. `find_page(cond.build())`), `.to_ast()` returns the parsed `QueryExpr`. Leaf predicates cover every value operator (comparison, membership, text, null/empty, set relations, and the hierarchy `descendant_of`/`ancestor_of`); combine them with `&` / `|` / `~` (the bitwise operators — parenthesize, don't use the `and`/`or`/`not` keywords). Passing another `Q.field(...)` as an operand to a comparison method builds a field-to-field `$fields` compare; `Q.field("items").any(...)` / `.all(...)` / `.none(...)` quantify array fields over an element predicate (`Q.elem()` is the scalar element, a bare scalar is shorthand for `== value`). It is **purely additive and changes no contract**: a built condition is interchangeable with the dict it lowers to, so the parser, capability check, and operator/type validation stay the single source of truth — the builder lowers faithfully and does not re-validate, so a malformed query raises the same clean error when used. New public exports `Q`, `QueryCondition`, `FieldRef` in `forze.application.contracts.querying`.
- **Hierarchy filter operators (`$descendant_of` / `$ancestor_of`) on a `TreePath` field:** type a read-model field as the new `TreePath` (a marker `str` subtype) and the DSL gains two label-aware, **inclusive** materialized-path operators — `$descendant_of` keeps rows at or below a node, `$ancestor_of` keeps rows at or above it (a node is its own ancestor and descendant). The operand is one path or a list (list = "any" / `OR`); "all" and "none" compose from `$and`/`$not`, so there are no dedicated quantified variants. Comparison is label-boundary correct: `top.science` is **not** a descendant of `top.sci`, nor an ancestor of `top.scientist`. Postgres renders native, index-backed `ltree` containment (`@>` / `<@`) on an `ltree` column and a `starts_with` label-prefix fallback on a plain `text` column; the in-memory mock is the oracle. The operators are gated by a new `QueryCapabilities.supports_hierarchy` axis (off by default) — only Postgres and the mock advertise it, so backends that can't express label-aware path containment (Mongo, Firestore, Meilisearch) reject them up front with a clean `query_feature_unsupported` precondition instead of mis-rendering. The operator/field-type validator confines them to `TreePath` fields (rejected on a plain string) while keeping equality/text/membership available on a `TreePath` (it is still a string). Discovery (OpenAPI `x-forze-query` / MCP) auto-reflects the `hierarchy` field type and its operators. Verified by a cross-backend tree-corpus parity suite on real Postgres (both `ltree` and `text` storage) against the oracle. New public exports: `TreePath`, `HierarchyOp`, `HierarchyValue` in `forze.application.contracts.querying`.
- **Aggregation — distinct counts, dispersion, and percentiles (Tier 1 + Tier 2):** computed aggregate fields gain `$count_distinct` (null-excluded, SQL `DISTINCT` semantics), the population/sample dispersion family `$stddev_pop` / `$stddev_samp` / `$var_pop` / `$var_samp` (sample stats over a single value yield `null`, not an error), and `$percentile` (the application form `{"$percentile": {"field": "amount", "p": 0.9}}`, `p` in `[0, 1]` required — no scalar shorthand). Each renders per backend: Postgres uses `count(distinct …)`, `stddev_*`/`var_*`, and `percentile_cont(p) WITHIN GROUP`; Mongo uses `$addToSet`+`$size`, `$stdDevPop`/`$stdDevSamp` (variance via `$pow`), and `$percentile` (approximate, following the existing `$median` precedent); the mock is the exact oracle. Verified by a cross-backend function-parity corpus on real Postgres and Mongo — percentile value-equality is checked only where exact (Postgres + mock), since Mongo's estimator is approximate. (`$first`/`$last` deferred.)
- **Mock runs operator/field-type validation (dev ↔ prod symmetry):** the in-memory mock now validates a filter's operator/field-type compatibility on every read (find/find_many/cursor/count), the same `validate_query_field_types` check the real gateways run in `compile_filters`. A type-incompatible filter (e.g. `$like` on a number) now raises the same `query_operator_type_mismatch` precondition in tests as in production, instead of silently matching nothing. This surfaced and fixed a validator false-positive: `$in` / `$nin` on an **array** field are valid (they compile to overlap / disjoint element-wise on every backend — `unnest … = ANY` on Postgres, `$in` on an array field in Mongo), so they're no longer rejected.
- **Aggregation `$having` — post-group filtering of aggregate rows:** an aggregates expression may now carry `$having`, a filter (same grammar as `filters`) applied to the *aggregated* rows by their output aliases — group keys and computed metrics — e.g. keep only groups whose `$count` ≥ N or `$sum` exceeds a threshold. It's the aggregate analogue of SQL `HAVING`. The parser validates that `$having` references only declared aliases. Implemented on every backend that supports aggregates: the mock filters the computed group rows, Mongo appends a `$match` after `$group`, and Postgres wraps the group query in a subquery and filters its output columns (so aliases are referenceable). Verified by a cross-backend parity corpus (count/sum thresholds, multi-key groups, group-key + metric combinations) on real Postgres and Mongo against the in-memory oracle. (Firestore continues to reject aggregates entirely in its MVP, so `$having` never applies there.)
- **Mixed-direction keyset pagination, per-key ``NULLS FIRST/LAST`` control, and coherent null ordering:** cursor (keyset) pagination now supports **mixed** ``asc``/``desc`` sort keys (e.g. ``{grp: asc, score: desc}``) — the uniform-direction restriction is removed; the composite seek compares each key in its own direction. Each sort key may also carry an explicit null placement via the new spec form ``{field: {"dir": "asc", "nulls": "last"}}`` (the plain ``"asc"`` shorthand still works and keeps the canonical default). Null ordering is **coherent across backends**: the default is a null sorting as the smallest value (``asc`` → nulls first, ``desc`` → nulls last), matching the in-memory oracle, Mongo, and Firestore; Postgres is brought into line (keyset and offset ``ORDER BY`` emit explicit ``NULLS FIRST``/``LAST``), and its keyset seek predicate is now null-aware — fixing a latent bug where a plain ``col > ?`` silently **dropped null-keyed rows** from cursor pages. Explicit overrides are honored by Postgres and the mock; backends that always order nulls-as-smallest (Mongo, Firestore) reject a non-default override with a clean ``query_feature_unsupported`` precondition rather than mis-ordering (Mongo can opt in to honor it — see below). Offset and cursor now sort identically (the mock's offset sort was rebuilt on the same type-aware comparison). The cursor token carries per-key null placement; pre-existing tokens stay valid (their nulls default to canonical). Verified by a cross-backend cursor-parity harness (multi-key, mixed-direction, nullable keys, explicit overrides) on real Postgres against the oracle. New public helpers: ``QuerySortNulls`` / ``QuerySortKeySpec`` types, ``resolve_sort_keys``, ``parse_sort_value``, ``ordered_compare``.
- **Query discovery metadata (OpenAPI + MCP):** filter-accepting document list operations now advertise their read model's *query surface* so a client (or an LLM) learns the contract up front instead of by trial and error. `build_query_discovery` (in `forze.application.contracts.querying`) turns a read model + its `QueryFieldPolicy` allow-sets into a `QueryDiscovery` — per-field type and the operators each field accepts (`$like` on a string but not a number, ordering on numbers/dates, set ops + element quantifiers on arrays), plus the sortable/aggregatable field lists. The doc kit attaches it to every list descriptor (`OperationDescriptor.query_discovery`); FastAPI projects it as the `x-forze-query` OpenAPI vendor extension, and MCP appends a "Filterable fields — …" line to the tool description. Backend-agnostic (type-derived upper bound). New public helpers `classify_field_type`, `field_value_operators`, `is_quantifiable_field`. Purely additive — operations without a filter (get/create/update/delete) are unchanged.
- **Mongo opt-in non-native null ordering (`computed_null_ordering`):** setting `MongoDocumentConfig(computed_null_ordering=True)` makes Mongo honor an explicit per-key `NULLS FIRST/LAST` that differs from its native null-as-smallest order, on offset reads, by sorting through an aggregation pipeline (an `$addFields` null-rank key + `$sort`) — verified against the in-memory oracle on real Mongo. It's an explicit opt-in because the computed sort key can't use an index (Mongo does an in-memory sort), so the cost is never silent; the canonical default ordering always uses the native indexed `find().sort()` regardless, and without the flag the override still rejects cleanly. Firestore can't express this server-side and stays gated.
- **Query DSL — scalar array-of-arrays quantifiers (`{$any: {$any: …}}`):** an element quantifier may now apply directly to an array element that is itself an array (a `list[list[…]]` field), e.g. `matrix $any {$any: "hot"}` ("any inner list contains 'hot'"), at any depth and under any outer quantifier. Previously a quantifier directly inside a quantifier was rejected by the parser; it now compiles on the mock, Postgres (nested `EXISTS` over the element itself), and Mongo (aggregation `$expr`), verified by the cross-backend parity suite on real Postgres and Mongo. The operator/field-type validator checks the chain (the element must be an array to nest into, the deepest element's type must fit the operator); search/MVP backends that don't support nested quantifiers reject it cleanly via the capability model. *(A quantifier key still can't be combined with other operators in the same map.)*
- **Query DSL — full nested element quantifiers (Postgres + Mongo) + operator/field-type validation:** nested array quantifiers (a quantifier inside another's element predicate, e.g. `items $any {tags $any "hot"}`) now compile on **every** document backend under **any** outer quantifier. Postgres uses alias-parameterized nested `EXISTS`; Mongo compiles a nested quantifier to an aggregation `$expr` (depth-indexed `$filter`), which composes recursively where the `$elemMatch` query form cannot (outer `$all`/`$none` can't negate a nested match — `$not`-of-`$not` is illegal). Both drop the `supports_nested_quantifiers` gate, so `POSTGRES_QUERY_CAPABILITIES` / `MONGO_QUERY_CAPABILITIES` are the full DSL surface, matching the mock oracle (verified by the cross-backend parity suite on real Postgres and Mongo). Separately, a backend-agnostic `validate_query_field_types` (in `forze.application.contracts.querying`) runs in the gateway's `compile_filters` and rejects operators that don't fit a field's read-model type — `$like` on a number, `$gt` on a boolean, a set operator or `$empty` on a scalar, a quantifier on a non-array — with a clean `precondition` (code `query_operator_type_mismatch`) instead of a runtime backend type error. Best-effort: fields whose type can't be resolved are skipped (never a false rejection); field *existence* and allow-sets stay in `field_policy`.
- **Self-service API-key management (kit ops + routes):** the authn aggregate gains `issue_api_key` / `list_api_keys` / `revoke_api_key` (`AuthnKernelOp`) — all `AuthnRequired` self-service ops acting on the *current* identity — projected by `attach_authn_routes` as `POST /api-keys` (201, the secret returned **once**), `GET /api-keys` (non-secret descriptors), and `DELETE /api-keys/{id}` (204). `issue` optionally mints a user→agent **delegation** key (`actor_principal_id`) and accepts a human `label`; if omitted, every key still carries a non-secret `hint` (`first4…last4`) so a "connected apps" UI can identify a key without ever seeing the secret. New `ApiKeyInfo` value object and `ApiKeyLifecyclePort.list_api_keys`; `ApiKeyAccount` gains `hint`/`label`. Not exposed via MCP. **Migration** (column-mapped backends): `ALTER TABLE <api_key_accounts> ADD COLUMN hint text, ADD COLUMN label text`. *(Breaking for `ApiKeyLifecyclePort` implementers: `issue_api_key` gains `label`, plus a new `list_api_keys`.)*
- **Delegation-aware API keys (user→agent):** an API key can be minted bound to a delegation **actor** — `issue_api_key(identity, actor_principal_id=...)` stores it on the `ApiKeyAccount` (new immutable `actor_principal_id`), the verifier emits it as the RFC 8693 `act` claim, and `authenticate_with_api_key` resolves it into `AuthnIdentity.actor` **intrinsically** (no `actor_claim` config — unlike the opt-in token path — since the framework mints these keys). The engine then enforces the least-privilege user×agent grant intersection. One agent service principal (e.g. per connector type) can back many keys, each independently revocable; rotation preserves the binding. On the MCP edge, `AccessTokenIdentityResolver` now prefers the key's own agent over its fixed `agent=` fallback, so per-connection MCP keys attribute and revoke independently. New `ACT_CLAIM` constant in `forze.application.contracts.authn`. **Migration** (column-mapped backends, e.g. Postgres): `ALTER TABLE <api_key_accounts> ADD COLUMN actor_principal_id uuid`. *(Breaking for `ApiKeyLifecyclePort` implementers: `issue_api_key` gains a keyword-only `actor_principal_id`.)*
- **MCP boundary API-key auth (`ForzeApiKeyVerifier` + `AccessTokenIdentityResolver`):** protect a FastMCP server with the same forze_identity brain as the HTTP edge — no OAuth flow. `ForzeApiKeyVerifier` plugs into `FastMCP(auth=...)` (and `build_mcp_server(auth=...)`), validates an inbound API-key bearer via `authenticate_with_api_key`, resolves the tenant, and hands FastMCP an `AccessToken` (unknown key → `None` → clean `401`; misconfiguration fails loud). `AccessTokenIdentityResolver` binds the verified principal per call, attaching a fixed agent service principal as the delegation actor so the engine enforces the least-privilege user×agent intersection. Reads-only by default (`include_writes=False`); the MCP server stays a Resource Server (OAuth/authorization-server is an external, deferred concern).
- **OpenAPI security from configured authn (`apply_openapi_security`) + `requires_authn` catalog signal:** the FastAPI route generators now emit `x-requires-authn` for every operation whose plan declares it needs a bound principal — a new `OperationCatalogEntry.requires_authn`, detected at freeze via a `DeclaresAuthn` marker (the `AuthnRequired` hook, or any authz hook, since authorization presupposes authentication). `forze_fastapi.security.apply_openapi_security(app, requirement)` derives OpenAPI `securitySchemes` from the same `AuthnRequirement` the security middleware uses (bearer for an `Authorization` token; `apiKey` in header/cookie otherwise) and attaches `security` to exactly those flagged operations — so protected routes show an Authorize button while token-minting routes (`/login`, `/refresh`) stay open. MCP tool descriptions gain a matching "Requires authentication" line off the same signal. The shipped authn kit now declares `AuthnRequired` on `logout` and `change_password` (they act on the current identity), so they are flagged `requires_authn` and project correctly — the 401 (`auth_required`) is unchanged. Documents auth; enforcement stays in the engine.
- **In-process L1 document cache (`CacheSpec(l1=L1Spec(...))`):** opt-in process-memory layer ahead of the distributed cache — hot reads skip the backend round-trip and the JSON decode (holds decoded read models). `L1Spec.ttl` is the cross-replica staleness budget (writes invalidate L1 on the writing replica; others serve until TTL; validated `l1.ttl < ttl`). Keys are tenant-scoped, instances isolated by `model_copy`, warms ride the after-commit path. Pluggable eviction via `L1Spec.store_factory` (default in-house LRU+TTL; ships a scan-resistant W-TinyLFU `TinyLfuStore` / `tiny_lfu_l1_store` — choose it when L1 stats show sustained evictions with a sagging hit rate). Off by default, byte-identical when off.
- **L1 push invalidation (`RedisCacheConfig(invalidation_push=True)`):** Redis client-side caching (`CLIENT TRACKING ON BCAST` over RESP3 push frames) shrinks the cross-replica staleness window from the L1 TTL to one round-trip; fails open, lets the L1 TTL be raised for hit rate. New `SupportsInvalidationPush` capability on the cache port.
- **L1 metrics (`instrument_document_l1`) + `CachePort.exists`:** per-scope L1 size/capacity/hit/miss/eviction OpenTelemetry metrics; `exists(key)` presence check on the cache query port (Redis + mock).
- **Cache stampede protection & adaptive freshness:** singleflight collapses concurrent read-through misses into one fetch; probabilistic early refresh (XFetch, `CacheSpec(early_refresh_beta=...)`) desynchronizes refreshes before expiry, optionally serving the still-valid entry while recomputing in a detached task (`early_refresh_background=True`); adaptive per-entry lifetimes (`age_ttl`, `sliding_ttl`) plus a keyword-only `ttl=` override on every cache setter (per-entry TTL on the contract).
- **Resilience strategies — adaptive bulkheads (`AdaptiveBulkheadStrategy`):** AIMD concurrency limits that back off multiplicatively under latency pressure and recover additively, for uncoordinated replicas sharing a downstream (latency-only signal; errors stay the breaker's job). Optional distributional breach via `latency_quantile=` (windowed P²). Mutually exclusive with `BulkheadStrategy`.
- **Bulkhead queue management (CoDel + adaptive LIFO):** both bulkhead kinds share one wait-queue; opt-in `queue_target`/`queue_interval` (CoDel time-based shedding) and `queue_adaptive_lifo=True` (serve newest waiter while congested). Deadline-expired waiters are dropped at wake.
- **Adaptive client throttling (`AdaptiveThrottleStrategy`):** probabilistic load shedding proportional to downstream degradation (rejects with `throttled`/429). Mutually exclusive with `CircuitBreakerStrategy`.
- **Tail-based hedging delay (`HedgeStrategy.adaptive_delay_quantile`):** the hedge delay tracks the observed latency tail via a windowed P² quantile estimator instead of a fixed guess; `delay_min`/`delay_max` clamp it; effective delay observable via `executor.hedge_delays()`.
- **`THROTTLED` exception kind + `RateLimitStrategy`:** retryable `exc.throttled` (429); token-bucket `RateLimitStrategy` as the outermost resilience strategy. `ResilienceDepsModule(port_policies=[...])` declaratively wraps any resolved port's methods in a named policy.
- **Distributed rate limiting (`RateLimitStore` + Redis):** pluggable store so N replicas enforce one fleet-wide rate (`RedisRateLimitStore`, server-clock Lua, fails open); same seam as `CircuitBreakerStore`. Bulkheads/budgets stay process-local by design.
- **Invocation deadlines + `TIMEOUT` exception kind:** gRPC-style per-operation time budgets declared on the plan (`registry.bind(op).with_deadline(...)`, tightest-wins merge) or bound per-call (`bind_deadline`); the whole plan and resilience chain are deadline-aware. Expiry raises non-retryable `exc.timeout` (504). Projected into the catalog, FastAPI routes, and MCP descriptions.
- **`instrument_resilience`:** always-on resilience metrics (events, breaker state, bulkhead queue depth) independent of the tracing gate.
- **`build_runtime` + `runtime_lifespan`:** one-call app assembler collapsing the registry/lifecycle freeze dance; `forze_fastapi.runtime_lifespan(runtime)` holds the scope open for the app lifetime.
- **Graceful drain on shutdown:** the scope stops admitting new invocations (retryable `THROTTLED`/`draining`) and gives in-flight work a bounded window (`drain_timeout`, default 10s) before dependent clients close.
- **Fleet deployment posture:** `build_runtime(deployment=DeploymentProfile.FLEET)` validates shared-state lifecycle steps are singleton-guarded; `singleton_lifecycle_step` (distributed-lock guard for idempotent startup); readiness probe (`attach_readiness_route`, 200 / 503-draining); deadline-budget propagation over HTTP (`X-Forze-Deadline-Budget`).
- **Tenant pool churn metrics (`instrument_tenant_pools`):** per-tenant routed-pool churn (size/capacity/created/disposed/evicted) as OpenTelemetry metrics — the signal that gates future pool-admission policy.
- **Message envelope headers + correlation propagation:** queue/stream/pubsub messages gain `headers` and `delivery_count`; ports accept `headers=`. The outbox relay forwards the full event envelope and `process_with_inbox` rebinds correlation/causation, so tracing survives broker hops. Optional tenant rebinding from headers (opt-in; forgeable trust model documented).
- **Outbox `ordering_key`:** per-aggregate ordering where transports support it (SQS FIFO `MessageGroupId`, stream partition key); dedup hardened to key on the event-id header. **Migration:** `ALTER TABLE … ADD COLUMN ordering_key TEXT`.
- **Kits queue-consumer runner (`run_consumer` / `queue_consumer_background_lifecycle_step`):** the consumer-side counterpart of the outbox relay — inbox exactly-once, transient-failure requeue, poison parking (`max_deliveries`), optional named retry policy, envelope rebinding.
- **Stream group pending-entry recovery:** `StreamGroupQueryPort.claim` (XAUTOCLAIM) + `pending` (XPENDING) so a crashed consumer's pending entries can be reclaimed (breaking for port implementers).
- **`AuthnOrchestrator` moved to core + working mock identity plane:** orchestrator now in `forze.application.integrations.authn`; `forze_mock` runs the full password-login → refresh-rotation → change-password → logout flow with grant-evaluated authz, including over the FastAPI routes.
- **`attach_authn_routes`:** login/refresh/logout/change-password/deactivate (+ password-reset) endpoints projected onto a user-owned `APIRouter`. `deactivate_principal` ships unguarded — bind authz before exposing.
- **Self-service password reset (`PasswordResetPort`):** single-use TTL-bounded token (digest-only storage), revokes all sessions on reset, uniform 202 ack (no enumeration), optional outbox delivery. Mock included.
- **Authn event sink + login lockout:** `AuthnEventSink` (`LoggingAuthnEventSink`, mock recorder) emitting best-effort lifecycle events (login digest, never the raw login); `AuthnDepsModule(lockout=LockoutConfig(...))` adds fixed-window login lockout (raises `THROTTLED`). Fully opt-in.
- **Presigned object-storage URLs:** `StorageQueryPort.presign_download` / `StorageCommandPort.presign_upload` return a repr-masked `PresignedUrl` (S3 SigV4, GCS V4 / IAM `signBlob`, mock deterministic). Minting an upload URL is a write (CQRS guard). Breaking for port implementers.
- **Catalog enrichment + registry ergonomics:** `OperationCatalogEntry` gains `supports_idempotency_key` and `required_permissions`, projected onto FastAPI routes (`Idempotency-Key` header, `x-required-permissions`) and MCP descriptions. Duplicate keys on `merge` now raise a configuration error (`override=True` escape hatch); one-step `registry.register(...)`.
- **`RecordingNotificationSenders`** test double; `AnalyticsDeps.command` alias of `.ingest()`; `TenancyDeps.require_resolver()`; `OperationDescriptor.tags` project onto generated FastAPI route tags.

### Changed

- **Argon2 password hashing no longer blocks the event loop (`forze_identity`):** `PasswordService.hash_password` / `verify_password` / `timing_dummy_hash` are now `async` on a bounded thread pool (`PasswordConfig.hashing_concurrency`, default 4, bounds peak hashing memory); blocking `*_sync` variants remain for scripts/tests.
- **Performance — engine hot path (measured):** the per-operation pipeline inlines ContextVar bookkeeping and precomputes plan emptiness (hookless op ~2.5 → ~1.2 µs, −52%; QUERY −56%; boundary `bind` −50%; unconfigured-transaction errors now surface at plan resolution).
- **Performance — document & Postgres (measured):** `Document.update()` short-circuits equal subtrees and copies only changed containers (−21% scalar / −44% nested; OCC history −30%; safer aliasing). Postgres root-tx options ride the `BEGIN` (4 → 3 statements, ~−21%); out-of-tx statements run on autocommit checkouts (−37%); `has_hybrid_patch_conflict` bucketed by root segment (2.3×–122×).
- **Performance — Mongo & codecs (measured):** Mongo `create` skips the read-back (−49%); single updates use `find_one_and_update` (−30%, closes a read-back race); the outbox claims batches in 3 round-trips (−90%, needs a sparse `claim_token` index); fixes a latent partial-model crash under `hydrate_from_write`. `trusted` decode now validates values (1.5–2.6× faster, fixes nested-model correctness); msgspec `forbid_extra` is free with `forbid_unknown_fields`, else 3–13× faster.
- **Performance — interceptor, relay, logging (measured):** exception interceptors materialize error context lazily on the failure path (~8–13 µs → ~0.2 µs); the outbox relay batches `mark_published`/`mark_retry` (~1k → ~18.4k rows/s); `trace` logging is opt-in (26×); log scrubbing is memoized + literal-prefiltered (~53× / ~27×).
- **Performance — simple-dependency resolution (measured):** `resolve_simple` (the transaction manager, domain-event dispatcher, and tenant resolver) is now memoized per scope, keyed by `(key, route)`, under the same `cache_resolved_ports` flag as configurable ports — so its factory is built once per scope and reused instead of re-invoked (and re-allocated) on every access (~1.5 → ~0.4 µs, −73%, now at parity with the configurable-port cache hit). Caching is bypassed while resolution tracing is active; the runtime tracer still records each access. The flag now gates both port and simple-dep memoization; disable only for a *stateful* simple-dep factory that must rebuild per call.
- **Performance — aggregate load conversion (measured):** `AggregateRepository.load` reconstructs the domain aggregate from the read model via `model_validate(read, from_attributes=True)` instead of dumping it to a dict and re-validating (`model_validate(read.model_dump())`). It runs the same domain validation and invariants and stays faithful to computed fields and serialization aliases, but skips the recursive dump roundtrip — ~−22% for a flat aggregate and ~−67% for a nested one. Behavior-preserving (pinned by a fidelity test against the old roundtrip).
- **FastAPI `style="rpc"` routes use REST verbs + query params, not `POST`-everything:** the RPC style now mirrors the REST verbs (`GET /notes.get?id=`, `PATCH /notes.update?id=&rev=` with the patch body, `DELETE /notes.kill?id=`, `PATCH /notes.delete|restore?id=&rev=` for soft deletion) while keeping its operation-named paths — the id/rev ride query parameters instead of a JSON body, so RPC reads are linkable and cacheable. `create` and list operations keep `POST /<op>` (genuine bodies). Storage RPC `delete` is now `DELETE /files.delete/{key}`. Breaking: RPC clients calling `POST /notes.get` etc. must switch to the new verb/parameter shape; REST style and MCP are unchanged.
- **`singleton_lifecycle_step` takes a `DistributedLockSpec`, not a live port:** the lock command port is now resolved from the scope at startup (`ctx.dlock.command(spec)`), so the guard composes into a lifecycle plan before any scope exists. Breaking: pass `spec=DistributedLockSpec(name=...)` instead of `cmd=`.
- **Release-coherence sweep:** the outbox→pubsub relay logs the at-least-once → fire-and-forget downgrade; Temporal `query`/`update`/`result` deserialize into declared return types; `ApiKeyConfig.prefix` is validated; the `key` field's per-backend semantics are documented in one table; saga `step_failed` stays `DOMAIN` (rationale recorded).

### Fixed

- **Post-commit work can no longer be silently skipped by task cancellation:** once a root-transaction commit succeeds, the deferred after-commit drain runs to completion as a cancellation-protected critical section (new `forze.base.asyncio.run_to_completion`), then re-raises the cancellation. Cancellation during the body still rolls back as before.

### Removed

- _Nothing yet._

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

See [Kits reference](pages/docs/reference/kits.md).

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

[unreleased]: https://github.com/morzecrew/forze/compare/v0.3.0...HEAD
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
