# Multi-tenancy

Forze separates **who is authenticated** (`AuthnIdentity`), **which tenant applies** (`TenantIdentity`), and **how data is isolated** (routed clients and `tenant_aware` document/search configuration).

## Request binding

Typical HTTP flow:

1. Authenticate the request into a principal-only `AuthnIdentity` plus optional issuer metadata (for example JWT `tid` surfaced as `issuer_tenant_hint`).
2. Resolve `TenantIdentity` via `TenantIdentityResolver`: validate optional issuer and header tenant hints against `TenantResolverPort.resolve_from_principal` when registered. The resolver is authoritative; hints never outrank principal membership.
3. `ExecutionContext.inv_ctx.bind` attaches both so adapters can call `ctx.inv_ctx.get_tenant()`.

If credential validation reads **tenant-scoped** document ports (`tenant_aware=True`) before step 2 completes, bootstrap can deadlock. Keep **authentication document routes** (`AUTHN_TENANT_UNAWARE_DOCUMENT_SPEC_NAMES` in `forze_identity.authn.application`) on **tenant-unaware** stores or global registry clients.

## Relation-level isolation (all integrations)

Integration configs can name physical resources **statically** or with a **`ValueResolver`** from `forze.application.contracts.resolution` (resolved per request from `ctx.inv_ctx.get_tenant()`).

| Spec type | Shape | Examples |
|-----------|--------|----------|
| **`RelationSpec`** | `(namespace, name)` tuple or resolver | Postgres `(schema, table)`; Mongo/Firestore `(database, collection)`; BigQuery/ClickHouse analytics ingest `(dataset, table)` / `(database, table)` |
| **`NamedResourceSpec`** | `str` or resolver | S3/GCS `bucket`; Redis/SQS/RabbitMQ `namespace`; Temporal `queue`; Meilisearch `index_uid`; Mongo Atlas/vector `index_name` |

Helpers in `forze.application.contracts.resolution`: `coerce_relation_spec`, `coerce_named_resource_spec`, `require_static_relation`, `require_static_named_resource` (startup validation when names must be fixed).

`warn_dynamic_relation_with_tenant_aware` in `forze.application.contracts.tenancy` logs when a route uses a dynamic resolver with `tenant_aware=True` (row filters are usually redundant for schema/collection/bucket-per-tenant layouts).

Author-written **analytics query SQL** is not resolved automatically; only **ingest** targets use `ingest_relation` where supported (Postgres, BigQuery, ClickHouse). See integration pages under [Analytics queries and tenancy](../integrations/postgres.md#analytics-queries-and-tenancy).

## Layering routed clients and RelationSpec

Isolation is **layered**. Routed clients and `RelationSpec` solve different problems; combine them deliberately.

| Layer | Mechanism | What you configure |
|-------|-----------|-------------------|
| **Account / cluster** | `Routed*Client` + `secret_ref_for_tenant` | Per-tenant DSN, URI, project, or API keys in secrets |
| **Database / project** | Payload inside the secret (or Mongo `database_name_for_tenant`) | Which database or project the connection uses |
| **Relation** | `RelationSpec` / `NamedResourceSpec` on routes | Which table, collection, bucket, queue, or ingest target after connected |
| **Row** | `tenant_aware=True` | `tenant_id` column filters, key prefixes, Meilisearch filters |

Guidelines:

- **Routed = where you connect.** `RoutedPostgresClient`, `RoutedMongoClient`, and siblings resolve credentials per `TenantIdentity` and LRU-dedupe pools by connection fingerprint when several tenants share the same physical endpoint.
- **RelationSpec = which resource** on that connection (for example `lambda tid: (f"tenant_{tid}", "orders")` on `PostgresDocumentConfig.read`).
- **Same DSN + dynamic `RelationSpec`** — schema-per-tenant (or collection-per-tenant) on a shared database.
- **Different DSNs + static relations** — database-per-tenant without resolvers on document routes.
- **`tenant_aware=True` with a routed client** — redundant row-level filtering; startup logs a warning (`validate_routed_client_tenancy_wiring`). Defense-in-depth is acceptable.

Postgres-only: `PostgresDepsModule.introspector_cache_partition_key` is **required** when using `RoutedPostgresClient` so catalog caches partition by tenant.

### Troubleshooting routed pools

- **Implementation** — routed clients share `TenantClientRegistry` plus DSN or structured fingerprint helpers from `forze.application.contracts.tenancy` (see `RoutedRedisClient` as the reference).
- **Wrong tenant after LRU eviction** — integration tests that share one DSN across tenants should use **distinct connection fingerprints** per tenant (see `tests/integration/_routed_lru_helpers.py`).
- **Catalog / schema validation on dynamic relations** — startup hooks that need fixed names call `require_static_relation` or skip dynamic routes; omit schema validation lifecycle steps for schema-per-tenant documents.

DSN fingerprinting and secret resolution are **not** changed by the `RelationSpec` rollout; do not expect resolver fields to replace per-tenant secrets.

## Integrations without RelationSpec config

### Inngest (`forze_inngest`)

`InngestEventConfig` has no physical resource fields (only options such as `include_execution_context`). Tenancy is **credential routing only**: `RoutedInngestClient` resolves per-tenant `app_id` and keys from secrets, LRU-dedupes by credential fingerprint, and requires a tenant on `send`.

Relation-level isolation for Inngest means **separate Inngest apps per tenant** (secrets + routed client), not `RelationSpec` on the deps module. Event names, function IDs, and `serve()` registration remain **application conventions** (for example `tenant:{tenant_id}:invoice.paid`). See [Inngest integration](../integrations/inngest.md).

### Mock (`forze_mock`)

`forze_mock` provides in-memory adapters (`MockState`) for unit tests and local demos. There is no physical backend, no `TenantAwareIntegrationConfig` on integration configs, and no routed client.

Mock is **tenant-agnostic** unless tests partition `MockState` themselves (separate module instances or explicit keys). Do not use mock for production tenancy patterns.

## Postgres isolation modes

Postgres does not use a separate `tenant_isolation` config field. Effective mode is **derived** from the client type and per-route `tenant_aware` flags on `PostgresDocumentConfig` / search configs:

| Mode | Client | Typical `tenant_aware` | Meaning |
|------|--------|------------------------|---------|
| **none** | `PostgresClient` (shared DSN) | `False` (default) | Single database; no row filter |
| **row** | `PostgresClient` | `True` on documents/searches | Shared DB; `tenant_id` column + gateway filter |
| **relation** | `PostgresClient` or `RoutedPostgresClient` | usually `False` | Shared or routed connection; per-tenant schema via `RelationSpec` resolver on document routes |
| **database** | `RoutedPostgresClient` | usually `False` | Per-tenant connection pool / database |

Document routes accept a static `(schema, table)` tuple or a `ValueResolver` from `forze.application.contracts.resolution` (resolved on each async gateway call from `TenantIdentity`). Use relation mode for schema-per-tenant layouts; avoid combining it with `tenant_aware=True` on the same route unless you want defense-in-depth.

`RoutedPostgresClient` and other routed integration clients (Redis, Mongo, S3, RabbitMQ, SQS, Temporal, BigQuery, ClickHouse, Meilisearch, GCS, Firestore, Inngest) deduplicate LRU pools by connection fingerprint when several tenants resolve to the same DSN or endpoint.

When using `RoutedPostgresClient`, set `PostgresDepsModule.introspector_cache_partition_key` to the same tenant identity used for routing (startup validation **fails** if it is missing). Optional schema validation warns when a write table has a `tenant_id` column but `tenant_aware=False`, or when `tenant_aware=True` on a routed client (redundant row filter — defense-in-depth is acceptable).

Use the integration lifecycle step for routed clients (for example `postgres_lifecycle_step` with `RoutedPostgresClient`). It wraps `routed_client_lifecycle_step` so pools start and close with the application scope. See [Application layer — Lifecycle plan](application-layer.md#lifecycle-plan).

Routed adapters and lifecycle hooks may call `require_tenant_id` from `forze.application.contracts.tenancy` when a bound `TenantIdentity` is mandatory, and `secret_ref_for_tenant` / `resolve_str_for_tenant` from `forze.application.contracts.secrets` when DSNs or API keys are stored per tenant.

See [Postgres integration](../integrations/postgres.md) for wiring and troubleshooting.

## Optional JWT tenant claim

Access tokens issued by `forze_identity.authn` may include optional claim ``tid`` (UUID string). On verification it becomes `issuer_tenant_hint` on the boundary authn result; it does not become canonical tenant context by itself. When tokens are explicitly issued with tenant metadata, refresh sessions persist that `tenant_id`.

## Reference package

`forze_identity.tenancy` provides example aggregates (`tenant_spec`, `principal_tenant_binding_spec`), `TenantResolverAdapter`, `TenantManagementAdapter`, `ConfigurableTenantResolver` / `ConfigurableTenantManagement`, and `TenancyDepsModule` to register resolver/management routes on the kernel `Deps`. Applications register matching document routes and wire `TenantResolverDepKey` / `TenantManagementDepKey` as needed.

For demos without a tenant document store, use [`LocalTenantResolver`](../recipes/local-identity.md) with a shared [`LocalIdentityConfig`](../recipes/local-identity.md) (not for production).
