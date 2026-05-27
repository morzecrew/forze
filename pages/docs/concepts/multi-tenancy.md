# Multi-tenancy

Forze separates **who is authenticated** (`AuthnIdentity`), **which tenant applies** (`TenantIdentity`), and **how data is isolated** (routed clients and `tenant_aware` document/search configuration).

## Request binding

Typical HTTP flow:

1. Authenticate the request into a principal-only `AuthnIdentity` plus optional issuer metadata (for example JWT `tid` surfaced as `issuer_tenant_hint`).
2. Resolve `TenantIdentity` via `TenantIdentityResolver`: validate optional issuer and header tenant hints against `TenantResolverPort.resolve_from_principal` when registered. The resolver is authoritative; hints never outrank principal membership.
3. `ExecutionContext.inv.bind` attaches both so adapters can call `ctx.inv.get_tenant()`.

If credential validation reads **tenant-scoped** document ports (`tenant_aware=True`) before step 2 completes, bootstrap can deadlock. Keep **authentication document routes** (`AUTHN_TENANT_UNAWARE_DOCUMENT_SPEC_NAMES` in `forze_identity.authn.application`) on **tenant-unaware** stores or global registry clients.

## Postgres isolation modes

Postgres does not use a separate `tenant_isolation` config field. Effective mode is **derived** from the client type and per-route `tenant_aware` flags on `PostgresDocumentConfig` / search configs:

| Mode | Client | Typical `tenant_aware` | Meaning |
|------|--------|------------------------|---------|
| **none** | `PostgresClient` (shared DSN) | `False` (default) | Single database; no row filter |
| **row** | `PostgresClient` | `True` on documents/searches | Shared DB; `tenant_id` column + gateway filter |
| **database** | `RoutedPostgresClient` | usually `False` | Per-tenant connection pool / database |

When using `RoutedPostgresClient`, set `PostgresDepsModule.introspector_cache_partition_key` to the same tenant identity used for routing (startup validation **fails** if it is missing). Optional schema validation warns when a write table has a `tenant_id` column but `tenant_aware=False`, or when `tenant_aware=True` on a routed client (redundant row filter — defense-in-depth is acceptable).

See [Postgres integration](../integrations/postgres.md) for wiring and troubleshooting.

## Optional JWT tenant claim

Access tokens issued by `forze_identity.authn` may include optional claim ``tid`` (UUID string). On verification it becomes `issuer_tenant_hint` on the boundary authn result; it does not become canonical tenant context by itself. When tokens are explicitly issued with tenant metadata, refresh sessions persist that `tenant_id`.

## Reference package

`forze_identity.tenancy` provides example aggregates (`tenant_spec`, `principal_tenant_binding_spec`), `TenantResolverAdapter`, `TenantManagementAdapter`, `ConfigurableTenantResolver` / `ConfigurableTenantManagement`, and `TenancyDepsModule` to register resolver/management routes on the kernel `Deps`. Applications register matching document routes and wire `TenantResolverDepKey` / `TenantManagementDepKey` as needed.
