# Tenancy

Binding a tenant at the boundary and routing infrastructure per tenant: isolation tiers and the floor that fails wiring closed below them, the admin plane, and provisioning a new tenant's resources.

## Tenancy and routed clients

`TenantIdentity` is the current tenant. Tenant-aware adapters derive routing from `ExecutionContext`, not from user DTO fields. Routed Postgres, Mongo, Redis, S3, RabbitMQ, SQS, Temporal, BigQuery, ClickHouse, Meilisearch, GCS, Firestore, and Inngest clients can choose per-tenant infrastructure at call time.

For database-per-tenant Postgres routing, set `PostgresDepsModule.introspector_cache_partition_key` so catalog metadata caches are partitioned per tenant/database.

`AuthnIdentity.tenant_id` is set by the resolver when the assertion carries an issuer tenant hint (e.g. a JWT `tid` claim or an OIDC tenant claim). `SecurityContextMiddleware` then calls `resolve_tenant_identity`, which coalesces the issuer hint with an optional `X-Tenant-Id` header and resolves the pair through `TenantResolverPort` (`ctx.tenancy.resolver()`). An unvalidated header tenant is refused unless `trust_tenant_header=True` declares a trusted gateway in front.

## Isolation tiers and the declared floor

Every tenant-aware deps module reports the isolation tier its wiring reaches — `none < tagged < namespace < dedicated` (storage-agnostic names):

- `tagged` — a shared store with a tenant marker (`tenant_aware=True`): a SQL `tenant_id` column, a Redis key prefix, an object-store path prefix, a graph property.
- `namespace` — a per-tenant container on a shared instance via a dynamic resolver (schema / dataset / bucket / collection).
- `dedicated` — a routed client with per-tenant credentials and connections.

Set `required_tenant_isolation` on a module to declare a **minimum** and fail wiring closed below it — checked once at startup, never per request:

```python
from forze_postgres import PostgresDepsModule, RoutedPostgresClient

PostgresDepsModule(
    client=RoutedPostgresClient(...),
    required_tenant_isolation="dedicated",
)
```

A floor the backend can never reach (e.g. `"dedicated"` on in-process DuckDB or single-client Neo4j) fails as a capability mismatch. Use it to refuse under-isolated wiring on untrusted or self-scoping query paths (raw SQL hatches, self-filtering analytics). Default `None` enforces nothing.

## Tenancy deps module

`TenancyDepsModule` (`from forze_identity.tenancy.execution import TenancyDepsModule`) registers `TenantResolverDepKey` and/or `TenantManagementDepKey` factories (`ConfigurableTenantResolver`, `ConfigurableTenantManagement`) for the route names you pass. Merge it into `DepsRegistry.from_modules` alongside Postgres/Mongo and auth modules when tenant catalog documents drive `TenantResolverPort` / `TenantManagementPort`.

```python
from forze_identity.tenancy.execution import TenancyDepsModule

TenancyDepsModule(
    tenant_resolver={"main"},
    tenant_management={"main"},
    verify_tenant_active=True,
)
```

See [Multi-tenancy](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/multi-tenancy/) for aggregates, adapters, and the FastAPI `resolve_tenant_identity` pairing.

## Tenant selector and admin plane

Two pre-built aggregates cover org switching and tenant administration:

- **Self-service selector** — `build_tenancy_registry(spec)` (`forze_kits.aggregates.tenancy`) → `attach_tenancy_routes(...)`: a principal lists their own memberships and switches the active tenant (the choice rides the signed `tid` claim, re-validated against live membership per request). Needs `TenancyDepsModule` with both `tenant_resolver` and `tenant_management` routes.
- **Admin plane** — `build_tenancy_admin_registry(ns)` (`forze_kits.aggregates.tenancy_admin`, ops in `TenancyAdminKernelOp`) → `attach_tenancy_admin_routes(...)`: create a tenant, invite/remove members, list members, deactivate. These ops ship **unguarded** (who may administer is your authz model) — bind `AuthnRequired` + an `AuthzBeforeAuthorize` on each op before exposing the router.

See [Tenant selector and admin recipe](https://morzecrew.github.io/forze/latest/recipes/tenant-selector-and-admin/).

## Tenant provisioning

The `namespace` / `dedicated` tiers assume the per-tenant container already exists. `TenantProvisionerPort` creates it on onboarding and tears it down on offboarding; wire it through `TenancyDepsModule.tenant_provisioner`:

```python
from forze.application.integrations.storage import ObjectStorageTenantProvisioner
from forze_identity.tenancy.execution import TenancyDepsModule

TenancyDepsModule(
    tenant_management={"main"},
    tenant_provisioner=ObjectStorageTenantProvisioner(
        client=s3_client,
        bucket=lambda tid: f"tenant-{tid}",
    ),
)
```

`TenantManagementPort.provision_tenant(...)` records the tenant first, then runs the provisioner — idempotent, so a failure leaves the record for retry; `deprovision_tenant(tenant_id)` runs the inverse. Provisioners receive the onboarded `TenantIdentity` **explicitly** (it is not the ambient bound tenant — an admin onboards tenant X without acting as X). Compose per-integration provisioners with `CompositeTenantProvisioner`, wrap a callable with `FunctionTenantProvisioner`, or omit it (`NoopTenantProvisioner`, the default). `forze_postgres` ships `PostgresSchemaTenantProvisioner` (`CREATE SCHEMA IF NOT EXISTS`); object storage ships `ObjectStorageTenantProvisioner`. Per-tenant encryption keys (BYOK) use the same seam — `VaultTransitTenantProvisioner` (`forze_vault`) and `AwsKmsTenantProvisioner` / `GcpKmsTenantProvisioner` / `YcKmsTenantProvisioner` (`forze_kms.*`) create a tenant's KEK on onboarding; see [KMS backends](kms-backends.md).

## Anti-patterns

- **Passing tenant ids through every DTO for routing** — bind `TenantIdentity` and use tenant-aware adapters.
- **Declaring strong isolation but never creating the namespace** — pair per-tenant resolvers / `required_tenant_isolation` with a `TenantProvisionerPort` so onboarding provisions the schema / bucket / dataset.

## Reference

- [Multi-tenancy](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/multi-tenancy/)
