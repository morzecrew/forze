# Multi-tenancy

Forze separates **who is authenticated** (`AuthnIdentity`), **which tenant applies** (`TenantIdentity`), and **how data is isolated** (routed clients and `tenant_aware` document/search configuration).

## Request binding

Typical HTTP flow:

1. Resolve `AuthnIdentity` (bearer token, API key, etc.).
2. Resolve `TenantIdentity` via `TenantIdentityResolver`: merge, in order, credential-bound `AuthnIdentity.tenant_id`, an optional header hint (`HeaderTenantIdentityCodec`), and `TenantResolverPort.resolve_from_principal` when registered.
3. `ExecutionContext.bind_call` attaches both so adapters can call `get_tenancy_identity()`.

If credential validation reads **tenant-scoped** document ports (`tenant_aware=True`) before step 2 completes, bootstrap can deadlock. Keep **authentication document routes** (`AUTHN_TENANT_UNAWARE_DOCUMENT_SPEC_NAMES` in `forze_authn.application`) on **tenant-unaware** stores or global registry clients.

## Optional JWT tenant claim

Access tokens issued by `forze_authn` may include optional claim ``tid`` (UUID string). It round-trips into `AuthnIdentity.tenant_id` and is persisted on refresh sessions when present.

## Reference package

`forze_tenancy` provides example aggregates (`tenant_spec`, `principal_tenant_binding_spec`), `TenantResolverAdapter`, `TenantManagementAdapter`, `ConfigurableTenantResolver` / `ConfigurableTenantManagement`, and `TenancyDepsModule` to register resolver/management routes on the kernel `Deps`. Applications register matching document routes and wire `TenantResolverDepKey` / `TenantManagementDepKey` as needed.
