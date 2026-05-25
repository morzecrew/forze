# Multi-tenancy

Forze separates **who is authenticated** (`AuthnIdentity`), **which tenant applies** (`TenantIdentity`), and **how data is isolated** (routed clients and `tenant_aware` document/search configuration).

## Request binding

Typical HTTP flow:

1. Authenticate the request into a principal-only `AuthnIdentity` plus optional issuer metadata (for example JWT `tid` surfaced as `issuer_tenant_hint`).
2. Resolve `TenantIdentity` via `TenantIdentityResolver`: validate optional issuer and header tenant hints against `TenantResolverPort.resolve_from_principal` when registered. The resolver is authoritative; hints never outrank principal membership.
3. `ExecutionContext.inv.bind` attaches both so adapters can call `ctx.inv.get_tenant()`.

If credential validation reads **tenant-scoped** document ports (`tenant_aware=True`) before step 2 completes, bootstrap can deadlock. Keep **authentication document routes** (`AUTHN_TENANT_UNAWARE_DOCUMENT_SPEC_NAMES` in `forze_authn.application`) on **tenant-unaware** stores or global registry clients.

## Optional JWT tenant claim

Access tokens issued by `forze_authn` may include optional claim ``tid`` (UUID string). On verification it becomes `issuer_tenant_hint` on the boundary authn result; it does not become canonical tenant context by itself. When tokens are explicitly issued with tenant metadata, refresh sessions persist that `tenant_id`.

## Reference package

`forze_tenancy` provides example aggregates (`tenant_spec`, `principal_tenant_binding_spec`), `TenantResolverAdapter`, `TenantManagementAdapter`, `ConfigurableTenantResolver` / `ConfigurableTenantManagement`, and `TenancyDepsModule` to register resolver/management routes on the kernel `Deps`. Applications register matching document routes and wire `TenantResolverDepKey` / `TenantManagementDepKey` as needed.
