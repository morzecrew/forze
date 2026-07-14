---
title: Tenant selector & administration
icon: lucide/building-2
summary: Self-service org-switching and the privileged tenant-admin plane over FastAPI
---

When a principal belongs to several tenants, two jobs appear: letting a user
**switch** between their own organizations, and letting an admin **manage**
organizations and their members. Both are projector-driven route sets that sit on
top of the [authn, authz & tenancy wiring](authn-authz-tenancy-fastapi.md) — no
custom handler code. The tenancy model itself is in
[Multi-tenancy](../identity-tenancy-enc/multi-tenancy.md).

## Tenant selector ("switch organization")

Ship a switcher with one extra projector. `build_tenancy_registry(spec)` reuses
the same authn `spec` (so it can re-mint tokens through that route's lifecycle)
and gives two operations that `attach_tenancy_routes` projects:

- `GET /tenants` — the principal's active memberships (`tenant_id`, `tenant_key`, `is_current`).
- `POST /tenants/{id}/activate` — validates the choice against membership via the
  `TenantResolverPort`, then returns a **new token pair scoped to the selected tenant** (the
  same body as `/login`); the client swaps to the new token.
- `DELETE /tenants/{id}` — leave a tenant. Keyed on the bound principal, so a caller can only
  drop their *own* membership; leaving the tenant on the current token makes its `tid` stop
  matching a live membership, so the next request fails closed (switch or re-authenticate).

```python
from forze_fastapi.routes import attach_tenancy_routes
from forze_kits.aggregates.tenancy import build_tenancy_registry

attach_tenancy_routes(
    router,
    registry=build_tenancy_registry(spec).freeze(),
    ns=spec.default_namespace,
    ctx_dep=ctx_dep,
)
```

The active tenant rides the signed `tid` claim and is re-validated against live membership on
every request, so removing a principal from a tenant invalidates their scoped token at once.
Requires a `TenancyDepsModule` wiring a `tenant_resolver` **and** `tenant_management` route
(see [Wire the planes](authn-authz-tenancy-fastapi.md#wire-the-planes)).

## Tenant administration

The selector is **self-service** — every op acts on the caller's *own* membership. Managing
*other* tenants and members (create an org, invite/remove a member, list members, deactivate a
tenant) is the privileged inverse, in a separate aggregate:
`build_tenancy_admin_registry(ns)` → `attach_tenancy_admin_routes` (`POST /tenants`,
`GET /tenants/{id}/members`, `POST /tenants/{id}/deactivate`, `POST`/`DELETE /memberships`).

Because *who* may administer a tenant is your authorization model — not something the framework
can define — these ops ship **unguarded**, exactly like `deactivate_principal` in the
[authn recipe](authn-authz-tenancy-fastapi.md#enforce-on-operations). Bind `AuthnRequired` plus
an `AuthzBeforeAuthorize` on each operation before exposing the router (or keep ops off it with
`include=`):

```python
from forze.application.hooks.authn import AuthnRequired
from forze.application.hooks.authz import AuthzBeforeAuthorize
from forze_fastapi.routes import attach_tenancy_admin_routes
from forze_kits.aggregates.tenancy_admin import TenancyAdminKernelOp, build_tenancy_admin_registry

reg = build_tenancy_admin_registry(spec.default_namespace)
for op in TenancyAdminKernelOp:
    reg = (
        reg.bind(spec.default_namespace.key(op))
        .bind_outer()
        .before(
            AuthnRequired().to_step(),
            AuthzBeforeAuthorize(spec=AUTHZ, action=f"tenants:{op}").to_step(),
        )
        .finish(deep=True)
    )

admin_router = APIRouter(prefix="/admin", tags=["tenant-admin"])
attach_tenancy_admin_routes(
    admin_router, registry=reg.freeze(), ns=spec.default_namespace, ctx_dep=ctx_dep
)
```

`list_members` returns principal ids only (`TenantManagementPort.list_tenant_principals`); join
them with identity-plane details out of band.

## Enumerating every tenant

The selector's `list_principal_tenants` answers *"which tenants may this principal see"*, so
anything driven from it visits only the tenants somebody happens to be a member of. To drive
work across the whole deployment — a migration, a per-tenant sweep, an export — use
`TenantManagementPort.list_tenants`, which is not membership-scoped:

```python
tenants, total = await mgmt.list_tenants(limit=100, offset=0)          # every tenant, paged
live, _ = await mgmt.list_tenants(active_only=True)                    # only the live ones
```

`active_only=False` is the default **on purpose**. Deactivating a tenant sets a flag; it does
not delete the row, and it certainly does not delete the tenant's documents, blobs or counters.
A sweep that quietly skipped them would drop real data and report success — so the complete
answer is what you get unless you ask for less. Pass `active_only=True` for a "who is live"
admin view, where that genuinely is the question.

Like the rest of the admin plane, this lists **every** tenant in the deployment, including ones
the caller belongs to none of — put it behind your authz hooks.

## Notes

- **Both build on the base wiring.** The middleware, deps modules, and enforcement hooks come
  from [Authn, authz & tenancy](authn-authz-tenancy-fastapi.md) — these route sets only add the
  org-switch and admin surfaces.
- **The selector is safe to ship as-is; the admin plane is not.** Selector ops are keyed on the
  caller's own principal; admin ops ship unguarded and must carry your authz hooks first.
