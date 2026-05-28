# Authorization

Forze separates authorization into three contract slices. Use the slice that matches the question you are answering; do not overload operation checks with row-level policy.

## Three slices

| Slice | Port | Question |
| --- | --- | --- |
| **Decision** | `AuthzDecisionPort` | May this subject invoke this operation/action? |
| **Data scoping** | `AuthzScopePort` | Which rows/documents may list/search/read paths return? |
| **Grant management** | `PrincipalRegistryPort`, `RoleAssignmentPort`, `GrantQueryPort` | How are principals, roles, permissions, and bindings provisioned? |

Value objects live under `forze.application.contracts.authz.value_objects`:

- **Catalog** (`catalog.py`): `PermissionRef`, `RoleRef`, `GroupRef`, `PrincipalRef`.
- **Decision** (`decision.py`): `AuthzSubject`, `AuthzScope`, `AuthzResource`, `AuthzRequest`, `AuthzDecision`.
- **Scoping** (`scoping.py`): `AuthzDocumentScopeRequest`, `AuthzDocumentScope`, `AuthzSensitiveAccessRequest`.
- **Grants** (`grants.py`): `EffectiveGrants`.

`AuthzSpec` describes backend behavior (`tenancy_mode`, `enforce_principal_active`). Route configurables with `AuthzDepsModule` (`decision`, `scope`, `grant_query`, …).

## Execution integration

`ExecutionContext` exposes `ctx.authz` (`AuthzDeps`):

- `ctx.authz.decision(spec)` — decision port for the configured route (`spec.name`).
- `ctx.authz.scope(spec)` — scoping port.
- `ctx.authz.grant_query(spec)` — effective grants snapshot.
- `ctx.authz.principal_registry(spec)` / `ctx.authz.role_assignment(spec)` — catalog and binding management.

Authoritative enforcement belongs on the **operation plan**, not in generic `DocumentCoordinator` or FastAPI route policies alone. Use `forze.application.hooks.authz` (not `contracts.authz`):

1. **`BeforeStep`** — operation-level allow/deny (`authorize_before_step` or `AuthzBeforeAuthorize(...).to_before_step()`).
2. **`wrap`** — inject policy filters into list/search DTOs (`document_scope_wrap_step` or `AuthzDocumentScopeWrap(...).to_middleware_step()`).

Helpers read `ctx.inv_ctx.get_authn()` and `ctx.inv_ctx.get_tenant()` to build `AuthzSubject` and `AuthzScope` (`policy_scope_from_invocation`).

```python
from forze.application.contracts.authz import AuthzSpec
from forze.application.hooks.authz import authorize_before_step, document_scope_wrap_step
from forze.application.execution.registry import OperationRegistry
from forze.base.primitives import str_key_selector

registry = (
    OperationRegistry(handlers={"widgets.list": list_factory, "widgets.read": read_factory})
    .patch(str_key_selector.prefix("widgets."))
    .bind_outer()
    .before(
        authorize_before_step(
            step_id="authz",
            spec=AuthzSpec(name="main"),
            action="widgets.read",
        ),
    )
    .wrap(
        document_scope_wrap_step(
            step_id="scope_list",
            spec=AuthzSpec(name="main"),
            document_name="widgets",
            operation="list",
            action="widgets.list",
        ),
    )
    .finish(deep=True)
    .freeze()
)
```

### Indirect access hardening

- **Operation authz** answers whether the use case may run.
- **Scoping** answers which data that use case may see (tenant filters, merged `$and` query filters).
- **Sensitive reads** must call `AuthzScopePort.authorize_sensitive_resource` (or a dedicated guarded helper) before loading nested documents by id.
- **Nested dispatch** inherits protection when child operations are covered by the same `OperationRegistry.patch(selector)` authz steps; parent permission alone is not enough.

## Built-in backend (`forze_identity.authz`)

`forze_identity.authz` implements the contracts with document-backed catalogs (roles, permissions, groups, bindings) and `AuthzPolicyService` (permission-key match plus optional owner checks on `AuthzResource.attributes["owner_id"]`).

Wire with `AuthzDepsModule(kernel=AuthzKernelConfig(), decision={"main"}, scope={"main"}, …)` merged into kernel `Deps`, plus Postgres/Mongo document routes for authz resource names.

`PrincipalRef` no longer carries `tenant_id`; use `AuthzScope` in decision and scoping requests.

## Transport boundary

FastAPI `ContextBindingMiddleware` binds `AuthnIdentity` and optional `TenantIdentity` into `ctx.inv_ctx`. HTTP features (`RequireAuthnFeature`, `RequireTenantFeature`) may fast-fail at the edge; kernel `BeforeStep` / `wrap` hooks remain authoritative for non-HTTP callers.

See also: [Authentication](authentication.md), [Middleware & Plans](middleware-plans.md), [Capability execution](capability-execution.md), [Authn, authz, and tenancy (FastAPI)](../recipes/authn-authz-tenancy-fastapi.md).
