# RFC 0082 — Tenant-scoped roles in the authorization plane

- **Status:** 📝 Draft — P1 is one PR; P2 is demand-gated.
- **Scope:** Adds **tenant-held role bindings**: a principal-role binding may name the one tenant
  it holds in. A principal can then be `analyst` in one tenant and `viewer` in another, under one
  role catalog shared by every tenant, and a binding that names no tenant still holds everywhere.
  The roles themselves stay the catalog's; only the binding is held per tenant. The change is:
  - a nullable `scope_tenant_id` on `PrincipalRoleBinding`;
  - a tenant predicate on the grant resolver's principal-role read, keyed on the bound invocation
    tenant only;
  - a tenant for the rows `assign_role` and `revoke_role` write and remove;
  - one new method, `RoleAssignmentPort.revoke_tenant_roles`, which `detach_principal` calls so
    that leaving a tenant drops the roles held in it.

  No new port, spec, document, `tenancy_mode` or package. Every deployment adds one nullable column
  to its principal-role binding table. Two behaviors change (Breaking):
  - inside a tenant-bound invocation, an unscoped `assign_role` binds in that tenant, where today
    it binds in every tenant;
  - an unscoped `revoke_role` inside a tenant-bound invocation is refused when the role is held
    globally, where today it removes the role.

  It does **not** let tenants define their own roles or permissions (that stays the per-tenant
  catalog of `require_invocation_tenant`). It does not scope direct permission grants, group
  membership or delegation grants (P2, demand-gated). It does not make a role imply tenant
  membership, does not reduce the number of documents the plane needs bound, and changes nothing
  in the grants cache or the [RFC 0068](0068-cross-process-grants-cache-invalidation.md) broadcast.
- **Related:**
  - [`src/forze_identity/authz/domain/models/bindings.py:41-62`](../src/forze_identity/authz/domain/models/bindings.py)
    (`PrincipalRoleBinding`, the row that gains the column);
  - [`src/forze_identity/authz/services/grants.py`](../src/forze_identity/authz/services/grants.py)
    (`_invocation_tenant` `:162`, `_require_scope_matches_invocation` `:174`,
    `resolve_effective_grants` `:241`, `_direct_role_ids` `:360`);
  - [`src/forze_identity/authz/adapters/role_assignment.py`](../src/forze_identity/authz/adapters/role_assignment.py)
    (`assign_role` `:72`, `revoke_role` `:110`, `list_roles` `:145`,
    `_find_principal_role_binding` `:166`);
  - [`src/forze_identity/authz/adapters/authorization.py:49-56`](../src/forze_identity/authz/adapters/authorization.py)
    and [`src/forze/application/contracts/authz/helpers.py:19-52`](../src/forze/application/contracts/authz/helpers.py)
    (how a decision's scope is resolved today);
  - [`src/forze_identity/authz/adapters/_utils.py:32-84`](../src/forze_identity/authz/adapters/_utils.py)
    (`validate_authz_query_ports`);
  - [`src/forze/application/contracts/authz/specs.py:9-39`](../src/forze/application/contracts/authz/specs.py)
    (`AuthzTenancyMode`) and [`ports.py`](../src/forze/application/contracts/authz/ports.py)
    (`RoleAssignmentPort`, which gains a method);
  - [`src/forze/application/contracts/tenancy/mixins.py:12-30`](../src/forze/application/contracts/tenancy/mixins.py)
    (`effective_tenant`, the rule reused for a written binding's tenant);
  - [`src/forze_identity/tenancy/adapters/management.py:126-139`](../src/forze_identity/tenancy/adapters/management.py)
    (`detach_principal`, which gains the role cascade) and
    [`src/forze_identity/tenancy/domain/models/principal_tenant_binding.py`](../src/forze_identity/tenancy/domain/models/principal_tenant_binding.py)
    (the precedent: a tenant named by a column on a row stored without tenant awareness);
  - [`src/forze_identity/authn/execution/deps/module.py:129`](../src/forze_identity/authn/execution/deps/module.py)
    (`AuthnDepsModule(authz_route=...)`, the cross-plane cascade this mirrors);
  - [`src/forze/application/hooks/authz/plans.py:48-56`](../src/forze/application/hooks/authz/plans.py)
    (`policy_scope_from_invocation`);
  - [`src/forze_identity/authz/services/grants_cache.py`](../src/forze_identity/authz/services/grants_cache.py)
    (the key `:36`, `forget` `:167`);
  - [`src/forze_mock/adapters/identity/authz.py:237-330`](../src/forze_mock/adapters/identity/authz.py)
    (the mock decision port, which already models global-or-tenant grants);
  - [RFC 0009](0009-external-identity-authz-integration.md) W2 and W3 (roles delivered by claims
    or decided by a remote engine), [RFC 0057](0057-derived-permissions-and-config-grants.md)
    (derived permissions, the alternative in §5.9), [RFC 0068](0068-cross-process-grants-cache-invalidation.md)
    (the broadcast this leaves alone), [RFC 0055](0055-scoped-disclosure.md) (grants bound to a
    resource, which this is not), [RFC 0081](0081-authentication-eligibility-without-an-authorization-plane.md)
    (the eligibility read this keeps working).
- **Origin:** bi-backend decision D49 (`bi-backend/DECISIONS.md:58`). Its layer of per-workspace
  roles is hand-built, because the authz plane "holds grants per principal across the
  deployment, not per tenant".

---

## 1. Summary

This RFC adds tenant-held role bindings. `PrincipalRoleBinding` gains
`scope_tenant_id: UUID | None`:

- `None` means the binding holds in every tenant and in calls with no tenant bound. That is what
  every binding means today.
- A UUID means the binding holds only in that tenant.

The role is still the shared catalog's; only the binding is held in one tenant.

**Decisions.** When a decision reads a principal's role bindings, it keeps the global rows and the
rows held in the **bound** invocation tenant. With no tenant bound it keeps the global rows only,
whatever scope the caller passes.

**Writes.** `assign_role` writes the row in the invocation's tenant. Only with nothing bound does
it write the row in an explicitly named tenant. `revoke_role` follows two rules:

- in a tenant, it refuses when the role is held globally, instead of quietly leaving that grant in
  force;
- with no tenant, it removes the role everywhere, as it does today.

**Leaving a tenant.** `detach_principal` revokes the roles the principal held in that tenant, then
removes the membership.

**Unchanged.** The catalog stays shared. The check side, the grants cache and the RFC 0068
broadcast need no change, because they already carry the invocation tenant. An existing deployment
adds one nullable column, and its existing grants keep their meaning.

## 2. Motivation

**bi-backend builds per-workspace roles by hand.** bi is the one multi-tenant consumer: a
workspace is a forze tenant, and a user can belong to several. D49 (`DECISIONS.md:58`) gives each
member one of two roles. An **analyst** may do everything. A **viewer** reads dashboards,
metrics, data quality and the questions log, may chat, and may sign in and switch workspaces.
D49 records why bi did not use the authz plane: *"forze's authz plane holds grants per principal
across the deployment, not per tenant, and needs half a dozen documents wired; a role column per
membership and one hook cover two roles."* It replaced D6 (`DECISIONS.md:15`), *"No
authorization plane yet … Roles come when there is a second kind of user to tell apart."* The
workaround is 268 lines in `bi/platform/members.py`, plus a table
(`bi-backend/db/migrations/postgres/0016__members.sql`). Code paths below are under
`bi-backend/app/src/`:

- a tenant-aware `members` document holding `(principal_id, role)` beside forze's tenancy binding
  (`members.py:36-44`; `wire/configs.py:66`);
- `role_of`, which reads the member row in the active workspace (`members.py:104-111`). It is
  called by `/auth/me` (`api/v1/routers/auth.py:82`), by the chat's tool palette
  (`bi/assistant/chat/handlers.py:104`) and by bootstrap (`bi/platform/bootstrap.py:93`);
- `analysts_only`, one before-hook that refuses a signed-in non-analyst with `analysts_only`
  (`members.py:114-129`). It is patched onto every operation outside an allowlist (`VIEWER_MAY`
  and `viewer_may`, `members.py:50-101`; `bi/registry.py:28-31`). Deny by default: *"an
  operation added later is an analyst's until it is"* listed;
- `ensure_member`, `SetMember` and `RemoveMember` (`members.py:132-268`). They attach or detach
  the tenancy binding together with the role row and keep at least one analyst per workspace;
- `eligibility="allow_all"` (`wire/execution.py:280`), because no policy principals exist.

What bi needed, exactly:

1. **Role definitions fixed in code and the same in every workspace.** Two roles, no editor.
   Nothing here is tenant-defined.
2. **A role bound to a principal in one workspace.** The same person can be an analyst in their
   shop and a viewer in a client's.
3. **Checks within the active workspace**, at the operation level, denied by default.
4. **The role in the active workspace, readable** for the UI and the chat.
5. Membership and role managed together, with app rules on top (the last analyst stays, the
   creator becomes analyst). Runs with no principal (the worker, the console) pass.

Items 1 and 3 exist in forze today. Item 2 does not, and it is the reason bi left the plane.
Items 4 and 5 follow once item 2 exists (§5.7).

**The other consumers do not need it yet; one could be hurt by a careless fix.** erp-backend
and eis-backend are single-tenant. Each uses the plane with global roles fixed in code: six in
erp (`erp-backend/app/src/erp/application/composition/permissions.py:55-63`), two in eis
(`eis-backend/app/src/eis/application/specs/auth.py:7-21`). Both call `assign_role` and
`revoke_role` with no tenant bound (`erp-backend/app/src/erp/application/handlers/users.py:198-206`;
`eis-backend/app/src/api/accounts.py:127-135`). eis keeps one role per account by hand, revoking
the others first. hh-backend-v2 has a single constant tenant. It does not wire the authz plane,
but it writes `authz_principal_role_bindings` with raw SQL in two places
(`hh-backend-v2/app/src/wire/adapters/accounts.py:59-62` and `wire/adapters/operator.py:72`) and
reads it with a raw join (`wire/adapters/principals.py:50-70`). Its table carries
`UNIQUE (principal_id, role_id)` (`hh-backend-v2/db/migrations/postgres/0013__identity_authz.sql:110`).
A design that changed what an existing row means, or what an unscoped call with no tenant bound
does, would break all three. This one changes neither (§5.8).

## 3. Current state

Verified against `main` at the commit this RFC was written on.

- **No authz row names a tenant.** `PrincipalRoleBinding` is `(principal_id, role_id)`
  (`bindings.py:41-62`). The other five bindings, the delegation grant and the role and
  permission definitions have no tenant field either (`bindings.py`, `role_definition.py`).
  `AuthzScope` is `tenant_id: UUID | None` and nothing else.
- **Only storage partitions by tenant, all or nothing.** The resolver says so: *"Tenant isolation
  of grant resolution is enforced by the storage layer scoping binding queries to the ambient
  invocation tenant"* (`grants.py:177-182`). A tenant-aware port adds `tenant_id = <bound>` to
  every read (`forze_postgres/kernel/gateways/base.py:248-279`). With no tenant bound it raises
  `tenant_required` (`mixins.py:48-61`). Under `tenancy_mode="require_invocation_tenant"`, the
  adapters refuse to build when any of the ten ports they read is not tenant-aware
  (`_utils.py:57-72`). The ten include the policy principal (`adapters/authorization.py:33-47`).
  That leaves two configurations:
  - **Shared (`global`, ports without tenant awareness).** One catalog and one binding set. A
    role a principal holds, it holds in every tenant. bi's item 2 cannot be expressed. Every
    consumer that uses the plane runs this way.
  - **Per tenant (`require_invocation_tenant`, every port tenant-aware).** Each tenant owns a
    whole catalog. Role definitions, permissions and role-permission bindings are seeded per
    tenant, and changing the catalog means migrating it in every tenant. No binding can hold in
    every tenant, so a platform operator needs a binding in each. The policy principal must be
    tenant-aware too. But authn's default eligibility gate reads that same route at sign-in
    (`authn/adapters/principal_eligibility.py:46-56`). A password login by a member of several
    tenants binds no tenant (`tenancy/adapters/resolver.py:73-77`), so the read raises
    `tenant_required`. This conflict is found by reading the code; no test combines the two. No
    consumer runs this configuration.
- **The check already happens inside the invocation tenant.** `AuthzBeforeAuthorize` and
  `require_permission` build the scope from the invocation (`plans.py:48-56`, `:170`). The
  resolver reads the tenant from `ctx` on every call (`grants.py:162-170`), and it refuses a scope
  that names a different tenant (`authz.scope_tenant_mismatch`, `:174-195`). A decision therefore
  already knows which tenant it is in. Only the rows it reads ignore that.
- **An explicit scope stands in for the tenant when none is bound.** `resolve_policy_scope`
  returns a caller's explicit scope whenever it names a tenant (`helpers.py:27-34`). The decision
  adapter passes the request's own scope as the "invocation tenant" (`authorization.py:50-54`).
  Today that is harmless, because bindings ignore the tenant. Once bindings carry one, it must not
  select them (§5.2).
- **The grants cache is keyed by tenant already.** The key is (principal, invocation tenant,
  requested scope) (`grants_cache.py:36-37`). `forget(principal)` drops that principal's entries
  in every tenant and scope (`:167-168`). RFC 0068's markers name a principal only, and the
  marker route must not be tenant-aware (`:332-337`).
- **Assignment reads no invocation tenant, and its lookup ignores the tenant.**
  `RoleAssignmentAdapter` passes the scope's own tenant to `resolve_policy_scope` as the
  invocation tenant (`role_assignment.py:79-83`, `:117-121`, `:151-155`). In
  `require_invocation_tenant` mode, an `assign_role` with no scope therefore fails with
  `tenant_required` even when the request has a tenant bound (`helpers.py:36-40`).
  `_find_principal_role_binding(principal_id, role_id)` returns the first of the principal's rows
  for the role (`:166-180`). Assign uses it to skip a duplicate (`:97-104`), and revoke uses it to
  pick the row it kills (`:136-138`). Idempotency is that read followed by a create; the canonical
  DDL has no unique index.
- **Leaving a tenant touches only membership.** `detach_principal` kills the
  principal-tenant binding rows and nothing else (`management.py:126-139`). It is called by the
  `tenancy_admin` kit's `RemoveMember` (`forze_kits/aggregates/tenancy_admin/handlers.py:75-81`),
  and directly by apps such as bi. The house already has one cross-plane cascade of this kind:
  authn's principal deactivation reaches authz through `AuthnDepsModule(authz_route=...)`
  (`authn/execution/deps/module.py:129`, `:313-322`; `authn/execution/deps/deps.py:676-693`).
- **The mock already models global-or-tenant grants.** `MockAuthzDecisionPort.seed_grant` takes
  an optional `tenant_id`. A grant seeded without one matches any scope, and one seeded with one
  matches only that tenant (`forze_mock/adapters/identity/authz.py:237-330`). The mock's
  `RoleAssignmentPort` is a no-op stub (`:108-135`).
- **The `global` docstring and warning overstate.** `specs.py:29-38` and `_utils.py:74-84` say
  that in `global` mode over tenant-aware ports, "roles and permissions are shared across all
  tenants". But the storage filter above applies in either mode. Over tenant-aware ports, `global`
  still partitions per bound tenant and still refuses calls with no tenant. All it skips is the
  adapter's own check. (Found by reading the code.)
- **Membership is a separate row with a tenant column.** `PrincipalTenantBinding` is
  `(principal_id, tenant_id)` on a route without tenant awareness (`principal_tenant_binding.py:10-24`).
  Ingress checks a requested tenant against it (`execution/ingress/authn.py:103-108`,
  `resolver.py:48-77`). A role binding that names its tenant in a column follows the same
  precedent.
- **Tenant-aware storage reserves the `tenant_id` column.** It is the partition column, `NOT NULL`
  (`forze/application/contracts/tenancy/constants.py:7`; `base.py:283-291`). A model field with
  that name would collide with it on a tenant-aware route.
- **Mechanics are already in place.** `$values` with `None` is a null check
  (`querying/expressions.py:17-18`), and `$or` exists (`:163`). Firestore renders OR
  (`forze_firestore/kernel/query/render.py:136-145`). Postgres refuses to boot on a missing mapped
  column (`forze_postgres/kernel/catalog/validation/validate_schema.py:166-169`), and its validator
  understands `NULLS NOT DISTINCT` (`:357`).

## 4. Goals / Non-goals

**Goals**

- A principal can hold a role in one tenant and not in another, with one catalog for all tenants.
- Global bindings keep working beside tenant ones: a platform operator, or a worker's service
  principal.
- A role bound in tenant T never grants outside T. It never grants in a call with no tenant bound,
  whatever scope that call passes.
- A revocation never silently leaves a grant in force. Leaving a tenant drops the roles held in
  it.
- Existing rows, and existing calls made with no tenant bound, keep their meaning.
- bi can delete its member table, its role lookup and its hook, and keep only its app rules.

**Non-goals**

- **Not tenant-defined roles.** A tenant that writes its own roles or permissions needs a
  catalog per tenant, which `require_invocation_tenant` already provides with its known costs
  (§3). bi does not need it, and no consumer has asked.
- **Not membership.** Whether a principal may enter a tenant stays the tenancy plane's question.
  A role answers what the principal may do once inside.
- **Not resource-scoped roles** (editor of project P). A tenant is a partition, not a resource;
  that is relationship-based authz, the RFC 0009 W3 engines' ground, or RFC 0055's per-grant
  shape.
- **Not exclusive roles.** "One role per principal per tenant", as bi and eis keep it, stays app
  logic. The binding model allows several.
- **Not less wiring.** D49's other complaint, the ten documents a decision reads, is real and
  separate (§8).

## 5. Design

### 5.1 The binding row

```python
class PrincipalRoleBinding(Document):
    principal_id: UUID = Field(frozen=True)
    role_id: UUID = Field(frozen=True)
    scope_tenant_id: UUID | None = Field(default=None, frozen=True)
    """The tenant this binding holds in. ``None`` holds in every tenant and in calls with no
    tenant bound."""

# CreatePrincipalRoleBindingCmd and ReadPrincipalRoleBinding gain the same field.
```

The field is frozen like its neighbours. Moving a binding to another tenant is a revoke and an
assign. It is named `scope_tenant_id`, after `AuthzScope.tenant_id`, because `tenant_id` is the
partition column of tenant-aware storage (§3). Under the name `tenant_id`, the row could not be
stored on a tenant-aware route, which the per-tenant configuration requires.

The canonical Postgres DDL gains:

```sql
ALTER TABLE authz_principal_role_bindings ADD COLUMN scope_tenant_id uuid;
-- one row per principal, role and tenant; NULL (every tenant) counts as one value
CREATE UNIQUE INDEX authz_principal_role_bindings_uq
    ON authz_principal_role_bindings (principal_id, role_id, scope_tenant_id) NULLS NOT DISTINCT;
```

A deployment whose table already has `UNIQUE (principal_id, role_id)`, as hh's does, must drop it
in the same migration. Otherwise a tenant row for a role the principal holds globally cannot be
inserted. Existing rows get `NULL` and keep meaning "every tenant". The route stays without tenant
awareness in the shared configuration: global rows and tenant rows live in one table, as
membership does.

### 5.2 Resolution: decisions read the bound tenant only

The rule fits in one line: **a binding holds in a decision when its tenant is `None` or equals the
bound invocation tenant.** The tenant is `_invocation_tenant()`, read from `ctx`
(`grants.py:162-170`). It is never `scope.tenant_id`. The two cases differ when nothing is bound:

- Today an explicit `AuthzScope(tenant_id=U)` reaches the decision as its scope
  (`helpers.py:27-34`, `authorization.py:50-54`).
- If that scope selected bindings, any code running with no tenant bound could read U's roles
  without the membership check ingress performs.

So with nothing bound, a decision keeps global rows only, whatever scope it was given. With a
tenant bound, a scope naming another tenant is still refused (`authz.scope_tenant_mismatch`,
`:174-195`).

```python
def _role_binding_filter(principal_id: UUID, tenant_id: UUID | None) -> QueryFilterExpression:
    if tenant_id is None:
        # Nothing bound: only bindings that hold everywhere.
        return {"$values": {"principal_id": principal_id, "scope_tenant_id": None}}

    return {
        "$and": [
            {"$values": {"principal_id": principal_id}},
            {"$or": [
                {"$values": {"scope_tenant_id": None}},
                {"$values": {"scope_tenant_id": tenant_id}},
            ]},
        ],
    }

# in _direct_role_ids: _role_binding_filter(principal_id, self._invocation_tenant())
```

`_direct_role_ids` reads principal-role bindings through this filter in place of
`{"$values": {"principal_id": ...}}` (`grants.py:365-371`). Both paths that read those bindings go
through it: `_catalog_grants` for decisions, scoping and grant queries, and `list_assigned_roles`
for `list_roles`. Group-role bindings, role lineage and permission reads stay as they are: the
catalog is shared. The cache key keeps its scope element. An unbound call with scope U gets an
entry of its own, computed from global rows only. Where the predicate runs, in the query or as a
filter over the principal's rows, is row 15.

What fails closed:

- A call with no tenant bound sees only global bindings, explicit scope or not. A tenant role
  never leaks into untenanted work: background jobs, the tenant selector, platform tooling.
- A scope naming tenant U inside a request bound to T is refused, as it is today.
- The tenant that selects bindings comes from the invocation, which ingress has checked against
  membership (§5.4). It never comes from the row's writer or from request data.

### 5.3 Assignment

`assign_role`, `revoke_role` and `list_roles` keep their signatures (`RoleAssignmentPort`).
Writing a binding is grant management, not a decision, so the tenant it writes in follows
`effective_tenant(bound=<invocation tenant>, requested=<scope.tenant_id>)` (`mixins.py:12-30`). An
explicit tenant counts only when nothing is bound, so platform tooling can act on a tenant. The
app exposes such writes only behind an operation guarded by a global permission, as the
`tenancy_admin` kit guards `tenants:<op>` (`forze_kits/aggregates/tenancy_admin/factories.py:38-45`).
A row written for T grants only in T, and only to a member who enters T.

| Invocation | `scope` | `assign_role` | `revoke_role` |
| --- | --- | --- | --- |
| tenant T bound | none or T | writes T's row | refuses `authz.role_held_globally` if a global row exists; else removes T's row |
| tenant T bound | U | refused (`authz.scope_tenant_mismatch`) | refused (`authz.scope_tenant_mismatch`) |
| no tenant bound | T | writes T's row | as for tenant T bound |
| no tenant bound | none | writes the global row | removes the global row **and every tenant row** of that role |

The lookup takes the tenant and matches it exactly: `NULL` against `NULL`, T against T. Today's
helper returns the first row for the role, whatever its tenant (§3). With that helper, assign in T
would find a global row and skip T's row, and revoke in T could kill the global row.

```python
async def _find_principal_role_binding(self, principal_id, role_id, tenant_id):
    return await self.pr_binding_qry.find(filters={"$values": {
        "principal_id": principal_id, "role_id": role_id,
        "scope_tenant_id": tenant_id,               # None is a null check: exact match either way
    }})


async def assign_role(self, subject, role_key, *, scope=None) -> None:
    tenant_id = self._write_tenant(scope)                    # effective_tenant (row 5)
    ...
    if await self._find_principal_role_binding(pid, role.id, tenant_id) is None:
        await self.pr_binding_cmd.create(
            CreatePrincipalRoleBindingCmd(principal_id=pid, role_id=role.id,
                                          scope_tenant_id=tenant_id),
            return_new=False,
        )
    await self.resolver.forget(pid)                          # unchanged (§5.5)


async def revoke_role(self, subject, role_key, *, scope=None) -> None:
    tenant_id = self._write_tenant(scope)
    ...
    if tenant_id is None:
        # No tenant: everywhere, as today. The global row and every tenant row of the role.
        rows = await self._role_bindings(pid, role.id)
    elif await self._find_principal_role_binding(pid, role.id, None) is not None:
        raise exc.conflict(
            f"{role_key!r} is held in every tenant; revoke it with no tenant bound",
            code="authz.role_held_globally",
        )
    else:
        rows = [row] if (row := await self._find_principal_role_binding(pid, role.id, tenant_id)) else []

    for row in rows:
        await self.pr_binding_cmd.kill(row.id)

    await self.resolver.forget(pid)
```

Assignment:

- **A forgotten scope narrows a grant; it never widens one.** Inside a tenant-bound request, an
  unscoped `assign_role` binds in that tenant. That is the opposite of today, and the direction
  that fails closed. Assume a bi `SetMember` that forgets the scope: today it would make the
  member an analyst of every workspace. After this change, it makes them one of this workspace.
  A global binding is written only from an invocation with no tenant bound: a migration, a CLI,
  a platform admin's operation.
- Assigning in T while a global row exists writes T's row too. It is redundant until the global
  row is revoked, and then it keeps the role in T, as asked.

Revocation never silently leaves a grant in force:

- **In a tenant, a global grant blocks the revoke.** Today, an unscoped `revoke_role` inside a
  tenant-bound request removes the role everywhere. If it now removed only T's row, every
  pre-existing global row would survive a revoke from a tenant-bound call. That would fail open
  for existing deployments. It is therefore refused with `authz.role_held_globally`, which names
  the role and the remedy.
- **With no tenant, the role goes everywhere.** An unscoped revoke with nothing bound removes the
  role in every tenant, as it does today. A platform admin's revoke reaches the tenant rows too.
- **Several roles at once.** `revoke_tenant_roles(subject, *, tenant_id)` removes every row the
  subject holds in T and leaves global rows alone. It is the call leaving a tenant makes (§5.4).
  Like the other writes, it refuses a `tenant_id` that contradicts a bound tenant.

Listing and fixes:

- `list_roles` returns the roles that hold in the bound tenant: global ones plus T's. With nothing
  bound it returns global ones only. That is what `/auth/me` wants. A listing of who holds what
  in T is a query on the binding spec, which is public (§5.7).
- The adapter reads the invocation tenant itself. This also fixes the wart in §3, where an
  unscoped assign in `require_invocation_tenant` mode fails with `tenant_required` inside a
  tenant-bound request. How the adapter reaches the tenant is row 16.

### 5.4 Membership

Membership stays the tenancy plane's. The two compose in both directions that matter:

- **Membership decides whether a principal may enter T; the role decides what it may do there.**
  With a tenancy resolver configured, ingress refuses to bind a tenant the principal is not a
  member of (`tenant_mismatch`, `resolver.py:64-69`).
- **Leaving T drops the roles held in T, by default.** `detach_principal(principal, T)` first calls
  `RoleAssignmentPort.revoke_tenant_roles(principal, tenant_id=T)`, then kills the membership row,
  in the caller's transaction. Global bindings and other tenants' bindings stay. A principal who
  is attached again starts with no roles in T.
  - The order is deliberate. A failure between the two steps leaves a member with no roles in T,
    never a former member who still holds them.
  - The `tenancy_admin` kit's `RemoveMember` and every app that calls `detach_principal` directly
    inherit the cascade, bi included.
- **Wiring.** `TenancyDepsModule(authz_route=...)` names the authz route whose role-assignment
  port `detach_principal` uses. This is the same shape as `AuthnDepsModule(authz_route=...)` for
  the deactivation cascade (§3). A runtime that registers tenant management and a role-assignment
  port but names no `authz_route` is refused at startup. Otherwise the default would be off
  exactly where it matters. Which check detects that pairing is row 18.
- **A role does not imply membership, and assigning does not check it.** A role may be granted
  before the principal joins. An invite flow grants it, accepting the invite attaches, and until
  then the row is unreachable. The authz plane gains no read of tenancy rows. A pre-staged role
  whose invite is abandoned stays until revoked (§9).

Without a tenancy resolver, ingress trusts a tenant carried on a verified credential
(`ingress/authn.py:113-125`). A tenant role then holds wherever that credential says. This is
the same trust every tenant-aware document already extends to it.

### 5.5 Cache and invalidation

Nothing changes, and the reason is checked rather than assumed:

- The key already holds the invocation tenant and the scope (`grants_cache.py:36-37`). An entry
  for T never answers for U, and an entry made with no tenant never answers for T.
- `assign_role`, `revoke_role` and `revoke_tenant_roles` call `forget(principal)` after commit.
  It drops the principal's entries in every tenant (`:167-168`). An assignment in T therefore
  also discards the principal's cached grants for U. This costs one extra read per other tenant
  and is never incorrect.
- RFC 0068 markers name a principal and are forgotten in every tenant (`principal:<uuid>`). The
  marker format and the rule that the marker route is not tenant-aware still fit.
- `invalidate_grants(ctx, kernel, principal_id)` after a plain document write to the binding
  table is unchanged.

### 5.6 `tenancy_mode`

No new mode. Tenant-held bindings belong to the shared configuration: `global` mode over ports
without tenant awareness. That becomes the recommended shape for any deployment with a fixed
catalog, single-tenant or not. `require_invocation_tenant` keeps its meaning (a catalog per
tenant) and its validator. In that configuration the column is redundant but harmless: storage
already confines each row to its tenant, and `assign_role` writes the bound tenant into
`scope_tenant_id`.

The two docstrings that overstate (§3) are rewritten. `global` reads as *"one catalog for every
tenant; a principal-role binding holds in every tenant unless it names one"*. The warning for
`global` over tenant-aware ports says what actually happens: storage partitions those ports per
bound tenant and refuses unbound calls. It suggests `require_invocation_tenant` when a
per-tenant catalog is meant, and tenant-unaware ports with tenant-held bindings otherwise.

### 5.7 What bi's layer becomes

| bi today | With this RFC |
| --- | --- |
| tenant-aware `members` row `(principal_id, role)` | `PrincipalRoleBinding(scope_tenant_id=<workspace>)` |
| `Role` enum, analyst and viewer | two catalog roles and one permission, `workspace.manage`, held by analyst; seeded once for the deployment |
| `analysts_only` patched onto every op outside `viewer_may` | `AuthzBeforeAuthorize(spec, action="workspace.manage")` on the same selector; denied by default, as before |
| `role_of` for `/auth/me`, the chat palette, bootstrap | `role_assignment(spec).list_roles(principal)` in the bound workspace |
| `ensure_member` | `attach_principal` + `assign_role` (the tenant comes from the bound workspace) |
| `SetMember` changing a role | `revoke_role` + `assign_role` in the workspace |
| `RemoveMember` | `detach_principal`, which revokes the workspace's roles first (§5.4) |
| `ListMembers` | a query on `principal_role_binding_spec` filtered by `scope_tenant_id` |
| runs with no principal pass | a service principal with a global binding: `AuthzBeforeAuthorize` refuses an anonymous call (`plans.py:246`) |
| last analyst kept, creator becomes analyst | unchanged; app rules |

bi keeps its selector and its rules. It deletes the member table, `role_of`, the hook body, and
the role column it maintains by hand. It still binds the ten authz documents and turns
eligibility back on, which D49 also counted against the plane; §8 names that separately.

### 5.8 Compatibility and migration

- **Schema (every deployment).** Add the nullable column, and the unique index if wanted (§5.1),
  before booting the release. Drop a `(principal_id, role_id)` pair constraint where one exists.
  Postgres refuses to boot when the model has a field the table lacks. Mongo and Firestore
  documents without the field read as `None` through the model default.
- **Existing rows:** `NULL`, global. Nothing they grant changes.
- **Calls with no tenant bound:** unchanged. These are all of erp's and eis's calls, every
  migration and seed, and every single-tenant app. An unscoped revoke with nothing bound still
  removes the role everywhere.
- **Breaking, assign:** an unscoped `assign_role` inside a tenant-bound invocation now binds in
  that tenant rather than globally. A deployment that relied on the old behavior keeps working,
  with narrower grants.
- **Breaking, revoke:** an unscoped `revoke_role` inside a tenant-bound invocation, for a role
  held globally, is now refused with `authz.role_held_globally`, where it used to remove the
  role. The refusal is loud by design; the fix is to revoke with no tenant bound.

  No known consumer makes either call (§2). The changelog states both under Breaking, as house
  policy allows during the beta.
- **Breaking, detach:** `detach_principal` now also removes the principal's role bindings in
  that tenant, and a runtime with tenant management and a role-assignment port must name
  `authz_route` (§5.4).
- **Raw SQL** keeps working while every row is global. This covers hh's two writers
  (`wire/adapters/accounts.py:59-62`, `wire/adapters/operator.py:72`), its seed
  (`db/migrations/postgres/0068__hosting.sql:36`) and its reader (`wire/adapters/principals.py:50-70`).
  Once tenant rows exist, a reader must apply the predicate in §5.2. A writer must also set
  `scope_tenant_id` deliberately, because leaving it out writes a global row. The recipe says so.

### 5.9 Alternatives considered

- **A per-tenant catalog through `require_invocation_tenant` (works today).** It expresses item 2
  at the cost of everything else in §3. The catalog is seeded in every tenant, and a role change
  becomes an N-tenant migration. There are no global bindings. The policy principal goes per
  tenant, which conflicts with the eligibility read at sign-in. It stays the answer for
  tenant-defined roles, which nobody needs yet.
- **The tenant in the role key** (`analyst@<tenant>`). The resolver stays unchanged, but the
  catalog grows a role and its role-permission rows per tenant: the same seeding as above, with
  tenancy encoded in strings no validator understands.
- **A second, tenant-aware binding table** (`authz_tenant_principal_role_bindings`), unioned by the
  resolver. Storage would enforce isolation instead of a predicate, and existing tables would
  stay untouched. But it is an eleventh grant-resolution document every deployment must bind
  ("bind all nine or none", `inventory.py:80-92`, would become ten), with two write paths and two
  read paths. Global rows and tenant rows could no longer be listed together. The predicate it
  avoids sits in one function, has a conformance test, and has a DST invariant (§6). It lost on
  weight: it is the wiring D49 already complains about.
- **A `PermissionProvider` over the app's own member table** ([RFC 0057](0057-derived-permissions-and-config-grants.md)).
  It works today with no framework change: a provider reads the tenant-aware member row and
  derives keys. But a role is an administered grant, not derived state. A provider's
  permissions never appear as roles (`list_roles` and `EffectiveGrants.roles` stay empty), are
  asked again on every decision with no cache, and are not invalidated by `assign_role`. Each app
  would also rewrite its role-to-permission mapping. RFC 0057 itself frames providers as being
  for "state, not a binding".
- **A new `tenancy_mode` value** for "shared catalog, tenant bindings". It is not needed: the
  column means the same thing in both existing configurations. It would be a third value whose
  only effect is a validator variant.
- **Revoke in T removes T's row only, even with a global row present.** It is simpler and
  symmetrical, but it fails open for every existing deployment (§5.3). It lost to the refusal.
- **Leave detach alone and document "revoke first".** This keeps tenancy free of authz. But every
  app that forgets the recipe resurrects old roles when a principal is attached again, bi
  included. It lost to the cascade, which follows a pattern the house already uses (§3).

## 6. Tests

- **Resolver (unit).** A principal with a global role, a role in T and a role in U, checked in
  each context:
  - in T, the global role and T's role;
  - in U, the global role and U's role;
  - with no tenant bound, only the global role;
  - **with no tenant bound and an explicit `AuthzScope(tenant_id=U)`, only the global role**, for
    decisions, grant queries and `list_roles` alike.

  A scope naming U inside T is refused. `list_roles` returns the same sets.
- **Assignment (unit).** Each cell of the §5.3 table, plus:
  - **global then tenant:** assign globally, then in T, writes two rows. Revoking in T is refused
    with `authz.role_held_globally` and both rows remain. Revoking with no tenant removes both;
  - **tenant then global:** assign in T, then globally, writes two rows (the global write is not
    skipped because T's row exists). Revoking in T is refused. Revoking with no tenant removes
    both;
  - **tenant only:** revoking in T removes T's row and never a global row;
  - assigning twice in T writes one row;
  - an unscoped assign in `require_invocation_tenant` mode inside a tenant-bound request succeeds
    (today it raises `tenant_required`).
- **Detach (unit and Postgres).**
  - `detach_principal(p, T)` removes p's rows in T and keeps the global row and U's rows.
  - It revokes before it kills the membership row.
  - Attaching p again grants nothing in T.
  - A runtime with tenant management, a role-assignment port and no `authz_route` is refused at
    startup.
- **Cache (unit, on the existing grants-cache DST suite).** Entries for T and U never cross. An
  assign in T forgets the principal in both. With the RFC 0068 broadcast on, a revoke in T on one
  process is denied in T on the other.
- **Conformance across backends.** The §5.2 filter and the exact-tenant lookup, with their `$or`
  and null checks, run on the mock, Postgres, Mongo and Firestore through the existing authz
  integration suites (`tests/integration/test_forze_authz/`). Postgres validates the schema with
  the new column and the `NULLS NOT DISTINCT` index. The mock's `RoleAssignmentPort` stays a stub,
  so these run the real adapter over mock document storage.
- **DST invariant.** Beside `no_permission_after_deactivation` (`forze_dst/oracle/invariants.py:787`),
  add a check that no permission from a role bound in T is ever granted in an invocation bound to
  another tenant or to none.
- **Not tested:** tenant-held direct permissions, groups or delegation grants. None of them
  exist (P2).

## 7. Docs

- `identity-tenancy-enc/identity.md` gains a short section, "Roles in one tenant". It covers:
  - the column and the rule in §5.2, including that a scope never selects bindings with nothing
    bound;
  - the assign and revoke table in §5.3, and the `authz.role_held_globally` refusal;
  - the fact that a global binding is written only with no tenant bound;
  - the detach cascade and its wiring (§5.4).

  It states the isolation rule plainly: *a role bound in a tenant never grants outside it*.
- `reference/contracts/identity.md`: the `tenancy_mode` row is reworded per §5.6, and
  `RoleAssignmentPort` lists `revoke_tenant_roles`.
- `reference/errors.md`: `authz.role_held_globally`.
- `recipes/provision-identity-postgres.md`: DDL for the principal-role binding table with the
  column and the index, how to replace a pair constraint, and a note for raw SQL readers and
  writers.
- `recipes/tenant-selector-and-admin.md`: adding a member (attach, then assign), and removing one
  (detach, which revokes).
- Changelog:
  - `Added`: tenant-held role bindings and `revoke_tenant_roles`;
  - `Breaking`: the unscoped assign and revoke inside a tenant-bound invocation, the detach
    cascade with its `authz_route` requirement, and the column every deployment adds.

## 8. Out of scope

- **A code-declared catalog.** D49 counts the wiring as the plane's second cost: ten documents for
  a decision, plus seeded catalog rows. Declaring roles and their permissions in code, as
  `ConfigGrants` does for principals, would remove most of those documents. It is a separate
  design with its own trade-off: the catalog stops being editable at runtime. Build it when a
  second consumer turns down the plane for its wiring.
- **Tenant-held direct permissions and group membership.** Same column, same predicate, on
  `PrincipalPermissionBinding` and `GroupPrincipalBinding` (P2). Build it on the first consumer
  that grants a single permission, or a team, inside one tenant.
- **Tenant-held delegation grants.** `may_act` in one tenant only. Nobody has asked, and RFC 0009
  §7.4 keeps delegation local anyway.
- **The conflict between `require_invocation_tenant` and eligibility (§3).** It is real, and
  untouched here because this design never makes the policy principal tenant-aware. It belongs
  with RFC 0081, or with whoever first needs per-tenant catalogs.
- **The mock decision port's use of the request scope.** `MockAuthzDecisionPort` matches tenant
  grants on `request.scope.tenant_id` (`forze_mock/adapters/identity/authz.py:318-324`). That
  scope is the invocation's whenever the hook builds it. Aligning the mock with §5.2's
  bound-tenant rule is a small follow-up, not a dependency of this design.
- **Claims and remote engines.** RFC 0009 W2 should apply the same invariant to claim-carried
  roles: a role a token grants for org X must not hold in tenant Y. Its `ZitadelProjectRoles(org_id=…)`
  filter and `RoleBridgePort.permissions_for(…, scope=)` are where that happens. W3 engines
  receive `scope.tenant_id` and own the answer. Neither reads the binding table.

## 9. Risks

- **Isolation rests on a predicate, not on storage.** A bug in §5.2 would leak one tenant's role
  into another. Mitigation: the predicate is in one function, the tenant comes only from the
  bound invocation, and the conformance tests and the DST invariant in §6 assert the rule on
  every backend. Accepted against the second table's weight (§5.9).
- **Callers meet a new refusal.** Code that used to revoke a global role from inside a
  tenant-bound request now gets `authz.role_held_globally`. It is loud and names the remedy, which
  is the point: the alternative left the grant in force without a word. No known consumer makes
  that call.
- **A no-tenant revoke reaches further than some callers expect.** A platform admin who revokes
  `analyst` globally also removes it from every tenant the principal held it in. That matches
  today's meaning of an unscoped revoke, and the docs say so.
- **Pre-staged roles can outlive an abandoned invite.** A role assigned in T before the principal
  joins stays until someone revokes it, and it applies if the principal later joins T by another
  path. Mitigation: `revoke_tenant_roles` clears it, and the invite recipe revokes on decline.
  Accepted, because pre-staging is how invites grant a role at all.
- **Detach now writes to the authz plane.** A failure there fails the detach. That is the intended
  trade: a membership removal that leaves roles behind is the defect being closed.
- **Raw SQL readers that ignore the column** would union tenant roles into every tenant once such
  rows exist, and raw writers would write global rows. Mitigation: the recipe note. hh writes no
  tenant rows.
- **Read as tenant-defined roles.** "Tenant-scoped roles" may suggest each tenant can create its
  own. The docs say "tenant-held role bindings", title the section "Roles in one tenant", and say
  the catalog is shared.

## 10. Unresolved questions

None blocks the design. Implementation settles each of these and logs the choice:

- Whether the predicate runs in the binding query or over the principal's rows in Python (row 15).
- How `RoleAssignmentAdapter` reaches the invocation tenant (row 16).
- How the runtime detects tenant management paired with role assignment and no `authz_route`
  (row 18).
- The final field name, if `scope_tenant_id` meets a collision this RFC did not find (row 3).

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | The gap closed is the **binding**: a principal-role binding may name the one tenant it holds in (tenant-held role bindings). Role definitions, permissions and role-permission bindings stay one catalog for every tenant. Tenant-defined roles remain the per-tenant catalog of `require_invocation_tenant`. Scoping the catalog too would change the model every authz adapter reads. |
| 2 | `LOCKED` | The representation is a nullable column on the existing `PrincipalRoleBinding` row. `NULL` holds in every tenant and in calls with no tenant bound; a value holds only in that tenant. Existing rows are `NULL`, so existing grants keep their meaning. It is not a second, tenant-aware binding table (§5.9). This puts a column in every deployment's DDL, and reversing it means a second migration. |
| 3 | `ASSUMED` | The field is `scope_tenant_id`, not `tenant_id`. Tenant-aware storage reserves `tenant_id` as its `NOT NULL` partition column (`tenancy/constants.py:7`, `forze_postgres/kernel/gateways/base.py:283-291`), so that name would collide on the tenant-aware routes of `require_invocation_tenant`. |
| 4 | `LOCKED` | Decisions, grant queries, scoping and `list_roles` select bindings by the **bound** invocation tenant only (`grants.py:162-170`), never by `scope.tenant_id`. A binding holds when its tenant is `NULL` or equals the bound tenant. With nothing bound, only global bindings hold, whatever scope the caller passes. A role bound in T never grants outside T. This is the isolation invariant; the tests and the DST check in §6 assert it. |
| 5 | `ASSUMED` | Writes (`assign_role`, `revoke_role`, `revoke_tenant_roles`) take their tenant from `effective_tenant(bound=<invocation tenant>, requested=<scope.tenant_id>)` (`mixins.py:12-30`). The bound tenant wins. An explicit tenant counts only when nothing is bound. A contradiction is refused as `authz.scope_tenant_mismatch`. Writes are grant management, which the app exposes only behind a global permission. |
| 6 | `LOCKED` | An unscoped `assign_role` inside a tenant-bound invocation binds in that tenant. A forgotten scope narrows a grant and never widens it across tenants. A global binding is written only with no tenant bound. **Breaking**: today such a call binds globally. Reversing this default later would turn forgotten scopes into cross-tenant grants. |
| 7 | `LOCKED` | A revoke never silently leaves a grant in force. In a tenant, `revoke_role` refuses with `authz.role_held_globally` when the role is held globally; otherwise it removes that tenant's row. With no tenant, it removes the role's global row and every tenant row, as an unscoped revoke does today. **Breaking** for a tenant-bound revoke of a global role, which today succeeds. Removing only the tenant's row would fail open for every existing deployment. |
| 8 | `ASSUMED` | The binding lookup in assign and revoke takes the tenant and matches it exactly, `NULL` against `NULL`. Today's helper returns the first row for the role whatever its tenant (`role_assignment.py:166-180`), which would skip T's row on assign and could kill the global row on revoke. |
| 9 | `ASSUMED` | No new `tenancy_mode`. Tenant-held bindings live in `global` mode over ports without tenant awareness, the recommended shape for a fixed catalog. `require_invocation_tenant` keeps its per-tenant catalog and its validator. The `global` docstring and warning are rewritten, because they overstate today (§3). |
| 10 | `ASSUMED` | `detach_principal(p, T)` revokes p's role bindings in T through `RoleAssignmentPort.revoke_tenant_roles`, then kills the membership row, in the caller's transaction. It is on by default, wired through `TenancyDepsModule(authz_route=...)` as `AuthnDepsModule(authz_route=...)` wires the deactivation cascade. A runtime with tenant management and a role-assignment port but no `authz_route` is refused at startup. Global bindings and other tenants' bindings stay. |
| 11 | `ASSUMED` | A tenant role does not imply membership, and `assign_role` does not check it. Pre-staging a role before the principal joins is allowed, so an invite can carry a role. The authz plane gains no read of tenancy rows. |
| 12 | `ASSUMED` | The policy principal stays without tenant awareness. Deactivation applies to the whole deployment, and authn's eligibility read at sign-in keeps working. |
| 13 | `ASSUMED` | The grants cache and the RFC 0068 broadcast do not change. The key already carries the invocation tenant and the scope (`grants_cache.py:36`), `forget` already clears a principal in every tenant (`:167`), and markers name principals only. A write in T discards the principal's entries in other tenants too; that costs misses only. |
| 14 | `ASSUMED` | P1 scopes principal-role bindings only. Direct principal-permission bindings, group membership and delegation grants stay global. Extending them takes the same column and predicate (P2, demand-gated). |
| 15 | `OPEN` | Where the predicate runs: in the binding query (an `$or` of a null check and an equality; every backend's renderer has both) or as a Python filter over the principal's rows. Settle it by whether every document adapter passes the §6 conformance test with the query form, and log the choice. |
| 16 | `OPEN` | How `RoleAssignmentAdapter` reads the invocation tenant. Today it reads none (`role_assignment.py:79-83`). Options are through the resolver's `ctx` or a `ctx` field on the adapter. Pick by what the deps factory already passes, and log it. |
| 17 | `ASSUMED` | The recipe's uniqueness is `UNIQUE (principal_id, role_id, scope_tenant_id) NULLS NOT DISTINCT`, which the schema validator already understands (`validate_schema.py:357`) and which needs Postgres 15 or later. A `(principal_id, role_id)` pair constraint is dropped in the same migration. The adapter's read-then-create idempotency stays as it is. |
| 18 | `OPEN` | How the runtime detects tenant management paired with a role-assignment port and no `authz_route` (row 10): at module build, or when the management port is first resolved. Pick the earliest point that sees both modules, and log it. |
| 19 | `ASSUMED` | Exclusive roles, one role per principal per tenant as bi and eis keep them, stay app logic. The binding model allows several. |

## 12. Phasing

- **P1** — one PR covering §5.1–§5.8:
  - the column on the model and commands;
  - the predicate in `_direct_role_ids`, keyed on the bound tenant;
  - the exact-tenant lookup;
  - the tenant and refusal rules on assign, revoke and list;
  - `revoke_tenant_roles` and the detach cascade with its wiring;
  - the docstring and warning rewrite;
  - the tests in §6, the docs in §7, and the changelog.
- **P2** — demand-gated: the same column on `PrincipalPermissionBinding` and
  `GroupPrincipalBinding` (§8), when a consumer grants a single permission or a team inside one
  tenant.
- **Follow-up, outside this RFC** — a code-declared catalog (§8), if wiring weight turns away a
  second consumer.
