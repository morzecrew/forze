---
title: Identity & access
icon: lucide/key-round
summary: Authentication as verify-then-resolve, and authorization as decision plus scope
---

Identity answers two questions, in order: **is this credential real**
(authentication), and **may this principal do what they're asking**
(authorization). Forze keeps both behind contracts in a separate plane,
`forze_identity`, so swapping an identity provider never reaches your handlers.

## Authentication: verify, then resolve

Proving a credential is valid and deciding *who* it represents are two separate
jobs. They meet at a single value object — and that's the whole design.

![Verification emits a VerifiedAssertion; resolution turns it into an AuthnIdentity](../_diagrams/light/authn-verify-resolve.svg#only-light){ data-src="../_diagrams/light/authn-verify-resolve.svg#only-light" }
![Verification emits a VerifiedAssertion; resolution turns it into an AuthnIdentity](../_diagrams/dark/authn-verify-resolve.svg#only-dark){ data-src="../_diagrams/dark/authn-verify-resolve.svg#only-dark" }

- **Verify** — a verifier proves the credential against its issuer (a JWT
  signature, an API-key hash, OIDC JWKS) and emits a **`VerifiedAssertion`**:
  vendor-flavoured proof carrying the issuer, subject, and claims.
- **Resolve** — a `PrincipalResolverPort` maps that assertion to a canonical
  **`AuthnIdentity`** with a `UUID` `principal_id`.

The verifier never invents a principal; the resolver never re-checks a
signature. The `VerifiedAssertion` is the entire seam between them — which is
why several verifiers (first-party JWT, OIDC, API keys) can sit behind one
orchestrator and feed the same resolver.

This is what keeps the rest of Forze provider-agnostic: your domain, tenancy,
and authorization code only ever see a `UUID` principal. Switching from Google
OIDC to internal SSO is a new verifier/resolver pair — not a change to a single
handler.

## Resolving to a principal

Three first-party resolvers cover the common shapes — the choice is about
whether you need stored accounts:

| Resolver | Maps subject → principal by… | Storage |
|----------|------------------------------|---------|
| **`JwtNativeUuidResolver`** | trusting a subject that's already a UUID (first-party Forze JWTs) | none |
| **`DeterministicUuidResolver`** | deriving a stable UUID from `(issuer, subject)` | none |
| **`MappingTableResolver`** | looking up `(issuer, subject)` in a table, with optional just-in-time provisioning | a mapping document |

## Plugging in a provider

A route's `AuthnSpec` selects verifiers and a resolver by **profile name**. An
integration registers a verifier under a profile; the spec references it without
owning any vendor knowledge:

```python
from forze.application.contracts.authn import AuthnSpec

api_authn = AuthnSpec(
    name="api",
    enabled_methods=frozenset({"token", "api_key"}),
    token_profile="oidc",
    resolver_profile="mapping",
)
```

Providers ship as integrations — see [OIDC](../integrations/oidc.md) and
[authentication](../integrations/authn.md). The
[external IdP recipe](../recipes/external-idp-oidc.md) wires a third-party identity
provider end to end.

## Authorization: may they?

Once a request carries an `AuthnIdentity`, authorization decides what it may do.
Two questions, two ports:

| Question | Port | Resolved via |
|----------|------|--------------|
| May this principal run this operation? | `AuthzDecisionPort` | `ctx.authz.decision(spec)` |
| Which rows may they see? | `AuthzScopePort` | `ctx.authz.scope(spec)` |

(A third slice — grant management — provisions the roles, permissions, and
bindings those decisions read.)

Two permission keys are **reserved by default**: a principal holding `admin`
or `<resource_type>.admin` (e.g. `invoice.admin`) bypasses the `owner_id`
ownership check on resources. Don't reuse those names for unrelated app
permissions — or change the convention via
`AuthzKernelConfig(owner_override_permissions=...)`: pass an empty set to
always enforce ownership, or your own keys (the literal `{resource_type}`
placeholder is substituted at evaluation time).

Enforcement belongs on the **operation plan**, not scattered across routes — so
it's authoritative for every caller, HTTP or not. Using the [stage
hooks](../core-concepts/application-layer.md) from the application layer: a `BeforeStep`
authorizes the operation, and a `wrap` step injects scope filters into
list/search queries. Both read the bound `AuthnIdentity` and `TenantIdentity` to
build the decision.

### Permissions derived from state

"Active member of X" is state, not a binding, and syncing bindings from it drifts. A
`PermissionProvider` derives permissions per decision from documents or reviewed configuration,
and the policy unions them with catalog grants:

| Source | Grants | Wins over |
|--------|--------|-----------|
| Catalog bindings (principal, role, group) | yes | — |
| A provider's `granted` | yes | — |
| A provider's `denied` | no | **every** grant, bindings included |

```python
from uuid import UUID

from forze.application.contracts.authz import DerivedPermissions
from forze_identity.authz import AuthzKernelConfig, permission_providers_lifecycle_step


class ActiveMembers:
    name = "active_members"
    keys = frozenset({"ledger.read", "ledger.write"})

    async def derive(self, principal_id: UUID, ctx) -> DerivedPermissions:
        member = ...  # read the member row through ctx
        if member is None or not member.active:
            return DerivedPermissions(denied=self.keys)  # deactivation closes every route
        return DerivedPermissions(granted=self.keys)


providers = (ActiveMembers(),)
kernel = AuthzKernelConfig(permission_providers=providers)
startup = permission_providers_lifecycle_step(providers)  # optional: fail at boot instead
```

- **Gate on permissions, never on roles** — that is what lets a derived denial close a route.
- A provider **reads and never writes**. It runs for the principal being decided; on a delegated
  call `AuthzBeforeAuthorize` and `AuthzDocumentScopeWrap` decide each actor in turn, so the
  provider runs for each of them: past those two guards, a delegation never exceeds what every
  principal in it holds.
- `keys` declares everything a provider may grant or deny, as a `frozenset`. A declared key with
  no catalog row refuses the first decision per tenant that runs the providers
  (`authz_provider_unknown_keys`), so a typo cannot go unnoticed; register the startup step to
  refuse at boot instead.
- A provider that **raises denies every key it declares** — an outage must not become a bypass.
  So does one whose result is not a `DerivedPermissions`, or names a key outside `keys` (a
  misspelt denial would otherwise deny the misspelling and leave the real key granted), and one
  that misses `permission_provider_timeout` (2 s by default): every decision runs every provider,
  so a hanging one would otherwise stall them all.
- `MockDepsModule(permission_providers=...)` runs the same providers in the mock's decision, so a
  mock-backed test sees a derived denial outrank a seeded grant, and refuses a declaration the
  kernel config would. The mock has no catalog to check keys against, and its scope and grant
  query ports do not run providers.
- Derived grants sit in `EffectiveGrants.derived`, attributed to their provider and apart from the
  catalog's `permissions`, so an administered grant can be told from a derived one.
- The DST invariant `no_permission_after_deactivation` checks that a derived permission closes
  when its state does: no guarded operation succeeds once a deactivation has returned.

### Permissions granted by configuration

Break-glass operators and one-off recipients are better as reviewed deployment config than as
rows an admin can grant at runtime. `ConfigGrantsProvider` declares the keys in code;
`ConfigGrants`, a settings model you mount on your own settings root, lists who holds each:

```python
from pydantic import BaseModel

from forze_identity.authz import AuthzKernelConfig, ConfigGrants, ConfigGrantsProvider


class Settings(BaseModel):  # your settings root
    config_grants: ConfigGrants = ConfigGrants()


# The deployment's value nests under the field you mounted it on:
settings = Settings.model_validate(
    {"config_grants": {"grants": {"ops.break_glass": ["<principal id>"]}}}
)
provider = ConfigGrantsProvider(
    keys=frozenset({"ops.break_glass"}),  # reviewed code, not configuration
    grants=settings.config_grants,
)
kernel = AuthzKernelConfig(permission_providers=(provider,))
```

- **The keys are code.** A configuration naming a key the provider does not declare is refused
  when the provider is built; a declared key it omits, or lists nobody under, is granted to
  nobody. Principals are exact ids: no wildcard, pattern or email match.
- **One source per key.** A key the provider owns may not also be granted through the catalog.
  A role, principal or group binding of it grants nothing whenever it was written, because the
  provider denies each of its keys to every principal it does not list. The startup step, where
  registered, refuses such a binding in the tenant it reads under
  (`authz_config_grant_overlap`); without it, a binding present when a tenant's check first runs
  in a process is logged (`authz.config_grant_overlap`), and one written later is not seen until
  the next start. Nor may another provider declare the
  key: its grants of it would never count, so the declaration is refused.
- The provider reads the configuration once, when it is built.
- Its keys are checked against the catalog like any provider's, so each needs a permission row.
  A provider with no keys at all is refused, as any provider declaring nothing is.

### Gating a route that runs no operation

Where a hand-written route runs no operation, so no hook guards it, `require_permission` makes the
same decision as a FastAPI dependency:

```python
from fastapi import Depends

from forze_fastapi.security import require_permission

ledger_write = require_permission("ledger.write", spec=AUTHZ, ctx_dep=ctx_dep)


@app.post("/ledger/import", dependencies=[Depends(ledger_write)])
async def import_ledger() -> None: ...
```

It runs `AuthzBeforeAuthorize`'s own check (`authorize_action`): derived and config grants count,
each actor of a delegated call is decided too, `may_act` is required when the spec enforces
delegation grants, and the denial is the hook's. It decides an action, not an object: a route
guarding one row by its owner needs an operation and the hook's `resource_factory`. Pass
`resource_type=` when the route is about a resource type, so a
[non-disclosing posture](../reference/errors.md#non-disclosing-denials) renders a refused
permission as that type's not-found; a missing `may_act` grant, like any refusal without a type,
stays a 403.

### Remembering grants between decisions

Every decision reads the principal's role, group and permission bindings. A busy service can
remember them, per process, for a fixed time:

```python
from datetime import timedelta

from forze_identity.authz import AuthzKernelConfig, GrantsCache

grants_cache = GrantsCache(ttl=timedelta(seconds=30))
kernel = AuthzKernelConfig(grants_cache=grants_cache)
```

- Nothing is cached unless you pass one.
- It remembers the roles and permissions the catalog bindings grant, per principal, tenant and
  scope, up to `max_entries` (10,000 by default), dropping the least recently used first.
- Read on every decision, never cached: whether the principal is active, what providers derive,
  delegation (`may_act`) grants and tenant membership. `list_roles` reads the bindings too.
- Decisions inside a transaction read the bindings and leave the cache alone: what they read may
  still roll back.
- **The TTL is how long a removed grant keeps working.** `assign_role` and `revoke_role` forget the
  principal in this process once their write commits; other processes see the change when their
  entry expires. A change written through plain document commands is not heard, so after it
  commits call:

    | Change | Call |
    |--------|------|
    | One principal's own role, permission or group-membership binding | `grants_cache.forget(principal_id)` |
    | A role's permissions or parent, a group's roles, permissions or active flag, a deleted role or permission | `grants_cache.clear()` |

- One cache serves one catalog: give each kernel over a different catalog its own.

## Authn events and login lockout

Authentication flows can narrate themselves. Wire an optional **authn event
sink** and every flow emits a structured `AuthnEvent` — login success/failure,
lockout, token refresh, **refresh-reuse detection** (the token-theft signal),
logout, password change, reset request/completion, principal deactivation:

```python
from datetime import timedelta

from forze.application.integrations.authn import LockoutConfig
from forze_identity.authn import AuthnDepsModule, ConfigurableLoggingAuthnEventSink

authn_module = AuthnDepsModule(
    kernel=kernel,
    authn={"main": frozenset({"password", "token"})},
    events=ConfigurableLoggingAuthnEventSink(),  # one log line per event
    lockout=LockoutConfig(threshold=5, window=timedelta(minutes=15)),
)
```

Emission is **best-effort by contract**: a sink failure (or no sink at all) never
fails the auth flow, and a failed-login event is emitted *after* the verifier's uniform
error, so the Argon2 timing parity between unknown-login and wrong-password failures is
untouched. Events carry `login_digest` (a SHA-256 of the login), never the raw login —
pseudonymization to keep logins out of logs and counter key spaces, not secrecy.

**Lockout** is a fixed window over `CounterPort`: after `threshold` failed attempts
within the window, further attempts raise `throttled` (`code="login_locked"`, HTTP 429)
*before* password verification, and unlock when the window rolls over. It counts
**login strings, not accounts**, so a nonexistent login locks exactly like a real one —
preserving the no-enumeration posture.

## The identity plane

All of this lives in `forze_identity`, separate from the core: `authn`, `authz`,
`tenancy`, `oidc`, and `oauth` subpackages, wired per route via the same deps
modules as any other integration.

For getting started, `forze_identity.builtin` ships presets — file/env API keys
(`local`) and Google / VK / Telegram Login over OIDC (`idp`). They're shipped-in
conveniences, not production defaults: adopt one only once you accept its trust
model (e.g. VK publishes no JWKS, so its preset verifies `id_token`s by
server-side introspection against VK rather than a local signature check).

### Cataloguing what it binds

The plane brings nineteen document specs your application never writes — sessions,
credentials, grants, tenant bindings. If you declare a [spec
inventory](../reference/spec-registry.md), merge them in rather than listing them by hand,
so an identity release cannot leave your catalogue behind:

```python
specs = SpecRegistry().register(order_spec).merge(forze_identity.spec_contributions())
```

Wiring only part of the plane? Name the parts you wire. `spec_contributions()` catalogues
all three planes, and the inventory check refuses a spec catalogued but never bound, so an
authn-only application has to narrow it:

```python
forze_identity.spec_contributions(planes=["authn"])
```

Narrow only for a plane you genuinely do not wire — the check is worth keeping strict, and
it still fires from the bound side if you narrow too far. One caveat that is easy to miss,
and it is the *default*: authn's principal-eligibility gate reads the authz
policy-principal document unless you opt out, so `planes=["authn"]` belongs with
`eligibility="allow_all"` on the authn module — the declared opt-out for a deployment with
no authz plane. Leave the gate on its default and authn binds an authz document, which the
inventory check will tell you about.

Identity settles *who* the caller is; scoping *which data* they may reach is
[Multi-tenancy](multi-tenancy.md).
