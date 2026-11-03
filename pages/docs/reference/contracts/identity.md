---
title: Identity (authn & authz)
icon: lucide/user-check
summary: The authentication and authorization contracts — every port, its dep key, its methods, and how to replace one
---

Identity splits into **authentication** (who is calling — `ctx.authn.*`) and
**authorization** (what they may do — `ctx.authz.*`), each a family of small ports. The
concepts are in [Identity & access](../../identity-tenancy-enc/identity.md) and the shipped
stack in the [authn](../../integrations/authn.md) / [OIDC](../../integrations/oidc.md)
integrations; this page is the port catalog — what each port promises, the dep key it binds
under, and how to replace one port without the rest.

Every port is routed by its spec's `name`: `ctx.authn.token_lifecycle(spec)` resolves
`TokenLifecycleDepKey` under the route `spec.name`. A provider is a factory
`(ctx, spec) -> port`. A **write** accessor hands back a port that refuses any use inside a
read-only (`QUERY`) operation.

## Authentication

`AuthnSpec` declares which credential families a route accepts:

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | route name — every authn port resolves under it |
| `enabled_methods` | `frozenset[AuthnMethod]` | `{"token"}` | credential families: `password` / `token` / `api_key`; an empty set is a configuration error, and `AuthnPort` refuses a family the route does not enable |

### Ports and dep keys

| Accessor `ctx.authn.<x>(spec)` | Port | Dep key | Write |
|--------------------------------|------|---------|-------|
| `authn` | `AuthnPort` | `AuthnDepKey` | |
| `eligibility` | `PrincipalEligibilityPort` | `PrincipalEligibilityDepKey` | |
| `token_lifecycle` | `TokenLifecyclePort` | `TokenLifecycleDepKey` | yes |
| `password_lifecycle` | `PasswordLifecyclePort` | `PasswordLifecycleDepKey` | yes |
| `password_reset` | `PasswordResetPort` | `PasswordResetDepKey` | yes |
| `api_key_lifecycle` | `ApiKeyLifecyclePort` | `ApiKeyLifecycleDepKey` | yes |
| `password_account_provisioning` | `PasswordAccountProvisioningPort` | `PasswordAccountProvisioningDepKey` | yes |
| `principal_deactivation` | `PrincipalDeactivationPort` | `PrincipalDeactivationDepKey` | yes |
| `event_sink` | `AuthnEventSink` | `AuthnEventSinkDepKey` | |

Six more keys have no accessor. The orchestrator behind `AuthnDepKey` resolves the first
four for its own route, and session issue and revocation resolve the last two:

| Port | Dep key | Resolved by |
|------|---------|-------------|
| `PasswordVerifierPort` · `TokenVerifierPort` · `ApiKeyVerifierPort` | `PasswordVerifierDepKey` · `TokenVerifierDepKey` · `ApiKeyVerifierDepKey` | the orchestrator, one per enabled family, under `spec.name` |
| `PrincipalResolverPort` | `PrincipalResolverDepKey` | the orchestrator, under `spec.name` |
| `RevocationStatesPort` | `RevocationStatesDepKey` | verification, session issue and revocation, under the route `AuthnKernelConfig.revocation_states` names (a `RevocationStatesSpec`) |
| `PrincipalEpochPort` | `PrincipalEpochDepKey` | session issue and revocation; one store per deployment, unrouted |

### Verifying a credential

The verify-then-resolve seam. A verifier proves a credential and emits a
`VerifiedAssertion`; the resolver maps it to an `AuthnIdentity`; the orchestrator runs both
and then checks eligibility.

| Port | Method | Notes |
|------|--------|-------|
| `AuthnPort` | `authenticate_with_password(credentials)` · `authenticate_with_token(credentials)` · `authenticate_with_api_key(credentials)` | each returns an `AuthnResult`; a family the route does not enable is refused as `authentication` |
| `PasswordVerifierPort` | `verify_password(credentials)` | `PasswordCredentials` → `VerifiedAssertion` |
| `TokenVerifierPort` | `verify_token(credentials)` | `AccessTokenCredentials` → `VerifiedAssertion`; the assertion's `issuer` is what a resolver discriminates on; refresh tokens never reach a verifier |
| `ApiKeyVerifierPort` | `verify_api_key(credentials)` | `ApiKeyCredentials` → `VerifiedAssertion` |
| `PrincipalResolverPort` | `resolve(assertion)` | `VerifiedAssertion` → `AuthnIdentity` — trust the subject as a UUID, derive one from `(issuer, subject)`, or look it up |

### Credential lifecycle

| Port | Method | Notes |
|------|--------|-------|
| `TokenLifecyclePort` | `issue_tokens(identity, *, tenant_id=None)` | returns `IssuedTokens`; refuses a delegated identity (`delegate_denied`). Issue and refresh check eligibility with a locking read, so run them in a transaction |
| | `refresh_tokens(refresh_token)` | rotates the pair; a reused token must revoke its family, then raise `RefreshTokenReuseError`, whose raise rolls that revocation back inside your transaction: catch it after the transaction ends and call `revoke_chain_of_tokens(error.principal_id, error.family_id)` in a fresh one, as `AuthnRefreshTokens` does; within a configured reuse grace a just-rotated token is refused as `refresh_superseded` and nothing is revoked |
| | `revoke_tokens(identity)` | logout |
| | `revoke_tokens_by_refresh_token(refresh_token)` | logout holding only a refresh token; an unknown, revoked, rotated or expired one revokes nothing and raises nothing |
| | `revoke_chain_of_tokens(principal_id, family_id)` | revoke every live session of one rotation family |
| `PasswordLifecyclePort` | `change_password(identity, current_password, new_password)` | re-authenticates with the current password first |
| `PasswordResetPort` | `request_reset(login)` | returns an `IssuedPasswordReset` carrying the raw token, or `None` for an unknown or ineligible login; a new reset invalidates the previous one. Keeping the *caller's* response uniform is the handler's job |
| | `reset_password(token, new_password)` | an invalid, expired or used token is one uniform `authentication` error; success marks the token used and revokes the principal's sessions (the shipped adapter keeps them only with `revoke_sessions_on_reset=False`) |
| `ApiKeyLifecyclePort` | `issue_api_key(identity, *, actor_principal_id=None, label=None)` · `refresh_api_key(credentials)` | return an `IssuedApiKey` |
| | `list_api_keys(identity)` | the caller's keys as `ApiKeyInfo` descriptors, never secrets |
| | `revoke_api_key(identity, key_id)` · `revoke_many_api_keys(identity, key_ids)` | the caller's own keys |
| | `list_principal_api_keys(principal_id)` · `revoke_principal_api_key(key_id)` | administrator calls: no identity check, so who may make them is the calling operation's authorization to settle; global lookup, since API-key accounts are not tenant-scoped; revoking an unknown key is `not_found` |
| `PasswordAccountProvisioningPort` | `register_with_password(principal_id, credentials)` | create the password account of a known principal id |
| | `provision_password_account(operator, principal_id, credentials)` | the same, on an operator's behalf |
| | `issue_password_invite(operator, principal_id)` · `accept_invite_with_password(invite_token, principal_id, credentials)` | invite flow; the first returns an `IssuedInvite` |

### Principal state and events

| Port | Method | Notes |
|------|--------|-------|
| `PrincipalEligibilityPort` | `require_authentication_allowed(principal_id, *, for_update=False)` | raises when the principal is missing or inactive; `for_update` takes a row lock held until the caller's transaction ends. Session issue passes it, so on Postgres a concurrent deactivation either waits for the new session and revokes it, or commits first and the issue is refused |
| `PrincipalDeactivationPort` | `deactivate(principal_id)` | idempotent; cascades logout and closes the password and API-key accounts of every store the application wires; with revocation states wired it must run inside a transaction (`authn_revocation_requires_transaction` otherwise) |
| `AuthnEventSink` | `record(event)` | receives an `AuthnEvent` (`AuthnEventKind`, principal, `login_digest` — never the raw login). Optional and best-effort: flows emit through `resolve_authn_event_emitter`, which treats a missing registration as emission off and a raising sink as a logged warning. `ctx.authn.event_sink(spec)` raises when none is registered |

### Revocation fast path

Postgres stays the truth; these cache what a session or API-key lookup read there, so a
verification can skip the row. A missing or unreadable state means "read Postgres", never
"admitted". Wiring is in [Check sessions and API keys from
Redis](../../integrations/authn.md#check-sessions-and-api-keys-from-redis).

| Port | Method | Notes |
|------|--------|-------|
| `RevocationStatesPort` | `read(principal_id, key)` | the principal's epoch and one family (`f:<id>`) or session (`s:<id>`) state, with the store's clock, in one read |
| | `fill(principal_id, states, *, expire_at_ms)` | set absent keys only; a write landing at or after `expire_at_ms` leaves nothing |
| | `revoke(principal_id, keys, *, ttl)` | overwrite with `revoked` — a revocation always wins |
| | `now_ms(principal_id)` · `raise_epoch(principal_id, epoch, *, expire_at_ms, state=None)` | the store's clock; keep the larger cached epoch |
| `ApiKeyStatesPort` | `read_api_key(digest)` · `fill_api_key(digest, state, *, expire_at_ms)` · `revoke_api_keys(digests, *, ttl)` | opt-in extension of the same store for API keys; the build refuses a states store that lacks it when API keys verify from cached states |
| `PrincipalEpochPort` | `current(principal_id)` | unlocked read; a principal with no row has epoch `0` |
| | `hold(principal_id)` · `advance(principal_id)` | issue creates the row if absent and holds the epoch under a share lock; a revocation raises it by one. Both refuse to run outside a transaction. The epoch only rises, and its row is never deleted while a token might carry it |

## Authorization

`AuthzSpec`:

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | route name |
| `tenancy_mode` | `"global" \| "require_invocation_tenant"` | `"global"` | whether grant resolution is partitioned by the invocation tenant — set `require_invocation_tenant` in a multi-tenant deployment; it refuses a call with no tenant bound as `tenant_required` (401), which asks the client to select a tenant, not to refresh its token |
| `enforce_principal_active` | `bool` | `True` | refuse a decision for a deactivated principal |
| `enforce_delegation_grant` | `bool` | `False` | a delegated identity (`actor` set) also needs a recorded delegation grant; enabling it with no delegation port wired is a configuration error |

### Ports and dep keys

| Accessor `ctx.authz.<x>(spec)` | Port | Dep key | Write |
|--------------------------------|------|---------|-------|
| `decision` | `AuthzDecisionPort` | `AuthzDecisionDepKey` | |
| `scope` | `AuthzScopePort` | `AuthzScopeDepKey` | |
| `grant_query` | `GrantQueryPort` | `GrantQueryDepKey` | |
| `role_assignment` | `RoleAssignmentPort` | `RoleAssignmentDepKey` | yes |
| `principal_registry` | `PrincipalRegistryPort` | `PrincipalRegistryDepKey` | yes |
| `delegation` | `DelegationPort` | `DelegationDepKey` | |
| `delegation_grant` | `DelegationGrantPort` | `DelegationGrantDepKey` | yes |

A *subject* below is any of `PrincipalRef \| UUID \| AuthnIdentity \| AuthzSubject`, and
`scope` is an optional `AuthzScope` policy partition.

| Port | Method | Notes |
|------|--------|-------|
| `AuthzDecisionPort` | `authorize(request)` | `AuthzRequest` → `AuthzDecision`: may this principal run this operation? |
| `AuthzScopePort` | `scope_document(request)` | `AuthzDocumentScopeRequest` → `AuthzDocumentScope`, a [query-DSL](../query-syntax.md) constraint for list, search and read paths. Scopes the request's subject alone; `AuthzDocumentScopeWrap` walks a delegation chain and conjoins the answers |
| | `authorize_sensitive_resource(request)` | `AuthzSensitiveAccessRequest` → `bool`; a delegated subject passes only when every principal in its chain does |
| `GrantQueryPort` | `resolve_effective_grants(subject, *, scope=None)` | the subject's `EffectiveGrants` |
| `RoleAssignmentPort` | `assign_role(subject, role_key, *, scope=None)` · `revoke_role(…)` | `role_key` names a catalog role |
| | `list_roles(subject, *, scope=None)` | `frozenset[RoleRef]` |
| `PrincipalRegistryPort` | `ensure_principal(principal_id, kind, *, is_active=True)` | idempotent create-or-return; `kind` is `"user"` or `"service"` |
| | `create_principal(kind)` · `get_principal(principal_id)` | a new `PrincipalRef`; a known one, or `None` |
| | `deactivate_principal(principal_id)` | mark inactive for policy |
| `DelegationPort` | `may_act(actor_id, subject_id, *, scope=None)` | the pairwise grant check, consulted only when `enforce_delegation_grant` is set |
| `DelegationGrantPort` | `grant_delegation(actor, subject, *, scope=None)` · `revoke_delegation(…)` | grant is idempotent |
| | `list_delegators(actor, *, scope=None)` | the principal ids `actor` may act for |

## Replacing one port

Register your factory under the port's key and the route, and leave that route out of the
module field that would register the shipped one. Two registrations of one key on one route
are refused when the deps merge (`Conflicting routed dependencies for '<key>': <route>`), so
the shipped default and yours never race:

```python
from forze.application.contracts.authn import PasswordLifecycleDepKey
from forze.application.contracts.deps import Deps
from forze.application.execution import DepsRegistry
from forze_identity.authn import AuthnDepsModule

deps = DepsRegistry(
    modules=(
        AuthnDepsModule(
            kernel=kernel,
            authn={"main": frozenset({"password", "token"})},
            token_lifecycle=["main"],  # password_lifecycle unset: that slot is yours
        ),
    ),
    deps=(Deps.routed({PasswordLifecycleDepKey: {"main": build_password_lifecycle}}),),
)
```

Which field registers each shipped port, and the exceptions to the rule:

| Dep key | Registered by | To replace it |
|---------|---------------|---------------|
| `AuthnDepKey` | `AuthnDepsModule.authn` | register your own orchestrator for a route you leave out of `authn` |
| `TokenVerifierDepKey` · `PasswordVerifierDepKey` · `ApiKeyVerifierDepKey` | `AuthnDepsModule.authn`, per enabled family | pass `token_verifiers` / `password_verifiers` / `api_key_verifiers` for the route; a token-verifier override also needs a `resolvers` override |
| `PrincipalResolverDepKey` | `AuthnDepsModule.authn` | pass `resolvers` for the route |
| `TokenLifecycleDepKey` · `PasswordLifecycleDepKey` · `PasswordResetDepKey` · `ApiKeyLifecycleDepKey` · `PasswordAccountProvisioningDepKey` · `PrincipalDeactivationDepKey` | the `AuthnDepsModule` field of the same name | leave the route out of that field |
| `PrincipalEligibilityDepKey` | `AuthnDepsModule`, for every route it wires | on those routes, choose `eligibility="policy_principal"` or `"allow_all"`; a second registration conflicts |
| `AuthnEventSinkDepKey` | `AuthnDepsModule.events`, for every route it wires | leave `events` unset and register a sink per route, or one unrouted sink for all |
| `RevocationStatesDepKey` · `PrincipalEpochDepKey` | `RedisDepsModule(revocation_states=…)` · `PostgresDepsModule(principal_epochs=…)` | register another store in their place |
| `AuthzDecisionDepKey` · `AuthzScopeDepKey` · `GrantQueryDepKey` · `RoleAssignmentDepKey` · `PrincipalRegistryDepKey` · `DelegationDepKey` · `DelegationGrantDepKey` | the `AuthzDepsModule` field named for the accessor (`decision`, `scope`, …) | leave the route out of that field |

Keep the contract obligations above when you replace a port — in particular the refresh
reuse signal, the uniform reset error, and the locking eligibility read — since the shipped
flows and kits rely on them. `forze_mock` implements every port on this page for tests.
