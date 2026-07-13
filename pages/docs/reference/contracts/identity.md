---
title: Identity (authn & authz)
icon: lucide/user-check
summary: The authentication and authorization contracts — a catalog of their ports and specs
---

Identity splits into **authentication** (who is calling — `ctx.authn.*`) and
**authorization** (what they may do — `ctx.authz.*`). Both are large port families; the
concepts and wiring are in [Identity](../../identity-tenancy-enc/identity.md) and the
[authn](../../integrations/authn.md) / [OIDC](../../integrations/oidc.md) integrations — this
is the port catalog.

## Authentication

`AuthnSpec` declares which credential families a route accepts and which named verifier /
resolver profiles to use (profiles are how external IdPs plug in without changing the
contract):

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | route name |
| `enabled_methods` | `frozenset[AuthnMethod]` | `{"token"}` | credential families: `password` / `token` / `api_key` |
| `token_profile` · `password_profile` · `api_key_profile` | `str \| None` | `None` | named verifier profiles (else the route default) |
| `resolver_profile` | `str \| None` | `None` | named principal-resolver profile |

Ports, via `ctx.authn.<x>(spec)`:

| Accessor | Port | For |
|----------|------|-----|
| `authn` | `AuthnPort` | authenticate a credential → `AuthnIdentity` |
| `eligibility` | `PrincipalEligibilityPort` | whether a principal may authenticate |
| `token_lifecycle` | `TokenLifecyclePort` | issue / refresh / revoke tokens |
| `password_lifecycle` | `PasswordLifecyclePort` | set / change a password |
| `password_reset` | `PasswordResetPort` | request / confirm a reset (the [recipe](../../recipes/password-reset.md)) |
| `api_key_lifecycle` | `ApiKeyLifecyclePort` | mint / rotate / revoke API keys |
| `password_account_provisioning` | `PasswordAccountProvisioningPort` | register / provision password accounts, issue + accept invites |
| `principal_deactivation` | `PrincipalDeactivationPort` | deactivate a principal (cascades logout) |
| `event_sink` | `AuthnEventSink` | structured authn events (login, lockout, refresh-reuse) |

## Authorization

`AuthzSpec`:

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | route name |
| `tenancy_mode` | `"global" \| "require_invocation_tenant"` | `"global"` | whether grant resolution is partitioned by the invocation tenant — set `require_invocation_tenant` in a multi-tenant deployment |
| `enforce_principal_active` | `bool` | `True` | refuse a decision for a deactivated principal |
| `enforce_delegation_grant` | `bool` | `False` | a delegated identity (`actor` set) also needs a recorded delegation grant |

Ports, via `ctx.authz.<x>(spec)`:

| Accessor | Port | For |
|----------|------|-----|
| `decision` | `AuthzDecisionPort` | may this principal run this operation? |
| `scope` | `AuthzScopePort` | which rows may they see — a [query-DSL](../query-syntax.md) filter |
| `grant_query` | `GrantQueryPort` | read roles / permissions / bindings |
| `role_assignment` | `RoleAssignmentPort` | assign / revoke roles |
| `principal_registry` | `PrincipalRegistryPort` | provision principals |
| `delegation` · `delegation_grant` | `DelegationPort` · `DelegationGrantPort` | act-on-behalf-of delegation |

## Implemented by

| Surface | Provider | Integration |
|---------|----------|-------------|
| Authn + authz | `forze_identity` (local + external-IdP presets) | [Authn](../../integrations/authn.md) · [OIDC](../../integrations/oidc.md) |

A mock identity provider implements the ports for tests.
