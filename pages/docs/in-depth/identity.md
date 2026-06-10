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

![Verification emits a VerifiedAssertion; resolution turns it into an AuthnIdentity](../_diagrams/light/authn-verify-resolve.svg#only-light){ loading=lazy }
![Verification emits a VerifiedAssertion; resolution turns it into an AuthnIdentity](../_diagrams/dark/authn-verify-resolve.svg#only-dark){ loading=lazy }

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

## Authorization: may they?

Once a request carries an `AuthnIdentity`, authorization decides what it may do.
Two questions, two ports:

| Question | Port | Resolved via |
|----------|------|--------------|
| May this principal run this operation? | `AuthzDecisionPort` | `ctx.authz.decision(spec)` |
| Which rows may they see? | `AuthzScopePort` | `ctx.authz.scope(spec)` |

(A third slice — grant management — provisions the roles, permissions, and
bindings those decisions read.)

Enforcement belongs on the **operation plan**, not scattered across routes — so
it's authoritative for every caller, HTTP or not. Using the [stage
hooks](../core-concepts/application-layer.md) from the application layer: a `BeforeStep`
authorizes the operation, and a `wrap` step injects scope filters into
list/search queries. Both read the bound `AuthnIdentity` and `TenantIdentity` to
build the decision.

## The identity plane

All of this lives in `forze_identity`, separate from the core: `authn`, `authz`,
`tenancy`, `oidc`, and `oauth` subpackages, wired per route via the same deps
modules as any other integration.

For getting started, `forze_identity.builtin` ships presets — file/env API keys
(`local`) and Google / VK / Telegram Login over OIDC (`idp`). They're shipped-in
conveniences, not production defaults: adopt one only once you accept its trust
model (e.g. VK publishes no JWKS, so its preset verifies `id_token`s by
server-side introspection against VK rather than a local signature check).
