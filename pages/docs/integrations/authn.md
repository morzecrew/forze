---
title: Authentication
icon: lucide/key-round
summary: First-party authentication — passwords, tokens, and API keys
---

`forze[authn]` is the built-in authentication stack — the `forze_identity.authn`
plane. It implements the verify-then-resolve seam from [Identity &
access](../in-depth/identity.md): password (Argon2), bearer-token (Forze JWT),
and API-key verifiers, paired with resolvers that map a `VerifiedAssertion` to a
`UUID` principal.

## Install

```bash
uv add 'forze[authn]'
```

It's document-backed — the account, session, and identity-mapping stores are
ordinary document specs you wire to a database.

## Wire it

`AuthnDepsModule` registers a first-party stack per route. `authn` enables the
credential families a route accepts; `kernel` carries the signing/secret config:

```python
from forze.application.execution import DepsRegistry
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig

deps = DepsRegistry.from_modules(
    AuthnDepsModule(
        kernel=AuthnKernelConfig(...),
        authn={"api": frozenset({"token", "api_key"})},
    ),
)
```

A route's [`AuthnSpec`](../in-depth/identity.md) selects verifiers and a resolver
by profile name; override the defaults via `token_verifiers`, `resolvers`, etc.

## What it provides

| Capability | Default implementation | Dep key |
|------------|------------------------|---------|
| Orchestrator | `AuthnOrchestrator` | `AuthnDepKey` |
| Token verifier | `ForzeJwtTokenVerifier` | `TokenVerifierDepKey` |
| Password verifier | `Argon2PasswordVerifier` | `PasswordVerifierDepKey` |
| API-key verifier | `HmacApiKeyVerifier` | `ApiKeyVerifierDepKey` |
| Principal resolver | `JwtNativeUuidResolver` | `PrincipalResolverDepKey` |

## Notes

- **Document-backed:** wire the account / session / identity-mapping specs to a
  document store (Postgres, Mongo, …). `kernel` is required only when a route
  mints first-party tokens.
- Authorization is a sibling plane — `forze_identity.authz` (`AuthzDepsModule`).
- The conceptual model (verify → resolve, resolver flavors) is in
  [Identity & access](../in-depth/identity.md); this page is the wiring.
- For external IdPs, pair this with the [OIDC](oidc.md) verifier.
