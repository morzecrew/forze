---
title: Accept external OIDC tokens
icon: lucide/badge-check
summary: Verify a third-party IdP's access tokens on every request — verify-then-resolve, no first-party minting
---

Sometimes you want to accept an IdP's tokens *directly* — every request carries a
third-party OIDC access token, and your API verifies it against the IdP's JWKS
and resolves it to a principal. No first-party tokens are minted (that's the
[bootstrap](external-bootstrap-forze-jwt.md) pattern); the verification is the
seam.

The conceptual model is in [OIDC](../integrations/oidc.md); this is the wiring.

## Build a verifier

An `OidcTokenVerifier` validates the JWT signature against a JWKS, plus issuer,
audience, and expiry. All arguments are keyword-only, and `cache_ttl` / `timeout`
are `timedelta`s:

```python
from datetime import timedelta
from forze_identity.oidc import JwksKeyProvider, OidcClaimMapper, OidcTokenVerifier

verifier = OidcTokenVerifier(
    key_provider=JwksKeyProvider(
        jwks_uri="https://idp.example.com/.well-known/jwks.json",
        cache_ttl=timedelta(minutes=10),
    ),
    issuer="https://idp.example.com/",
    audience="my-api",
    claim_mapper=OidcClaimMapper(tenant_claim="org_id"),  # optional tenant claim
)
```

Issuer and audience are required by default (the verifier refuses to start
without them).

## Register it on a route

A verifier only *proves* the token — a **resolver** decides the principal.
Register both on the auth route via `AuthnDepsModule`. For real SSO, the
mapping-table resolver provisions a principal on first sight:

```python
from forze_identity.authn import (
    AuthnDepsModule,
    AuthnKernelConfig,
    ConfigurableMappingTableResolver,
)

deps = DepsRegistry.from_modules(
    AuthnDepsModule(
        kernel=AuthnKernelConfig(),  # no signing secret — this route only verifies
        authn={"api": frozenset({"token"})},
        token_verifiers={"api": verifier},
        resolvers={"api": ConfigurableMappingTableResolver(provision_on_first_sight=True)},
    ),
)
```

Pick the resolver by your model:

| Resolver | Use when |
|----------|----------|
| `ConfigurableMappingTableResolver(provision_on_first_sight=True)` | external SSO — map `(issuer, subject)` → a stable principal, creating it on first login |
| `ConfigurableDeterministicUuidResolver()` | stateless — derive a deterministic principal id from the token, no table |

## At the boundary

The route is protected exactly like any other — a `HeaderTokenAuthn` ingress with
the route's `AuthnSpec` (see [Authn, authz & tenancy](authn-authz-tenancy-fastapi.md)):

```python
from forze.application.contracts.authn import AuthnSpec
from forze_fastapi.security import AuthnRequirement, HeaderTokenAuthn

API = AuthnSpec(name="api", enabled_methods=frozenset({"token"}))
requirement = AuthnRequirement(
    ingress=(HeaderTokenAuthn(authn_spec=API, header_name="Authorization"),),
)
```

## Notes

- The shipped `ConfigurableOidcIdpVerifier` + `OidcIdpPreset` cover the common
  provider cases — use them instead of hand-building a verifier when one fits.
- Verifying on every request means every request depends on the IdP's JWKS being
  reachable (the provider caches it for `cache_ttl`). If that coupling is a
  problem, mint first-party tokens with the [bootstrap](external-bootstrap-forze-jwt.md)
  pattern instead.
- Forze logout can't revoke a third-party token — enforce eligibility on your
  side.
