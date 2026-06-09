---
title: OIDC
icon: lucide/badge-check
summary: Verify external OIDC tokens in the authentication pipeline
---

`forze[oidc]` (the `forze_identity.oidc` plane) is a generic OIDC token verifier
— RS256/ES256/HS256 JWTs validated against a JWKS. It's a **verifier only**: it
slots into the [authentication](authn.md) verify-then-resolve seam and reuses the
authn resolvers to produce a `UUID` principal.

## Install

```bash
uv add 'forze[oidc]'
```

## Wire it

OIDC has no deps module of its own — register its verifier under
`TokenVerifierDepKey` for the routes that accept external tokens, and choose a
resolver. Construct the verifier from a JWKS key provider:

```python
from datetime import timedelta
from forze_identity.oidc import JwksKeyProvider, OidcTokenVerifier

verifier = OidcTokenVerifier(
    key_provider=JwksKeyProvider(jwks_uri="https://idp.example.com/.well-known/jwks.json"),
    issuer="https://idp.example.com/",
    audience="my-api",
)
```

Wire it onto a route via `AuthnDepsModule.token_verifiers` (the shipped
`ConfigurableOidcIdpVerifier` + `OidcIdpPreset` cover the routed-factory case).

## What it provides

| Capability | Type |
|------------|------|
| OIDC token verification | `OidcTokenVerifier` → a `TokenVerifierPort` |
| JWKS key resolution (cached) | `JwksKeyProvider` (or `StaticKeyProvider`) |
| Claim mapping | `OidcClaimMapper` (incl. an optional tenant claim) |

## Notes

- The verifier only proves the token; the **resolver** decides the principal —
  pair with `MappingTableResolver` (production SSO), `DeterministicUuidResolver`,
  or `JwtNativeUuidResolver`. See [Identity & access](../in-depth/identity.md).
- Forze logout does **not** revoke third-party access tokens — enforce
  eligibility instead.
- `JwksKeyProvider`'s `cache_ttl` and `timeout` are `timedelta`s. Issuer and
  audience are required by default.
- Vendor presets (Google, VK, Telegram) live in `forze_identity.builtin.idp`.
