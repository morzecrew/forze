---
title: OIDC
icon: lucide/badge-check
summary: Verify external OIDC tokens and sessions in the authentication pipeline
---

`forze[oidc]` (the `forze_identity.oidc` plane) verifies credentials an external
identity provider issued: JWTs against the issuer's JWKS, opaque access tokens by
RFC 7662 introspection, and Ory Kratos sessions at `/sessions/whoami`. It's a
**verifier only**: each verifier slots into the [authentication](authn.md)
verify-then-resolve seam and reuses the authn resolvers to produce a `UUID`
principal.

## Install

```bash
uv add 'forze[oidc]'
```

## Wire it

OIDC has no deps module of its own — register a verifier factory under
`TokenVerifierDepKey` for the routes that accept external credentials (via
`AuthnDepsModule.token_verifiers`), and choose a resolver. For a JWT issuer, build
the preset from its discovery document once, at startup:

```python
from forze_identity.oidc import ConfigurableOidcIdpVerifier, OidcIdpPreset


async def build_verifier() -> ConfigurableOidcIdpVerifier:
    preset = await OidcIdpPreset.from_issuer("https://idp.example.com", audience="my-api")
    return ConfigurableOidcIdpVerifier(preset=preset)
```

`from_issuer` reads `{issuer}/.well-known/openid-configuration`, refuses a document
whose `issuer` differs from the one you asked for, and fills `jwks_uri`. A
hand-built `OidcIdpPreset(issuer=..., jwks_uri=..., audience=...)` is the pinned,
offline path and needs no network at startup. Opaque tokens and Kratos sessions are
wired the same way through `ConfigurableIntrospectionVerifier` and
`ConfigurableWhoamiVerifier` — the [recipe](../recipes/external-idp-oidc.md) shows
all three.

## What it provides

| Capability | Type |
|------------|------|
| JWT verification against a JWKS | `OidcTokenVerifier` → a `TokenVerifierPort` |
| Discovery | `fetch_oidc_configuration` → `OidcProviderMetadata`; `OidcIdpPreset.from_issuer` |
| Opaque-token introspection (RFC 7662) | `IntrospectionTokenVerifier` + `IntrospectionConfig`, client auth `BasicClientAuth` or `PrivateKeyJwtAuth` |
| Kratos session verification | `WhoamiSessionVerifier` + `WhoamiConfig` |
| Routed factories (built once) | `ConfigurableOidcIdpVerifier`, `ConfigurableIntrospectionVerifier`, `ConfigurableWhoamiVerifier` |
| JWKS key resolution (cached) | `JwksKeyProvider` (or `StaticKeyProvider`) |
| Claim mapping | `OidcClaimMapper` — tenant claim, plus opt-in `email_claim`, `roles`, `groups` |
| Role and group extractors | `ClaimsPath` (dotted path; `item_key` for lists of objects), `ZitadelProjectRoles` |

`OidcClaimMapper` writes what `email_claim`, `roles` and `groups` extract under one
key, `FORZE_CLAIMS` (`"_forze"`), inside the assertion's claims — the same shape for
every issuer:

| IdP | Roles extractor |
|-----|-----------------|
| Keycloak realm roles | `ClaimsPath("realm_access.roles")` |
| Keycloak client roles | `ClaimsPath("resource_access.{client}.roles", client="my-api")` |
| Zitadel project roles | `ZitadelProjectRoles(org_id=...)` |
| Casdoor (default JWT) | `ClaimsPath("roles", item_key="name")` |
| Kratos session | `ClaimsPath("metadata.roles")` |

## Notes

- The verifier only proves the credential; the **resolver** decides the principal —
  pair with `MappingTableResolver` (production SSO) or `DeterministicUuidResolver`.
  A `token_verifiers` override without a `resolvers` override is refused, because
  the default `JwtNativeUuidResolver` would take an external subject as an internal
  principal id. See [Identity & access](../identity-tenancy-enc/identity.md).
- Forze logout does **not** revoke third-party access tokens — enforce
  eligibility instead, or use introspection, which sees a revocation within its
  cache TTL.
- Discovery is construction-time only: an IdP outage can delay startup, never fail
  a request. It does not read algorithms from the document — pass `algorithms`
  yourself, so a remote file cannot widen the allowlist.
- Discovery, introspection and whoami endpoints must be `https` (plain `http` only
  on a loopback host); redirects are not followed.
- Introspection and whoami fail closed. An inactive, revoked, expired, foreign-issuer
  or foreign-audience token is an authentication failure with one shape
  (`invalid_oidc_token`; `invalid_session_token` for whoami). A timeout, a non-2xx
  status or a malformed body is an infrastructure failure — never an accepted token.
- Their caches keep only active verdicts, keyed by the token's SHA-256 digest, for
  `cache_ttl` (default 30 s, at most 5 minutes, `0` disables) clamped to the token's
  own expiry. A revoked token keeps working until its entry expires, never longer.
  Build each verifier once (the `Configurable*` factories do), or the cache never
  hits.
- Introspection requires an `audience` unless you set `require_audience=False`, and
  records the configured `issuer` (RFC 7662 makes `iss` optional; a response that
  carries a different one is refused). Keycloak answers `active: false` to an
  introspecting client that is not in the token's audience.
- Client secrets and private keys take a `str` or a pydantic `SecretStr` and stay
  out of `repr`; tokens and secrets are never logged.
- Kratos lets users edit their own `traits`: read roles from `metadata`
  (`metadata_public`, admin-written), never from `traits`. `_forze.email` is not
  proof the user owns the address.
- `JwksKeyProvider`'s `cache_ttl` and `timeout` are `timedelta`s. Issuer and
  audience are required by default.
- `require_nonce=True` (on the verifier or `OidcIdpPreset`) rejects tokens
  missing a `nonce` claim — presence-only; bind the nonce *value* in your
  callback handler with `verify_id_token_nonce`.
- Vendor presets (Google, VK, Telegram) live in `forze_identity.builtin.idp`.
