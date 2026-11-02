---
title: Accept external OIDC tokens
icon: lucide/badge-check
summary: Verify a third-party IdP's access tokens on every request — verify-then-resolve, no first-party minting
---

Sometimes you want to accept an IdP's credentials *directly* — every request
carries a third-party access token (or session), and your API verifies it and
resolves it to a principal. No first-party tokens are minted (that's the
[bootstrap](external-bootstrap-forze-jwt.md) pattern); the verification is the
seam.

The conceptual model is in [OIDC](../integrations/oidc.md); this is the wiring.

## Build a verifier

Pick the verifier by what your IdP puts on the request, and build its factory once,
at startup — each factory builds its verifier once, so the JWKS or verdict cache
spans requests.

### JWT access tokens (Keycloak, Casdoor, Zitadel in JWT mode)

Build the preset from the issuer's discovery document, and say which claims carry
roles and groups:

```python
from forze_identity.oidc import (
    ClaimsPath,
    ConfigurableOidcIdpVerifier,
    OidcClaimMapper,
    OidcIdpPreset,
)


async def jwt_verifier() -> ConfigurableOidcIdpVerifier:
    preset = await OidcIdpPreset.from_issuer(
        "https://kc.example.com/realms/acme",
        audience="my-api",
        claim_mapper=OidcClaimMapper(
            email_claim="email",
            roles=ClaimsPath("realm_access.roles"),
            groups=ClaimsPath("groups"),
        ),
    )
    return ConfigurableOidcIdpVerifier(preset=preset)
```

The extracted values land under `_forze` in the assertion's claims
(`{"email": ..., "roles": [...], "groups": [...]}`). For a pinned, offline setup,
build `OidcIdpPreset(issuer=..., jwks_uri=..., audience=...)` by hand instead, or
construct an `OidcTokenVerifier` over a `JwksKeyProvider` directly. Issuer and
audience are required by default.

### Opaque access tokens (Ory Hydra, Zitadel by default)

An opaque token can only be checked by asking the issuer. Introspect it with the
endpoint discovery advertises:

```python
from datetime import timedelta

from pydantic import SecretStr

from forze_identity.oidc import (
    BasicClientAuth,
    ConfigurableIntrospectionVerifier,
    IntrospectionConfig,
    OidcProviderMetadata,
)


def introspection_verifier(
    metadata: OidcProviderMetadata,
    client_secret: SecretStr,
) -> ConfigurableIntrospectionVerifier:
    if metadata.introspection_endpoint is None:
        raise RuntimeError("the issuer advertises no introspection endpoint")

    return ConfigurableIntrospectionVerifier(
        config=IntrospectionConfig(
            endpoint=metadata.introspection_endpoint,
            issuer=metadata.issuer,
            audience="my-api",
            client_auth=BasicClientAuth(client_id="my-api", client_secret=client_secret),
            cache_ttl=timedelta(seconds=30),
        ),
    )
```

Get the metadata from `fetch_oidc_configuration(issuer)` or from
`preset.metadata`. For Zitadel's recommended `private_key_jwt`, pass
`PrivateKeyJwtAuth(client_id=..., private_key=..., key_id=..., audience=issuer)`
instead. `active: false` and any refusal are an authentication failure; an
unreachable endpoint is an infrastructure failure, never an accepted token. The
`cache_ttl` bounds how long a revoked token keeps working.

### Kratos sessions

Ory Kratos issues sessions, not tokens. Forward the session cookie (what a
`CookieTokenAuthn` ingress delivers) or a session token to `/sessions/whoami`:

```python
from forze_identity.oidc import (
    ClaimsPath,
    ConfigurableWhoamiVerifier,
    OidcClaimMapper,
    WhoamiConfig,
)

whoami = ConfigurableWhoamiVerifier(
    config=WhoamiConfig(
        endpoint="https://kratos.example.com/sessions/whoami",
        issuer="https://kratos.example.com",
        credential_style="cookie",
        claim_mapper=OidcClaimMapper(
            email_claim="traits.email",
            roles=ClaimsPath("metadata.roles"),
        ),
    ),
)
```

The subject is the Kratos identity id. Read roles from `metadata`
(`metadata_public`), never from `traits`, which users can edit themselves.

## Register it on a route

A verifier only *proves* the token — a **resolver** decides the principal.
Register both on the auth route via `AuthnDepsModule`, passing the factory you
built above (a factory, not a bare verifier). For real SSO, the mapping-table
resolver provisions a principal on first sight:

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
        token_verifiers={"api": factory},
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
  reachable (the provider caches it for `cache_ttl`) — and, for introspection and
  whoami, on one round trip per token per cache TTL. If that coupling is a
  problem, mint first-party tokens with the [bootstrap](external-bootstrap-forze-jwt.md)
  pattern instead.
- Forze logout can't revoke a third-party token — enforce eligibility on your
  side, or introspect, which sees a revocation within `cache_ttl`.
