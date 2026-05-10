# OIDC Integration (`forze_oidc`)

## Page opening

`forze_oidc` is a generic OpenID Connect verifier package that implements the `TokenVerifierPort` seam introduced by the [authentication pipeline](../concepts/authentication.md). It is the reference for plugging external IdPs (generic OIDC providers, Casdoor, Firebase Auth, Auth0, internal SSO with a JWKS endpoint, etc.) into Forze without changing core contracts.

| Topic | Details |
|------|---------|
| What it provides | A generic `TokenVerifierPort` implementation (`OidcTokenVerifier`), a configurable `OidcClaimMapper`, and pluggable `SigningKeyProviderPort` implementations (`JwksKeyProvider`, `StaticKeyProvider`). |
| Supported Forze contracts | `TokenVerifierDepKey` from `forze.application.contracts.authn`. Pairs with any `PrincipalResolverPort` (typically `MappingTableResolver` or `DeterministicUuidResolver` from `forze_authn`). |
| When to use it | Use when an external IdP issues OIDC-style JWTs (RS256/ES256/HS256) and you want Forze to consume them without leaking vendor specifics into domain or application code. |

## Installation

```bash
uv add 'forze[oidc]'
```

| Requirement | Notes |
|-------------|-------|
| Package extra | `oidc` installs `pyjwt[crypto]` for JWS verification and JWKS fetching. |
| Required service | The IdP (issuer URL + JWKS URI). No local service runs. |
| Local development dependency | None beyond the extra; tests use `StaticKeyProvider` to avoid network. |

Importing any module from `forze_oidc` calls `require_oidc()` and raises a clear `RuntimeError("forze_oidc requires 'forze[oidc]' extra")` when the extra is missing.

## Minimal setup

### Key provider

`OidcTokenVerifier` accepts any `SigningKeyProviderPort`:

```python
from forze_oidc import JwksKeyProvider, StaticKeyProvider

jwks = JwksKeyProvider(
    jwks_uri="https://idp.example.com/.well-known/jwks.json",
    cache_ttl_seconds=300,
    timeout=10,
)

# For tests / single-tenant HS256:
hs_keys = StaticKeyProvider(key=b"some-shared-secret")
```

`JwksKeyProvider` lazily creates an underlying `jwt.PyJWKClient` and caches signing keys by `kid`; instantiate one per IdP issuer.

### Claim mapper

`OidcClaimMapper` defaults follow the OIDC core spec (`iss`, `sub`, `aud`, `iat`, `exp`). Override the claim names when an IdP uses non-standard keys or to enable tenant resolution:

```python
from forze_oidc import OidcClaimMapper

# Default: tenant_hint stays None.
default_mapper = OidcClaimMapper()

# Casdoor includes the org id under "organization".
casdoor_mapper = OidcClaimMapper(tenant_claim="organization")

# Firebase puts the project tenant under "firebase.tenant" (nested keys are not
# supported out of the box; subclass to extract them).
```

| Field | Default | Purpose |
|-------|---------|---------|
| `issuer_claim` | `"iss"` | Source for `VerifiedAssertion.issuer`. |
| `subject_claim` | `"sub"` | Source for `VerifiedAssertion.subject`. |
| `audience_claim` | `"aud"` | Optional; takes the first string when the IdP returns an array. |
| `issued_at_claim` / `expires_at_claim` | `"iat"` / `"exp"` | Coerced to `datetime` when present as integer/float. |
| `tenant_claim` | `None` | When set, the resolver picks tenant context from this claim. |

### Verifier

`OidcTokenVerifier` validates signature, issuer, audience, and expiry with the configured leeway, then delegates the claim payload to the mapper:

```python
from datetime import timedelta

from forze_oidc import OidcTokenVerifier

oidc_verifier = OidcTokenVerifier(
    key_provider=jwks,
    algorithms=("RS256",),
    audience="my-api",
    issuer="https://idp.example.com/",
    leeway=timedelta(seconds=10),
    claim_mapper=default_mapper,
)
```

| Field | Default | Notes |
|-------|---------|-------|
| `key_provider` | required | Any `SigningKeyProviderPort`. |
| `algorithms` | `("RS256",)` | JWS algorithm allowlist; rejects everything else. |
| `audience` | `None` | Required `aud` value(s); skip enforcement by leaving `None`. |
| `issuer` | `None` | Required `iss` value; skip enforcement by leaving `None`. |
| `leeway` | `timedelta(seconds=10)` | Clock-skew tolerance for `iat`/`exp`/`nbf`. |
| `claim_mapper` | `OidcClaimMapper()` | Maps the verified payload to `VerifiedAssertion`. |

The verifier raises `AuthenticationError(code="oidc_token_expired")` for expired tokens and `AuthenticationError(code="invalid_oidc_token")` for any other validation failure.

## Wiring into AuthnDepsModule

Register the OIDC verifier under `TokenVerifierDepKey` for one or more routes via `AuthnDepsModule.token_verifiers`:

```python
from collections.abc import Mapping
from typing import final

import attrs

from forze.application.contracts.authn import AuthnSpec, TokenVerifierPort
from forze.application.execution import ExecutionContext

from forze_oidc import JwksKeyProvider, OidcTokenVerifier


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableOidcTokenVerifier:
    """Routed factory shape for OidcTokenVerifier."""

    key_provider: JwksKeyProvider
    audience: str
    issuer: str

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> TokenVerifierPort:
        _ = ctx, spec
        return OidcTokenVerifier(
            key_provider=self.key_provider,
            algorithms=("RS256",),
            audience=self.audience,
            issuer=self.issuer,
        )


jwks = JwksKeyProvider(jwks_uri="https://idp.example.com/.well-known/jwks.json")

token_verifiers: Mapping[str, ConfigurableOidcTokenVerifier] = {
    "api": ConfigurableOidcTokenVerifier(
        key_provider=jwks,
        audience="my-api",
        issuer="https://idp.example.com/",
    ),
}
```

Pass the mapping into `AuthnDepsModule(..., token_verifiers=token_verifiers)`. Each route listed in `AuthnDepsModule.authn` whose enabled methods include `"token"` will receive this verifier instead of the first-party `ForzeJwtTokenVerifier`. See [Recipe — External IdPs over OIDC](../recipes/external-idp-oidc.md) for the full module wiring.

## Pairing with a resolver

`OidcTokenVerifier` only proves the token is valid; the resolver decides which Forze `principal_id` it maps to.

| Resolver | When to choose it |
|----------|-------------------|
| `MappingTableResolver(provision_on_first_sight=True)` | Production SSO with admin overrides, account merging, or future multi-IdP linking; needs the `IdentityMapping` document store wired. |
| `MappingTableResolver(provision_on_first_sight=False)` | Invitation-only deployments where principal rows are pre-created and mapped out of band. |
| `DeterministicUuidResolver` | Stateless prototyping or read-only deployments; principal id is `uuid4({"iss": issuer, "sub": subject})` — no DB writes. |
| `JwtNativeUuidResolver` | Only useful when the IdP's `sub` is already a UUID string for your internal principal. |

Override the resolver per route via `AuthnDepsModule.resolvers`:

```python
from forze_authn import ConfigurableMappingTableResolver

resolvers = {
    "api": ConfigurableMappingTableResolver(provision_on_first_sight=True),
}
```

## Provider notes

- **Generic OIDC** — supply `issuer`, `audience`, and the standard JWKS URI. Defaults of `OidcClaimMapper` are sufficient.
- **Casdoor** — set `tenant_claim="organization"` (or whatever your installation uses) and verify the `iss` matches the Casdoor instance origin.
- **Firebase Auth** — `iss` is `https://securetoken.google.com/<project-id>`, `aud` is the project id, JWKS URI is `https://www.googleapis.com/service_accounts/v1/jwks/securetoken@system.gserviceaccount.com`. Subclass `OidcClaimMapper` if you need to extract `firebase.tenant` from the nested claim.
- **Auth0** — `iss` is `https://<tenant>.auth0.com/`, `aud` is the API identifier; JWKS URI is `https://<tenant>.auth0.com/.well-known/jwks.json`.
- **Internal SSO** — set the `issuer` and `audience` to whatever your service mints; HS256 with `StaticKeyProvider` is appropriate when key distribution is private.

## Testing

`StaticKeyProvider` and `OidcClaimMapper` are inert; combined with `pyjwt`'s in-memory signing they cover unit tests without network access. See `tests/unit/test_forze_oidc/` for working examples.

## Cross-links

- [Concept — Authentication pipeline](../concepts/authentication.md)
- [Reference — Authentication contracts](../reference/authentication.md)
- [Recipe — External IdPs over OIDC](../recipes/external-idp-oidc.md)
- [Recipe — Authn, authz, and tenancy with FastAPI](../recipes/authn-authz-tenancy-fastapi.md)
