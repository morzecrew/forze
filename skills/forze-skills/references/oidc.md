# External IdPs (OIDC)

Delegating authentication to an external issuer: token verifiers, claim mapping, and resolving an external subject to an internal principal. The pipeline it plugs into is [authentication](authn.md).

## External IdPs (forze_identity.oidc)

`forze_identity.oidc` (extra `forze[oidc]`) verifies three kinds of external credential, each behind `TokenVerifierPort`:

| The request carries | Verifier | Routed factory |
|---------------------|----------|----------------|
| A JWT (Keycloak, Casdoor, Zitadel in JWT mode, Google) | `OidcTokenVerifier` over `JwksKeyProvider` | `ConfigurableOidcIdpVerifier(preset=OidcIdpPreset(...))` |
| An opaque access token (Ory Hydra, Zitadel by default) | `IntrospectionTokenVerifier` (RFC 7662) | `ConfigurableIntrospectionVerifier(config=IntrospectionConfig(...))` |
| An Ory Kratos session cookie or token | `WhoamiSessionVerifier` | `ConfigurableWhoamiVerifier(config=WhoamiConfig(...))` |

Register the factory under `TokenVerifierDepKey` for the relevant routes (`AuthnDepsModule.token_verifiers`) and register a resolver factory on the same route (`AuthnDepsModule.resolvers`): `ConfigurableMappingTableResolver(provision_on_first_sight=True)` (production SSO) or `ConfigurableDeterministicUuidResolver()` (stateless prototyping) — a `token_verifiers` override without a `resolvers` override is refused.

Build the JWT preset from the issuer's discovery document once, at startup:

```python
from forze_identity.oidc import (
    ClaimsPath,
    ConfigurableOidcIdpVerifier,
    OidcClaimMapper,
    OidcIdpPreset,
)


async def build_verifier() -> ConfigurableOidcIdpVerifier:
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

`from_issuer` refuses a discovery document whose `issuer` differs from the one asked for; `preset.metadata.introspection_endpoint` feeds `IntrospectionConfig(endpoint=...)`. A hand-built `OidcIdpPreset(issuer=..., jwks_uri=..., audience=...)` is the offline path.

`OidcClaimMapper(email_claim=..., roles=..., groups=...)` normalizes what it extracts under `_forze` in `VerifiedAssertion.claims` — same keys for every IdP. Extractors: `ClaimsPath("realm_access.roles")`, `ClaimsPath("resource_access.{client}.roles", client="my-api")` (Keycloak), `ClaimsPath("roles", item_key="name")` (Casdoor role objects), `ZitadelProjectRoles(org_id=...)`, `ClaimsPath("metadata.roles")` (Kratos). A malformed claim yields no roles, never an authn error.

Forze stays UUID-native: external `subject` strings become canonical UUID `principal_id`s via the chosen resolver, so domain / authz / tenancy code never sees a vendor identifier.

See [External IdP (OIDC) recipe](https://morzecrew.github.io/forze/latest/recipes/external-idp-oidc/) and [OIDC integration](https://morzecrew.github.io/forze/latest/integrations/oidc/).

## Anti-patterns

- **Storing external IdP subject strings as principal ids** — always go through a `PrincipalResolverPort` so internal identifiers stay UUID.
- **Re-validating tokens inside resolvers** — verification is the verifier's job; resolvers only translate `(issuer, subject, tenant_hint)`.
- **Building a verifier per request** — the shipped `Configurable*` factories build theirs once, so the JWKS and introspection caches span requests; a factory that constructs a new verifier on every call refetches keys or re-introspects every time.
- **Running discovery per request** — `OidcIdpPreset.from_issuer` is a startup call; an IdP outage should delay boot, not fail requests.
- **Deriving roles from Kratos `traits`** — users can edit their own traits; read roles from `metadata` (`metadata_public`).

## Reference

- [External IdP (OIDC) recipe](https://morzecrew.github.io/forze/latest/recipes/external-idp-oidc/)
- [OIDC integration](https://morzecrew.github.io/forze/latest/integrations/oidc/)
