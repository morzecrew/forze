# External IdPs (OIDC)

Delegating authentication to an external issuer: token verifiers, claim mapping, and resolving an external subject to an internal principal. The pipeline it plugs into is [authentication](authn.md).

## External IdPs (forze_identity.oidc)

`forze_identity.oidc` (extra `forze[oidc]`) provides `OidcTokenVerifier`, `JwksKeyProvider`, `StaticKeyProvider`, and `OidcClaimMapper`. Wrap the verifier in a routed factory and register it under `TokenVerifierDepKey` for the relevant routes; pair with `MappingTableResolver` (production SSO) or `DeterministicUuidResolver` (stateless prototyping).

```python
from forze.application.contracts.authn import AuthnSpec, TokenVerifierPort
from forze.application.execution import ExecutionContext
from forze_identity.oidc import JwksKeyProvider, OidcClaimMapper, OidcTokenVerifier


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableOidcTokenVerifier:
    key_provider: JwksKeyProvider
    audience: str
    issuer: str
    tenant_claim: str | None = None

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> TokenVerifierPort:
        _ = ctx, spec
        return OidcTokenVerifier(
            key_provider=self.key_provider,
            algorithms=("RS256",),
            audience=self.audience,
            issuer=self.issuer,
            enforce_issuer_and_audience=True,
            claim_mapper=OidcClaimMapper(tenant_claim=self.tenant_claim),
        )
```

Forze stays UUID-native: external `subject` strings become canonical UUID `principal_id`s via the chosen resolver, so domain / authz / tenancy code never sees a vendor identifier.

See [External IdP (OIDC) recipe](https://morzecrew.github.io/forze/latest/recipes/external-idp-oidc/) and [OIDC integration](https://morzecrew.github.io/forze/latest/integrations/oidc/).

## Anti-patterns

- **Storing external IdP subject strings as principal ids** — always go through a `PrincipalResolverPort` so internal identifiers stay UUID.
- **Re-validating tokens inside resolvers** — verification is the verifier's job; resolvers only translate `(issuer, subject, tenant_hint)`.

## Reference

- [External IdP (OIDC) recipe](https://morzecrew.github.io/forze/latest/recipes/external-idp-oidc/)
- [OIDC integration](https://morzecrew.github.io/forze/latest/integrations/oidc/)
