# External IdPs over OIDC

Use this recipe when an external identity provider (Casdoor, Firebase Auth, Auth0, internal SSO with a JWKS endpoint, etc.) issues access tokens and Forze should accept them without leaking vendor specifics into the application or domain layer.

## Ingredients

- The [authentication pipeline](../concepts/authentication.md) (verify-then-resolve seam).
- `forze_authn` for `AuthnDepsModule`, `AuthnKernelConfig`, the document specs, and the resolver factories.
- `forze_oidc` for the generic OIDC `TokenVerifierPort` (`pip install forze[oidc]`).
- A document storage integration that exposes `identity_mapping_spec` (any `forze` document adapter; the spec must be wired to a tenant-unaware store).
- A FastAPI app with `ContextBindingMiddleware` (or any other boundary that resolves credentials).

## Steps

1. Choose a route name (e.g. `"api"`) and decide which credential families it accepts via `AuthnSpec.enabled_methods`.
2. Configure `AuthnKernelConfig`. For an OIDC-only route you do not need an access-token secret on the kernel — the kernel section is required only when the *first-party* `ForzeJwtTokenVerifier` would be used.
3. Build a configurable factory wrapping `OidcTokenVerifier` and a `JwksKeyProvider` per IdP issuer.
4. Pick a `PrincipalResolverPort`: `MappingTableResolver` for production SSO, `DeterministicUuidResolver` for stateless prototyping.
5. Pass the verifier and resolver overrides into `AuthnDepsModule.token_verifiers` / `AuthnDepsModule.resolvers`.
6. Wire `HeaderAuthnIdentityResolver` on the FastAPI middleware with the matching `AuthnSpec`.

## Configurable factories

Wrap `OidcTokenVerifier` and the resolver in `Configurable*` factories the same way `forze_authn` does:

```python
from typing import final

import attrs

from forze.application.contracts.authn import AuthnSpec, TokenVerifierPort
from forze.application.execution import ExecutionContext
from forze_oidc import JwksKeyProvider, OidcClaimMapper, OidcTokenVerifier


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableOidcTokenVerifier:
    """Build an OidcTokenVerifier from a shared JWKS provider."""

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
            claim_mapper=OidcClaimMapper(tenant_claim=self.tenant_claim),
        )
```

`forze_authn` already ships `ConfigurableMappingTableResolver` and `ConfigurableDeterministicUuidResolver`; reuse them directly.

## Module wiring

```python
from forze.application.execution import DepsPlan
from forze_authn import (
    AuthnDepsModule,
    AuthnKernelConfig,
    ConfigurableMappingTableResolver,
)
from forze_oidc import JwksKeyProvider

ROUTE = "api"

jwks = JwksKeyProvider(jwks_uri="https://idp.example.com/.well-known/jwks.json")

oidc_verifier = ConfigurableOidcTokenVerifier(
    key_provider=jwks,
    audience="my-api",
    issuer="https://idp.example.com/",
    tenant_claim=None,
)

authn_module = AuthnDepsModule(
    kernel=AuthnKernelConfig(),
    authn={ROUTE: frozenset({"token"})},
    token_verifiers={ROUTE: oidc_verifier},
    resolvers={ROUTE: ConfigurableMappingTableResolver(provision_on_first_sight=True)},
)

deps = DepsPlan.from_modules(authn_module)
```

`MappingTableResolver(provision_on_first_sight=True)` mints a fresh `UUID` the first time it sees an `(issuer, subject)` pair and persists the mapping in `identity_mapping_spec`. Use `provision_on_first_sight=False` (default) when principals are pre-provisioned and unknown subjects must be rejected.

## Document storage for identity mappings

`identity_mapping_spec` (resource name `authn_identity_mappings`) must be registered with a **tenant-unaware** document store, just like the other authn aggregates listed in `AUTHN_TENANT_UNAWARE_DOCUMENT_SPEC_NAMES`. Caching and history on this spec are forbidden for security reasons (`MappingTableResolver` checks both at construction time). See [Multi-tenancy](../concepts/multi-tenancy.md) for tenant-unaware document wiring patterns.

## FastAPI boundary

```python
from forze.application.contracts.authn import AuthnSpec
from forze_fastapi.middlewares.context import (
    ContextBindingMiddleware,
    HeaderAuthnIdentityResolver,
)

api_authn = AuthnSpec(
    name="api",
    enabled_methods=frozenset({"token"}),
)

app.add_middleware(
    ContextBindingMiddleware,
    ctx_dep=get_ctx,
    authn_identity_resolver=HeaderAuthnIdentityResolver(
        spec=api_authn,
        when_multiple_credentials="reject",
    ),
)
```

The header resolver forwards `Authorization: Bearer <jwt>` into `TokenCredentials`. `OidcTokenVerifier` ignores `scheme` / `kind` (they are routing hints) and goes straight to signature/claims validation.

## Multi-IdP topology

A single `AuthnDepsModule` can host more than one route, each with its own verifier+resolver pair:

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/authn-multi-idp-routes.svg" alt="One AuthnDepsModule serving an internal route with first-party JWT plus JwtNativeUuidResolver and an external route with OidcTokenVerifier plus MappingTableResolver">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/authn-multi-idp-routes.svg" alt="One AuthnDepsModule serving an internal route with first-party JWT plus JwtNativeUuidResolver and an external route with OidcTokenVerifier plus MappingTableResolver">
</div>

```python
authn_module = AuthnDepsModule(
    kernel=AuthnKernelConfig(
        access_token_secret=internal_secret,
    ),
    authn={
        "internal": frozenset({"token", "password"}),
        "api": frozenset({"token"}),
    },
    token_verifiers={"api": oidc_verifier},
    resolvers={"api": ConfigurableMappingTableResolver(provision_on_first_sight=True)},
)
```

Routes without an entry in `token_verifiers` / `resolvers` fall back to the first-party defaults (`ForzeJwtTokenVerifier` + `JwtNativeUuidResolver`).

## Trade-offs

| Choice | Best when | Trade-off |
|--------|-----------|-----------|
| `MappingTableResolver(provision_on_first_sight=True)` | SSO with admin overrides or future account merging. | One DB write on first sight per principal. |
| `MappingTableResolver(provision_on_first_sight=False)` | Invitation-only, tightly controlled access. | Requires out-of-band provisioning before first login. |
| `DeterministicUuidResolver` | Read-only deployments, prototyping, or environments without writable storage. | No row to attach admin metadata, audit, or future account merges to. |
| Multiple `TokenVerifierPort` per route via profiles | Same route accepts both first-party and external tokens. | Requires explicit `TokenCredentials.profile` (or `AuthnSpec.token_profile`) routing. |

## Learn more

- [Concept — Authentication pipeline](../concepts/authentication.md)
- [Reference — Authentication contracts](../reference/authentication.md)
- [Integration — OIDC](../integrations/oidc.md)
- [Recipe — Authn, authz, and tenancy with FastAPI](authn-authz-tenancy-fastapi.md)
- [Concept — Multi-tenancy](../concepts/multi-tenancy.md)
