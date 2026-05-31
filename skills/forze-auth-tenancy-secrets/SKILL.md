---
name: forze-auth-tenancy-secrets
description: >-
  Uses Forze authn/authz, tenancy, call context, secrets, routed clients, and
  FastAPI/worker identity binding. Use when adding authentication,
  authorization, tenant-aware infrastructure, secret-backed configuration, or
  external IdP integrations such as OIDC, Casdoor, or Firebase Auth.
---

# Forze auth, tenancy, and secrets

Use when identity, tenant routing, authorization, secret resolution, or external IdP integration affects application behavior. Keep binding at the boundary; handlers read context and resolve ports.

## Boundary binding

`ExecutionContext` stores call, authn, and tenancy state in context variables. Bind them in HTTP middleware, Socket.IO adapters, queue workers, or Temporal interceptors.

```python
from forze.application.execution import InvocationMetadata

metadata = InvocationMetadata(
    execution_id=execution_id,
    correlation_id=correlation_id,
)
with ctx.inv_ctx.bind(metadata=metadata, authn=authn_identity, tenant=tenant_identity):
    await handler(args)
```

Handlers call `ctx.inv_ctx.get_authn()` / `ctx.inv_ctx.get_tenant()` and never call `inv_ctx.bind(...)` themselves.

## Verify-then-resolve pipeline

Authentication is split into two seams:

1. **Verifier** (`PasswordVerifierPort`, `TokenVerifierPort`, `ApiKeyVerifierPort`) — vendor-specific; proves the credential and emits a `VerifiedAssertion(issuer, subject, aud, tenant_hint, claims)`.
2. **Resolver** (`PrincipalResolverPort`) — vendor-agnostic; turns the assertion into a canonical `AuthnIdentity(principal_id: UUID, tenant_id: UUID | None)`.

`AuthnPort` (`AuthnOrchestrator` from `forze_identity.authn`) composes them per credential family and gates each `authenticate_with_*` call by `AuthnSpec.enabled_methods`.

`forze_identity.authn` ships:

- Verifiers — `Argon2PasswordVerifier`, `ForzeJwtTokenVerifier`, `HmacApiKeyVerifier`.
- Resolvers — `JwtNativeUuidResolver` (subject is already a UUID), `DeterministicUuidResolver` (`uuid4({"iss": ..., "sub": ...})`), `MappingTableResolver` (document-backed registry with optional just-in-time provisioning).
- `AuthnOrchestrator` and the `Configurable*` factories that compose them through the dep keys.

External IdPs (`forze_identity.oidc`, `forze_firebase_auth`, …) plug in via `TokenVerifierPort` and reuse a Forze resolver — install the matching identity extra; handlers stay on existing authn ports.

See [Authentication](https://morzecrew.github.io/forze/docs/concepts/authentication/) for the full architectural rationale.

## FastAPI identity

`ContextBindingMiddleware` accepts a `Sequence` of single-source resolvers (`authn_identity_resolvers`) plus a `when_multiple_credentials` policy. Use `HeaderTokenAuthnIdentityResolver` for `Authorization`-style bearer headers, `HeaderApiKeyAuthnIdentityResolver` for API-key headers, and `CookieTokenAuthnIdentityResolver` for cookie-held tokens — wire only the sources you actually accept.

```python
from forze.application.contracts.authn import AuthnSpec
from forze_fastapi.middlewares.context import (
    ContextBindingMiddleware,
    HeaderApiKeyAuthnIdentityResolver,
    HeaderTokenAuthnIdentityResolver,
)

authn_spec = AuthnSpec(
    name="api",
    enabled_methods=frozenset({"token", "api_key"}),
)

app.add_middleware(
    ContextBindingMiddleware,
    ctx_dep=ctx_dep,
    authn_identity_resolvers=(
        HeaderTokenAuthnIdentityResolver(spec=authn_spec),
        HeaderApiKeyAuthnIdentityResolver(spec=authn_spec),
    ),
    when_multiple_credentials="reject",
)
```

The resolvers forward `scheme` and API-key `prefix` as routing hints; the verifier's signature/claims (or HMAC tag) are the security boundary, not the header shape.

## Authn dep keys

| Key | Resolves to | Notes |
|-----|-------------|-------|
| `AuthnDepKey` | `AuthnPort` (`AuthnOrchestrator`) | Composed from the four keys below + spec. |
| `PasswordVerifierDepKey` / `TokenVerifierDepKey` / `ApiKeyVerifierDepKey` | `*VerifierPort` | One factory per route or per profile. The seam external IdPs hook into. |
| `PrincipalResolverDepKey` | `PrincipalResolverPort` | Default per route is `JwtNativeUuidResolver`; override via `AuthnDepsModule.resolvers`. |
| `PasswordLifecycleDepKey` / `TokenLifecycleDepKey` / `ApiKeyLifecycleDepKey` | `*LifecyclePort` | Lifecycle ports live under `forze.application.contracts.authn.ports.lifecycle`; re-exported from the package root. Required for the OAuth2 token template routes. |
| `PasswordAccountProvisioningDepKey` | `PasswordAccountProvisioningPort` | Lives under `forze.application.contracts.authn.ports.provisioning`. |

```python
from forze.application.contracts.authn import AuthnDepKey, PasswordCredentials

authn = ctx.deps.resolve_configurable(
    ctx, AuthnDepKey, authn_spec, route=authn_spec.name
)
identity = await authn.authenticate_with_password(
    PasswordCredentials(login=email, password=password)
)
```

## AuthnDepsModule wiring

```python
from forze_identity.authn import (
    AuthnDepsModule,
    AuthnKernelConfig,
    ConfigurableMappingTableResolver,
)

authn_module = AuthnDepsModule(
    kernel=AuthnKernelConfig(
        access_token_secret=internal_secret,
        refresh_token_pepper=refresh_pepper,
        password=password_config,
    ),
    authn={
        "internal": frozenset({"token", "password"}),
        "api": frozenset({"token"}),
    },
    token_verifiers={"api": ConfigurableOidcTokenVerifier(...)},
    resolvers={"api": ConfigurableMappingTableResolver(provision_on_first_sight=True)},
    token_lifecycle={"internal"},
    password_lifecycle={"internal"},
    password_account_provisioning={"internal"},
)
```

Routes without verifier/resolver overrides fall back to the first-party defaults (`ForzeJwtTokenVerifier` + `JwtNativeUuidResolver`). Lifecycle / provisioning sets are independent of `authn` and may be empty.

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
            claim_mapper=OidcClaimMapper(tenant_claim=self.tenant_claim),
        )
```

Forze stays UUID-native: external `subject` strings become canonical UUID `principal_id`s via the chosen resolver, so domain / authz / tenancy code never sees a vendor identifier.

See [External IdP (OIDC) recipe](https://morzecrew.github.io/forze/docs/recipes/external-idp-oidc/) and [OIDC integration](https://morzecrew.github.io/forze/docs/integrations/oidc/).

## Authn document specs

`forze_identity.authn` exposes four `DocumentSpec`s (`password_account_spec`, `api_key_account_spec`, `session_spec`, `identity_mapping_spec`). All four are members of `AUTHN_TENANT_UNAWARE_DOCUMENT_SPEC_NAMES` and must be wired to **tenant-unaware** document stores so authentication can run before `TenantIdentity` is bound. `PrincipalEligibilityPort` additionally requires tenant-unaware `authz_policy_principals` (`policy_principal_spec`). User offboarding uses `PrincipalDeactivationPort`, not `deactivate_principal` alone. `MappingTableResolver` forbids cache and history on `identity_mapping_spec`.

## Authz

`forze_identity.authz` provides document-backed authorization (catalog, bindings, adapters for authz ports). `PrincipalRef` shares the `principal_id` UUID with `AuthnIdentity`, so authz bindings outlive the IdP choice.

## Tenancy and routed clients

`TenantIdentity` is the current tenant. Tenant-aware adapters derive routing from `ExecutionContext`, not from user DTO fields. Routed Postgres, Mongo, Redis, S3, RabbitMQ, SQS, Temporal, BigQuery, ClickHouse, Meilisearch, GCS, Firestore, and Inngest clients can choose per-tenant infrastructure at call time.

For database-per-tenant Postgres routing, set `PostgresDepsModule.introspector_cache_partition_key` so catalog metadata caches are partitioned per tenant/database.

`AuthnIdentity.tenant_id` is set by the resolver when the assertion carries a `tenant_hint` (e.g. JWT `tid` claim or an OIDC tenant claim). `TenantIdentityResolver` then merges credential-bound tenant id, optional header hint, and `TenantResolverPort` results.

## Tenancy deps module

`TenancyDepsModule` (`from forze_identity.tenancy.execution import TenancyDepsModule`) registers `TenantResolverDepKey` and/or `TenantManagementDepKey` factories (`ConfigurableTenantResolver`, `ConfigurableTenantManagement`) for the route names you pass. Merge it into `DepsPlan.from_modules` alongside Postgres/Mongo and auth modules when tenant catalog documents drive `TenantResolverPort` / `TenantManagementPort`.

```python
from forze_identity.tenancy.execution import TenancyDepsModule

TenancyDepsModule(
    tenant_resolver={"main"},
    tenant_management={"main"},
    verify_tenant_active=True,
)
```

See [Multi-tenancy](https://morzecrew.github.io/forze/docs/concepts/multi-tenancy/) for aggregates, adapters, and FastAPI `TenantIdentityResolver` pairing.

## Secrets

`SecretsDepKey` registers a `SecretsPort`. `SecretRef` is a logical path, and `resolve_structured()` validates JSON secrets into Pydantic models.

```python
from forze.application.contracts.secrets import SecretRef, SecretsDepKey, resolve_structured

secrets = ctx.deps.provide(SecretsDepKey)
dsn = await resolve_structured(secrets, SecretRef("postgres/main"), PostgresDsnSecret)
```

Use secrets for credentials and routed client configuration; avoid putting secret values in specs.

## Anti-patterns

1. **Binding identity inside handlers** — bind at the boundary only.
2. **Passing tenant ids through every DTO for routing** — bind `TenantIdentity` and use tenant-aware adapters.
3. **Hard-coding credentials in deps modules** — resolve via secrets/config.
4. **Treating authz as domain-only state** — use authz ports for policy decisions that depend on external grants.
5. **Forgetting authn document specs need storage wiring** — `forze_identity.authn` and `forze_identity.authz` specs are still `DocumentSpec`s; `identity_mapping_spec` must allow neither cache nor history.
6. **Storing external IdP subject strings as principal ids** — always go through a `PrincipalResolverPort` so internal identifiers stay UUID.
7. **Re-validating tokens inside resolvers** — verification is the verifier's job; resolvers only translate `(issuer, subject, tenant_hint)`.
8. **Using `AccessTokenCredentials.scheme` / `profile` as a security gate** — they are routing hints; the verifier's signature/claim checks are the boundary.

## Reference

- [Authentication](https://morzecrew.github.io/forze/docs/concepts/authentication/)
- [Authentication reference](https://morzecrew.github.io/forze/docs/reference/authentication/)
- [Authn, authz, tenancy (FastAPI) recipe](https://morzecrew.github.io/forze/docs/recipes/authn-authz-tenancy-fastapi/)
- [External IdP (OIDC) recipe](https://morzecrew.github.io/forze/docs/recipes/external-idp-oidc/)
- [OIDC integration](https://morzecrew.github.io/forze/docs/integrations/oidc/)
- [FastAPI integration](https://morzecrew.github.io/forze/docs/integrations/fastapi/)
- [Multi-tenancy](https://morzecrew.github.io/forze/docs/concepts/multi-tenancy/)
