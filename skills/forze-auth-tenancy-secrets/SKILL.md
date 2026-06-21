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

See [Authentication](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/identity/) for the full architectural rationale.

## FastAPI identity

`SecurityContextMiddleware` binds `InvocationMetadata`, `AuthnIdentity`, and `TenantIdentity` at the boundary from an `AuthnRequirement` — a tuple of ingress methods — plus a `when_multiple_credentials` policy. Use `HeaderTokenAuthn` for `Authorization`-style bearer headers, `HeaderApiKeyAuthn` for API-key headers, and `CookieTokenAuthn` for cookie-held tokens; each ingress dispatches through an `AuthnSpec`. Wire only the sources you actually accept.

```python
from forze.application.contracts.authn import AuthnSpec
from forze_fastapi.middlewares import SecurityContextMiddleware
from forze_fastapi.security import AuthnRequirement, HeaderApiKeyAuthn, HeaderTokenAuthn

authn_spec = AuthnSpec(
    name="api",
    enabled_methods=frozenset({"token", "api_key"}),
)

app.add_middleware(
    SecurityContextMiddleware,
    ctx_dep=ctx_dep,
    authn=AuthnRequirement(
        ingress=(
            HeaderTokenAuthn(authn_spec=authn_spec, header_name="Authorization"),
            HeaderApiKeyAuthn(authn_spec=authn_spec, header_name="X-API-Key"),
        ),
    ),
    when_multiple_credentials="reject",
)
```

Handlers read identity only from `ExecutionContext`. The ingress `scheme` and API-key header name are routing hints; the verifier's signature/claims (or HMAC tag) are the security boundary, not the header shape.

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
            enforce_issuer_and_audience=True,
            claim_mapper=OidcClaimMapper(tenant_claim=self.tenant_claim),
        )
```

Forze stays UUID-native: external `subject` strings become canonical UUID `principal_id`s via the chosen resolver, so domain / authz / tenancy code never sees a vendor identifier.

See [External IdP (OIDC) recipe](https://morzecrew.github.io/forze/latest/recipes/external-idp-oidc/) and [OIDC integration](https://morzecrew.github.io/forze/latest/integrations/oidc/).

## Authn document specs

`forze_identity.authn` exposes five `DocumentSpec`s (`password_account_spec`, `api_key_account_spec`, `password_invite_spec`, `session_spec`, `identity_mapping_spec`). All five are members of `AUTHN_TENANT_UNAWARE_DOCUMENT_SPEC_NAMES` and must be wired to **tenant-unaware** document stores so authentication can run before `TenantIdentity` is bound. `password_invite_spec` is only needed when you enable single-use password invites (`AuthnKernelConfig.invite_token_pepper`). `PrincipalEligibilityPort` additionally requires tenant-unaware `authz_policy_principals` (`policy_principal_spec`). User offboarding uses `PrincipalDeactivationPort`, not `deactivate_principal` alone. `MappingTableResolver` forbids cache and history on `identity_mapping_spec`.

## Authz

`forze_identity.authz` provides document-backed authorization (catalog, bindings, adapters for authz ports). `PrincipalRef` shares the `principal_id` UUID with `AuthnIdentity`, so authz bindings outlive the IdP choice.

## Tenancy and routed clients

`TenantIdentity` is the current tenant. Tenant-aware adapters derive routing from `ExecutionContext`, not from user DTO fields. Routed Postgres, Mongo, Redis, S3, RabbitMQ, SQS, Temporal, BigQuery, ClickHouse, Meilisearch, GCS, Firestore, and Inngest clients can choose per-tenant infrastructure at call time.

For database-per-tenant Postgres routing, set `PostgresDepsModule.introspector_cache_partition_key` so catalog metadata caches are partitioned per tenant/database.

`AuthnIdentity.tenant_id` is set by the resolver when the assertion carries a `tenant_hint` (e.g. JWT `tid` claim or an OIDC tenant claim). `TenantIdentityResolver` then merges credential-bound tenant id, optional header hint, and `TenantResolverPort` results.

## Isolation tiers and the declared floor

Every tenant-aware deps module reports the isolation tier its wiring reaches — `none < tagged < namespace < dedicated` (storage-agnostic names):

- `tagged` — a shared store with a tenant marker (`tenant_aware=True`): a SQL `tenant_id` column, a Redis key prefix, an object-store path prefix, a graph property.
- `namespace` — a per-tenant container on a shared instance via a dynamic resolver (schema / dataset / bucket / collection).
- `dedicated` — a routed client with per-tenant credentials and connections.

Set `required_tenant_isolation` on a module to declare a **minimum** and fail wiring closed below it — checked once at startup, never per request:

```python
PostgresDepsModule(
    client=RoutedPostgresClient(...),
    required_tenant_isolation="dedicated",
)
```

A floor the backend can never reach (e.g. `"dedicated"` on in-process DuckDB or single-client Neo4j) fails as a capability mismatch. Use it to refuse under-isolated wiring on untrusted or self-scoping query paths (raw SQL hatches, self-filtering analytics). Default `None` enforces nothing.

## Tenancy deps module

`TenancyDepsModule` (`from forze_identity.tenancy.execution import TenancyDepsModule`) registers `TenantResolverDepKey` and/or `TenantManagementDepKey` factories (`ConfigurableTenantResolver`, `ConfigurableTenantManagement`) for the route names you pass. Merge it into `DepsRegistry.from_modules` alongside Postgres/Mongo and auth modules when tenant catalog documents drive `TenantResolverPort` / `TenantManagementPort`.

```python
from forze_identity.tenancy.execution import TenancyDepsModule

TenancyDepsModule(
    tenant_resolver={"main"},
    tenant_management={"main"},
    verify_tenant_active=True,
)
```

See [Multi-tenancy](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/multi-tenancy/) for aggregates, adapters, and FastAPI `TenantIdentityResolver` pairing.

## Tenant provisioning

The `namespace` / `dedicated` tiers assume the per-tenant container already exists. `TenantProvisionerPort` creates it on onboarding and tears it down on offboarding; wire it through `TenancyDepsModule.tenant_provisioner`:

```python
from forze.application.integrations.storage import ObjectStorageTenantProvisioner
from forze_identity.tenancy.execution import TenancyDepsModule

TenancyDepsModule(
    tenant_management={"main"},
    tenant_provisioner=ObjectStorageTenantProvisioner(
        client=s3_client,
        bucket=lambda tid: f"tenant-{tid}",
    ),
)
```

`TenantManagementPort.provision_tenant(...)` records the tenant first, then runs the provisioner — idempotent, so a failure leaves the record for retry; `deprovision_tenant(tenant_id)` runs the inverse. Provisioners receive the onboarded `TenantIdentity` **explicitly** (it is not the ambient bound tenant — an admin onboards tenant X without acting as X). Compose per-integration provisioners with `CompositeTenantProvisioner`, wrap a callable with `FunctionTenantProvisioner`, or omit it (`NoopTenantProvisioner`, the default). `forze_postgres` ships `PostgresSchemaTenantProvisioner` (`CREATE SCHEMA IF NOT EXISTS`); object storage ships `ObjectStorageTenantProvisioner`.

## Secrets

`SecretsDepKey` registers a `SecretsPort`. `SecretRef` is a logical path, and `resolve_structured()` validates JSON secrets into Pydantic models.

```python
from forze.application.contracts.secrets import SecretRef, SecretsDepKey, resolve_structured

secrets = ctx.deps.provide(SecretsDepKey)
dsn = await resolve_structured(secrets, SecretRef("postgres/main"), PostgresDsnSecret)
```

### Backends

Wire one `SecretsPort` backend for the route. Bundled in `forze_kits` (no extra):

```python
from forze_kits.adapters.secrets import (
    DirectorySecrets,
    EnvSecrets,
    MappingSecrets,
    SecretsDepsModule,
)

secrets_module = SecretsDepsModule(secrets=EnvSecrets())
# DirectorySecrets(root=Path("/etc/secrets")) for file-backed secrets;
# MappingSecrets(data={...}) for in-memory development/tests.
```

For HashiCorp Vault (extra `forze[vault]`), `forze_vault` ships a KV v2 backend that registers `SecretsDepKey` for you:

```python
from forze_vault import VaultClient, VaultConfig, VaultDepsModule, vault_lifecycle_step

vault_module = VaultDepsModule(
    client=VaultClient(config=VaultConfig(url="https://vault:8200", token="...")),
)
# add vault_lifecycle_step() to your LifecyclePlan
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
9. **Declaring strong isolation but never creating the namespace** — pair per-tenant resolvers / `required_tenant_isolation` with a `TenantProvisionerPort` so onboarding provisions the schema / bucket / dataset.

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version (e.g. `.../forze/0.3/...`) or use the version selector on the site.

- [Authentication](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/identity/)
- [Authentication reference](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/identity/)
- [Authn, authz, tenancy (FastAPI) recipe](https://morzecrew.github.io/forze/latest/recipes/authn-authz-tenancy-fastapi/)
- [External IdP (OIDC) recipe](https://morzecrew.github.io/forze/latest/recipes/external-idp-oidc/)
- [OIDC integration](https://morzecrew.github.io/forze/latest/integrations/oidc/)
- [FastAPI integration](https://morzecrew.github.io/forze/latest/integrations/fastapi/)
- [Multi-tenancy](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/multi-tenancy/)
