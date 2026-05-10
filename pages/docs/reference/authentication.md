# Authentication contracts

Reference for the authentication contract group (`forze.application.contracts.authn`) and the first-party document-backed implementation in `forze_authn`. For the architectural rationale, see [Authentication pipeline](../concepts/authentication.md). For an end-to-end OIDC example, see [External IdPs over OIDC](../recipes/external-idp-oidc.md).

## Layout

| Module | Role |
|--------|------|
| `forze.application.contracts.authn` | Value objects, ports, `AuthnSpec`, dep keys (re-exports the submodules below). |
| `forze.application.contracts.authn.value_objects` | `AuthnIdentity`, `VerifiedAssertion`, `*Credentials`, `*Response`, `OAuth2Tokens`, `CredentialLifetime`. |
| `forze.application.contracts.authn.ports.authn` | `AuthnPort` orchestration facade. |
| `forze.application.contracts.authn.ports.verification` | `PasswordVerifierPort`, `TokenVerifierPort`, `ApiKeyVerifierPort`. |
| `forze.application.contracts.authn.ports.resolution` | `PrincipalResolverPort`. |
| `forze.application.contracts.authn.ports.lifecycle` | `PasswordLifecyclePort`, `TokenLifecyclePort`, `ApiKeyLifecyclePort`. |
| `forze.application.contracts.authn.ports.provisioning` | `PasswordAccountProvisioningPort`. |
| `forze.application.contracts.authn.specs` | `AuthnSpec`, `AuthnMethod`. |
| `forze.application.contracts.authn.deps` | All dep keys (`AuthnDepKey`, verifier keys, resolver key, lifecycle keys, provisioning key). |
| `forze_authn` | First-party orchestrator, verifiers, resolvers, configurable factories, `AuthnDepsModule`, `AuthnKernelConfig`, document specs. |
| `forze_oidc` | Generic OIDC `TokenVerifierPort`, JWKS / claim helpers (separate package). |

All names continue to be importable from the package root (`from forze.application.contracts.authn import ...`); the submodule layout is informational.

## Value objects

### AuthnIdentity

Canonical authenticated subject inside Forze; everything downstream of the boundary sees only this minimal shape.

| Field | Type | Purpose |
|-------|------|---------|
| `principal_id` | `UUID` | Internal principal identifier; aligns with `forze.application.contracts.authz.PrincipalRef`. |
| `tenant_id` | `UUID \| None` | When set, the credential or token was bound to this tenant scope. |

### VerifiedAssertion

Vendor-flavored proof produced by a `*VerifierPort` and consumed by a `PrincipalResolverPort`. Single seam between verification and resolution.

| Field | Type | Purpose |
|-------|------|---------|
| `issuer` | `str` | Stable identifier of the authority (e.g. `"forze:jwt"`, OIDC `iss` URL). |
| `subject` | `str` | Raw external subject identifier (string form). |
| `audience` | `str \| None` | Optional `aud` value the assertion is bound to. |
| `tenant_hint` | `str \| None` | Raw tenant identifier as provided by the issuer. |
| `issued_at` / `expires_at` | `datetime \| None` | Optional timestamps. |
| `claims` | `Mapping[str, Any]` | Opaque claim snapshot for resolvers and audit trails. |

### Credentials

Raw credential value objects accepted by the orchestrator:

| Type | Required fields | Optional hint fields |
|------|------------------|----------------------|
| `PasswordCredentials` | `login`, `password` | — |
| `TokenCredentials` | `token` | `scheme`, `kind`, `profile` (routing hints; verifiers decide whether to consult them) |
| `ApiKeyCredentials` | `key` | `prefix` |

### Token responses

| Type | Purpose |
|------|---------|
| `ApiKeyResponse` | Issued API key (`key`, optional `key_id`, optional `lifetime`). |
| `TokenResponse` | Single issued token (`token`, optional `lifetime`). |
| `OAuth2Tokens` | Optional `access_token` + optional `refresh_token` (refresh-only flows omit access). |
| `OAuth2TokensResponse` | `access_token: TokenResponse` + optional `refresh_token: TokenResponse`. |
| `CredentialLifetime` | Optional `expires_in`, `expires_at`, `issued_at`. |

## Ports

### AuthnPort

Orchestration facade; one method per credential family. Default implementation: `forze_authn.AuthnOrchestrator`.

| Method | Returns |
|--------|---------|
| `authenticate_with_password(PasswordCredentials)` | `AuthnIdentity` |
| `authenticate_with_token(TokenCredentials)` | `AuthnIdentity` |
| `authenticate_with_api_key(ApiKeyCredentials)` | `AuthnIdentity` |

Invoking a method whose family is not in `AuthnSpec.enabled_methods` raises `AuthenticationError(code="method_disabled")`.

### Verifier ports

Each verifier proves the credential against its issuer and emits a `VerifiedAssertion`.

| Port | Method | Returns |
|------|--------|---------|
| `PasswordVerifierPort` | `verify_password(PasswordCredentials)` | `VerifiedAssertion` |
| `TokenVerifierPort` | `verify_token(TokenCredentials)` | `VerifiedAssertion` |
| `ApiKeyVerifierPort` | `verify_api_key(ApiKeyCredentials)` | `VerifiedAssertion` |

### PrincipalResolverPort

Maps a `VerifiedAssertion` to a canonical `AuthnIdentity`.

| Method | Returns |
|--------|---------|
| `resolve(VerifiedAssertion)` | `AuthnIdentity` |

### Lifecycle ports

| Port | Methods |
|------|---------|
| `PasswordLifecyclePort` | `change_password(identity, new_password)` |
| `TokenLifecyclePort` | `issue_tokens(identity)`, `refresh_tokens(OAuth2Tokens)`, `revoke_tokens(identity)` |
| `ApiKeyLifecyclePort` | `issue_api_key(identity)`, `refresh_api_key(ApiKeyCredentials)`, `revoke_api_key(key_id)`, `revoke_many_api_keys(key_ids)` |

### PasswordAccountProvisioningPort

| Method | Purpose |
|--------|---------|
| `register_with_password(principal_id, credentials)` | Self-service registration. |
| `provision_password_account(operator, principal_id, credentials)` | Operator-driven provisioning. |
| `accept_invite_with_password(invite, principal_id, credentials)` | Token-bound invite redemption. |

## Spec

### AuthnSpec

```python
from forze.application.contracts.authn import AuthnSpec

api_authn = AuthnSpec(
    name="api",
    enabled_methods=frozenset({"token", "api_key"}),
    token_profile="oidc",
    resolver_profile="mapping",
)
```

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | Logical route name (matches the `AuthnDepsModule.authn` key). |
| `enabled_methods` | `frozenset[AuthnMethod]` | `frozenset({"token"})` | Credential families this route accepts. Must be non-empty. |
| `token_profile` | `str \| None` | `None` | Selects a registered `TokenVerifierPort` when more than one is wired. |
| `password_profile` | `str \| None` | `None` | Selects a registered `PasswordVerifierPort`. |
| `api_key_profile` | `str \| None` | `None` | Selects a registered `ApiKeyVerifierPort`. |
| `resolver_profile` | `str \| None` | `None` | Selects a registered `PrincipalResolverPort`. |

`AuthnMethod` is `Literal["password", "token", "api_key"]`.

## Dependency keys

All dep keys live in `forze.application.contracts.authn` (re-exported from `.deps`).

| Key | Resolves to | Notes |
|-----|-------------|-------|
| `AuthnDepKey` | `AuthnPort` | Orchestration facade; one factory per route. |
| `PasswordVerifierDepKey` | `PasswordVerifierPort` | One factory per route (or per profile when overrides are wired). |
| `TokenVerifierDepKey` | `TokenVerifierPort` | One factory per route; the seam external IdPs hook into. |
| `ApiKeyVerifierDepKey` | `ApiKeyVerifierPort` | One factory per route. |
| `PrincipalResolverDepKey` | `PrincipalResolverPort` | One factory per route; default is `JwtNativeUuidResolver`. |
| `PasswordLifecycleDepKey` | `PasswordLifecyclePort` | Optional; only registered when `password_lifecycle` routes are listed. |
| `TokenLifecycleDepKey` | `TokenLifecyclePort` | Required for `attach_oauth2_password_token_template_routes`. |
| `ApiKeyLifecycleDepKey` | `ApiKeyLifecyclePort` | Optional; for API-key issuance/revocation flows. |
| `PasswordAccountProvisioningDepKey` | `PasswordAccountProvisioningPort` | Optional; for self-service or operator-driven account creation. |

Resolve from `ExecutionContext` with `ctx.dep(KEY, route=spec.name)(ctx, spec)`.

## forze_authn surface

### AuthnOrchestrator

Default `AuthnPort` implementation. Composes per-method verifiers + a single resolver, gates each `authenticate_with_*` call by `enabled_methods`.

| Field | Type | Purpose |
|-------|------|---------|
| `resolver` | `PrincipalResolverPort` | Used by all enabled credential families on this route. |
| `enabled_methods` | `frozenset[str]` | Snapshot of `AuthnSpec.enabled_methods`. |
| `password_verifier` / `token_verifier` / `api_key_verifier` | `*VerifierPort \| None` | Required when the matching method is in `enabled_methods`. |

Convenience factory: `AuthnOrchestrator.from_spec(spec, *, resolver, password_verifier=None, ...)`.

### Verifiers

| Verifier | Issuer label | Notes |
|----------|--------------|-------|
| `Argon2PasswordVerifier` | `ISSUER_FORZE_PASSWORD` (`"forze:password_account"`) | Verifies against `password_account_spec`; emits the principal id as `subject`. |
| `ForzeJwtTokenVerifier` | Verified `iss` claim (`ISSUER_FORZE_JWT` for first-party tokens) | Treats `scheme` / `kind` as routing hints only; signature/claims are the security boundary. |
| `HmacApiKeyVerifier` | `ISSUER_FORZE_API_KEY` (`"forze:api_key"`) | Verifies against `api_key_account_spec`. |

### Resolvers

| Resolver | Storage | Behavior |
|----------|---------|----------|
| `JwtNativeUuidResolver` | None | Trusts `assertion.subject` as a UUID; `tenant_hint` as a UUID when present. |
| `DeterministicUuidResolver` | None | `principal_id = uuid4({"iss": issuer, "sub": subject})` via `forze.base.primitives.uuid4`. Helper: `derive_principal_id(issuer, subject)`. |
| `MappingTableResolver` | `IdentityMapping` document | Looks up `(issuer, subject)`; optional just-in-time provisioning when `provision_on_first_sight=True` and a command port is supplied. |

### IdentityMapping document model

| Field | Type | Notes |
|-------|------|-------|
| `issuer` | `String` | Frozen. |
| `subject` | `String` | Frozen. |
| `principal_id` | `UUID` | Frozen; the internal Forze principal id. |

Document spec: `identity_mapping_spec` (resource name `authn_identity_mappings`). The spec is included in `AUTHN_TENANT_UNAWARE_DOCUMENT_SPEC_NAMES` and must use a tenant-unaware document store (caching and history are forbidden by `MappingTableResolver.__attrs_post_init__`).

### AuthnKernelConfig

Secrets and service tuning shared across all authn routes for one `AuthnDepsModule` instance.

| Field | Required for | Constraint |
|-------|--------------|------------|
| `access_token_secret` | `"token"` method, token lifecycle | min 32 bytes |
| `refresh_token_pepper` | token lifecycle | min 32 bytes |
| `password` | `"password"` method, password lifecycle, password provisioning | `PasswordConfig` |
| `api_key_pepper` | `"api_key"` method, API-key lifecycle | min 32 bytes |

Each section also has a tuning sub-config (`AccessTokenConfig`, `RefreshTokenConfig`, `ApiKeyConfig`). `build_authn_shared_services(kernel)` produces `AuthnSharedServices`; `validate_route_methods` / `validate_shared_matches_route_sets` enforce that enabled methods have matching kernel sections.

### AuthnDepsModule

Registers the full authn stack for one or more routes.

| Field | Type | Purpose |
|-------|------|---------|
| `kernel` | `AuthnKernelConfig \| None` | Required when any route registration is non-empty. |
| `authn` | `Mapping[K, frozenset[AuthnMethod]] \| None` | Per-route enabled credential families. |
| `resolvers` | `Mapping[K, PrincipalResolverDepPort] \| None` | Per-route resolver overrides; default per route is `JwtNativeUuidResolver`. |
| `token_verifiers` / `password_verifiers` / `api_key_verifiers` | `Mapping[K, BaseDepPort] \| None` | Per-route verifier overrides; defaults are the first-party verifiers above. |
| `token_lifecycle` / `password_lifecycle` / `api_key_lifecycle` | `Collection[K] \| None` | Routes that should expose the matching lifecycle port. |
| `password_account_provisioning` | `Collection[K] \| None` | Routes that should expose `PasswordAccountProvisioningPort`. |

The module emits a routed `Deps[K]`; merge with other modules' output before constructing `ExecutionRuntime`.

### Configurable factories

Each `Configurable*` class is the dep factory shape for one component. Lifecycle and provisioning factories take `shared: AuthnSharedServices`; verifier factories take `shared`; resolver factories take none (`JwtNativeUuidResolver`, `DeterministicUuidResolver`) or just a flag (`MappingTableResolver` accepts `provision_on_first_sight`).

| Factory | Output |
|---------|--------|
| `ConfigurableArgon2PasswordVerifier` | `Argon2PasswordVerifier` |
| `ConfigurableForzeJwtTokenVerifier` | `ForzeJwtTokenVerifier` |
| `ConfigurableHmacApiKeyVerifier` | `HmacApiKeyVerifier` |
| `ConfigurableJwtNativeUuidResolver` | `JwtNativeUuidResolver` |
| `ConfigurableDeterministicUuidResolver` | `DeterministicUuidResolver` |
| `ConfigurableMappingTableResolver` | `MappingTableResolver` |
| `ConfigurableAuthn` | `AuthnOrchestrator` (composed via dep keys) |
| `ConfigurableTokenLifecycle` | `TokenLifecycleAdapter` |
| `ConfigurablePasswordLifecycle` | `PasswordLifecycleAdapter` |
| `ConfigurableApiKeyLifecycle` | `ApiKeyLifecycleAdapter` |
| `ConfigurablePasswordAccountProvisioning` | `PasswordAccountProvisioningAdapter` |

### Document specs

| Spec | Resource name | Purpose |
|------|---------------|---------|
| `principal_spec` | `authn_principals` | Principal directory (read-only at the spec level). |
| `password_account_spec` | `authn_password_accounts` | Password-account aggregate. |
| `api_key_account_spec` | `authn_api_key_accounts` | API-key-account aggregate. |
| `session_spec` | `authn_token_sessions` | Refresh-session storage for token lifecycle. |
| `identity_mapping_spec` | `authn_identity_mappings` | `(issuer, subject) -> principal_id` mappings used by `MappingTableResolver`. |

All five names are members of `AUTHN_TENANT_UNAWARE_DOCUMENT_SPEC_NAMES` (see [Multi-tenancy](../concepts/multi-tenancy.md)) and must be wired to **tenant-unaware** document stores so authentication can run before `TenantIdentity` is bound.

### Issuer constants

Stable issuer labels for first-party `VerifiedAssertion` outputs:

| Constant | Value |
|----------|-------|
| `ISSUER_FORZE_JWT` | `"forze:jwt"` |
| `ISSUER_FORZE_PASSWORD` | `"forze:password_account"` |
| `ISSUER_FORZE_API_KEY` | `"forze:api_key"` |

External IdP integrations expose their own issuer labels (typically the verified `iss` claim).

## forze_oidc surface

See [Integration: OIDC](../integrations/oidc.md) for the wiring guide.

| Component | Purpose |
|-----------|---------|
| `OidcTokenVerifier` | Generic OIDC `TokenVerifierPort` (RS256/ES256/HS256). Pluggable key provider and claim mapper. |
| `OidcClaimMapper` | Maps a verified claim payload onto `VerifiedAssertion`; configurable claim names. |
| `SigningKeyProviderPort` | Resolves a signing key for a JWT (typically by `kid`). |
| `JwksKeyProvider` | Production provider; fetches and caches JWKS via `jwt.PyJWKClient`. |
| `StaticKeyProvider` | Returns a single key for every token (HS256 / single-tenant / tests). |

## Cross-links

- [Concept — Authentication pipeline](../concepts/authentication.md)
- [Recipe — Authn, authz, and tenancy with FastAPI](../recipes/authn-authz-tenancy-fastapi.md)
- [Recipe — External IdPs over OIDC](../recipes/external-idp-oidc.md)
- [Integration — OIDC](../integrations/oidc.md)
- [Concept — Multi-tenancy](../concepts/multi-tenancy.md)
