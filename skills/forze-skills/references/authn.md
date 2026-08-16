# Authentication

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

External IdPs plug in via `TokenVerifierPort` and reuse a Forze resolver — `forze_identity.oidc` covers any OIDC-compliant IdP (Google, Firebase Auth, Casdoor, …); handlers stay on existing authn ports.

See [Authentication](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/identity/) for the full architectural rationale.

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

## Authn document specs

`forze_identity.authn` exposes five `DocumentSpec`s (`password_account_spec`, `api_key_account_spec`, `password_invite_spec`, `session_spec`, `identity_mapping_spec`). All five are members of `AUTHN_TENANT_UNAWARE_DOCUMENT_SPEC_NAMES` and must be wired to **tenant-unaware** document stores so authentication can run before `TenantIdentity` is bound. `password_invite_spec` is only needed when you enable single-use password invites (`AuthnKernelConfig.invite_token_pepper`). `PrincipalEligibilityPort` additionally requires tenant-unaware `authz_policy_principals` (`policy_principal_spec`). User offboarding uses `PrincipalDeactivationPort`, not `deactivate_principal` alone. `MappingTableResolver` forbids cache and history on `identity_mapping_spec`.

## Authz

`forze_identity.authz` provides document-backed authorization (catalog, bindings, adapters for authz ports). `PrincipalRef` shares the `principal_id` UUID with `AuthnIdentity`, so authz bindings outlive the IdP choice.

## Anti-patterns

- **Binding identity inside handlers** — bind at the boundary only.
- **Treating authz as domain-only state** — use authz ports for policy decisions that depend on external grants.
- **Forgetting authn document specs need storage wiring** — `forze_identity.authn` and `forze_identity.authz` specs are still `DocumentSpec`s; `identity_mapping_spec` must allow neither cache nor history.
- **Using `AccessTokenCredentials.scheme` / `profile` as a security gate** — they are routing hints; the verifier's signature/claim checks are the boundary.

## Reference

- [Authentication](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/identity/)
