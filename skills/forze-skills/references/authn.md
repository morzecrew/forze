# Authentication

Authenticating a request at the boundary and turning a credential into a principal: the verify-then-resolve pipeline, the dep keys it uses, how to wire it, and the authorization decision that follows. Binding it to FastAPI is [FastAPI identity](fastapi-identity.md); an external issuer is [OIDC](oidc.md).

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

External IdPs plug in via `TokenVerifierPort` and reuse a Forze resolver — `forze_identity.oidc` covers any OIDC-compliant IdP (Google, Firebase Auth, Casdoor, Keycloak, …), opaque tokens by RFC 7662 introspection (Ory Hydra, Zitadel) and Ory Kratos sessions — see [OIDC](oidc.md); handlers stay on existing authn ports.

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
from forze_identity.oidc import ConfigurableOidcIdpVerifier

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
    token_verifiers={"api": ConfigurableOidcIdpVerifier(preset=preset)},
    resolvers={"api": ConfigurableMappingTableResolver(provision_on_first_sight=True)},
    token_lifecycle={"internal"},
    password_lifecycle={"internal"},
    password_account_provisioning={"internal"},
)
```

Routes without verifier/resolver overrides fall back to the first-party defaults (`ForzeJwtTokenVerifier` + `JwtNativeUuidResolver`). Every override key must be a route in `authn`, and a verifier override's route must enable its method (`token`, `password`, `api_key`); anything else is refused when the module is built. A `token_verifiers` override also needs a `resolvers` override on the same route, since the default resolver would take an external subject as an internal principal id. Lifecycle / provisioning sets are independent of `authn` and may be empty.

## Authn document specs

`forze_identity.authn` exposes five `DocumentSpec`s (`password_account_spec`, `api_key_account_spec`, `password_invite_spec`, `session_spec`, `identity_mapping_spec`). All five are members of `AUTHN_TENANT_UNAWARE_DOCUMENT_SPEC_NAMES` and must be wired to **tenant-unaware** document stores so authentication can run before `TenantIdentity` is bound. `password_invite_spec` is only needed when you enable single-use password invites (`AuthnKernelConfig.invite_token_pepper`). `PrincipalEligibilityPort` additionally requires tenant-unaware `authz_policy_principals` (`policy_principal_spec`). User offboarding uses `PrincipalDeactivationPort`, not `deactivate_principal` alone. `MappingTableResolver` forbids cache and history on `identity_mapping_spec`.

## Authz

`forze_identity.authz` provides document-backed authorization (catalog, bindings, adapters for authz ports). `PrincipalRef` shares the `principal_id` UUID with `AuthnIdentity`, so authz bindings outlive the IdP choice.

Every port is opt-in by route, so you wire only the surface you use — a service that checks permissions but issues no delegations registers `decision` and leaves `delegation` out:

```python
from forze_identity.authz import AuthzDepsModule, AuthzKernelConfig, AuthzResourceName

authz_module = AuthzDepsModule(
    kernel=AuthzKernelConfig(
        # Permission keys that bypass the owner_id check. `admin` and
        # `{resource_type}.admin` are the default — widen this and you have widened
        # who can act on a record they do not own.
        owner_override_permissions={"admin", "{resource_type}.admin"},
    ),
    principal_registry=[AuthzResourceName.POLICY_PRINCIPALS],
    role_assignment=[AuthzResourceName.PRINCIPAL_ROLE_BINDINGS],
    grant_query=[AuthzResourceName.ROLE_PERMISSION_BINDINGS],
    decision=[AuthzResourceName.POLICY_PRINCIPALS],
)
```

The `AuthzResourceName` members are document spec names, so each one you list needs a store wired under that name like any other `DocumentSpec` — and `POLICY_PRINCIPALS` must be tenant-unaware, since eligibility is checked before a tenant is bound.

## One `RevocationStates` for authn and authz

With `RevocationStates` on the authn kernel, access tokens (and, with `key_mac`, API keys)
are verified from cached states in Redis. Pass the **same object** to the authz kernel too,
and every write that may deactivate a policy principal raises the principal's epoch, so the
eligibility read after a verification is skipped:

```python
from datetime import timedelta

from forze_identity.authn import AuthnKernelConfig, RevocationStates, StateMacKeys
from forze_identity.authz import AuthzKernelConfig

states = RevocationStates(
    route="authn-revocations",  # the revocation states port's route: Redis, or the mock
    ttl=timedelta(minutes=5),
    key_mac=StateMacKeys(active=mac_key),  # required for the skip: the epochs are signed
)
authn_kernel = AuthnKernelConfig(..., revocation_states=states)
authz_kernel = AuthzKernelConfig(revocation_states=states)
```

- **What it skips:** the eligibility read right after a token or a cached API key was
  admitted on its principal's current epoch. A later authz decision about the same
  principal, in the same task and outside a transaction, skips its policy principal read
  too, for at most `ttl`. Logins, refreshes and tenant switches still read.
- **Refusals:** an authz `revocation_states` without `key_mac` is refused
  (`authn_eligibility_skip_requires_key_mac`); authn and authz carrying different
  configurations are refused when the eligibility gate is built
  (`authn_revocation_states_mismatch`). The authz skip compares by identity, so build the
  value once and pass it to both.
- **Postgres only:** the skip needs the policy principal and the principal epochs on
  Postgres. Elsewhere, the mock included, every verification reads.
- **Configure every process that writes policy principals.** A process without the authz
  configuration raises no epoch, so a token it should have ended stays valid until its `exp`.

A custom verifier built on the shipped revocation services gets a `FreshEpoch | None` from
`SessionRevocation.check` and `ApiKeyRevocation.fresh` (all in
`forze_identity.authn.services`). Pass that value on unchanged as
`remember_admitted_session(..., fresh=...)` or `remember_admitted_api_key(..., fresh=...)`.
Never pass a bool: `fresh=True` is stored as the epoch and fails on the next eligibility
check.

## Anti-patterns

- **Binding identity inside handlers** — bind at the boundary only.
- **Treating authz as domain-only state** — use authz ports for policy decisions that depend on external grants.
- **Forgetting authn document specs need storage wiring** — `forze_identity.authn` and `forze_identity.authz` specs are still `DocumentSpec`s; `identity_mapping_spec` must allow neither cache nor history.
- **Using `AccessTokenCredentials.scheme` / `profile` as a security gate** — they are routing hints; the verifier's signature/claim checks are the boundary.
- **Caching `ForzeJwtTokenVerifier` results in front of it** — it already reuses a token's verified signature and claims for `AccessTokenConfig.verify_cache_ttl` (30 s), only while the key, algorithm, issuer and audience are unchanged, and still runs the expiry, session and revocation checks on every request; a cache in front would skip those. A custom `SignerPort` whose `verification_key()` returns a new, non-equal object each call gets no reuse.
- **Building `RevocationStates` twice, once per kernel** — two equal values still lose the authz skip, which compares by identity; build one and pass it to both.

## Reference

- [Authentication](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/identity/)
- [Skip the eligibility read](https://morzecrew.github.io/forze/latest/integrations/authn/#skip-the-eligibility-read)
