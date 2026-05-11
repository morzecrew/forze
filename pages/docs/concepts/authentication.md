# Authentication pipeline

## What problem this solves

External identity providers — generic OIDC, Firebase Auth, Casdoor, Auth0, internal SSO — describe their subjects with vendor-flavored payloads. Forze stays on a small, vendor-agnostic identity (`AuthnIdentity` with a `UUID` `principal_id`) so document, tenancy, and authorization layers do not have to know which IdP issued a request. The authentication contract group splits the work into two cooperating seams so each side can evolve independently.

## When you need this

Read this when you wire authentication for a new route, integrate an external IdP, choose how to map external subjects to internal Forze principals, or design a custom verifier/resolver pair.

## How it works

<div class="d2-diagram">
  <img class="d2-light" src="/forze/assets/diagrams/light/authn-verify-resolve.svg" alt="Authentication pipeline: header/cookie boundary resolves credentials, orchestrator dispatches to a verifier, verifier emits VerifiedAssertion, resolver returns canonical AuthnIdentity">
  <img class="d2-dark" src="/forze/assets/diagrams/dark/authn-verify-resolve.svg" alt="Authentication pipeline: header/cookie boundary resolves credentials, orchestrator dispatches to a verifier, verifier emits VerifiedAssertion, resolver returns canonical AuthnIdentity">
</div>

1. **Boundary** — one or more single-source resolvers (`HeaderTokenAuthnIdentityResolver` / `HeaderApiKeyAuthnIdentityResolver` / `CookieTokenAuthnIdentityResolver`) extract raw credentials from the request and ask the configured `AuthnPort` to authenticate them. `ContextBindingMiddleware` accepts a `Sequence` of resolvers plus a `when_multiple_credentials` policy to fail closed on ambiguous credentials.
2. **Orchestration** — `AuthnPort` (default implementation: `AuthnOrchestrator` from `forze_authn`) dispatches by credential family (`password`, `token`, `api_key`).
3. **Verification** — A `*VerifierPort` proves the credential is valid against its issuer (signature, hash, JWKS, etc.) and emits a `VerifiedAssertion` carrying `(issuer, subject, audience, tenant_hint, claims)`.
4. **Resolution** — A `PrincipalResolverPort` maps the assertion to a canonical `AuthnIdentity` with `UUID` `principal_id` and optional `tenant_id`.
5. **Binding** — The middleware binds the resolved identity onto `ExecutionContext` so usecases, document/tenancy ports, and authz guards read it via `ctx.get_authn_identity()`.

The verifier and resolver are **two separable concerns** that meet at the `VerifiedAssertion` value object — that is the entire seam.

## VerifiedAssertion: the seam

A `VerifiedAssertion` (see [`src/forze/application/contracts/authn/value_objects/assertion.py`](https://github.com/morzecrew/forze/blob/main/src/forze/application/contracts/authn/value_objects/assertion.py)) describes a successful credential proof in vendor-flavored terms:

| Field | Purpose |
|-------|---------|
| `issuer` | Stable identifier of the authority that produced the assertion (e.g. `"forze:jwt"`, an OIDC `iss` URL, `"firebase:project-id"`). |
| `subject` | Raw external subject identifier (string form, **not** coerced to a UUID). |
| `audience` | Optional `aud` value the assertion is bound to. |
| `tenant_hint` | Raw tenant identifier as provided by the issuer; the resolver decides how to interpret it. |
| `issued_at` / `expires_at` | Optional timestamps. |
| `claims` | Opaque snapshot of all claims for resolvers and audit trails (not consumed by domain code). |

`forze_authn` defines stable issuer labels for first-party sources (`ISSUER_FORZE_JWT`, `ISSUER_FORZE_PASSWORD`, `ISSUER_FORZE_API_KEY`); external IdPs use whatever the verifier received in `iss` (or another well-known field).

Verifiers never invent UUIDs and resolvers never re-validate signatures — keeping each side honest.

## Three resolver flavors

`forze_authn` ships three first-party `PrincipalResolverPort` implementations that cover the common deployment shapes:

| Resolver | Best fit | Storage | Trust model |
|----------|----------|---------|-------------|
| [`JwtNativeUuidResolver`](https://github.com/morzecrew/forze/blob/main/src/forze_authn/resolvers/jwt_native_uuid.py) | First-party Forze JWTs (`ForzeJwtTokenVerifier`) and any token whose `subject` is already a UUID string. | None | Trusts the verifier's subject as the canonical principal id. |
| [`DeterministicUuidResolver`](https://github.com/morzecrew/forze/blob/main/src/forze_authn/resolvers/deterministic_uuid.py) | Stateless mapping of an external subject to a stable Forze UUID; prototyping, read-only deployments. | None | Derives `principal_id = uuid4({"iss": issuer, "sub": subject})` via the deterministic helper in `forze.base.primitives`. |
| [`MappingTableResolver`](https://github.com/morzecrew/forze/blob/main/src/forze_authn/resolvers/mapping_table.py) | Production SSO with admin overrides, account merging, or invitation-only flows. | `IdentityMapping` document spec | Looks up `(issuer, subject) -> principal_id`; optional just-in-time provisioning when `provision_on_first_sight=True`. |

Multiple resolvers can co-exist behind the same orchestrator on a per-route basis (selected via `AuthnSpec.resolver_profile` and the `resolvers` mapping on `AuthnDepsModule`).

## Why Forze stays UUID-native

Internal `principal_id` and `tenant_id` are always `UUID`s, not opaque strings, for three reasons:

1. **No vendor lock-in.** Domain, tenancy, and authz code never sees a Firebase UID or an OIDC URL — switching IdPs only means rewiring a verifier/resolver pair.
2. **Stable cross-system references.** `AuthnIdentity.principal_id` aligns with `forze.application.contracts.authz.PrincipalRef` and any `forze_tenancy` binding row, so bindings outlive the IdP choice.
3. **Deterministic mapping when needed.** [`forze.base.primitives.uuid4`](https://github.com/morzecrew/forze/blob/main/src/forze/base/primitives/uuid.py) is **deterministic** when fed the same input — same string in, same UUID out. `DeterministicUuidResolver` uses this to convert any external subject into a stable internal id without a database round-trip; `MappingTableResolver` mints a fresh UUID once and stores it for explicit account ownership.

External IdPs that expose UUID subjects (e.g. internal SSO that already uses UUIDs) can use `JwtNativeUuidResolver` directly; everything else picks `DeterministicUuidResolver` or `MappingTableResolver` based on whether account management needs persistent rows.

## AuthnSpec walkthrough

`AuthnSpec` is the per-route configuration consumed by the boundary resolver and the orchestrator (see [`src/forze/application/contracts/authn/specs.py`](https://github.com/morzecrew/forze/blob/main/src/forze/application/contracts/authn/specs.py)):

| Field | Purpose |
|-------|---------|
| `name` | Logical route — matches the key registered on `AuthnDepsModule.authn`. |
| `enabled_methods` | `frozenset[AuthnMethod]` — which credential families this route accepts. The orchestrator raises `AuthenticationError(code="method_disabled")` when invoked with a disabled method. |
| `token_profile` | Optional name selecting a registered `TokenVerifierPort` when more than one is wired (for example, first-party JWT vs OIDC). |
| `password_profile` / `api_key_profile` | Same idea for password and API key verifiers. |
| `resolver_profile` | Optional name selecting a registered `PrincipalResolverPort`; `None` means "use the route's default resolver". |

Profile fields are how external IdPs plug in: an integration package registers a `TokenVerifierPort` under a profile name, and `AuthnSpec` references that name without owning any vendor-specific knowledge.

```python
from forze.application.contracts.authn import AuthnSpec

api_authn = AuthnSpec(
    name="api",
    enabled_methods=frozenset({"token", "api_key"}),
    token_profile="oidc",
    resolver_profile="mapping",
)
```

## Routing and dependency keys

Each seam has its own `DepKey` (`AuthnDepKey`, `PasswordVerifierDepKey`, `TokenVerifierDepKey`, `ApiKeyVerifierDepKey`, `PrincipalResolverDepKey`). `AuthnDepsModule` from `forze_authn` registers a default first-party stack per route, plus optional overrides for verifiers and resolvers. See [Reference: Authentication contracts](../reference/authentication.md) for the full surface and [Recipe: External IdPs over OIDC](../recipes/external-idp-oidc.md) for an end-to-end wiring example.

## Cross-links

- [Recipe — Authn, authz, and tenancy with FastAPI](../recipes/authn-authz-tenancy-fastapi.md): boundary middleware, request binding, and OpenAPI alignment.
- [Recipe — External IdPs over OIDC](../recipes/external-idp-oidc.md): wiring a generic OIDC verifier alongside a Forze resolver.
- [Integration — OIDC (`forze_oidc`)](../integrations/oidc.md): JWKS, claim mappers, and key providers.
- [Concept — Multi-tenancy](multi-tenancy.md): how `tenant_id` flows through the same pipeline.
