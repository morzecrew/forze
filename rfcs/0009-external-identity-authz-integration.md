# RFC 0009 — External identity integration: BYO IdP authn + external authz engines

- **Status:** 📝 Draft
- **Scope:** Make delegating identity and authorization to dedicated external systems (Casdoor, Ory Kratos/Hydra/Keto, Zitadel, Keycloak; OpenFGA/SpiceDB/Cerbos/OPA) a first-class, config-only story — the same way database work is delegated to databases — in four workstreams: **(W1)** authn completeness: OIDC discovery autoconfiguration, a generic RFC 7662 introspection verifier (opaque tokens: Ory Hydra, default Zitadel), a Kratos session/whoami verifier, and a claim mapper that finally consumes `email`/`roles`/`groups`; **(W2)** claims-carried authorization: the one deliberate framework touch — an optional request-scoped `VerifiedAssertion` slot on `InvocationContext` — plus a claims-backed `GrantQueryPort` with configurable claims-path extraction and a `role_key → permission_keys` bridge, so IdP-managed roles drive the existing permission-key enforcement with zero network calls on the hot path; **(W3)** remote decision engines: `AuthzDecisionPort` adapters for Zanzibar-style and policy engines as standard integration packages, with request-scoped memoization, batch, an optional consistency token, and a fail-closed posture; `AuthzScopePort.scope_document` deliberately stays local in v1; **(W4)** provisioning/lifecycle sync: JIT profile provisioning on first sight, a webhook-ingestion recipe (Kratos/Casdoor), and a claims-derived tenancy resolver option for org-native IdPs (Zitadel). No handler, kit, transport, or enforcement-hook changes; `AuthnIdentity` stays principal-only.
- **Related:** The seams being implemented — `TokenVerifierPort`/`ApiKeyVerifierPort` + `PrincipalResolverPort` ([`authn/ports/verification.py`](../src/forze/application/contracts/authn/ports/verification.py), [`authn/ports/resolution.py`](../src/forze/application/contracts/authn/ports/resolution.py)), `VerifiedAssertion` ([`authn/value_objects/assertion.py`](../src/forze/application/contracts/authn/value_objects/assertion.py)), `AuthzDecisionPort`/`AuthzScopePort`/`GrantQueryPort` ([`authz/ports.py`](../src/forze/application/contracts/authz/ports.py)), dep keys ([`authn/deps.py`](../src/forze/application/contracts/authn/deps.py), [`authz/deps.py`](../src/forze/application/contracts/authz/deps.py)). Enforcement location — `AuthnRequired` ([`hooks/authn/plans.py`](../src/forze/application/hooks/authn/plans.py)), `AuthzBeforeAuthorize` + `AuthzDocumentScopeWrap` ([`hooks/authz/plans.py`](../src/forze/application/hooks/authz/plans.py)), `InvocationContext.bind_identity` ([`execution/context/invocation.py`](../src/forze/application/execution/context/invocation.py)). The machinery being extended — `OidcTokenVerifier`/`JwksKeyProvider`/`OidcClaimMapper`/`OidcIdpPreset` ([`forze_identity/oidc/`](../src/forze_identity/oidc/)), `oidc_bootstrap_identity_deps` ([`builtin/idp/_oidc.py`](../src/forze_identity/builtin/idp/_oidc.py)), `MappingTableResolver` ([`authn/resolvers/mapping_table.py`](../src/forze_identity/authn/resolvers/mapping_table.py)), reference RBAC ([`forze_identity/authz/`](../src/forze_identity/authz/)), tenancy membership resolver ([`tenancy/adapters/resolver.py`](../src/forze_identity/tenancy/adapters/resolver.py)). Transport edges — `SecurityContextMiddleware` ([`forze_fastapi/middlewares/security.py`](../src/forze_fastapi/middlewares/security.py)), MCP resource-server ([`forze_mcp/auth.py`](../src/forze_mcp/auth.py), [[mcp-auth-plan]]). Prior identity work — [[identity-hardening-plan-2026-07]], [[tenancy-isolation-policy]], [[tenant-selector-feature]]. Honesty bar — [[mock-horizon-ceiling]], [[adapter-conformance-harness]] (which already flags identity as a missing conformance leg). Integration-package shape precedent — [[inference-seam-rfc]] (`forze_inference.http`/`.sagemaker` submodule pattern).
- **Origin:** "I'm mostly about unloading heavyweight to proper systems (similarly how we unload database work to databases) rather than work only with self-hosted custom solution we have here (which is quite limited in terms of role management and so on)." Investigation (2026-07-24) found the contracts were evidently designed for this — `VerifiedAssertion` is issuer-agnostic, `AuthzRequest` is engine-agnostic, tenancy validates hints instead of trusting them, enforcement sits below every transport — but the convenience/semantics layer is missing: no discovery, no generic introspection, and external `roles`/`groups`/`email` claims are silently dropped by the claim mapper, so IdP-managed roles can never reach the authz plane today.

---

## 1. Summary

```python
# W1 — any standard OIDC IdP becomes an authn source with config only:
preset = await OidcIdpPreset.from_issuer(          # .well-known discovery + JWKS
    "https://auth.example.com", audience="my-client-id"
)
deps = oidc_bootstrap_identity_deps(authn_route="main", token_verifier=ConfigurableOidcIdpVerifier(preset))

# Opaque tokens (Ory Hydra, default Zitadel) — RFC 7662, same port:
verifier = IntrospectionTokenVerifier(config=IntrospectionConfig(
    endpoint=..., client_auth=PrivateKeyJwtAuth(...), cache_ttl=timedelta(seconds=30)))

# W2 — IdP-managed roles drive the existing permission-key enforcement, no network on the hot path:
grants = ClaimsGrantResolver(
    roles_path=ClaimsPath("realm_access.roles"),               # Keycloak; Zitadel/Casdoor extractors ship too
    role_bridge=MappingRoleBridge({"billing-admin": {"invoices.admin"}}),
)
# AuthzBeforeAuthorize / ownership-ABAC / catalog required_permissions: all unchanged.

# W3 — or every decision is a remote check (Keto/OpenFGA/SpiceDB/Cerbos):
decision = OpenFgaAuthzDecision(client=..., store_id=..., consistency="minimize_latency")
# implements AuthzDecisionPort.authorize(AuthzRequest) -> AuthzDecision; memoized per invocation

# W4 — first sight of an unknown (issuer, subject) provisions mapping + policy principal + profile:
resolver = MappingTableResolver(..., provision_on_first_sight=True, jit=JitProvisioning(...))
```

Two operating modes, both supported and documented (§9): **resource-server mode** (every request carries the IdP's token; W1 validates it) and **Forze-session mode** (the existing bootstrap recipe: external `id_token` validated once at login, then first-party Forze JWT + refresh session). API keys and agent delegation stay Forze-native in both.

## 2. Motivation

- The self-hosted authz plane is honest RBAC but **role management is ports-only**: `RoleAssignmentPort`/`PrincipalRegistryPort`/`DelegationGrantPort` exist, yet there is no operation vertical, no admin surface, no UI story — and building one would be swimming upstream. Delegated administration, role catalogs, group hierarchies, audit trails, and SSO are exactly the product surface of Casdoor/Zitadel/Keycloak/Ory. The framework should make *consuming* them trivial, not compete.
- The cryptographic core already exists: `OidcTokenVerifier` + `JwksKeyProvider` validate tokens from **any** JWKS-publishing issuer today. But every preset hand-codes `issuer`/`jwks_uri`; there is no `.well-known/openid-configuration` fetch anywhere in the repo. "Add Zitadel" should be one line, not a vendored constants file.
- **Opaque tokens are the default for two of the three named target systems** (Zitadel, Ory Hydra). JWKS-only support quietly excludes them. Generic RFC 7662 introspection is not optional — the only introspection-shaped code today is the VK-vendor-specific `public_info` verifier.
- **External claims die at the mapper.** `OidcClaimMapper` consumes `iss`/`sub`/`aud`/`iat`/`exp` and one `tenant_claim`; `roles`, `groups`, `email` survive only in the opaque `VerifiedAssertion.claims` snapshot, which is discarded after principal resolution — nothing reaches `AuthnIdentity` (correctly: it is principal-only by design) *or the authz plane* (the gap). An IdP can manage roles perfectly and Forze will never see them.
- There are **two disjoint external-authz delivery models** — "roles arrive in the token" (Keycloak `realm_access.roles`, Zitadel `urn:zitadel:iam:org:project:roles`, Casdoor claim profiles) and "every check is a remote call" (Keto/OpenFGA/SpiceDB/Cerbos/OPA/Casdoor-enforce; Ory puts *nothing* in claims). They plug in at different seams (grants resolution vs the decision port) and both must be supported.
- The architecture makes all of this cheap: enforcement reads `ctx.inv_ctx.get_authn()` inside the engine (plan hooks at priorities 10/20/40/50), so a port-level swap is automatically authoritative for HTTP, MCP, Socket.IO, Temporal, and any future driving adapter. No transport work anywhere in this RFC.

**Goals**

- OIDC discovery autoconfiguration (`OidcIdpPreset.from_issuer`) with cached document fetch and JWKS derivation.
- `IntrospectionTokenVerifier` (RFC 7662): Basic and `private_key_jwt` client auth, bounded result cache, `active:false` → clean authn failure.
- `WhoamiSessionVerifier` (Ory Kratos): forward cookie/`X-Session-Token` to `/sessions/whoami`, map identity + traits into a `VerifiedAssertion`.
- Claim mapper growth: declarative `email`/`roles`/`groups` extraction with per-IdP claims-path strategies (flat list, dotted path, Zitadel's role-keyed org dict).
- Request-scoped assertion availability at authorize time (the single framework touch, additive and optional).
- `ClaimsGrantResolver` implementing `GrantQueryPort` + a `role_key → permission_keys` bridge (static mapping or document-backed), reusing the existing `AuthzPolicyService.decide` matching, ownership-ABAC, and delegation-chain enforcement unchanged.
- `forze_openfga`, `forze_keto`, `forze_cerbos` integration packages implementing `AuthzDecisionPort` (SpiceDB/OPA follow the same shape when demanded), with per-invocation memoization, batch where available, optional consistency token, fail-closed on engine unavailability.
- Thin presets `builtin/idp/{zitadel,keycloak,casdoor,ory}` following the Google-preset pattern (constants + claims-path strategy + `to_preset()`), including Casdoor's `/api/enforce` as an `AuthzDecisionPort` inside its preset.
- JIT provisioning hook on `MappingTableResolver` (mapping row → policy principal → optional profile document), a webhook-ingestion recipe for deactivation/sync, and a claims-derived `TenantResolverPort` option for Zitadel org claims.
- Conformance: a shared `AuthzRequest` fixture battery run against local RBAC **and** every remote engine adapter, plus verifier conformance cases (expired/wrong-aud/revoked), with real-engine differentials (testcontainers) per [[mock-horizon-ceiling]].

**Non-goals**

- **No role-management admin vertical.** Assign-role/create-role operation kits are permanently out of scope — that administration surface is precisely what is being offloaded. The existing ports remain for the self-hosted plane.
- **`AuthnIdentity` stays principal-only.** Roles, scopes, claims, email never ride the identity object (decision #1). No identity enrichment, no `AuthnIdentity` subclassing.
- **No Oathkeeper/edge-proxy integration.** In-service verification via the ports is strictly more portable; community signal puts Oathkeeper in maintenance mode.
- **No UMA 2.0 RPT flow, no SAML, no RFC 8693 token-exchange *grant*** in v1 (inbound `act`-claim delegation already works and is untouched). All additive later if demanded.
- **`scope_document` is not delegated in v1** (decision #7). Row-level filtering stays on the local tenant-scoping implementation; the asymmetry is documented loudly. A Cerbos query-plan adapter and OpenFGA list-objects `$in` injection are recorded as follow-ups, not promised.
- **Forze does not become an IdP.** No login UI, no consent screens, no SAML IdP. The existing first-party issuance (Forze JWT + sessions) remains what it is: a session layer, optionally fronted by an external IdP.
- **No secret values / raw tokens in logs, journals, or DST value capture.** The assertion slot carries verified claims, marked sensitive; introspection credentials resolve via the secrets plane.

## 3. Current state (what this builds on)

| Piece | State |
|---|---|
| Contracts (`forze.application.contracts.{authn,authz,tenancy}`) | All ports are `Protocol`s under string dep keys (`"authn_token_verifier"`, `"authn_principal_resolver"`, `"authz_decision"`, `"authz_scope"`, `"tenant_resolver"`, …), resolved per `AuthnSpec`/`AuthzSpec` profile + route. `forze_identity` is one pluggable implementation — **stays exactly as is** |
| Authn model | Verify-then-resolve: verifier → `VerifiedAssertion{issuer, subject, audience, issuer_tenant_hint, iat/exp, claims}` → `PrincipalResolverPort` → principal-only `AuthnIdentity` (+ `actor` delegation chain) |
| Generic OIDC | **Done and generic**: `OidcTokenVerifier` (RS256/ES256/HS256 allowlist, iss/aud/exp/nbf/nonce, off-loop JWKS), `JwksKeyProvider` (cached), `OidcIdpPreset`, `ConfigurableOidcIdpVerifier` (build-once). Missing only discovery |
| Presets | Google (pure OIDC constants over the shared machinery — the template W5 presets copy), Telegram (OIDC + widget-HMAC), VK (vendor introspection — proves the introspection shape fits `TokenVerifierPort`), local file/env API keys |
| Principal resolution | `JwtNativeUuidResolver`, `DeterministicUuidResolver`, `MappingTableResolver` (race-safe JIT `(issuer, subject) → principal_id`; no profile provisioning). Safety guard already forbids external verifier + `JwtNativeUuidResolver` |
| First-party issuance | Bootstrap recipe shipped: external `id_token` validated once → Forze HS256/RS256 JWT + opaque refresh + session row, rotation + reuse/theft detection, own JWKS endpoint |
| Authz reference impl | Document-backed RBAC: 11 specs, role inheritance, group bindings, `EffectiveGrants` union, permission-key matching + ownership-ABAC (`admin`/`{resource_type}.admin`), delegation grants |
| Enforcement | Plan hooks below all transports: `AuthnRequired` (10) → `TenantRequired` (20) → `AuthzDocumentScopeWrap` (40) → `AuthzBeforeAuthorize` (50, re-authorizes each delegation actor independently — least-privilege intersection) |
| Tenancy | Membership-authoritative: issuer/header tenant is a *hint validated against* `principal_tenant_binding` (`tenant_mismatch`/`tenant_ambiguous`/`tenant_inactive`) |
| Identity binding | `InvocationContext.bind_identity(*, authn, tenant)` → ContextVars + structlog fields; bound by FastAPI middleware, MCP verifier, Socket.IO, Temporal interceptor |
| Kits | authn (password login, refresh, API keys incl. user→agent delegation), tenancy self-service (switch/leave), tenancy_admin (unguarded by design). **No role-management vertical — deliberate, stays that way** |

## 4. External-surface survey (condensed; drives the design)

| System | Authn surface | Authz surface | Notes |
|---|---|---|---|
| Zitadel | OIDC + discovery; tokens **opaque by default** (per-app JWT switch); `/oauth/v2/introspect` (Basic or `private_key_jwt`) | Project roles as `urn:zitadel:iam:org:project:roles` claim: `{role_key: {orgId: orgDomain}}` — the claim names the granting org | Best tenancy fit: Organizations ≅ tenants; SCIM v2 inbound (license-gated) |
| Casdoor | OIDC/OAuth/SAML/LDAP; access token **is a JWT** (user object as claims, selectable profiles) | Casbin `POST /api/enforce`/`batch-enforce` (Basic app-cred auth, `[sub, obj, act]` tuple) or `roles[]`/`permissions[]` claims | Org webhooks for sync; official async Python SDK |
| Ory | Kratos sessions → forward cookie/`X-Session-Token` to `GET /sessions/whoami` (no JWT by default); Hydra OAuth2 → **opaque** tokens, `/admin/oauth2/introspect` (RFC 7662) | Keto Zanzibar: `POST /relation-tuples/check` → `{allowed}`. **Nothing in claims** | Kratos registration/settings webhooks (Jsonnet) for identity sync |
| Keycloak | OIDC + discovery; JWT; RFC 8693 token exchange official since 26.2 | `realm_access.roles` / `resource_access.{client}.roles` claims; UMA 2.0 RPT | Baseline reference; realm-per-tenant is heavy |
| OpenFGA / SpiceDB / Keto | — | `check(user, relation, object)` + batch; consistency tokens (ZedToken / consistency enum); ~1–10 ms remote | Model tenants as namespaces/objects |
| Cerbos / OPA | — | `CheckResources(principal{id, roles, attr}, resource, actions)`; Cerbos query-plan returns filter ASTs; typically sidecar → near-local | The one credible future `scope_document` delegate (Cerbos) |

Two load-bearing facts: **(a)** opaque-by-default tokens make introspection mandatory for Zitadel and Ory; **(b)** the two authz delivery models (claims vs remote check) are disjoint and plug in at different seams — W2 and W3 respectively.

## 5. W1 — Authn completeness (pure adapters, zero framework change)

All of W1 lives in `forze_identity/oidc/` (+ the `oidc` extra growing an httpx dependency it already effectively has via the exchange helpers).

### 5.1 Discovery — `oidc/discovery.py`

```python
async def fetch_oidc_configuration(issuer: str, *, timeout: float = 10.0) -> OidcProviderMetadata: ...

@classmethod  # on OidcIdpPreset
async def from_issuer(cls, issuer: str, *, audience: str | Sequence[str], **overrides) -> OidcIdpPreset: ...
```

Fetches `{issuer}/.well-known/openid-configuration`, validates `issuer` equality (RFC 8414 §3.3 — a mismatched issuer in the document is fail-closed, `exc.configuration`), extracts `jwks_uri`, `introspection_endpoint`, `token_endpoint`, `userinfo_endpoint` into a frozen `OidcProviderMetadata`. `OidcIdpPreset.from_issuer` is a convenience that fills `jwks_uri` (and stashes metadata for the introspection/exchange configs). Discovery is a **construction-time** operation (wiring/startup), never per-request; the existing hand-configured `OidcIdpPreset(...)` constructor remains the offline/pinned path (decision #4 — discovery is convenience, not a runtime dependency).

### 5.2 Introspection verifier — `oidc/introspection.py`

```python
@final
@attrs.define(frozen=True, kw_only=True)
class IntrospectionConfig:
    endpoint: str
    client_auth: BasicClientAuth | PrivateKeyJwtAuth          # Zitadel recommends private_key_jwt
    expected_issuer: str | None = None                        # cross-check `iss` in the response when present
    audience: str | Sequence[str] | None = None
    cache_ttl: timedelta = timedelta(seconds=30)              # BOUNDED — introspection exists to catch revocation
    cache_max_entries: int = 1024
    timeout: float = 10.0
    claim_mapper: OidcClaimMapper = ...                       # reused; introspection responses are claims maps

class IntrospectionTokenVerifier(TokenVerifierPort): ...
```

`verify_token` POSTs `token=...` with the configured client auth; `active: false` (or 2xx-with-error per RFC 7662) → `exc.authentication(code="token_invalid")` — never a distinguishable "revoked" signal to the caller. The response claims map feeds the same `OidcClaimMapper` → `VerifiedAssertion` (so W1.4 roles/email extraction works identically for opaque tokens). The cache key is a **digest of the token** (never the token itself in memory longer than needed, never in logs); TTL is clamped to `min(cache_ttl, exp - now)`. Client credentials resolve via the secrets plane (`SecretRef`), not inline strings, matching [[identity-hardening-plan-2026-07]] posture.

### 5.3 Kratos whoami verifier — `oidc/whoami.py` (name notwithstanding, it is protocol-generic "session endpoint" shaped)

```python
class WhoamiSessionVerifier(TokenVerifierPort):
    """Forward the session credential to a whoami endpoint; map the JSON identity to an assertion."""
```

Config: `endpoint` (Kratos public `/sessions/whoami`), `credential_style: "cookie" | "x_session_token"`, `identity_path` defaults (`identity.id` → subject, configured issuer constant, `expires_at` from the session), `traits_to_claims: bool = True` (traits + `metadata_public` land in `VerifiedAssertion.claims` under `traits.*`/`metadata.*`). The existing `CookieTokenAuthn` ingress already delivers the cookie value as the token — no middleware change. Cache TTL short and bounded, same posture as §5.2 (Kratos sessions are revocable).

### 5.4 Claim mapper growth — `oidc/claims.py`

`OidcClaimMapper` gains optional declarative extraction (additive, all default-off):

```python
email_claim: str | None = None                    # "email"
roles: ClaimsExtractor | None = None
groups: ClaimsExtractor | None = None
```

`ClaimsExtractor` is a tiny strategy family covering the real variance found in the survey:

- `ClaimsPath("realm_access.roles")` — dotted path to a string list (Keycloak realm roles, Casdoor `roles`).
- `ClaimsPath("resource_access.{client}.roles", client=...)` — parametrized (Keycloak client roles).
- `ZitadelProjectRoles(project_id=None, org_id=None)` — the role-keyed org-map dict `{role_key: {orgId: domain}}`; when `org_id` is set, keeps only roles granted by that org and can emit the org id as the tenant hint (this is what makes Zitadel's claim *also* a tenancy source, §8.3).

Extracted values land on the assertion as **normalized, namespaced claims** (`_forze.roles`, `_forze.groups`, `_forze.email` inside `VerifiedAssertion.claims`) — not new top-level fields on the frozen value object (decision #2: the assertion schema stays stable; normalization is a mapper concern). Downstream (W2) reads only the normalized keys, so one grant resolver serves every IdP.

## 6. W2 — Claims-carried authorization

### 6.1 The one framework touch: the assertion slot

`InvocationContext.bind_identity` grows one optional keyword; two accessors appear beside `get_authn`:

```python
def bind_identity(self, *, authn=None, tenant=None, assertion: VerifiedAssertion | None = None): ...
def get_assertion(self) -> VerifiedAssertion | None: ...
```

- Additive and default-`None`: every existing call site compiles and behaves identically; core hooks never require it.
- The FastAPI middleware, MCP verifier, and Socket.IO connection binder pass the assertion they already hold (it flows through `AuthnResult` today as far as the transport, then dies — `AuthnResult` gains an optional `assertion` field so the orchestrator can hand it up; also additive).
- **Never logged, never in DST value capture, never serialized into journals** — the slot is marked sensitive; `_identity_log_fields` does not touch it. Background/kit flows that bind tenant-only are unaffected.
- Lifetime = the invocation, exactly like the identity itself. No caching concern: the token was verified this request; its claims are as fresh as the authn.

This is deliberately a *context* slot and not an `AuthnIdentity` field: identity is a stable, cacheable, principal-only value object that crosses process boundaries (outbox, durable runs); claims are request-ephemeral evidence (decision #1).

### 6.2 Claims-backed grants — `forze_identity/authz/adapters/claims_grants.py`

```python
class ClaimsGrantResolver(GrantQueryPort):
    """EffectiveGrants from the bound assertion's normalized claims — zero I/O."""
```

Reads `ctx.inv_ctx.get_assertion()`; missing slot or missing normalized roles → **empty grants** (fail-closed: no assertion means no external roles, never an error that masks as allow). Roles pass through the **role bridge**:

```python
class RoleBridgePort(Protocol):
    def permissions_for(self, role_keys: frozenset[str], *, scope: AuthzScope) -> Awaitable[frozenset[str]]: ...

MappingRoleBridge(mapping: Mapping[str, frozenset[str]])          # static wiring-time config — the default
DocumentRoleBridge(...)                                           # optional: an `external_role_binding` spec, admin-editable
IdentityRoleBridge()                                              # role_key IS the permission key (IdP manages permission keys directly)
```

The output is ordinary `EffectiveGrants{roles, permissions}` — so `AuthzPolicyService.decide` (permission-key matching, ownership-ABAC with `admin`/`{resource_type}.admin` overrides), the delegation-chain intersection in `AuthzBeforeAuthorize`, and the catalog's `required_permissions` surface all work **unchanged**. A composed decision adapter (`ClaimsBackedAuthzDecision`) wires resolver + existing policy service; `enforce_principal_active` becomes optional there (the IdP owns account state; deactivation propagates via W4 sync or introspection). Hot path: no network, no document reads with `MappingRoleBridge`.

Hybrid composition is free: because `GrantQueryPort` resolves per `AuthzSpec` route, an app can run claims-backed grants on one route and the document-backed RBAC on another — or union them via a small `CompositeGrantResolver` (recorded as a follow-up; not needed for v1).

## 7. W3 — Remote decision engines

### 7.1 Packaging and shape

Each engine is a standard integration package with the common shape (`kernel/` client + config, `adapters/`, `execution/` deps module) and its own extra: `forze_openfga`, `forze_keto`, `forze_cerbos` in v1 order (OpenFGA first — official async Python SDK, testcontainer-friendly, batch + consistency knobs exercise the whole contract). SpiceDB and OPA are shape-identical follow-ups; Casdoor's `/api/enforce` ships inside the Casdoor preset (§8.1) rather than as a package — it is app-credentialed HTTP, not a client kernel.

### 7.2 The adapter contract

`AuthzRequest` maps almost 1:1 onto every surveyed check API:

| `AuthzRequest` | OpenFGA / SpiceDB / Keto | Cerbos |
|---|---|---|
| `subject.principal_id` | `user: "user:{id}"` | `principal.id` |
| `action` | `relation` / `permission` | `actions[0]` |
| `resource.resource_type` + `resource_id` | `object: "{type}:{id}"` | `resource.kind` + `id` |
| `scope.tenant_id` | namespace/object prefix or contextual tuple (configurable `TenantMapping`) | `principal.attr.tenant_id` |
| `resource.attributes` / `context` | contextual tuples (OpenFGA) / caveat context (SpiceDB) | `resource.attr` |

Adapter obligations, uniform across engines:

- **Fail closed.** Engine unreachable/timeout → `AuthzDecision(allowed=False, reason="authz_engine_unavailable")` + a distinct exception code for observability — never fail-open, never cached-allow beyond TTL (decision #5).
- **Per-invocation memoization.** `AuthzBeforeAuthorize` re-authorizes each delegation actor with the same action/resource; a request-scoped memo (keyed on the full request tuple) makes the actor chain cost one round-trip per distinct subject. No cross-request cache in v1 (revocation semantics; engines already cache server-side).
- **Batch where the engine has it** (`batch-check`/`CheckBulkPermissions`) behind the same memo, used when the hook family issues multiple checks in one wave.
- **Optional consistency token.** `consistency: "minimize_latency" | "higher_consistency"` config default + a per-request override seam (recorded; plumbed when a read-after-write consumer materializes — the write side of Zanzibar tuples is out of scope in v1, see §7.4).
- **Missing resource** (`resource=None`, pure action checks): mapped to a configurable synthetic object (`"app:global"` by convention) — engines require an object; the mapping is explicit config, not magic.

### 7.3 `scope_document` doctrine (v1: stays local)

Zanzibar engines answer "may X do Y to Z", not "which Zs" — ListObjects/LookupResources return ID sets that only inject safely as `$in` filters at bounded cardinality. **v1 policy (decision #7):** when the decision port is remote, `AuthzScopePort` remains the local tenant-scoping implementation (rows are tenant-partitioned regardless; the remote engine governs *operations*). The docs state the asymmetry in one loud sentence: *"the engine decides whether you may list; tenancy decides what the list contains."* Follow-ups recorded, demand-gated: a Cerbos query-plan `AuthzScopePort` (filter AST → Forze filter DSL — the one natural fit) and an OpenFGA list-objects injector with a hard cardinality cap and fail-closed overflow.

### 7.4 Delegation and tuple writes

The delegation chain needs nothing engine-specific — the hook already asks pairwise questions. `enforce_delegation_grant` keeps using the **local** `DelegationPort` by default even when decisions are remote (delegation edges are Forze-native API-key semantics, not IdP state). Modeling `may_act` as an engine relation is possible and documented, not built. Writing relationship tuples from Forze lifecycle events (e.g. aggregate created → `owner` tuple) is a real need but a separate concern — an outbox-consumer recipe sketch ships in docs; a `TupleSyncPort` is explicitly deferred until a consumer exists.

## 8. W4 — Provisioning, sync, tenancy authority

### 8.1 Presets — `builtin/idp/{zitadel,keycloak,casdoor,ory}/`

Each follows the Google template (~2 small files over shared machinery): a frozen config with `to_preset()`/verifier factory, the right `ClaimsExtractor` defaults, and provider constants. Specifics: **zitadel** — discovery-based, introspection config for opaque mode, `ZitadelProjectRoles` extractor, optional org-claim tenancy (§8.3); **keycloak** — realm URL → discovery, realm/client roles extractors; **casdoor** — JWT-claims mode (default) + `CasdoorEnforceAuthzDecision` (the `/api/enforce` adapter) as the remote-mode option; **ory** — Kratos whoami verifier config + Hydra introspection config (Keto lives in `forze_keto`, referenced not vendored).

### 8.2 JIT provisioning — `authn/resolvers/mapping_table.py` growth

`MappingTableResolver` gains an optional `jit: JitProvisioning | None`:

```python
@final
@attrs.define(frozen=True, kw_only=True)
class JitProvisioning:
    ensure_policy_principal: bool = True     # PrincipalRegistryPort.ensure(kind="user")
    profile_spec: DocumentSpec | None = None # optional profile row from normalized claims (email, name)
```

Runs only on the first-sight path (the existing race-safe CONFLICT handling extends to the principal ensure — both are idempotent upserts). No claims → still provisions mapping + principal; profile row is best-effort-with-log, never blocks authn (decision #6). Account linking stays what it is: multiple `(issuer, subject)` rows pointing at one principal, managed out of band.

### 8.3 Tenancy authority (decision #8)

Default unchanged: **local membership is authoritative** — `principal_tenant_binding` validated exactly as today; external org claims are hints. Two supported postures:

- **Sync-in (recommended):** webhook/SCIM ingestion (§8.4) upserts tenant bindings, so `tenant_mismatch`/`tenant_ambiguous`/`tenant_inactive` semantics, offline validation, and the tenant-selector kit all keep working.
- **Claims-derived (JWT-mode Zitadel):** `ClaimsTenantResolver(TenantResolverPort)` derives membership from the verified org claim (`ZitadelProjectRoles` org map). Honest constraints documented: membership is only as fresh as the token; `list_tenants` self-service needs the claim to carry all orgs; no `tenant_inactive` signal. Config-gated, never default.

### 8.4 Sync-in recipe (docs + one helper, not a plane)

A documented recipe — deliberately not a new contract: a FastAPI ingestion route (HMAC/signature-verified per provider) that maps Kratos identity events / Casdoor org webhooks onto existing ports (`PrincipalRegistryPort.ensure`/deactivate, mapping upsert, tenant-binding upsert, `PrincipalDeactivationPort` on delete — closing the deactivation-drift gap that introspection only covers for tokens). A SCIM *client* (outbound) is out of scope; Zitadel's SCIM *server* (inbound to Zitadel) doesn't involve Forze. If three providers' recipes converge, promoting a `IdentityEventIngestion` helper into kits is the recorded follow-up.

## 9. Two operating modes (doctrine, documented as such)

| | Mode 1 — resource server | Mode 2 — Forze session (exists today) |
|---|---|---|
| Every request carries | the IdP's access token (JWT → W1 JWKS; opaque → W1 introspection) | Forze's own JWT (IdP `id_token` validated once at login via the bootstrap recipe) |
| Revocation | JWT: at `exp` (or introspect); opaque: introspection TTL | session binding (`sid`) — logout/rotation revoke immediately |
| Latency | JWKS local / +1 introspection RTT (cached, bounded) | local |
| IdP roles | live from claims every request (W2) | snapshot at login unless re-fetched — documented staleness |
| Fit | pure APIs, service-to-service, short-lived tokens | browser sessions, mixed API-key traffic, IdP-outage tolerance |

API keys (including user→agent delegation keys) stay Forze-native in both modes — no surveyed system models them as well. The two modes compose per-route via `AuthnSpec` profiles, which is the existing multi-IdP mechanism, untouched.

## 10. Decisions (locked)

1. **`AuthnIdentity` stays principal-only.** External claims are request-ephemeral evidence, carried by the invocation context, never by the identity value object that crosses process boundaries.
2. **Normalization at the mapper, stable assertion schema.** Roles/groups/email land as namespaced normalized claims inside `VerifiedAssertion.claims` (`_forze.*`), not as new value-object fields. One grant resolver serves every IdP.
3. **The assertion slot is the only framework change.** Additive optional keyword on `bind_identity` + `get_assertion()` + optional `AuthnResult.assertion`; sensitive, never logged/journaled/captured.
4. **Discovery is construction-time convenience, never a runtime dependency.** Hand-configured presets remain the pinned/offline path; a discovery outage can delay startup, never fail a request.
5. **Remote authz fails closed.** Engine unavailable → deny with a distinct reason code. No fail-open flag exists.
6. **JIT provisioning never blocks authn on optional work.** Mapping + principal ensure are the transaction; profile enrichment is best-effort.
7. **`scope_document` stays local in v1.** Remote engines govern operations; tenancy governs rows. Cerbos-plan / list-objects injection are demand-gated follow-ups.
8. **Local membership stays the default tenancy authority.** Claims-derived tenancy is an explicit, config-gated posture with documented staleness constraints.
9. **Introspection/whoami caches are bounded and short.** TTL clamped by token `exp`; keys are token digests; these caches exist to absorb burst, not to extend trust.
10. **No role-management vertical, no Oathkeeper, no UMA/SAML/token-exchange grant in v1.** Offloading, not competing; edges stay in-service.
11. **Engine adapters are integration packages; IdP glue is presets.** Network authz backends get the standard `forze_<name>` shape and extras; IdPs get ~2-file presets over shared `forze_identity` machinery.

## 11. Testing & conformance

- **Verifier battery** (unit, shared across JWKS/introspection/whoami): expired, not-yet-valid, wrong audience, wrong issuer, alg-confusion (HS256 token against RS256 allowlist), introspection `active:false`, introspection 5xx (fail-closed), cache-TTL-bounds (revoked token dies at TTL, never later), token-digest-only cache keys.
- **Claims extraction table tests**: Keycloak realm/client shapes, Zitadel org-map (incl. multi-org and org-filtered), Casdoor arrays, missing/malformed paths → empty (never throw into authn).
- **Shared `AuthzRequest` fixture battery** run against: reference RBAC, `ClaimsBackedAuthzDecision`, and every remote engine adapter — same fixtures, same expected decisions (allow, deny, ownership-override, actor-chain intersection, inactive principal where applicable). This is the identity leg [[adapter-conformance-harness]] flags as missing.
- **Real-engine differentials** per [[mock-horizon-ceiling]] — proofs vs mock are tautology: OpenFGA + Keto testcontainers in v1 (both ship lean images); Zitadel/Keycloak containers exercise discovery→JWKS→claims and introspection end-to-end (env-gated if boot cost bites); Casdoor container for enforce + JWT-claims modes. Mock introspection endpoint for the unit tier.
- **Wiring guards**: the existing external-verifier + `JwtNativeUuidResolver` guard extends to the new verifiers; `check_wiring` resolves the new deps at freeze time as today.
- **DST**: no new plane — but the assertion slot gets a determinism check (never serialized into durable journals) and the memoized decision adapter gets a delegation-chain case (N actors → 1 engine call per distinct subject).

## 12. Phasing

- **P1 — OIDC completeness (W1):** discovery, `IntrospectionTokenVerifier`, `WhoamiSessionVerifier`, claim-mapper growth + extractors. After P1 any standard IdP works in resource-server mode with config only. Small, pure adapters, immediately shippable.
- **P2 — Claims-carried authz (W2):** assertion slot (+`AuthnResult.assertion`), `ClaimsGrantResolver` + role bridges, `ClaimsBackedAuthzDecision`. The highest-leverage phase — this is what actually offloads role management for the claims family. Contains the only framework touch; lands with the transport binders passing the assertion.
- **P3 — Remote engines (W3):** `forze_openfga` first (full contract exercise), then `forze_keto`, `forze_cerbos`; shared conformance battery + differentials.
- **P4 — Presets + sync (W4, §8):** zitadel/keycloak/casdoor/ory presets (zitadel can ship with P1 in JWKS-mode), JIT provisioning, webhook recipe, `ClaimsTenantResolver`, docs page under `identity-tenancy-enc/` (external-identity page: modes table, per-IdP quickstarts, the scope-document sentence).

P1 and P2 are independent of P3; P4 presets partially ride P1. Each phase is standalone-useful.

## 13. Flagged facts to verify before the affected slice ships

- Self-hosted Kratos session-to-JWT tokenizer status (affects an ory-preset variant only, not the whoami verifier).
- Zitadel SCIM licensing scope (affects one sentence of the sync recipe).
- Keycloak first-party SCIM absence (same).
- Oathkeeper maintenance status (affects only the non-goal's rationale, not the decision).
- Casdoor Python SDK coverage for enforce/batch-enforce (the preset can speak plain httpx if the SDK lags).
