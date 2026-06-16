---
title: Identity & access
icon: lucide/key-round
summary: Authentication as verify-then-resolve, and authorization as decision plus scope
---

Identity answers two questions, in order: **is this credential real**
(authentication), and **may this principal do what they're asking**
(authorization). Forze keeps both behind contracts in a separate plane,
`forze_identity`, so swapping an identity provider never reaches your handlers.

## Authentication: verify, then resolve

Proving a credential is valid and deciding *who* it represents are two separate
jobs. They meet at a single value object — and that's the whole design.

![Verification emits a VerifiedAssertion; resolution turns it into an AuthnIdentity](../_diagrams/light/authn-verify-resolve.svg#only-light){ data-src="../_diagrams/light/authn-verify-resolve.svg" }
![Verification emits a VerifiedAssertion; resolution turns it into an AuthnIdentity](../_diagrams/dark/authn-verify-resolve.svg#only-dark){ data-src="../_diagrams/dark/authn-verify-resolve.svg" }

- **Verify** — a verifier proves the credential against its issuer (a JWT
  signature, an API-key hash, OIDC JWKS) and emits a **`VerifiedAssertion`**:
  vendor-flavoured proof carrying the issuer, subject, and claims.
- **Resolve** — a `PrincipalResolverPort` maps that assertion to a canonical
  **`AuthnIdentity`** with a `UUID` `principal_id`.

The verifier never invents a principal; the resolver never re-checks a
signature. The `VerifiedAssertion` is the entire seam between them — which is
why several verifiers (first-party JWT, OIDC, API keys) can sit behind one
orchestrator and feed the same resolver.

This is what keeps the rest of Forze provider-agnostic: your domain, tenancy,
and authorization code only ever see a `UUID` principal. Switching from Google
OIDC to internal SSO is a new verifier/resolver pair — not a change to a single
handler.

## Resolving to a principal

Three first-party resolvers cover the common shapes — the choice is about
whether you need stored accounts:

| Resolver | Maps subject → principal by… | Storage |
|----------|------------------------------|---------|
| **`JwtNativeUuidResolver`** | trusting a subject that's already a UUID (first-party Forze JWTs) | none |
| **`DeterministicUuidResolver`** | deriving a stable UUID from `(issuer, subject)` | none |
| **`MappingTableResolver`** | looking up `(issuer, subject)` in a table, with optional just-in-time provisioning | a mapping document |

## Plugging in a provider

A route's `AuthnSpec` selects verifiers and a resolver by **profile name**. An
integration registers a verifier under a profile; the spec references it without
owning any vendor knowledge:

```python
from forze.application.contracts.authn import AuthnSpec

api_authn = AuthnSpec(
    name="api",
    enabled_methods=frozenset({"token", "api_key"}),
    token_profile="oidc",
    resolver_profile="mapping",
)
```

## Authorization: may they?

Once a request carries an `AuthnIdentity`, authorization decides what it may do.
Two questions, two ports:

| Question | Port | Resolved via |
|----------|------|--------------|
| May this principal run this operation? | `AuthzDecisionPort` | `ctx.authz.decision(spec)` |
| Which rows may they see? | `AuthzScopePort` | `ctx.authz.scope(spec)` |

(A third slice — grant management — provisions the roles, permissions, and
bindings those decisions read.)

Two permission keys are **reserved by default**: a principal holding `admin`
or `<resource_type>.admin` (e.g. `invoice.admin`) bypasses the `owner_id`
ownership check on resources. Don't reuse those names for unrelated app
permissions — or change the convention via
`AuthzKernelConfig(owner_override_permissions=...)`: pass an empty set to
always enforce ownership, or your own keys (the literal `{resource_type}`
placeholder is substituted at evaluation time).

Enforcement belongs on the **operation plan**, not scattered across routes — so
it's authoritative for every caller, HTTP or not. Using the [stage
hooks](../core-concepts/application-layer.md) from the application layer: a `BeforeStep`
authorizes the operation, and a `wrap` step injects scope filters into
list/search queries. Both read the bound `AuthnIdentity` and `TenantIdentity` to
build the decision.

## Authn events and login lockout

Authentication flows can narrate themselves. Wire an optional **authn event
sink** and every flow emits a structured `AuthnEvent` — login success/failure,
lockout, token refresh, **refresh-reuse detection** (the token-theft signal),
logout, password change, reset request/completion, principal deactivation:

```python
from datetime import timedelta

from forze.application.integrations.authn import LockoutConfig
from forze_identity.authn import AuthnDepsModule, ConfigurableLoggingAuthnEventSink

authn_module = AuthnDepsModule(
    kernel=kernel,
    authn={"main": frozenset({"password", "token"})},
    events=ConfigurableLoggingAuthnEventSink(),  # one log line per event
    lockout=LockoutConfig(threshold=5, window=timedelta(minutes=15)),
)
```

Emission is **best-effort by contract**: a sink failure (or no sink at all)
never fails the auth flow, and the failed-login event is emitted *after* the
verifier has produced its uniform error, so the Argon2 timing parity between
unknown-login and wrong-password failures is untouched. The shipped
`LoggingAuthnEventSink` logs failures, lockouts, and refresh reuse at WARNING
and everything else at INFO; in tests, `MockDepsModule(authn_events=True)`
records events onto `state.authn_events` for inspection.

**Privacy: events carry a digest, never the login.** `AuthnEvent.login_digest`
is `sha256("lockout:" + login.lower())` — unpeppered *pseudonymization, not
secrecy*: it keeps raw logins out of logs and counter key spaces, while anyone
who can already read those stores could brute-force a known login anyway. The
same digest keys the lockout counters, so a locked login correlates with its
events.

**Lockout is a fixed window over `CounterPort`.** After `threshold` failed
attempts within the current window, further attempts raise a `throttled` error
(`code="login_locked"`, HTTP 429 — retryable by kind) *before* password
verification, and unlock when the window rolls over. The window is fixed —
bucketed by `floor(unix_now / window_seconds)` — because `CounterPort` has no
TTL surface: without key expiry there is nothing to hang a sliding window or a
`lock_for` duration on (both are noted as future counter-port capabilities, as
is backend-side expiry of stale buckets, which today remain as dead value-only
keys). Lockout counts **login strings, not accounts**: a nonexistent login
locks exactly like a real one, preserving the no-enumeration posture.

## The identity plane

All of this lives in `forze_identity`, separate from the core: `authn`, `authz`,
`tenancy`, `oidc`, and `oauth` subpackages, wired per route via the same deps
modules as any other integration.

For getting started, `forze_identity.builtin` ships presets — file/env API keys
(`local`) and Google / VK / Telegram Login over OIDC (`idp`). They're shipped-in
conveniences, not production defaults: adopt one only once you accept its trust
model (e.g. VK publishes no JWKS, so its preset verifies `id_token`s by
server-side introspection against VK rather than a local signature check).
