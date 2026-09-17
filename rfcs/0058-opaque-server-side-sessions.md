# RFC 0058 — Opaque server-side sessions with instant revoke

- **Status:** 📝 Draft — design-locked, **not scheduled**: it adds a second access-path model to authn, which is the kind of decision worth a named deployment before it ships.
- **Scope:** A session mode for `forze_identity.authn` where the cookie carries an opaque session id resolved server-side on every request, with a sliding idle TTL, a hard absolute TTL, a per-session CSRF token, and revocation by row deletion. Touches the session document and its family semantics, the authn resolvers, and `forze_fastapi.security` (a carrier plus a CSRF alternative). **No change to the JWT path**, which stays the default for APIs.
- **Related:** [`src/forze_identity/authn/domain/models/session.py:18-64`](../src/forze_identity/authn/domain/models/session.py) (`Session` — the refresh family: `refresh_digest`, `family_id`, `expires_at`, `revoked_at`, `rotated_at`, `replaced_by`), [`src/forze_identity/authn/application/specs.py:108`](../src/forze_identity/authn/application/specs.py) (`session_spec`, `sensitive=True`), [`src/forze_identity/authn/services/access_token.py`](../src/forze_identity/authn/services/access_token.py) (the JWT this does not replace), [`src/forze_fastapi/security/value_objects.py:46`](../src/forze_fastapi/security/value_objects.py) (`CookieCsrf` — the Origin/Referer gate and its stated limitation), `:184` (`CookieTokenAuthn`, the cookie ingress), [`src/forze_fastapi/security/cookies.py:38`](../src/forze_fastapi/security/cookies.py) (`AuthnCookieCarrier`, the shipped outbound carrier this mode extends rather than replaces), [RFC 0057](0057-derived-permissions-and-config-grants.md) (a session that is bound to a subject is the state a provider reads), [RFC 0060](0060-audit-spec.md) (session revocation is an auditable action).
- **Origin:** A working-time ledger whose sessions are opaque random tokens stored hashed, bound to both the employee row and the OIDC subject at login, with idle and absolute TTLs, a per-session CSRF token, and a check that the row still maps to the same subject so a remap cannot revive a cookie. Deactivation or remap deletes every session.

---

## 1. Summary

A browser-only deployment can choose a session whose cookie value is a random id: every request
resolves it against a row, so a revoked session is dead on the next request rather than at token
expiry. The idle TTL slides, the absolute TTL caps, the session is bound to the subject it was
minted for, and a per-session CSRF token is available where the Origin header cannot be trusted.

## 2. Motivation

Forze's authn issues a JWT access token plus an opaque refresh token. That is right for APIs and
wrong for one case: a regulated internal application, browser-only, where "disable this account"
has to mean *now*. With a JWT, now means "within the access token's lifetime" — and the reviewer
who asks about it is not satisfied by a five-minute window, because the window is exactly when a
dismissed employee is most motivated.

The mechanism forze already has is the refresh family, which is close and not the same thing: it
bounds how long a *refresh* is valid, not how long an access decision survives revocation.

## 3. Current state

**The session document is the refresh family.** Verified at
[`session.py:18-64`](../src/forze_identity/authn/domain/models/session.py): immutable
`principal_id`, `tenant_id`, `family_id`, `refresh_digest`, `expires_at`; mutable `revoked_at`,
`rotated_at`, `replaced_by`. The spec is `sensitive=True`, so the read model is already treated
as credential-adjacent. What it does **not** carry: an idle window, a CSRF token, a binding to
the external subject, or a last-seen stamp.

**Access is a JWT.** `services/access_token.py` plus the `forze_jwt_token` verifier. Revocation
of a principal does not invalidate an issued access token; nothing in the request path reads a
row.

**The cookie ingress exists, and its CSRF gate has a stated limitation.** `CookieTokenAuthn`
carries a token in a cookie; `CookieCsrf` proves origin on unsafe methods via `Origin` with a
`Referer` fallback, and its own docstring says a deployment behind a proxy that strips `Origin`
should use a header ingress instead — which is precisely the deployment that wants a per-session
CSRF token.

**The session family is queried by digest.** So resolving an opaque id is the same access pattern
already in place; what is new is doing it on *every* request rather than on refresh.

## 4. Goals / Non-goals

**Goals**

- Revocation takes effect on the next request, because the next request reads the row.
- Idle and absolute TTLs, both enforced server-side.
- The session is bound to the external subject it was minted for; a remap invalidates it.
- A per-session CSRF token as an alternative to the Origin check.
- The JWT path is untouched and stays the default.

**Non-goals**

- **Not a replacement for the JWT path.** An API with service clients wants a stateless token.
  This is a mode, chosen per deployment.
- **Not SSO session management.** Back-channel logout and OIDC session lifecycle are the IdP's.
- **Not a session store abstraction.** The session is a document on the shipped ports; Redis is a
  document route, not a new port.
- **Not "sessions everywhere".** A deployment mixing both modes on one surface is out of scope —
  §9 says why that is the sharpest risk.

## 5. Design

### 5.1 The additions to the session document

```python
subject_binding: str | None      # the external subject (OIDC sub) bound at mint
idle_expires_at: datetime | None # slides on use
absolute_expires_at: datetime    # hard cap, never extended
csrf_digest: str | None          # per-session CSRF token, stored hashed
last_seen_at: datetime | None
```

Additive on a `sensitive=True` spec: the existing refresh family keeps working with the new
fields null, so the mode is a wiring choice and not a migration for deployments that do not want
it.

The cookie value is a random id; what is stored is its digest, as `refresh_digest` already is.
Nothing reversible is written, so a database read does not yield a usable cookie.

### 5.2 Resolution on every request

1. Look up by digest. Absent ⇒ unauthenticated.
2. `revoked_at` set, `absolute_expires_at` passed, or `idle_expires_at` passed ⇒ unauthenticated,
   with **one** message for all three (a message distinguishing "expired" from "revoked" tells a
   holder of a stolen cookie which one they have).
3. `subject_binding` differs from the current mapping for that principal ⇒ unauthenticated, and
   the session is deleted. This is the remap case: a row remapped to a different external
   identity must not be reachable through a cookie minted for the previous one.
4. Slide `idle_expires_at`, stamp `last_seen_at`.

Step 4 is a write on every authenticated request, which is the design's real cost. Mitigation is
a declared write granularity — slide only when more than *n* seconds have passed — stated as a
knob rather than hidden, because the honest cost of an instant-revoke session is a read and
sometimes a write per request.

### 5.3 CSRF

`csrf_digest` is minted with the session and returned once at login; the client echoes it in a
header on unsafe methods. Offered **beside** `CookieCsrf` rather than replacing it: the Origin
check needs nothing from the client and is the better default where the header survives.

### 5.4 Revocation

Deleting the row is the revocation. Deactivating a principal deletes every session for it, which
is where this meets [RFC 0057](0057-derived-permissions-and-config-grants.md): a derived denial
closes the routes, and session deletion closes the door — two controls, deliberately, because
one of them is cheap to forget in an app's own admin path.

### Alternatives considered

- **Short-lived JWTs plus a revocation list.** Keeps statelessness and adds a store that must be
  consulted anyway — the read this design makes explicit, with a list that grows and needs
  eviction.
- **Reuse the refresh family as the access path.** Tempting (the document exists) and it collapses
  two lifecycles: a refresh token rotates, an access session slides. Rotation semantics
  (`replaced_by`, `rotated_at`) would then mean two different things.
- **Session id in a header rather than a cookie.** No CSRF surface at all, and it gives up the
  browser-managed storage that is the reason for the mode.

## 6. Tests

- Resolution: valid, unknown, revoked, idle-expired, absolutely-expired — the last four with the
  identical message and code.
- Remap: minting under subject A, remapping the principal to B, then presenting A's cookie ⇒
  unauthenticated and the row gone.
- Sliding: a request inside the idle window extends it; one past it does not resurrect the
  session; the absolute cap is never extended by activity.
- Write granularity: with a slide threshold set, repeated requests inside the threshold produce no
  write (asserted by counting port calls, not by timing).
- CSRF: an unsafe method without the header refused; with a stale token refused; the Origin gate
  still works when the per-session token is not configured.
- Deactivation: every session for a principal is gone after the admin path runs.
- **Not tested:** cookie attributes' browser behaviour. The carrier's `SameSite`/`Secure` output is
  asserted; what a browser does with it is not ours.

## 7. Docs

A mode table in the authn page: JWT (stateless, revocation bounded by expiry, right for APIs)
versus opaque session (a read and sometimes a write per request, revocation immediate, right for
browser-only internal apps), plus the CSRF choice and the remap rule. The cost sentence is
mandatory: **instant revocation is paid for on every request.**

## 8. Out of scope

- **Concurrent session limits** ("one device at a time"). A policy an app declares; named because
  regulated deployments ask for it.
- **Session listing for the user** ("your active sessions"). A read over the same rows, and a UI.
- **Sliding via a background job** instead of inline. Would remove the write from the request path
  and add a lag that the idle TTL has to absorb.
- **Mixed-mode surfaces.** §9.

## 9. Risks

- **A second access-path model in one framework.** Two ways to authenticate means two paths for
  every future authn feature to support. Mitigation is the status line — not scheduled without a
  named deployment — and the doctrine that the JWT path stays default.
- **A mixed-mode surface is a downgrade attack.** If one route accepts either, a revoked session
  holder may still hold a valid JWT. Mitigation: the mode is per ingress and a surface that
  declares both is a wiring refusal (decision 5).
- **The per-request write.** At high traffic this is a write amplification nobody expected from
  "sessions". Mitigation: the slide threshold, the port-call assertion in the battery, and the
  documented cost.
- **`sensitive=True` and a per-request read.** The session row is credential-adjacent and now on
  the hot path, so it appears in more traces and more caches. Mitigation: the spec's existing
  `sensitive` handling, and no caching of the row.

## 10. Unresolved questions

- **Is the subject binding mandatory in this mode?** It is the origin's strongest property, and it
  requires the current mapping to be readable on every request — another read, or a join.
- **Does the slide threshold default to zero (always write) or to a value?** Zero is correct and
  expensive; a default hides a lag in the idle window.
- **Which store is recommended?** A document route can be Postgres or Redis. Redis fits the access
  pattern and makes revocation eventual on a replica.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | The JWT path is **untouched and stays the default**. This is a mode for browser-only deployments that need revocation to be immediate, not a direction for authn. |
| 2 | `LOCKED` | Only a **digest** of the session id is stored, as with `refresh_digest`. A readable session table is a credential dump. |
| 3 | `LOCKED` | Unknown, revoked, idle-expired and absolutely-expired resolve to **one** message and code. Distinguishing them tells the holder of a stolen cookie which kind they hold. |
| 4 | `LOCKED` | A session is invalidated when the principal's external subject mapping changes. A remapped row must not be reachable through a cookie minted for the previous identity. |
| 5 | `LOCKED` | One ingress surface accepts **one** mode; declaring both is a wiring refusal, or a revoked session holder falls back to a still-valid JWT. |
| 6 | `ASSUMED` | The fields are additive on the existing `session_spec` rather than a second document. The refresh family keeps working with them null, so adopting the mode is wiring, not migration. |
| 7 | `ASSUMED` | The per-session CSRF token is offered **beside** `CookieCsrf`, not instead of it: the Origin check needs nothing from the client and is the better default where the header survives. |
| 8 | `OPEN` | Whether the subject binding is mandatory in this mode, what the slide threshold defaults to, and which store is recommended. |

## 12. Phasing

- **P1** — the additive fields, resolution with both TTLs, digest storage, the single-message
  refusal, revocation by delete, batteries.
- **P2** — the subject binding and the remap invalidation (needs the mapping readable per request).
- **P3** — the per-session CSRF token and the carrier in `forze_fastapi.security`.
- **P4** *(demand-gated)* — concurrent session limits, session listing, background sliding.
