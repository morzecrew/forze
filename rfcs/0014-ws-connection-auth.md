# RFC 0014 — Shipped WebSocket connection auth: cookie-first resolver (lite)

- **Status:** ✅ Complete — shipped 2026-09-09
- **Scope:** Ship the `WsConnectionResolver` every app currently hand-writes. A factory building a resolver with a fixed credential ladder — **cookie first**, `Authorization` header second, query-param token **opt-in only** — that verifies through the wired authn plane and fills `WsConnection` completely (principal, tenant, `ClientIdentity`, tz-aware `expires_at` from the token's `exp`). One construction-time safety rule: enabling the cookie source without an Origin allowlist is a configuration error. Small surface, one PR.
- **Related:** `attach_realtime_ws_route` defines the seam this fills (`WsConnect`/`WsConnection`/`WsConnectionResolver`) and already enforces everything downstream — continuous `expires_at` expiry, `realtime.reauth` in-place credential swap, fail-closed topics, and the Origin check whose docstring states the constraint this RFC operationalizes: disabling `allowed_origins` is "safe only when the resolver never honors cookie/ambient credentials". The identity plane supplies verification (`AccessTokenService.verify_token` / token-verifier stack) and the cookie carrier (`AuthnCookieCarrier`) whose cookie this resolver reads. The realtime recipe's hand-rolled resolver is the pattern being promoted. The Socket.IO transport's `identity_resolver` is the same job on the other transport — the shared ladder is written once where both can reach it.
- **Origin:** The eis-dag evaluation (2026-07-30) hit the gap directly (its `/ws?token=` auth has no framework ingress); review round 2026-07-31 locked the direction: not a new HTTP `AuthnIngress` type, but a shipped resolver — cookie-first, because a browser WebSocket sends cookies on the upgrade request automatically, so cookie-mode apps get WS auth with zero client-side token plumbing, consistent with their HTTP surface.

---

## 1. The gap

The governed middlewares refuse websocket scopes by design, so a WS connection's identity comes from an app-supplied resolver reading the upgrade request. The seam is right; what's missing is the stock implementation. Every adopter re-derives the same ~30 lines — pick a credential source, call the verifier, map claims to principal/tenant, remember to set `expires_at` (and make it tz-aware, or the route errors at enforcement time), build `ClientIdentity` from a device id, handle the reauth payload — and the subtle parts (expiry, reauth, the cookie/Origin interaction) are exactly the parts a quick hand-rolled version skips. The recipe resolver is honest about being a demo (it returns a fixed principal); production apps deserve better than copying it.

## 2. Design

`build_ws_connection_resolver(...)` (home: the transport-neutral realtime integration layer, importable by both the FastAPI WS route and the Socket.IO gateway's `identity_resolver` adapter):

**Credential ladder, fixed order, each source individually enabled:**

1. **Cookie** (`cookie_name=`, default aligned with `AuthnCookieCarrier`) — the browser path. Ambient credential ⇒ cross-site risk ⇒ the construction rule in §3.
2. **`Authorization: Bearer`** header — non-browser clients (services, mobile, CLIs) that can set headers on the upgrade.
3. **Query parameter** (`query_param="token"`, **disabled by default**) — the ecosystem's lowest common denominator (browser `WebSocket()` cannot set headers; cookie-less cross-origin clients need *something*). Off by default because query strings leak into access logs, proxy logs, and referrer-adjacent tooling; enabling it is a decision, and the docs say to pair it with short-lived tokens and log scrubbing.
4. **Reauth payload** — on a `realtime.reauth` frame, `WsConnect.auth` carries the fresh credential (`{"token": ...}` convention); the same verification path runs, and the route already enforces same-principal/same-tenant on the swap.

First source that *presents* a credential is used; a present-but-invalid credential **refuses the connection** (client-safe error) rather than falling through — fallthrough on invalid would turn a revoked cookie into a silent anonymous downgrade attempt against the next source.

**Verification and mapping:** the resolver calls the wired authn plane (the spec-resolved token verifier — framework-issued JWTs or any configured OIDC/introspection verifier; nothing token-format-specific lives in the resolver). From the verified identity: `authn` (principal), `tenant` from the claim mapping, `expires_at` from the token's `exp` **as an aware UTC datetime** (the route refuses naive), `ClientIdentity` from `device_id`/`session_id` query params or claims (`client_source=` knob), so per-device mailbox cursors work out of the box.

## 3. The one safety rule

Constructing the resolver with the cookie source enabled requires the caller to attest the route's Origin allowlist (`require_origin_allowlist=True` is the default for cookie mode; the factory refuses cookie-mode construction when the attestation is explicitly waived without acknowledgment). Rationale, verbatim from the route's own docs: the WS handshake has no CORS preflight, the browser attaches cookies to cross-site upgrades and enforces nothing — the server-side Origin check is the entire cross-site perimeter. A shipped cookie-mode resolver that lets an app skip that check would be the framework packaging a CSRF hole. Header and query sources carry no ambient credential and have no such requirement.

(The factory cannot *see* the route's `allowed_origins` — they meet only at the attach call — so the enforcement is an explicit-acknowledgment knob plus documentation, and the docs page shows the paired attach: cookie-mode resolver + `allowed_origins=[...]` in one snippet. A boot-time cross-check via the websocket allowlist verifier is recorded as a possible hardening follow-up.)

## 4. Proof obligations

1. Cookie path: a browser-shaped upgrade (cookie, no header) authenticates; `expires_at` equals the token's `exp` (aware); expiry closes the socket via the route's guard.
2. Ladder precedence: cookie beats header beats query; an invalid presented credential refuses (never falls through to the next source).
3. Query source disabled by default: a `?token=` upgrade with only the defaults is refused as anonymous.
4. Reauth: a fresh payload token extends `expires_at` in place; a reauth token for a different principal is refused by the route (pinned through the resolver path).
5. Cookie mode without the Origin attestation fails at construction (configuration kind).
6. Tokens never land in logs from the resolver's paths (scrubbing pinned by test).
7. The same ladder drives a Socket.IO `identity_resolver` (shared-home test, both transports).

## 5. Non-goals

- No new HTTP `AuthnIngress` type — the governed HTTP surface is untouched.
- No SSE change — SSE identity already comes from the bound context via the middlewares.
- No session pinning, no IP binding, no token minting — verification only, against whatever the authn plane is wired with.

## 6. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Shipped resolver, not a new ingress type; the resolver seam is the right seam | locked |
| 2 | Cookie-first ladder; query-param source exists but is off by default with documented leak caveats | locked |
| 3 | Present-but-invalid refuses; no fallthrough downgrade | locked |
| 4 | Cookie mode requires Origin-allowlist attestation at construction | locked |
| 5 | Home is the transport-neutral realtime layer; one ladder serves WS and Socket.IO | proposed |
| 6 | Reauth payload convention `{"token": ...}`; same verification path as connect | locked |
| 7 | Resolves row 5, which shipped as proposed: the ladder is transport-neutral and takes an `AuthnPort` plus primitive views of the request — cookies, headers, query, the reauth payload. Each transport ships the thin factory that reads its own request shape and builds its own connection value object. Added by execution 2026-09-09 — see `logs/T-0014.md` (D-5). | assumed |
| 8 | `AuthnResult` gains an optional `expires_at`, filled by the orchestrator from the verified assertion on every credential path. The one core-contract change the RFC needed and did not name: the verifier already asserts when a credential expires and the boundary result dropped it, so a connection — which outlives the request shape that result was designed for — had no way to the instant it must close on but decoding the token a second time behind the verifier's back. Additive; a verifier asserting no expiry still returns `None`. Added by execution 2026-09-09 — see `logs/T-0014.md` (Unlisted, 12:30Z). | assumed |
| 9 | Cookie mode requires `origin_allowlist_attested=True`, passed explicitly, and constructing a cookie-source resolver without it is a configuration error naming the paired `allowed_origins` argument. §3 states the rule and the factory cannot see the attach call where the allowlist is configured, so the attestation is what carries it across. Added by execution 2026-09-09 — see `logs/T-0014.md` (Unlisted, 12:32Z). | assumed |
| 10 | The ladder returns a kernel-level resolved identity and each transport factory maps it onto its own connection type; merging the two value objects is a separate change this RFC does not ask for. Added by execution 2026-09-09 — see `logs/T-0014.md` (Unlisted, 12:36Z). | assumed |
| 11 | The client identity is read from the request's own query parameters and the reauth payload only. A deployment that keeps the device id in a claim maps it in its principal resolver, where the claims already are. Added by execution 2026-09-09 — see `logs/T-0014.md` (Unlisted, 12:50Z). | assumed |
| 12 | A naive `expires_at` is refused by the ladder itself, with a message naming the verifier as its source. It never assumes UTC: a wrong guess would enforce expiry at the wrong instant rather than failing. Added by execution 2026-09-09 — see `logs/T-0014.md` (Unlisted, 12:52Z). | assumed |
| 13 | Departs from §2 on which sources ship enabled, which row 2 does not cover — row 2 locks the ladder's **order**, and that is honored. The cookie source is first in the order and **off until named**; the header rung carries the default, being the one rung that needs no perimeter. §2's default cookie name would otherwise make cookie mode the shipped default, which §3 then refuses to construct without the attestation — so the resolver's own defaults would raise for every caller. Naming the cookie is now the same act as attesting the allowlist. Added by execution 2026-09-09 — see `logs/T-0014.md` (Departures, 13:20Z). | assumed |
| 14 | Departs from §2's numbering: the reauth payload rung precedes the three request rungs. A payload that lost to the cookie could never refresh a cookie-mode connection — the stale cookie would answer every reauth — and Socket.IO carries its connect credential in that same payload. §2's order governs the **request** sources, which is what "cookie-first" is about, and a payload carrying no token falls through to them. Added by execution 2026-09-09 — see `logs/T-0014.md` (Departures, 13:22Z). | assumed |
