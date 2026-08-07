# RFC 0024 — Outbound OAuth2: authorization-code acquisition + token-endpoint exchanger

- **Status:** 📝 Draft (demand-gated on a first consumer — Linecust's Ingestion service is the named candidate)
- **Scope:** Close the one seam the rotating credential store deliberately left to the app: the **initial acquisition** of an outbound OAuth2 credential (the app acting as a *client* to a third-party SaaS, to pull the user's data). Provides the provider-agnostic OAuth2 authorization-code machinery that produces the first `ExchangedCredential` and hands it to the rotating store's `put` — and, from the *same* provider config, the `CredentialExchangerPort` that the proactive-refresh sweeper drives. Two grant types, one token-endpoint client. Layered: a **pure** authorize-URL primitive in `forze_identity/oauth/` (HTTP-free, direction-neutral, joins `pkce.py`/`state.py`); the **I/O** token client + rotating-store handoff in `forze_kits/integrations/secrets/` (co-located with the rotator and sweeper). The provider catalog, connect UI, and per-source quirks stay app-level (§6).
- **Related:** the rotating credential store (the storage + the `put` this feeds; the `CredentialExchangerPort` this implements). The proactive credential-refresh sweeper (drives the exchanger this ships). The secrets lifecycle plane (`SecretRef`, versioning). `forze_identity/oauth/{pkce,state}.py` (the primitives; their "session is the storage" doctrine is inherited). `forze_http` (the token-endpoint transport; the RFC 0022 sensitive-egress gate applies to it). RFC 0009 (external IdP — the **inbound** twin; §1 draws the line so they are never conflated, and §2 shares the pure authorize primitive between them). The social sign-in recipe's callback-hardening checklist (the security baseline §5 reuses).
- **Origin:** The Linecust gap analysis's mitigable-gap #3 (2026-07-31, re-reviewed 2026-08-01): "connect your Bitrix24 / ad account" flows are hand-built. Review found the *storage + refresh* half already shipped (the rotating store and its sweeper) and the *acquisition* half genuinely missing — the store says in as many words that "the app writes the initial `put`." Hand-rolling that front door means re-deriving, per app: the token-endpoint POST, the callback CSRF check, PKCE threading, single-use-code discipline, and — the sharp one — the persist-before-observe handoff that the rotating store built its whole contract around. It is the same crash-consistency class, one step earlier in the credential's life.

---

## 1. The gap, and the direction that defines it

The rotating credential store owns an outbound credential's *life* — store it, serialize refresh, survive counterparty rotation, burn it on rejection — but not its *birth*. Birth is the OAuth2 authorization-code flow: redirect the user to the provider, receive a `code` on the callback, exchange it at the token endpoint for the first `{access, refresh}` pair, and `put` that pair into the store. Today an app writes all of it by hand.

**The direction is the whole point, and it is the opposite of `forze_identity`'s existing OAuth.** Two flows share the OAuth2 *mechanic* (authorize redirect → code → token exchange) but use the result for opposite purposes:

| | Inbound (exists: `forze_identity` OIDC, RFC 0009) | Outbound (this RFC) |
|---|---|---|
| The app is | a *relying party* — someone logs **in** | a *client* — the app calls a SaaS **out** |
| The token is used to | validate an `id_token` → establish a session | authenticate outbound data-pull calls |
| The result lands in | a principal/session (identity plane) | the rotating credential store (secrets plane) |
| Lifecycle after | short session + refresh-token family | long-lived grant, counterparty-rotated (rotating store + sweeper) |

Conflating them is the trap: a "we already have OAuth" reflex points at the login path, which cannot store a per-tenant SaaS grant or feed the refresh sweeper. This RFC is the outbound client; §2 shares only the one genuinely-common, purpose-neutral primitive between the two so the OAuth2 mechanic is not duplicated.

## 2. Layering — pure primitive shared, I/O in the kit

Three seams, placed by dependency and by direction-neutrality:

1. **`forze_identity/oauth/authorize.py` — `build_authorize_url(...)` (pure, HTTP-free, direction-neutral).** Assembles the authorization-endpoint URL from client id, redirect URI, scopes, `state` (`generate_state`), and the PKCE `code_challenge` (`generate_pkce`), plus arbitrary extra provider params. No I/O, no storage — it returns a string and leaves the verifier+state for the caller to stash in the session (the inherited "session is the storage" doctrine). Lives beside `pkce.py`/`state.py` because it is the same layer, and it is **shared with the inbound OIDC login path** (login builds the same URL; only what it does with the callback differs) — so the OAuth2 authorize mechanic is written once. `forze_identity` stays HTTP-free; this adds no dependency.
2. **`forze_kits/integrations/secrets/oauth_client.py` — `OAuth2TokenClient` (the I/O).** The token-endpoint client over `forze_http`. One object per provider config, two grant types:
   - `exchange_code(*, code, code_verifier, redirect_uri) -> ExchangedCredential` — `grant_type=authorization_code`, the acquisition call.
   - `refresh(ref, *, refresh_token, metadata) -> ExchangedCredential` — `grant_type=refresh_token`, **this method is the `CredentialExchangerPort`** the proactive-refresh sweeper and the rotating store's on-demand `refresh` drive. Same endpoint, same client credentials, same error mapping.
   
   Lives in kits (not `forze_identity`) because it does HTTP and hands off to the rotating store — the I/O layer composes the plane primitives, keeping identity dependency-free. Co-located with the rotator/sweeper so the whole outbound-credential lifecycle (acquire → store → auto-refresh) sits in one place.
3. **`forze_kits/integrations/secrets/oauth_acquire.py` — the callback handoff.** `complete_authorization(store, client, ref, *, code, code_verifier, redirect_uri)` = `exchange_code` then `store.put` — the persist-before-observe unit (§4). A forze_fastapi *helper* (not a generated route) does the session `state` check → `complete_authorization`; the app owns the actual route (§6).

Provider config (kit-level, one per source integration):

```python
OAuth2ProviderConfig(
    authorization_endpoint: str,
    token_endpoint: str,
    client_id: str,
    client_secret: SecretRef | None,   # confidential client; None = public (PKCE-only)
    scopes: tuple[str, ...],
    use_pkce: bool = True,             # OAuth 2.1 default; defense-in-depth even for confidential clients
    audience: str | None = None,       # providers that require it
    extra_authorize_params: Mapping[str, str] = {},
    token_auth: Literal["basic", "post"] = "post",  # client-secret placement at the token endpoint
)
```

## 3. One client, both grant types — the unification that earns the kit

The compelling shape: acquisition and refresh are the *same token-endpoint call* with a different `grant_type`. So `OAuth2TokenClient` serves both, and its `refresh` **is** the `CredentialExchangerPort` — meaning a single provider config wires the entire outbound lifecycle:

```
build_authorize_url ──▶ (user consents) ──▶ callback: exchange_code ──▶ store.put   [acquisition, this RFC]
                                                                          │
store.get / store.refresh(observed=…) ◀── OAuth2TokenClient.refresh ◀────┘          [refresh, rotating store]
CredentialSweeper ──────────────────────▶ OAuth2TokenClient.refresh                  [proactive refresh]
```

Before this RFC, an app implemented `CredentialExchangerPort` by hand (a token-endpoint POST) *and* hand-wrote acquisition separately, duplicating the same HTTP. After it, both are the one client. This is why it is a kit and not two recipes: the value is the single token-endpoint client that closes the loop the rotating store and its sweeper left open at both ends.

## 4. Crash-consistency — the same class as the rotating store, one step earlier, and honestly milder

The acquisition handoff has the rotating store's persist-before-use shape: `exchange_code` consumes a single-use authorization code and yields a token pair that is not yet durable; a crash between the exchange and the `put` loses it. So `complete_authorization` must **persist before it returns** — no caller observes a credential that is not already in the store — and map a post-exchange persist failure loudly, not as a retryable error.

But the honest distinction from the rotating store's refresh path, which changes the stakes and must be stated so an implementer calibrates correctly: **an acquisition-window crash is benign — the user re-clicks "connect."** The wasted artifact is an authorization code (cheap, re-mintable by re-consenting), not a live refresh token whose loss bricks an idle grant. So the kit still persists-before-observe (correctness), but it does *not* need the rotating store's heavy single-flight-and-burn machinery for acquisition (a second consent simply issues a fresh code; there is no token family to revoke by racing). The RFC records this explicitly: acquisition = persist-before-observe + re-consent on failure; refresh = the full rotating-store serialize-and-burn discipline. Do not copy the latter onto the former.

## 5. Callback security — the classic attack surface, hardened by checklist

The callback is where OAuth clients get compromised. The kit's callback helper enforces, and the docs reproduce as a checklist (reusing the social sign-in recipe's baseline):

- **`state` first, constant-time.** Compare the returned `state` with the session value via `hmac.compare_digest` **before anything else**; mismatch or absence → reject. CSRF on the callback.
- **PKCE verifier from the session, never the request.** The `code_verifier` comes from where the authorize step stashed it; a request-supplied verifier is ignored.
- **`redirect_uri` exact-match.** The value sent to the token endpoint must equal the registered, pre-configured URI — never reflected from the request.
- **Provider error responses are failures, not successes.** `error=access_denied|consent_required|…` on the callback is handled and surfaced, never mistaken for a code.
- **Single-use code.** Exchanged exactly once; a replayed callback finds the session state already consumed and is rejected.
- **`client_secret` from secrets.** Resolved from `SecretRef` at call time, placed per `token_auth`, never in a URL, log, or the `repr`. Prompts/codes and the token leave the trust boundary → the RFC 0022 `egress_sensitive` gate marks the token-endpoint route.
- **Scope-downgrade visibility.** Granted scope (from the token response) is compared to requested; a downgrade is surfaced in `metadata`, not silently accepted (a source connected with fewer scopes than needed should be a visible, not a mysterious-later-failure, condition).
- **Open-redirect prevention.** Any post-callback app redirect target is validated against an allowlist — the helper refuses to bounce to an arbitrary returned URL.

## 6. What stays app-level (the direction-specific and catalog concerns)

- **The provider catalog** — which SaaS, their endpoints, default scopes, quirks. `OAuth2ProviderConfig` is populated per source integration; forze ships no catalog of providers (that is the "Sources" component of the app, and it is where provider-specific knowledge belongs and changes fastest).
- **The connect UI and routes** — the "Connect Bitrix24" button, the authorize redirect, the success/failure pages, and the mapping of a connection to a `SecretRef` (typically `(tenant, provider, account)`). The kit provides the *callback logic* as a helper the app's route calls; the route itself is app-owned because its redirects and source-id mapping are app concerns.
- **Per-provider token-response quirks** — non-standard fields, rotating vs non-rotating refresh tokens, absolute-vs-relative expiry. The client handles RFC 6749-standard responses; a provider that deviates gets a thin per-provider adapter in the app (or a follow-up preset, §9).
- **Non-OAuth source auth** (static API keys, basic auth) — out of scope; those use the static `SecretsPort` / rotating store `put` directly with no acquisition flow.

## 7. Tenancy

Acquisition runs inside a request bound to the connecting user's tenant (Linecust's *project = tenant*), so the callback executes under that tenant's context and `complete_authorization` `put`s into that tenant's `(tenant, ref)` slot — the rotating store's ambient per-tenant scoping applies unchanged, and no cross-tenant reach is expressible. The `SecretRef` scheme is the app's (§6), but the tenant binding is the framework's, fail-closed as everywhere else.

## 8. DST, mock, and battery

- The token-endpoint call is `forze_http`, already mockable; a `MockOAuth2TokenClient` returns a scripted `ExchangedCredential` (or raises `INVALID_GRANT_CODE` / a transient error) so acquisition and refresh paths are deterministic and DST-legal. Conformance tests the *seam* (persist-before-observe, state rejection, error mapping), never a real provider.
- `build_authorize_url` is pure → a straightforward unit (correct params, PKCE challenge present, state embedded, extra params merged).

Battery:
1. Happy path: `build_authorize_url` → scripted callback → `exchange_code` → `put`; `store.get` returns the access token, refresh held internal. *(mock ≡ real token endpoint via forze_http test leg)*
2. Persist-before-observe: exchange succeeds, `put` fails → the helper raises loudly (not a silent retryable), no half-stored credential observable; re-running the connect flow from a fresh code succeeds. *(unit + DST crash-injection)*
3. `state` mismatch/absence on the callback → rejected before any exchange; single-use code replay → rejected (state already consumed). *(unit)*
4. Provider `error=access_denied` on callback → surfaced as a connect failure, never exchanged. *(unit)*
5. Unification: the same `OAuth2TokenClient` instance drives acquisition (`authorization_code`) and, as `CredentialExchangerPort`, a subsequent `store.refresh` (`refresh_token`) — one config, both grant types, correct grant_type on each. *(mock ≡ real)*
6. `client_secret` never in URL/log/repr; token-endpoint route carries the `egress_sensitive` marker. *(unit)*
7. Scope downgrade recorded in `metadata`, not swallowed. *(unit)*
8. Tenancy: callback under tenant A `put`s only into A's slot; no cross-tenant path. *(mock ≡ real)*

## 9. Phases

- **P1** — `build_authorize_url` (pure, in `forze_identity/oauth`) + `OAuth2TokenClient` (`exchange_code` + `refresh`=`CredentialExchangerPort`) over `forze_http` + `complete_authorization` handoff + mock + battery 1–6, 8. Ships against Linecust Ingestion.
- **P2** — the forze_fastapi callback helper (state-check → complete → app-redirect with open-redirect guard) + scope-downgrade surfacing (battery 7) + the connect-flow docs recipe/checklist.
- **P3 (demand-gated)** — provider presets (a Google/Bitrix/ad-platform `OAuth2ProviderConfig` + quirk adapter set), mirroring the `forze_identity/builtin/idp` preset pattern; only against named source consumers.

## 10. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Closes the acquisition front door the rotating credential store punted; outbound direction, distinct from `forze_identity`/RFC 0009 inbound login — never conflated | locked |
| 2 | One `OAuth2TokenClient` serves both grant types; its `refresh` **is** the `CredentialExchangerPort` the rotating store and its sweeper drive — a single provider config closes the whole outbound lifecycle. This unification is why it is a kit, not two recipes | locked |
| 3 | Layered: pure `build_authorize_url` in `forze_identity/oauth` (HTTP-free, shared with inbound login); I/O client + handoff in `forze_kits/integrations/secrets` (co-located with rotator/sweeper) | locked |
| 4 | Acquisition = persist-before-observe + re-consent on failure (benign window); it does **not** inherit the rotating store's serialize-and-burn refresh discipline — the honest calibration is recorded so it is not over-copied | locked |
| 5 | Callback hardening is a fixed checklist (state-first constant-time, session-side PKCE, exact redirect_uri, provider-error handling, single-use code, secret-from-secrets, scope-downgrade visibility, open-redirect guard) | locked |
| 6 | Provider catalog, connect UI/routes, `SecretRef` scheme, and non-OAuth source auth stay app-level; the kit ships the callback *logic* as a helper, not a generated route | locked |
| 7 | token-endpoint route marked `egress_sensitive` (RFC 0022 gate) — the token and code cross the trust boundary | locked |
| 8 | Demand-gated on a first consumer (Linecust Ingestion); provider presets are a P3 follow-up against named sources, mirroring `builtin/idp` | recorded |
