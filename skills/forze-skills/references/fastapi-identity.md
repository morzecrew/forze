# FastAPI identity

Binding identity at the HTTP boundary — the middleware, cookie mode for browser clients, and which principals a route accepts. The plane behind it is [authentication](authn.md).

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

### Cookie mode

For browsers that must not hold a token in JavaScript, pair the inbound `CookieTokenAuthn` with the outbound `AuthnCookieCarrier`:

```python
from fastapi import APIRouter
from forze_fastapi.routes import attach_authn_routes
from forze_fastapi.security import AuthnCookieCarrier

router = APIRouter(prefix="/api/auth")
cookies = AuthnCookieCarrier(access_path="/api", refresh_path="/api/auth")
attach_authn_routes(router, registry=registry, ctx_dep=ctx_dep, cookies=cookies)
```

Login and refresh then set and rotate two `HttpOnly` cookies and strip the token strings from the body (scheme and lifetimes stay); refresh falls back to its cookie for no body or `{}`; logout revokes every session of the principal its access cookie's identity or, without one, its refresh cookie names, and expires both idempotently. Scope `refresh_path` to cover refresh and logout (as above) so logout still revokes after the access cookie expires; `runtime_lifespan` refuses an app where the refresh cookie's path misses logout. No single cookie path covers only `/refresh` and `/logout`: with `refresh_path="/api/auth"` the browser sends the refresh cookie to every authn route under that prefix, and only refresh and logout read it. Point `CookieTokenAuthn(cookie_name=...)` at the same `access_cookie`. `ns=` may be omitted: the routes use the registry's one authn namespace (`spec.default_namespace`).

Two things break cookie mode if you skip them:

- **`SecurityContextMiddleware(anonymous_paths={"/auth/login", "/auth/refresh", "/auth/password-reset/request", "/auth/password-reset/confirm"})`** (plus `/auth/logout` in cookie mode, as mounted) — a request no ingress authenticates is refused by default (`AuthnRequirement(required=True)`, except for a real CORS preflight), and a stale access cookie would 401 the exact route that replaces it. On those paths a missing credential or an authentication-kind failure binds no identity instead of refusing; other failure kinds still error. With the middleware on an app mounted under a prefix (`parent.mount("/v1", app)`), an entry may also be relative to the mount (`/auth/login` for `/v1/auth/login`).
- **CSRF posture** — cookies are `HttpOnly` always, `Secure` and `samesite="lax"` by default. Server-side, `CookieTokenAuthn`'s `CookieCsrf` gate (on by default) makes an unsafe request using the access cookie, and every cookie-mode login, logout and refresh prove a same-host or allowlisted `Origin` (or `Referer`). List cross-origin frontends in `CookieCsrf(allowed_origins={...})`; `csrf=None` opts out of all of them, for a deployment with its own CSRF layer, but only when every `CookieTokenAuthn` reading the access cookie declares it.

### Principal eligibility

Every wired route checks the authenticating principal before it authenticates, issues or changes a credential. `AuthnDepsModule(eligibility=...)` chooses the check:

- `"policy_principal"` (default): an active `policy_principal` document, the authz plane's registry.
- A gate factory `(ctx, spec) -> PrincipalEligibilityPort` over your own registry, such as a users table. It must refuse a principal it does not know with `authentication`, read state that is not tenant-aware, and lock the row when asked `for_update` inside a transaction. It reads on every check; only the default gate skips after a fast-path admission.
- `"allow_all"`: nobody is checked. Revocation then rests on credential lifecycle alone: deactivating a principal no longer blocks token issuance. It warns when constructed, and under a `ProductionPosture` each route refuses the boot unless `Exempt(target="authn_eligibility:<route>", reason=...)` covers it, whether the module or its built `Deps` (e.g. `local_identity_deps`) is passed.

Any other value is refused at construction (`authn_eligibility_invalid`). The default gate with neither an `authz_policy_principals` route nor a plain document fallback refuses the boot (`dependency_requirement_unbound`); so do `principal_deactivation` routes without the authz principal registry on `authz_route`. `local_identity_deps` keeps the default gate where the policy-principal document resolves (the mock's plain fallback counts), and otherwise admits exactly the principals its key file names; pass `eligibility=` to choose.

## Anti-patterns

- **Binding identity inside route handlers** — let `SecurityContextMiddleware` bind at the boundary.

## Reference

- [Authn, authz, tenancy (FastAPI) recipe](https://morzecrew.github.io/forze/latest/recipes/authn-authz-tenancy-fastapi/)
