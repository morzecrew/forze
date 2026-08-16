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
from forze_fastapi.security import AuthnCookieCarrier

cookies = AuthnCookieCarrier(access_path="/api", refresh_path="/auth/refresh")
attach_authn_routes(router, registry=registry, ctx_dep=ctx_dep, cookies=cookies)
```

Login and refresh then set and rotate two `HttpOnly` cookies and strip the token strings from the body (scheme and lifetimes stay); refresh falls back to its cookie; logout expires both idempotently. Point `CookieTokenAuthn(cookie_name=...)` at the same `access_cookie`.

Two things break cookie mode if you skip them:

- **`SecurityContextMiddleware(anonymous_paths={"/auth/login", "/auth/refresh"})`** — otherwise a stale access cookie 401s the exact route that would replace it. On those paths an authentication-kind failure binds no identity instead of refusing; other failure kinds still error.
- **CSRF posture** — cookies are `HttpOnly` always and `Secure` by default, and `samesite="lax"` (the default) is the shipped CSRF defense. A `samesite="none"` deployment must add its own CSRF layer; the carrier ships none.

### Principal eligibility

Every wired route checks the authenticating principal against an active `policy_principal` document. A token-only service with no authz plane opts out explicitly with `AuthnDepsModule(eligibility="allow_all")` — a declared decision (an unknown value is refused at wiring), and one that moves revocation entirely onto credential lifecycle: with it, deactivating a principal no longer blocks token issuance.

## Anti-patterns

- **Binding identity inside route handlers** — let `SecurityContextMiddleware` bind at the boundary.

## Reference

- [Authn, authz, tenancy (FastAPI) recipe](https://morzecrew.github.io/forze/latest/recipes/authn-authz-tenancy-fastapi/)
