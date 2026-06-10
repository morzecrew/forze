---
title: External bootstrap → Forze JWT
icon: lucide/key-square
summary: Verify an external OIDC token once at login, then issue first-party Forze JWTs for the session
---

You want users to sign in with an external IdP, but you don't want every API
request to depend on that IdP's latency and availability. The **bootstrap**
pattern: at login, verify the external OIDC token *once* and mint a first-party
**Forze JWT**; every steady-state request then verifies the fast, local token.

This is the hub for all [social sign-in](social-sign-in.md) — Google, VK, and
Telegram all plug into the same two-route shape.

## Two routes, two auth routes

- **`/login`** uses the **`bootstrap`** auth route — its verifier checks the
  external IdP's `id_token`.
- **Everything else** uses the **`api`** auth route — its verifier checks
  first-party Forze JWTs, and it owns the token *lifecycle* that mints them.

## Wire both routes

The bootstrap route comes from a [provider preset](social-sign-in.md); the API
route is a first-party `AuthnDepsModule` with signing secrets and
`token_lifecycle` enabled (that's what lets it *issue* tokens):

```python
from forze.application.execution import Deps, DepsRegistry
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig
from forze_identity.builtin.idp.google import GoogleOidcConfig, google_identity_deps

bootstrap = google_identity_deps(
    GoogleOidcConfig(client_id="<google-oauth-client-id>"),
    authn_route="bootstrap",
)

api = AuthnDepsModule(
    kernel=AuthnKernelConfig(
        access_token_secret=access_secret,    # bytes, ≥ 32
        refresh_token_pepper=refresh_pepper,   # bytes, ≥ 32
    ),
    authn={"api": frozenset({"token"})},
    token_lifecycle={"api"},  # this route may issue + refresh tokens
)

deps = DepsRegistry.from_modules(lambda: Deps.merge(bootstrap, api()))
```

The bootstrap route needs no signing secret — it only *verifies* an external
token (it has a `token_verifiers` override), it never mints one.

## The login handler

Resolve the bootstrap orchestrator to verify the external token, then the API
route's token lifecycle to issue the Forze JWTs:

```python
from forze.application.contracts.authn import AccessTokenCredentials, AuthnSpec

BOOTSTRAP = AuthnSpec(name="bootstrap", enabled_methods=frozenset({"token"}))
API = AuthnSpec(name="api", enabled_methods=frozenset({"token"}))


@app.post("/login")
async def login(id_token: str = Body(..., embed=True)) -> dict:
    c = ctx()
    # verify the external id_token → an authenticated identity
    result = await c.authn.authn(BOOTSTRAP).authenticate_with_token(
        AccessTokenCredentials(token=id_token)
    )
    # mint first-party tokens for that identity
    issued = await c.authn.token_lifecycle(API).issue_tokens(result.identity)
    return {
        "access_token": issued.access.token.token,
        "refresh_token": issued.refresh.token.token if issued.refresh else None,
    }
```

`authenticate_with_token` takes only the credentials (the route is fixed when the
orchestrator is resolved) and returns an `AuthnResult` — pass `result.identity`
to `issue_tokens`. The raw JWT string is `issued.access.token.token`.

## Steady state

Every other route is protected by the **`api`** `AuthnSpec` through the
[`SecurityContextMiddleware`](authn-authz-tenancy-fastapi.md) — fast, local
verification with no IdP round-trip. The first-party token also carries the
tenant, so [tenancy and authz](authn-authz-tenancy-fastapi.md) work unchanged.

!!! note "Logout is local only"

    Revoking a Forze refresh token ends the Forze session; it does **not** revoke
    the user's access at the external IdP. Enforce eligibility on your side.
