---
title: Authn, authz & tenancy
icon: lucide/shield-check
summary: Separate boundary authentication, tenant context, and per-operation authorization in a FastAPI app
---

Three distinct concerns, three distinct seams:

- **Authentication** — *who is calling* — happens at the HTTP boundary, in
  middleware.
- **Tenancy** — *whose data* — is resolved at the boundary and bound into the
  context.
- **Authorization** — *may they do this* — is enforced per-operation, inside the
  application.

Forze keeps them separate. The conceptual model is in
[Identity & access](../in-depth/identity.md); this recipe is the wiring.

## The shape

There is **no `Depends(...)` for the current user**. The
`SecurityContextMiddleware` authenticates the request and binds the principal +
tenant into the execution context (`ctx.inv_ctx`). Your handlers run an operation
through the registry, and the operation's **hooks** read that binding and
enforce. A failed check raises `exc.authentication` / `exc.authorization`, which
the exception handlers map to `401` / `403`.

## Install the middleware

`SecurityContextMiddleware` authenticates; `InvocationMetadataMiddleware` binds
request ids and the idempotency key. Both take a `ctx_dep` that returns the
per-scope context:

```python
from forze.application.contracts.authn import AuthnSpec
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.middlewares import InvocationMetadataMiddleware, SecurityContextMiddleware
from forze_fastapi.security import AuthnRequirement, HeaderTokenAuthn

API = AuthnSpec(name="api", enabled_methods=frozenset({"token"}))

app = FastAPI()
register_exception_handlers(app)  # exc.authentication → 401, exc.authorization → 403

app.add_middleware(
    SecurityContextMiddleware,
    ctx_dep=lambda: runtime.get_context(),
    authn=AuthnRequirement(
        ingress=(HeaderTokenAuthn(authn_spec=API, header_name="Authorization"),),
    ),
    when_multiple_credentials="first_in_order",
)
app.add_middleware(InvocationMetadataMiddleware, ctx_dep=lambda: runtime.get_context())
```

Ingress options are `HeaderTokenAuthn`, `HeaderApiKeyAuthn` (`header_name`), and
`CookieTokenAuthn` (`cookie_name`) — each carries the `AuthnSpec` whose `name`
selects the route's verifier and resolver.

## Wire the planes

Three sibling deps modules. `AuthnDepsModule` registers the verify-then-resolve
stack; `AuthzDepsModule` the decision + scope ports; `TenancyDepsModule` the
tenant resolver. The `"api"` route name is the same string across all three:

```python
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig
from forze_identity.authz import AuthzDepsModule, AuthzKernelConfig
from forze_identity.tenancy import TenancyDepsModule

deps = DepsRegistry.from_modules(
    AuthnDepsModule(
        kernel=AuthnKernelConfig(access_token_secret=secret),  # bytes, ≥ 32
        authn={"api": frozenset({"token"})},
    ),
    AuthzDepsModule(kernel=AuthzKernelConfig(), decision={"api"}, scope={"api"}),
    TenancyDepsModule(tenant_resolver={"api"}),
)
```

The authn stack is **document-backed** — the account/session stores are document
specs you wire to a database (see the [authn integration](../integrations/authn.md)).

## Enforce on operations

Attach hooks to the operations that need them, using the same registry-binding
chain as [idempotency](add-idempotency.md). `AuthnRequired` and `TenantRequired`
are **before-hooks** (`.before`); the authz scope filter is a **wrap**
(`.wrap`):

```python
from forze.application.contracts.authz import AuthzSpec
from forze.application.hooks.authn import AuthnRequired
from forze.application.hooks.authz import AuthzBeforeAuthorize, AuthzDocumentScopeWrap
from forze.application.hooks.tenancy import TenantRequired

AUTHZ = AuthzSpec(name="api")
CREATE = ORDER_SPEC.default_namespace.key(DocumentKernelOp.CREATE)
LIST = ORDER_SPEC.default_namespace.key(DocumentKernelOp.FIND_MANY)

registry = (
    build_document_registry(ORDER_SPEC, DocumentDTOs(read=ReadOrder, create=CreateOrder))
    .bind(CREATE)
        .bind_outer()
        .before(
            AuthnRequired().to_step(),                                  # step_id "authn.principal"
            TenantRequired().to_step(step_id="tenant.required"),
            AuthzBeforeAuthorize(spec=AUTHZ, action="orders:create").to_step(step_id="authz.create"),
        )
        .finish(deep=True)
    .bind(LIST)
        .bind_outer()
        .before(AuthnRequired().to_step(), TenantRequired().to_step(step_id="tenant.required"))
        .wrap(AuthzDocumentScopeWrap(spec=AUTHZ, document_name="orders", operation="find_many").to_step(step_id="authz.scope"))
        .finish(deep=True)
    .freeze()
)
```

- **`AuthnRequired`** demands a principal; **`TenantRequired`** demands a bound
  tenant — both raise `401`/`403` when missing.
- **`AuthzBeforeAuthorize`** asks the decision port whether the subject may
  perform `action`; a deny raises `403`.
- **`AuthzDocumentScopeWrap`** asks the scope port for a row filter and **merges
  it into the query** ($and) before the list runs — so a caller only ever sees
  rows they're entitled to. Authorization scoping *is* [query-DSL](../reference/query-syntax.md)
  filter injection.

## HTTP login endpoints

The login flows themselves come for free: `build_authn_registry` registers the
password-login, refresh, logout, change-password, password-reset, and
deactivate operations, and `attach_authn_routes` projects them onto a router —
`POST /auth/login`, `/auth/refresh`, `/auth/logout`, `/auth/change-password`,
`/auth/password-reset/request`, `/auth/password-reset/confirm`,
`/auth/deactivate`:

```python
from fastapi import APIRouter
from forze_fastapi.routes import attach_authn_routes
from forze_kits.aggregates.authn import build_authn_registry

AUTH = AuthnSpec(name="api", enabled_methods=frozenset({"password", "token"}))

auth_router = APIRouter(prefix="/auth", tags=["auth"])
attach_authn_routes(
    auth_router,
    registry=build_authn_registry(AUTH).freeze(),
    ns=AUTH.default_namespace,
    ctx_dep=lambda: runtime.get_context(),
)
app.include_router(auth_router)
```

```bash
curl -X POST /auth/login -d '{"login": "alice", "password": "…"}'
# → {"access_token": "…", "refresh_token": "…", "access_token_type": "Bearer", …}
curl -X POST /auth/logout -H "Authorization: Bearer <access_token>"   # → 204
```

`/login` and `/refresh` are deliberately reachable **without** a bearer token —
the operations authenticate via their request bodies. `/logout` and
`/change-password` answer `401` on their own when the middleware bound no
identity. `/deactivate` (`deactivate_principal`) is the exception: it ships
**unguarded** — bind `AuthnRequired` plus an `AuthzBeforeAuthorize` on that
operation (the same chain as [above](#enforce-on-operations)) before exposing
it, or keep it off the router with `include=`.

## Self-service password reset

The reset pair is also part of the registry. `/password-reset/request` answers
a **uniform 202** for known and unknown logins alike (no account enumeration)
and never returns the token; `/password-reset/confirm` consumes the single-use
token (1 hour TTL by default), sets the new password, and revokes **all** of
the principal's sessions — the same "log out everywhere" cascade as
change-password. Any bad token — wrong, expired, used, superseded — is a
uniform `401`.

Wiring needs two things on top of the login stack: the reset pepper on the
kernel, and the `password_reset` route set:

```python
AuthnDepsModule(
    kernel=AuthnKernelConfig(
        access_token_secret=secret,
        refresh_token_pepper=refresh_pepper,
        password=PasswordConfig(),
        reset_token_pepper=reset_pepper,  # bytes, ≥ 32 — separate from invite_token_pepper
    ),
    authn={"api": frozenset({"password", "token"})},
    token_lifecycle={"api"},
    password_reset={"api"},
)
```

Only the token's HMAC digest is persisted (`authn_password_resets`, a
`sensitive` document spec like the other credential stores); issuing a new
reset supersedes the previous outstanding one (single active reset per
principal).

**Delivery — getting the token to the user.** The raw token must reach the
account holder out of band, never via the HTTP response. The registry has an
outbox seam for exactly this:

```python
from forze.application.contracts.outbox import OutboxSpec
from forze_kits.aggregates.authn import AuthnPasswordResetRequestedPayload

RESET_EVENTS = OutboxSpec(
    name="authn_events",
    codec=PydanticModelCodec(AuthnPasswordResetRequestedPayload),
    destination=OutboxDestination.queue(route="jobs", channel="notify"),
)

registry = build_authn_registry(AUTH, reset_events=RESET_EVENTS).freeze()
```

With `reset_events` set, a successful request stages an
`authn.password_reset_requested` integration event (payload: `login`,
`principal_id`, raw `token`, `expires_at`). From there it is the standard
outbox → relay → notify pipeline: relay the route to your queue and map the
event to an e-mail/SMS command in your notify consumer (a `NotificationRouter`
event-mapper turns the payload into a message embedding the reset link).
Unknown logins stage nothing — the uniform ack is all an outside observer ever
sees.

Two caveats, by design:

- The raw token transits the outbox row. The 1-hour TTL and single-use
  semantics bound the exposure, but treat the outbox store like the credential
  stores (and keep its retention tight). Apps wanting zero persistence of the
  raw token skip `reset_events` and call `ctx.authn.password_reset(spec)` from
  a custom handler that hands the token straight to a mailer.
- Without `reset_events` **or** a custom delivery handler, requesting a reset
  mints a token nobody ever receives — wire one of the two before exposing the
  route. And rate-limit `/password-reset/request` at the edge: it is an
  unauthenticated write.

## Tenant selector ("switch organization")

When a principal belongs to several tenants, ship a switcher with one extra projector — no
custom code. `build_tenancy_registry(spec)` reuses the same authn `spec` (so it can re-mint
tokens through that route's lifecycle) and gives two operations that `attach_tenancy_routes`
projects:

- `GET /tenants` — the principal's active memberships (`tenant_id`, `tenant_key`, `is_current`).
- `POST /tenants/{id}/activate` — validates the choice against membership via the
  `TenantResolverPort`, then returns a **new token pair scoped to the selected tenant** (the
  same body as `/login`); the client swaps to the new token.
- `DELETE /tenants/{id}` — leave a tenant. Keyed on the bound principal, so a caller can only
  drop their *own* membership; leaving the tenant on the current token makes its `tid` stop
  matching a live membership, so the next request fails closed (switch or re-authenticate).

```python
from forze_fastapi.routes import attach_tenancy_routes
from forze_kits.aggregates.tenancy import build_tenancy_registry

attach_tenancy_routes(
    router,
    registry=build_tenancy_registry(spec).freeze(),
    ns=spec.default_namespace,
    ctx_dep=ctx_dep,
)
```

The active tenant rides the signed `tid` claim and is re-validated against live membership on
every request, so removing a principal from a tenant invalidates their scoped token at once.
Requires a `TenancyDepsModule` wiring a `tenant_resolver` **and** `tenant_management` route.

## Tenant administration

The selector is **self-service** — every op acts on the caller's *own* membership. Managing
*other* tenants and members (create an org, invite/remove a member, list members, deactivate a
tenant) is the privileged inverse, in a separate aggregate:
`build_tenancy_admin_registry(ns)` → `attach_tenancy_admin_routes` (`POST /tenants`,
`GET /tenants/{id}/members`, `POST /tenants/{id}/deactivate`, `POST`/`DELETE /memberships`).

Because *who* may administer a tenant is your authorization model — not something the framework
can define — these ops ship **unguarded**, exactly like `/deactivate`. Bind `AuthnRequired` plus
an `AuthzBeforeAuthorize` on each operation before exposing the router (or keep ops off it with
`include=`):

```python
from forze.application.hooks.authn import AuthnRequired
from forze.application.hooks.authz import AuthzBeforeAuthorize
from forze_fastapi.routes import attach_tenancy_admin_routes
from forze_kits.aggregates.tenancy_admin import TenancyAdminKernelOp, build_tenancy_admin_registry

reg = build_tenancy_admin_registry(spec.default_namespace)
for op in TenancyAdminKernelOp:
    reg = (
        reg.bind(spec.default_namespace.key(op))
        .bind_outer()
        .before(
            AuthnRequired().to_step(),
            AuthzBeforeAuthorize(spec=AUTHZ, action=f"tenants:{op}").to_step(),
        )
        .finish(deep=True)
    )

admin_router = APIRouter(prefix="/admin", tags=["tenant-admin"])
attach_tenancy_admin_routes(
    admin_router, registry=reg.freeze(), ns=spec.default_namespace, ctx_dep=ctx_dep
)
```

`list_members` returns principal ids only (`TenantManagementPort.list_tenant_principals`); join
them with identity-plane details out of band.

## Notes

- **Tenant binding.** The tenant comes from the verified credential's issuer hint
  or, with `trust_tenant_header=True`, an `X-Tenant-Id` header. A bound tenant
  flows into every authz check via the operation's scope.
- **Ordering is enforced.** `AuthzBeforeAuthorize` declares
  `requires=("authn.principal",)`, so the principal is always resolved before the
  authorization decision.
- This is the foundation the other identity recipes build on — see
  [Local identity](local-identity.md), [External bootstrap → Forze JWT](external-bootstrap-forze-jwt.md),
  and [Social sign-in](social-sign-in.md).
