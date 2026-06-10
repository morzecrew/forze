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
