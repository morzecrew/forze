---
title: Local identity
icon: lucide/key-round
summary: File- or env-defined API keys with tenant mapping — for demos and MVPs, not production
---

Before you have an IdP, you still want auth. **Local identity** authenticates
requests against a static set of API keys defined in a file (or env var), each
mapped to a principal and tenant. It's a shipped preset — zero infrastructure —
and explicitly **not for production** (no rotation, revocation, or audit).

## The key file

A JSON document mapping API keys to principals, with optional per-key tenants:

```json
{
  "api_keys": {
    "dev-service-key": {
      "principal_id": "550e8400-e29b-41d4-a716-446655440000",
      "tenant_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
    }
  },
  "principal_tenants": {},
  "default_tenant_id": null
}
```

Load it from a path or the environment:

```python
from forze_identity.builtin.local import LocalIdentityConfig, from_env, from_json_path

config = from_json_path("identity.local.json")
# or: config = from_env()   # FORZE_IDENTITY_LOCAL_FILE, else FORZE_IDENTITY_LOCAL_CONFIG (inline JSON)
```

## Wire it

`local_identity_deps` registers the API-key verifier and the tenant resolver in
one call:

```python
from forze_identity.builtin.local import local_identity_deps

local = local_identity_deps(config, authn_route="main", tenancy_route="main")

deps = DepsRegistry.from_modules(local)
```

??? note "Wiring it by hand"

    `local_identity_deps` is a thin wrapper over the two planes — drop to this if
    you want to mix local keys with other routes:

    ```python
    from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig
    from forze_identity.builtin.local import (
        ConfigurableLocalApiKeyVerifier, ConfigurableLocalTenantResolver,
    )
    from forze_identity.tenancy import TenancyDepsModule

    authn = AuthnDepsModule(
        kernel=AuthnKernelConfig(),
        authn={"main": frozenset({"api_key"})},
        api_key_verifiers={"main": ConfigurableLocalApiKeyVerifier(config=config)},
    )
    tenancy = TenancyDepsModule(
        tenant_resolver={"main"},
        tenant_resolvers={"main": ConfigurableLocalTenantResolver(config=config)},
    )
    ```

## At the boundary

Local identity uses **API keys**, so the middleware ingress is `HeaderApiKeyAuthn`
(see [Authn, authz & tenancy](authn-authz-tenancy-fastapi.md) for the full
boundary setup):

```python
from forze.application.contracts.authn import AuthnSpec
from forze_fastapi.security import AuthnRequirement, HeaderApiKeyAuthn

MAIN = AuthnSpec(name="main", enabled_methods=frozenset({"api_key"}))
requirement = AuthnRequirement(
    ingress=(HeaderApiKeyAuthn(authn_spec=MAIN, header_name="X-API-Key"),),
)
```

A request carrying `X-API-Key: dev-service-key` is resolved to that principal and
tenant — the same `ctx.inv_ctx` binding every authz/tenancy hook reads.

!!! warning "Demo trust model"

    Keys are compared in a constant-time linear scan against a static table —
    fine for a demo or MVP, but there's no rotation, revocation, or audit trail.
    Move to [first-party tokens](external-bootstrap-forze-jwt.md) or an
    [external IdP](social-sign-in.md) before production.
