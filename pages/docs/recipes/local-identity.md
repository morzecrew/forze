# Local identity (demo / MVP)

File- or env-backed API key authentication and principal→tenant mapping for **local development and demos only**. Not for production: no key rotation, audit trail, or revocation.

For production, use document-backed [`forze_identity.authn`](../reference/authentication.md) (`HmacApiKeyVerifier`) and [`forze_identity.tenancy`](../concepts/multi-tenancy.md) (`TenantResolverAdapter`).

## Configuration

Define a JSON file or inline env var:

| Variable | Purpose |
|----------|---------|
| `FORZE_IDENTITY_LOCAL_FILE` | Path to a JSON identity file |
| `FORZE_IDENTITY_LOCAL_CONFIG` | Inline JSON (same shape as the file) |

Example file:

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

Load in Python:

```python
from forze_identity.builtin.local import LocalIdentityConfig, from_json_path, from_env

config = from_json_path("identity.local.json")
# or: config = from_env()
```

## Wiring

One-shot merge into kernel `Deps`:

```python
from forze_identity.builtin.local import local_identity_deps

deps = local_identity_deps(config)
kernel_deps = kernel_deps.merge(deps)
```

Manual wiring (same behavior):

```python
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig
from forze_identity.builtin.local import (
    ConfigurableLocalApiKeyVerifier,
    ConfigurableLocalTenantResolver,
)
from forze_identity.tenancy import TenancyDepsModule

authn = AuthnDepsModule(
    kernel=AuthnKernelConfig(),
    authn={"main": frozenset({"api_key"})},
    api_key_verifiers={
        "main": ConfigurableLocalApiKeyVerifier(config=config),
    },
)()

tenancy = TenancyDepsModule(
    tenant_resolver={"main"},
    tenant_resolvers={
        "main": ConfigurableLocalTenantResolver(config=config),
    },
)()
```

Register an `AuthnSpec` with `enabled_methods=("api_key",)` and enable API key ingress on FastAPI routes (see [Authn, authz, and tenancy (FastAPI)](authn-authz-tenancy-fastapi.md)).

## Components

| Piece | Module |
|-------|--------|
| Config model | `forze_identity.builtin.local.LocalIdentityConfig` |
| API key verifier | `forze_identity.builtin.local.LocalApiKeyVerifier` |
| Tenant resolver | `forze_identity.builtin.local.LocalTenantResolver` |
| Issuer label | `forze:local_api_key` (distinct from document-backed `forze:api_key`) |

Principal resolution reuses `JwtNativeUuidResolver` because the local verifier sets `subject` to the canonical principal UUID string.

## Limits

- Keep the number of API keys small (linear scan with constant-time compare per entry).
- Do not commit real secrets; use env-specific files ignored by git.
- This is separate from [`forze_kits.secrets`](../reference/contracts.md) (`SecretsPort` resolves DSNs and peppers, not caller identity).
