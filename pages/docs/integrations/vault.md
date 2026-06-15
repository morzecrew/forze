---
title: Vault
icon: lucide/lock
summary: Resolve secrets from HashiCorp Vault (KV v2)
---

`forze[vault]` implements the secrets contract against HashiCorp Vault (KV v2).
It supplies a `SecretsPort` so the rest of Forze — especially the **routed,
per-tenant clients** — can resolve credentials by reference instead of holding
them in config.

## Install

```bash
uv add 'forze[vault]'
```

Needs a Vault server with a KV v2 mount.

## Wire it

Build a client with its config, register the deps module (which publishes a
`SecretsPort`), and wire the lifecycle step:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_vault import VaultClient, VaultConfig, VaultDepsModule, vault_lifecycle_step

vault = VaultClient(config=VaultConfig(url="https://vault.example.com", token="…"))

deps = DepsRegistry.from_modules(VaultDepsModule(client=vault))
lifecycle = LifecyclePlan.from_steps(vault_lifecycle_step())
```

## What it provides

| Contract | Implementation | Dep key |
|----------|---------------|---------|
| Secrets (`resolve_str`, `exists`) | `VaultKvSecrets` (KV v2) | `SecretsDepKey` |
| Raw client | `VaultClient` | `VaultClientDepKey` |
| Key management (envelope encryption) | `VaultTransitKeyManagement` (Transit) | `KeyManagementDepKey` |
| Per-tenant KEK provisioning | `VaultTransitTenantProvisioner` (Transit) | via `TenantProvisionerPort` |
| Token signing (RS256 / ES256) | `VaultTransitSigner` (Transit) | via the identity authn signer |

## Notes

- **KV v2 only.** The mount is set once on `VaultConfig.mount_point`; a
  `SecretRef.path` is mount-relative.
- The client needs the lifecycle step — `VaultDepsModule` only registers an
  already-constructed client; it doesn't initialize it.
- This is what powers per-tenant secret routing (`secret_ref_for_tenant`) for the
  routed Postgres/Mongo/HTTP/… clients — see
  [Multi-tenancy](../in-depth/multi-tenancy.md).
- **Transit** is a separate mount from KV. `VaultTransitKeyManagement` is the KMS
  backend for [envelope encryption](../in-depth/encryption.md) (the KEK never
  leaves Vault), `VaultTransitTenantProvisioner` creates a tenant's Transit key
  on onboarding, and `VaultTransitSigner` signs JWTs (RS256/ES256) without the
  private key leaving Vault.
