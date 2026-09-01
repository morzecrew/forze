---
title: Vault
icon: lucide/lock
summary: Resolve secrets from HashiCorp Vault (KV v2)
---

`forze[vault]` implements the secrets contract against HashiCorp Vault (KV v2).
It supplies a `SecretsPort` so the rest of Forze — especially the **routed,
per-tenant clients** — can resolve credentials by reference instead of holding
them in config.

Works with [OpenBao](https://openbao.org) too: the integration suite passes
unchanged against it (verified on OpenBao 2.6.1 — KV v2, Transit, key
provisioning, JWT signing, token renewal).

## Install

```bash
uv add 'forze[vault]'
```

Needs a Vault server with a KV v2 mount.

### Settings

`VaultSettings` is the mountable form of `VaultConfig`: address, token, mount points and
namespace as a pydantic model. The address must be `https://` unless it is a loopback one
— the token rides on every request and every response carries a secret, so `http://` to
anything but this machine puts both on the wire. See
[connection settings](index.md#connection-settings).

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
| Versioned reads (`resolve_versioned`, `current_version`) | `VaultKvSecrets` (native KV v2 versions) | `SecretsDepKey` |
| Control-plane writes (`put`, rotator-facing) | `VaultKvSecrets` | `SecretsAdminDepKey` |
| Dynamic credentials (leases) | `VaultDynamicSecrets` (database engine) | `SecretsLeaseDepKey` (opt-in) |
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
  [Multi-tenancy](../identity-tenancy-enc/multi-tenancy.md).
- **Transit** is a separate mount from KV. `VaultTransitKeyManagement` is the KMS
  backend for [envelope encryption](../identity-tenancy-enc/encryption.md) (the KEK never
  leaves Vault), `VaultTransitTenantProvisioner` creates a tenant's Transit key
  on onboarding, and `VaultTransitSigner` signs JWTs (RS256/ES256) without the
  private key leaving Vault.
- KV v2 assigns **native version tokens**, so the secrets lifecycle plane
  (watchers, hot reload, the rotator) works over Vault without content hashing —
  and `current_version` is served from KV metadata, never reading the payload.
  See [Credential rotation](../running-in-prod/credential-rotation.md).
- **Dynamic credentials** need the database secrets engine enabled
  (`VaultConfig.database_mount`, default `database`); register
  `VaultDynamicSecrets` via `VaultDepsModule(dynamic_secrets=…)` and pair it with
  the kit lease manager. Where you adopt leases for a backend, short TTLs *are*
  the rotation — skip the rotator there.
