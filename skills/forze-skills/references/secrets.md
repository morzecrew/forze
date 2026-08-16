# Secrets

## Secrets

`SecretsDepKey` registers a `SecretsPort`. `SecretRef` is a logical path, and `resolve_structured()` validates JSON secrets into Pydantic models.

```python
from forze.application.contracts.secrets import SecretRef, SecretsDepKey, resolve_structured

secrets = ctx.deps.provide(SecretsDepKey)
dsn = await resolve_structured(secrets, SecretRef("postgres/main"), PostgresDsnSecret)
```

### Backends

Wire one `SecretsPort` backend for the route. Bundled in `forze_kits` (no extra):

```python
from forze_kits.adapters.secrets import (
    DirectorySecrets,
    EnvSecrets,
    MappingSecrets,
    SecretsDepsModule,
)

secrets_module = SecretsDepsModule(secrets=EnvSecrets())
# DirectorySecrets(root=Path("/etc/secrets")) for file-backed secrets;
# MappingSecrets(data={...}) for in-memory development/tests.
```

For HashiCorp Vault (extra `forze[vault]`), `forze_vault` ships a KV v2 backend that registers `SecretsDepKey` for you:

```python
from forze_vault import VaultClient, VaultConfig, VaultDepsModule, vault_lifecycle_step

vault_module = VaultDepsModule(
    client=VaultClient(config=VaultConfig(url="https://vault:8200", token="...")),
)
# add vault_lifecycle_step() to your LifecyclePlan
```

Use secrets for credentials and routed client configuration; avoid putting secret values in specs.

## Anti-patterns

- **Hard-coding credentials in deps modules** — resolve via secrets/config.

## Reference

- [Authentication](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/identity/)
