---
name: forze-encryption-kms
description: >-
  Encrypts Forze data with envelope encryption: FieldEncryption on document/search
  specs, object-storage and outbox payload encryption, CryptoDepsModule keyring
  wiring, KMS backends (Vault Transit, AWS/GCP/Yandex Cloud KMS), per-tenant KEKs
  (BYOK) with onboarding provisioners, key rotation vs replacement, and
  re-encryption sweeps. Use when adding encryption at rest, choosing a key
  backend, or rotating/replacing keys.
---

# Forze encryption and KMS

Use when data must be encrypted at rest or end-to-end, when wiring a key backend, or when rotating / replacing keys. Forze uses **envelope encryption**: a key backend (KMS) holds the key-encryption key (KEK) and only wraps/unwraps short-lived data keys — the KEK never leaves the backend. Pair with [`forze-auth-tenancy-secrets`](../forze-auth-tenancy-secrets/SKILL.md) for tenancy and provisioning.

## Wiring the keyring

`CryptoDepsModule` composes the whole crypto stack from a key backend and a directory that maps a tenant to its KEK reference. Merge it into `DepsRegistry.from_modules` like any other module; integrations that opt into encryption resolve the keyring from it — never construct one by hand.

```python
from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.execution import CryptoDepsModule
from forze_vault import VaultTransitKeyManagement

CryptoDepsModule(
    kms=VaultTransitKeyManagement(client=vault),  # Transit mount lives on the client config
    directory=StaticKeyDirectory(KeyRef(key_id="app-kek")),  # one KEK for the deployment
)
```

Useful knobs: `dek_ttl_seconds` bounds how long a cached data key stays usable — without it, a KEK rotation/revocation only takes effect after a process restart. `deterministic_root` (>= 32 bytes, from a secret store) enables `searchable` fields; `required_reach` sets a deployment-wide encryption floor for messaging routes.

## Choosing a key backend

| Backend | Extra | `kms=` | `key_id` names |
|---------|-------|--------|----------------|
| HashiCorp Vault Transit | `forze[vault]` | `VaultTransitKeyManagement` (from `forze_vault`) | a Transit key name |
| AWS KMS | `forze[kms-aws]` | `AwsKmsKeyManagement` (from `forze_kms.aws`) | CMK id, ARN, or `alias/<name>` |
| Google Cloud KMS | `forze[kms-gcp]` | `GcpKmsKeyManagement` (from `forze_kms.gcp`) | a CryptoKey resource name (`projects/…/cryptoKeys/…`) |
| Yandex Cloud KMS | `forze[kms-yc]` | `YcKmsKeyManagement` (from `forze_kms.yc`) | a symmetric key id |
| In-memory (dev/test only) | — | `MockKeyManagement` (from `forze_mock`) | anything — protects nothing |

All implement the same `KeyManagementPort`, so swapping backends is a one-line change in `CryptoDepsModule`. Each cloud backend ships a client + deps module + lifecycle step; credentials default to the platform's ambient chain (botocore chain / application-default credentials / instance metadata):

```python
from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.execution import CryptoDepsModule, DepsRegistry, LifecyclePlan
from forze_kms.aws import (
    AwsKmsClient,
    AwsKmsDepsModule,
    AwsKmsKeyManagement,
    awskms_lifecycle_step,
)

kms = AwsKmsClient()

deps = DepsRegistry.from_modules(
    AwsKmsDepsModule(client=kms),
    CryptoDepsModule(
        kms=AwsKmsKeyManagement(client=kms),
        directory=StaticKeyDirectory(KeyRef(key_id="alias/app-kek")),
    ),
)
lifecycle = LifecyclePlan.from_steps(awskms_lifecycle_step(region_name="eu-central-1"))
```

GCP and Yandex Cloud follow the same shape (`GcpKmsClient` / `GcpKmsDepsModule` / `gcpkms_lifecycle_step`, `YcKmsClient` / `YcKmsDepsModule` / `yckms_lifecycle_step`). Leave `key_management` unset on the KMS deps module — `CryptoDepsModule` registers that port itself, and registering it twice conflicts.

In tests, `MockDepsModule` wires the whole crypto stack in-memory, so encrypted specs run end-to-end with no KMS.

## What gets encrypted

Each surface opts in independently:

- **Document fields** — `DocumentSpec(encryption=FieldEncryption(...))`; the same policy object is shared by the `SearchSpec` / analytics / graph specs over the same data, so sealed-field sets cannot drift.
- **Object storage** — per route: `S3StorageConfig(bucket="uploads", encrypt=True)` (client-side; the backend only stores the envelope).
- **Outbox / queue / stream / pub-sub payloads** — `OutboxSpec(name="events", codec=codec, encryption="end_to_end")`; tiers `none` < `at_rest` (relay decrypts) < `end_to_end` (consumer decrypts).
- **Idempotency result cache** — `IdempotencySpec(name="orders", encrypt_result=True)`.

```python
from forze.application.contracts.crypto import FieldEncryption

DocumentSpec(
    name="patients",
    read=Patient,
    encryption=FieldEncryption(
        encrypted={"ssn", "diagnosis"},   # randomized — confidential, never queryable
        searchable={"email"},             # deterministic — $eq/$in filters still work
        binds_record_id=True,             # bind row id into the AAD (randomized fields only)
    ),
)
```

`encrypted` and `searchable` must be disjoint; sealed fields cannot be sorted or content-searched (that is physics, not a limit). Everything is **fail-closed**: a spec that marks a field but finds no keyring (`CryptoDepsModule` missing) refuses to wire rather than writing plaintext. `required_encryption` on a deps module (e.g. `PostgresDepsModule(required_encryption="field")`) makes coverage prescriptive — any route below the floor fails at startup.

## Strict mode after backfill

By default the read path **tolerates plaintext** in a sealed field, so you can enable encryption on a live table and backfill without downtime. That tolerance is a fail-open hole once backfill is done — a ciphertext swapped for chosen plaintext would be accepted. After the backfill sweep, set `reject_plaintext=True` on the `FieldEncryption` policy: a non-ciphertext value in a sealed slot then raises `core.crypto.plaintext_rejected` on every plane sharing the policy, and id-bound ciphertext stops accepting the legacy pre-binding fallback.

## Per-tenant keys (BYOK)

Give each tenant its own KEK by swapping the directory; one tenant's data becomes unreadable with another's key:

```python
from forze.application.contracts.crypto import TenantTemplateKeyDirectory

directory = TenantTemplateKeyDirectory(
    template="alias/tenant-{tenant_id}",
    default_key_id="alias/shared-kek",  # used when no tenant is bound
)
```

The KEK itself is created on onboarding through the same `TenantProvisionerPort` seam as schemas and buckets — pass a KMS provisioner to `TenancyDepsModule(tenant_provisioner=...)`, composing with other provisioners via `CompositeTenantProvisioner`:

- `VaultTransitTenantProvisioner` (from `forze_vault`)
- `AwsKmsTenantProvisioner` / `GcpKmsTenantProvisioner` / `YcKmsTenantProvisioner` (from `forze_kms.aws` / `.gcp` / `.yc`)

Each resolves through the **same** directory instance the keyring encrypts with, so the provisioned key and the encrypt-path key can never drift. Yandex Cloud mints its key ids, so it pairs with `YcKmsKeyDirectory` (name lookup) instead of a template. Teardown is opt-in (`allow_deletion=True`) — deleting a KEK is irreversible data loss.

## Rotation vs replacement — do not confuse them

**Rotating a key version needs no action.** Envelopes are self-describing and the KMS decrypts a wrapped data key without being told which version sealed it: new writes wrap under the new version, old ciphertext keeps decrypting. No sweep, nothing to migrate.

**Replacing the key itself is different.** The keyring refuses an envelope whose `key_id` is not the one the directory resolves for that tenant (the same guard that stops one tenant's key unwrapping another's). So **repointing a tenant's `key_id` at a new key bricks everything written under the old one** — it cannot even be read back to migrate it. Instead, open a two-phase **previous-key overlap**: writes go to the new key while reads still accept the old one, sweep the data across, then drop the previous key.

```python
directory = TenantTemplateKeyDirectory(
    template="tenant/{tenant_id}/kek-v2",           # new writes land here
    previous_template="tenant/{tenant_id}/kek-v1",  # ...and old reads still work
    default_key_id="shared-kek",
)
```

`StaticKeyDirectory(previous_key_ref=...)` is the single-key equivalent; a store-backed directory (one BYOK customer replacing their own key) implements `KeyDirectoryWithPrevious` directly.

The sweeps — one per persistent surface, resumable, tolerant of rows/objects deleted mid-pass (`ReencryptReport.rewritten` / `.skipped_missing`):

```python
from forze.application.integrations.crypto import reencrypt_documents, reencrypt_objects

await reencrypt_documents(
    ctx.document.query(patient_spec),
    ctx.document.command(patient_spec),
    to_update=lambda d: PatientUpdate(ssn=d.ssn),  # read → write re-seals under current keys
)
await reencrypt_objects(ctx.storage.query(files_spec), ctx.storage.command(files_spec))
```

The same sweeps serve a suspected-compromise re-encryption (fresh envelopes under fresh data keys). Rotating the **deterministic (searchable) root** uses the same two-phase shape on the crypto module: set `deterministic_previous_root` to the old secret, run `reencrypt_documents` over the searchable fields, then drop it.

## Anti-patterns

1. **Repointing a tenant's `key_id` to a new key without the previous-key overlap** — the keyring's key guard makes the old ciphertext unreadable, including for migration. Always overlap → sweep → drop.
2. **Running a re-encrypt sweep for a routine KEK version rotation** — envelopes are self-describing; version rotation needs nothing.
3. **Destroying the old KEK before the sweep finished** — check `ReencryptReport` and drop the previous reference first; only then delete the key.
4. **`MockKeyManagement` in production** — it derives keys locally and protects nothing; it exists so tests exercise the encryption paths.
5. **Marking a field `searchable` when equality lookups aren't needed** — deterministic encryption leaks equality/frequency; default to `encrypted` (randomized).
6. **Leaving `reject_plaintext=False` after the backfill is complete** — the plaintext tolerance becomes an accept-chosen-plaintext hole.
7. **A provisioner with its own key-naming logic** — pass the *same* directory instance to the provisioner and `CryptoDepsModule`, or the provisioned key and the encrypt path drift.
8. **Expecting revocation to bite immediately** — cached data keys outlive the KEK until restart; set `dek_ttl_seconds` to bound the window.
9. **Sorting, range-filtering, or content-searching sealed fields** — randomized ciphertext has no order and no content; only `searchable` fields support equality.

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version (e.g. `.../forze/0.3/...`) or use the version selector on the site.

- [Encryption](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/encryption/)
- [Cloud KMS integration](https://morzecrew.github.io/forze/latest/integrations/kms/)
- [Vault integration](https://morzecrew.github.io/forze/latest/integrations/vault/)
- [Encryption matrix reference](https://morzecrew.github.io/forze/latest/reference/encryption-matrix/)
- [Multi-tenancy](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/multi-tenancy/)
- [`forze-auth-tenancy-secrets`](../forze-auth-tenancy-secrets/SKILL.md)
