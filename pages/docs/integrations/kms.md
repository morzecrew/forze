---
title: KMS backends
icon: lucide/key-round
summary: Hold the key-encryption key in AWS, Google Cloud, or Yandex Cloud KMS — or self-hosted, in process
---

`forze[kms-aws]`, `forze[kms-gcp]`, and `forze[kms-yc]` each supply a
`KeyManagementPort` backed by a managed key service, so the key-encryption key
never leaves the KMS — reach for one when you want
[envelope encryption](../identity-tenancy-enc/encryption.md) without running Vault.
For deployments with no cloud at all, `forze_kms.local` supplies the same port
over operator-provided master keys, with no extra to install — see
[Self-hosted keys](#self-hosted-keys-no-cloud).

## Install

=== "AWS"

    ```bash
    uv add 'forze[kms-aws]'
    ```

=== "Google Cloud"

    ```bash
    uv add 'forze[kms-gcp]'
    ```

=== "Yandex Cloud"

    ```bash
    uv add 'forze[kms-yc]'
    ```

## Wire it

Build the client and register its deps module — that publishes the client so the
lifecycle step can open it. `CryptoDepsModule` composes the keyring over the
adapter and registers `KeyManagementDepKey` itself:

=== "AWS"

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

=== "Google Cloud"

    ```python
    from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
    from forze.application.execution import CryptoDepsModule, DepsRegistry, LifecyclePlan
    from forze_kms.gcp import (
        GcpKmsClient,
        GcpKmsDepsModule,
        GcpKmsKeyManagement,
        gcpkms_lifecycle_step,
    )

    kms = GcpKmsClient()
    key = "projects/acme/locations/europe-west1/keyRings/app/cryptoKeys/app-kek"

    deps = DepsRegistry.from_modules(
        GcpKmsDepsModule(client=kms),
        CryptoDepsModule(
            kms=GcpKmsKeyManagement(client=kms),
            directory=StaticKeyDirectory(KeyRef(key_id=key)),
        ),
    )
    lifecycle = LifecyclePlan.from_steps(gcpkms_lifecycle_step())
    ```

=== "Yandex Cloud"

    ```python
    from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
    from forze.application.execution import CryptoDepsModule, DepsRegistry, LifecyclePlan
    from forze_kms.yc import (
        YcKmsClient,
        YcKmsDepsModule,
        YcKmsKeyManagement,
        yckms_lifecycle_step,
    )

    kms = YcKmsClient()

    deps = DepsRegistry.from_modules(
        YcKmsDepsModule(client=kms),
        CryptoDepsModule(
            kms=YcKmsKeyManagement(client=kms),
            directory=StaticKeyDirectory(KeyRef(key_id="abjq…")),
        ),
    )
    lifecycle = LifecyclePlan.from_steps(yckms_lifecycle_step())
    ```

Leave `key_management` unset on the KMS deps module: `CryptoDepsModule` already
registers the port, and registering it twice is a conflicting dependency.

## Per-tenant keys

Give each tenant its own KEK, and create it when the tenant is onboarded. A
provisioner resolves through the **same** directory the keyring encrypts through,
so the provisioned key and the encrypt-path key can never drift:

=== "AWS"

    ```python
    from forze.application.contracts.crypto import TenantTemplateKeyDirectory
    from forze_kms.aws import AwsKmsTenantProvisioner

    # KMS mints the CMK id, so a tenant's key is addressed by a caller-chosen alias.
    directory = TenantTemplateKeyDirectory(
        template="alias/tenant-{tenant_id}",
        default_key_id="alias/shared-kek",
    )
    provisioner = AwsKmsTenantProvisioner(client=kms, directory=directory)
    ```

=== "Google Cloud"

    ```python
    from forze.application.contracts.crypto import TenantTemplateKeyDirectory
    from forze_kms.gcp import GcpKmsTenantProvisioner

    ring = "projects/acme/locations/europe-west1/keyRings/app"
    directory = TenantTemplateKeyDirectory(
        template=f"{ring}/cryptoKeys/tenant-{{tenant_id}}",
        default_key_id=f"{ring}/cryptoKeys/shared-kek",
    )
    # The key ring is shared and long-lived; only the CryptoKey is per-tenant.
    provisioner = GcpKmsTenantProvisioner(client=kms, directory=directory)
    ```

=== "Yandex Cloud"

    ```python
    from forze_kms.yc import YcKmsKeyDirectory, YcKmsTenantProvisioner

    # Yandex Cloud mints the key id, so a template cannot address a tenant's key —
    # this directory looks it up by the name the provisioner creates.
    directory = YcKmsKeyDirectory(client=kms, folder_id="b1g…", template="tenant-{tenant_id}")
    provisioner = YcKmsTenantProvisioner(client=kms, directory=directory)
    ```

Pass the provisioner to `TenancyDepsModule(tenant_provisioner=…)` — alongside a
schema or bucket provisioner via `CompositeTenantProvisioner` — so onboarding a
tenant readies every backend at once. Provisioning is idempotent, so a retried
onboarding is safe.

## Self-hosted keys (no cloud)

`LocalKeyManagement` implements the same port entirely in process: it wraps data
keys under raw 32-byte master keys you supply, with AES-256-GCM. It needs no
extra — the `cryptography` library is already a core dependency — and no client,
deps module, or lifecycle step:

```python
from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.execution import CryptoDepsModule
from forze_kms.local import LocalKeyManagement

CryptoDepsModule(
    kms=LocalKeyManagement({"k1": load_master_key()}),  # key id → raw 32 bytes
    directory=StaticKeyDirectory(KeyRef(key_id="k1")),
)
```

**Know the trust model.** The master keys live in process memory and in your
configuration — there is no HSM, no non-exportability, no backend audit log.
Compromise of the host or the config is compromise of the keys. That is the
honest boundary of a self-hosted deployment; if you need keys that never leave a
service, use one of the cloud backends or Vault above. How the raw bytes get to
the process (env var, file mount, secret manager) is your application's choice,
same as `deterministic_root`.

**Replacing a key** uses the standard
[previous-key overlap](../identity-tenancy-enc/encryption.md#replacing-a-key),
with one extra rule: the outgoing key stays **in the map** until the sweep is
done — the directory widens what reads accept; the map is what can unwrap:

```python
CryptoDepsModule(
    kms=LocalKeyManagement({"k2": new_key, "k1": old_key}),  # old key still unwraps
    directory=StaticKeyDirectory(
        KeyRef(key_id="k2"),                                 # new writes seal here
        previous_key_ref=KeyRef(key_id="k1"),                # old reads still accepted
    ),
)
```

Run the [re-encryption sweeps](../identity-tenancy-enc/encryption.md#re-encrypting-stored-data)
(or let natural rewrites drain the old key), then drop `k1` from both places in
the same deploy. Dropping it from the map too early fails closed with an error
telling you to put it back.

**Fleets work — same keys, every replica.** "Local" means the wrap is computed
in process, not that the deployment is single-node: any replica holding the same
key map can open any envelope, so a fleet just injects the same secret into
every process (a mounted Secret, env injection — however you already ship
`deterministic_root`). Two rules follow. *Rotate in two phases*: first roll out
the new key to every replica's **map** (directory unchanged), then flip
`key_ref` in a second rollout — otherwise a mid-rollout replica that already
seals under the new key writes envelopes its not-yet-updated peers cannot open.
And *every config holder is a key holder* — there is no central place that
audits or revokes access.

Drift between replicas is fail-closed but worth catching early: each instance
logs its key ids and a one-way `fingerprint` of the key map at construction —
compare that value across the fleet (grep startup logs, or attach it to your
own metrics) and any divergence is a ten-second diagnosis. In particular,
fleet-wide `core.crypto.aead_auth_failed` right after a deploy usually means
two replicas hold *different bytes under the same key id* — differing
fingerprints confirm it; envelopes sealed by the odd node out are fine once its
config is corrected. If you need keys that never leave one service, that
property is exactly what requires a networked unwrap: use
[Vault Transit](vault.md) (or an API-compatible self-hosted service) or a cloud
backend above.

Moving to a cloud backend later changes wiring only — envelopes and call sites
are identical across backends — but envelopes wrapped under a local master key
still have to be re-encrypted under the new backend's key: an overlap window
plus a sweep, the same procedure as replacing any key.

## What it provides

| Contract | Implementation | Dep key |
|----------|---------------|---------|
| Key management (envelope encryption) | `AwsKmsKeyManagement` · `GcpKmsKeyManagement` · `YcKmsKeyManagement` · `LocalKeyManagement` | `KeyManagementDepKey` (registered by `CryptoDepsModule`) |
| Per-tenant KEK provisioning | `AwsKmsTenantProvisioner` · `GcpKmsTenantProvisioner` · `YcKmsTenantProvisioner` | via `TenantProvisionerPort` |
| Key directory (Yandex Cloud only) | `YcKmsKeyDirectory` | passed to `CryptoDepsModule(directory=…)` |
| Raw client | `AwsKmsClient` · `GcpKmsClient` · `YcKmsClient` | `AwsKmsClientDepKey` · `GcpKmsClientDepKey` · `YcKmsClientDepKey` |

What a `KeyRef.key_id` names, per provider:

| Provider | Extra | `key_id` | Credentials when unset |
|----------|-------|----------|------------------------|
| AWS | `forze[kms-aws]` | a CMK id, ARN, or `alias/<name>` | the botocore chain (env, profile, instance role) |
| Google Cloud | `forze[kms-gcp]` | a CryptoKey resource name (`projects/…/cryptoKeys/…`) | application-default credentials |
| Yandex Cloud | `forze[kms-yc]` | a symmetric key id | the instance metadata service |
| Self-hosted | none needed | an operator-chosen name keying the master-key map | — (keys are supplied directly) |

## Notes

- **Credentials are implicit by default** — each lifecycle step falls back to its
  platform's ambient chain (the table above). Pass them explicitly when you must:
  `access_key_id` / `secret_access_key` on `awskms_lifecycle_step`, `credentials`
  on `gcpkms_lifecycle_step`, and `iam_token` / `oauth_token` /
  `service_account_key` on `yckms_lifecycle_step`.
- **Rotation is transparent.** A wrapped data key is decryptable by the KMS without
  being told which version sealed it, so rotating the KEK never orphans data: new
  writes wrap under the new version while old ciphertext still decrypts. Nothing to
  migrate, no re-encrypt sweep — see [Searchable fields and rotation](../identity-tenancy-enc/encryption.md#searchable-fields-and-rotation)
  for the one case that *does* need a re-index. Pointing a tenant at a
  *different* `key_id` is a migration, not a rotation — it needs a previous-key
  read overlap and a re-encrypt sweep; see
  [Replacing a key](../identity-tenancy-enc/encryption.md#replacing-a-key).
  The self-hosted backend has no key versions at all: rotating it *is* replacing
  the key ([above](#self-hosted-keys-no-cloud)).
- **Data-key length** is `dek_bytes` on the adapter — 32 bytes (AES-256) by
  default, matching the keyring's AEAD; 16 selects AES-128.
- **Teardown is opt-in and never immediate.** `deprovision` does nothing unless you
  set `allow_deletion=True` — destroying a KEK makes every value wrapped under it
  unrecoverable. Even then the platform protects you: AWS drops the alias and
  *schedules* the CMK for deletion after `pending_window_days` (7–30, so you can
  cancel); Google Cloud cannot delete a CryptoKey at all, so the provisioner
  destroys its versions and the empty key resource remains; Yandex Cloud deletes
  the key outright.
- **Google Cloud KMS has no data-key API.** The adapter mints the data key itself
  from the framework's CSPRNG entropy seam and wraps it with `Encrypt`; AWS and
  Yandex Cloud use their native `GenerateDataKey`. The envelope is identical
  either way.
- **The Yandex Cloud SDK is blocking.** Calls are driven off the event loop, so a
  KMS round-trip never stalls the runtime.
- Each client needs its lifecycle step — the deps module only registers an
  already-constructed client; it doesn't open it.
