---
title: Cloud KMS
icon: lucide/key-round
summary: Hold the key-encryption key in AWS, Google Cloud, or Yandex Cloud KMS
---

`forze[kms-aws]`, `forze[kms-gcp]`, and `forze[kms-yc]` each supply a
`KeyManagementPort` backed by a managed key service, so the key-encryption key
never leaves the KMS — reach for one when you want
[envelope encryption](../identity-tenancy-enc/encryption.md) without running Vault.

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

## What it provides

| Contract | Implementation | Dep key |
|----------|---------------|---------|
| Key management (envelope encryption) | `AwsKmsKeyManagement` · `GcpKmsKeyManagement` · `YcKmsKeyManagement` | `KeyManagementDepKey` (registered by `CryptoDepsModule`) |
| Raw client | `AwsKmsClient` · `GcpKmsClient` · `YcKmsClient` | `AwsKmsClientDepKey` · `GcpKmsClientDepKey` · `YcKmsClientDepKey` |

What a `KeyRef.key_id` names, per provider:

| Provider | Extra | `key_id` | Credentials when unset |
|----------|-------|----------|------------------------|
| AWS | `forze[kms-aws]` | a CMK id, ARN, or `alias/<name>` | the botocore chain (env, profile, instance role) |
| Google Cloud | `forze[kms-gcp]` | a CryptoKey resource name (`projects/…/cryptoKeys/…`) | application-default credentials |
| Yandex Cloud | `forze[kms-yc]` | a symmetric key id | the instance metadata service |

## Notes

- **Credentials are implicit by default** — each lifecycle step falls back to its
  platform's ambient chain (the table above). Pass them explicitly when you must:
  `access_key_id` / `secret_access_key` on `awskms_lifecycle_step`, `credentials`
  on `gcpkms_lifecycle_step`, and `iam_token` / `oauth_token` /
  `service_account_key` on `yckms_lifecycle_step`.
- **Rotation is transparent.** Every wrapped data key names the key version that
  seals it, so rotating the KEK never orphans data: new writes wrap under the new
  version while old ciphertext still decrypts. Nothing to migrate, no re-encrypt
  sweep — see [Searchable fields and rotation](../identity-tenancy-enc/encryption.md#searchable-fields-and-rotation)
  for the one case that *does* need a re-index.
- **Data-key length** is `dek_bytes` on the adapter — 32 bytes (AES-256) by
  default, matching the keyring's AEAD; 16 selects AES-128.
- **Per-tenant KEKs are not provisioned for you.** Point
  `TenantTemplateKeyDirectory` at per-tenant keys as usual, but unlike
  [Vault](vault.md) — which ships `VaultTransitTenantProvisioner` — no cloud
  backend ships a `TenantProvisionerPort`, so create and destroy a tenant's key
  through your own provisioner or out of band.
- **Google Cloud KMS has no data-key API.** The adapter mints the data key itself
  from the framework's CSPRNG entropy seam and wraps it with `Encrypt`; AWS and
  Yandex Cloud use their native `GenerateDataKey`. The envelope is identical
  either way.
- **The Yandex Cloud SDK is blocking.** Calls are driven off the event loop, so a
  KMS round-trip never stalls the runtime.
- Each client needs its lifecycle step — the deps module only registers an
  already-constructed client; it doesn't open it.
