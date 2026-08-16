# KMS backends and key lifecycle

Where the key lives and what happens when it changes: Vault and the cloud KMS backends, per-tenant keys, and the difference between rotating a key and replacing one — which is the distinction that makes ciphertext unreadable when confused. What gets sealed is [field encryption](field-encryption.md).

## Choosing a key backend

| Backend | Extra | `kms=` | `key_id` names |
|---------|-------|--------|----------------|
| HashiCorp Vault Transit | `forze[vault]` | `VaultTransitKeyManagement` (from `forze_vault`) | a Transit key name |
| AWS KMS | `forze[kms-aws]` | `AwsKmsKeyManagement` (from `forze_kms.aws`) | CMK id, ARN, or `alias/<name>` |
| Google Cloud KMS | `forze[kms-gcp]` | `GcpKmsKeyManagement` (from `forze_kms.gcp`) | a CryptoKey resource name (`projects/…/cryptoKeys/…`) |
| Yandex Cloud KMS | `forze[kms-yc]` | `YcKmsKeyManagement` (from `forze_kms.yc`) | a symmetric key id |
| Self-hosted local | — | `LocalKeyManagement` (from `forze_kms.local`) | a key id in the master-key map |
| In-memory (dev/test only) | — | `MockKeyManagement` (from `forze_mock`) | anything — protects nothing |

`LocalKeyManagement` is the no-dependency option for deployments with no cloud KMS: it wraps data keys under operator-provided **32-byte** master keys held in a `{key_id: bytes}` map (a wrong-length key is refused at construction). Unlike the mock, it is real AES-256-GCM envelope encryption — but the master key lives in your process, so your secret delivery is the whole of its trust model.

```python
from forze_kms.local import LocalKeyManagement

kms = LocalKeyManagement(keys={"app-kek-2": active_key, "app-kek-1": previous_key})
```

Keep the previous key in the map for the rotation overlap described below: which key *seals* is decided by the directory-resolved `KeyRef`, which key *opens* by the envelope's own id, so a key id rotated out of the map fails closed on read. `kms.fingerprint()` is one-way and safe to log — compare it across replicas to spot a fleet whose key maps have drifted.

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

GCP and Yandex Cloud follow the same shape — same three pieces, different credential source:

```python
from forze_kms.gcp import GcpKmsClient, GcpKmsDepsModule, GcpKmsKeyManagement, gcpkms_lifecycle_step
from forze_kms.yc import YcKmsClient, YcKmsDepsModule, YcKmsKeyManagement, yckms_lifecycle_step

gcp = GcpKmsClient()
gcp_deps = GcpKmsDepsModule(client=gcp)
gcp_kms = GcpKmsKeyManagement(client=gcp)
gcp_step = gcpkms_lifecycle_step()          # application-default credentials

yc = YcKmsClient()
yc_deps = YcKmsDepsModule(client=yc)
yc_kms = YcKmsKeyManagement(client=yc)
yc_step = yckms_lifecycle_step(service_account_key=sa_key)
```

Either `*KeyManagement` goes in `CryptoDepsModule(kms=...)` exactly as `AwsKmsKeyManagement` does above; only the `key_id` spelling differs, per the table. Leave `key_management` unset on the KMS deps module — `CryptoDepsModule` registers that port itself, and registering it twice conflicts.

In tests, `MockDepsModule` wires the whole crypto stack in-memory, so encrypted specs run end-to-end with no KMS. The mock runs the **real** field-encryption path on every field plane — document, graph, search (hub, federated and snapshots), analytics and procedures all resolve the same fail-closed encrypting codecs as a real backend. Two consequences for your tests: a suite asserting on raw stored values now sees ciphertext, and a text query no longer matches sealed content. That is the point — a query that cannot work in production now fails under the mock too.

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

- **Repointing a tenant's `key_id` to a new key without the previous-key overlap** — the keyring's key guard makes the old ciphertext unreadable, including for migration. Always overlap → sweep → drop.
- **Running a re-encrypt sweep for a routine KEK version rotation** — envelopes are self-describing; version rotation needs nothing.
- **Destroying the old KEK before the sweep finished** — check `ReencryptReport` and drop the previous reference first; only then delete the key.
- **`MockKeyManagement` in production** — it derives keys locally and protects nothing; it exists so tests exercise the encryption paths.
- **A provisioner with its own key-naming logic** — pass the *same* directory instance to the provisioner and `CryptoDepsModule`, or the provisioned key and the encrypt path drift.
- **Expecting revocation to bite immediately** — cached data keys outlive the KEK until restart; set `dek_ttl_seconds` to bound the window.

## Reference

- [Cloud KMS integration](https://morzecrew.github.io/forze/latest/integrations/kms/)
- [Vault integration](https://morzecrew.github.io/forze/latest/integrations/vault/)
