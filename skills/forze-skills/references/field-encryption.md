# Field encryption

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

`encrypted` and `searchable` must be disjoint. Sealed fields are **refused** as filter and sort keys on every backend including the mock — a randomized field cannot be filtered (`core.crypto.encrypted_field_not_filterable`; deterministic `searchable` fields keep equality), and *any* sealed field is refused as a sort key, a spec's default sort included. Previously these returned wrong answers rather than an error. Content search over sealed data is likewise physics, not a limit. Everything is **fail-closed**: a spec that marks a field but finds no keyring (`CryptoDepsModule` missing) refuses to wire rather than writing plaintext. `required_encryption` on a deps module (e.g. `PostgresDepsModule(required_encryption="field")`) makes coverage prescriptive — any route below the floor fails at startup.

## Strict mode after backfill

By default the read path **tolerates plaintext** in a sealed field, so you can enable encryption on a live table and backfill without downtime. That tolerance is a fail-open hole once backfill is done — a ciphertext swapped for chosen plaintext would be accepted. After the backfill sweep, set `reject_plaintext=True` on the `FieldEncryption` policy: a non-ciphertext value in a sealed slot then raises `core.crypto.plaintext_rejected` on every plane sharing the policy, and id-bound ciphertext stops accepting the legacy pre-binding fallback.

## Anti-patterns

- **Filtering or sorting on an encrypted field** — mark it `searchable` (deterministic) for equality, keep a plaintext companion field for ranges and ordering, or accept that the field is write-only to queries.
- **Marking a field `searchable` when equality lookups aren't needed** — deterministic encryption leaks equality/frequency; default to `encrypted` (randomized).
- **Leaving `reject_plaintext=False` after the backfill is complete** — the plaintext tolerance becomes an accept-chosen-plaintext hole.
- **Sorting, range-filtering, or content-searching sealed fields** — randomized ciphertext has no order and no content; only `searchable` fields support equality.

## Reference

- [Encryption](https://morzecrew.github.io/forze/latest/identity-tenancy-enc/encryption/)
- [Encryption matrix reference](https://morzecrew.github.io/forze/latest/reference/encryption-matrix/)
