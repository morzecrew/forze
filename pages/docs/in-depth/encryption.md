---
title: Encryption
icon: lucide/lock
summary: Envelope encryption with your own keys — applied per field, per object, or end-to-end across the outbox, and refused below a declared floor
---

Forze encrypts data with **envelope encryption**: a key backend you control (a
KMS) holds the key-encryption key (the *KEK*), and the framework only ever asks
it to wrap and unwrap short-lived data keys (*DEKs*). The KEK never leaves the
backend, so the same model covers managed keys and full *bring-your-own-key*
(BYOK) — the difference is only which backend, and whose tenant, the key belongs
to.

The result is the same shape you already know from
[multi-tenancy](multi-tenancy.md): a cross-cutting concern declared on the spec,
applied by adapters without a line in your handlers, and **fail-closed** — a
field marked for encryption that finds no key wired refuses to persist as
plaintext rather than degrading silently.

## The envelope

![A key backend wraps a data key; the AEAD encrypts the value with it; the wrapped key and ciphertext travel together as a self-describing envelope](../_diagrams/light/crypto-envelope.svg#only-light){ loading=lazy }
![A key backend wraps a data key; the AEAD encrypts the value with it; the wrapped key and ciphertext travel together as a self-describing envelope](../_diagrams/dark/crypto-envelope.svg#only-dark){ loading=lazy }

Each value is sealed into a self-describing `EncryptedEnvelope` — the wrapped
DEK, the AEAD ciphertext, and the algorithm metadata travel together, so a
reader needs only the key backend to open it, never an out-of-band scheme. The
`Keyring` generates a DEK, has the backend wrap it, and **caches and reuses** it
across a scope up to `max_dek_messages` before rotating — one KMS round-trip
amortized over many values.

Every ciphertext is bound to **associated data** (AAD): at minimum the tenant
and the field name, so an envelope lifted from one tenant or column cannot be
replayed into another. The cipher is AEAD (AES-256-GCM by default, ChaCha20-
Poly1305 optional) — tampering fails the open, it doesn't return garbage.

## Wiring the keyring

`CryptoDepsModule` composes the whole stack from a key backend and a directory
that maps a tenant to its KEK reference:

```python
from forze.application.execution import CryptoDepsModule
from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze_vault.adapters import VaultTransitKeyManagement

CryptoDepsModule(
    kms=VaultTransitKeyManagement(client=vault),  # mount lives on the client config
    directory=StaticKeyDirectory(KeyRef(key_id="app-kek")),  # one KEK for the deployment
)
```

That registers the key manager, the AEAD, the directory, and the composed
`Keyring` under their dep keys. Integrations that opt into encryption resolve the
keyring from here — they never construct one. For per-tenant keys, swap the
directory (see [Per-tenant keys](#per-tenant-keys-byok) below).

!!! warning "`MockKeyManagement` is dev/test only"

    The in-memory `MockKeyManagement` from `forze_mock` derives keys locally —
    it is for tests and local runs, never production. It exists so the encryption
    paths exercise end-to-end without a real KMS; it protects nothing.

## Where data gets encrypted

Three surfaces opt in independently, each declaring its own coverage tier. You
encrypt only what needs it.

### Document fields

A document spec names the fields to seal. Tiers run weakest to strongest —
`none` < `field` < `envelope` — and a spec derives its tier from what it marks:

```python
DocumentSpec(
    name="patients",
    read=Patient,
    encryption=FieldEncryption(
        encrypted=frozenset({"ssn", "diagnosis"}),
        searchable=frozenset({"email"}),   # deterministic — see below
        binds_record_id=True,              # bind the row id into the AAD
    ),
)
```

A single `FieldEncryption` policy declares the whole shape, and the `SearchSpec`
over the same table shares the *same* object — so the two can't drift.
`encrypted` fields are randomized AEAD ciphertext; `searchable` fields use a
deterministic cipher so equality queries still match. Setting `binds_record_id=True`
folds the record's `id` into the AAD of every randomized field, so a ciphertext
can't be copied between rows — it applies only to randomized fields, never
searchable ones (whose ciphertext must stay record-independent to compare).

!!! warning "Marking a field requires a wired keyring"

    A spec that marks any field for encryption but finds no `KeyringDepKey` (or
    no deterministic cipher, when it declares searchable fields) raises at
    factory time rather than writing plaintext. The check is fail-closed by
    design.

The **same** `FieldEncryption` policy carries across planes. Point a `SearchSpec`,
an `AnalyticsSpec`, or a graph node/edge kind at it and those surfaces seal the
same fields on write and decrypt them out of every read path — search results,
warehouse rows (offset / cursor / chunked / projections), and graph
get / neighbors / walk / shortest-path. Encrypted fields stay **confidential**:
they're never content-searchable, aggregatable, or matchable in a graph predicate
(that's physics, not a limit) — so encrypt what you store-and-return but never
query by, and use `searchable` (deterministic) fields for the equality lookups you
do need. Each plane fails closed the same way (`core.{search,analytics,graph}.encryption_wiring`).

### Object storage

Object bytes encrypt per route with a single flag — the stored object is the
envelope, decrypted transparently on read:

```python
S3StorageConfig(bucket="uploads", encrypt=True)
```

### Outbox and inbox

The transactional outbox chooses **how far** ciphertext travels, via
`OutboxEncryptionTier` on the spec — `none` < `at_rest` < `end_to_end`:

| Tier | Encrypted where | Decrypted by |
|------|-----------------|--------------|
| `none` | nowhere | — |
| `at_rest` | the outbox row | the relay, before publishing |
| `end_to_end` | row **and** broker payload | the consumer, before the handler |

```python
OutboxSpec(name="events", codec=codec, encryption="end_to_end")
```

At `at_rest` the payload is ciphertext in your database and plaintext on the
wire; at `end_to_end` it stays sealed through the broker and is opened only by
the consumer after dedup — the message broker never sees plaintext. The payload
AAD is reconstructable from the envelope headers (tenant and event id), so any
transport carries it: queue, stream, or pub/sub, across every messaging
backend. Legacy plaintext rows written before a tier was raised still relay.

## Searchable fields and rotation

Deterministic (searchable) fields need a stable root secret, set on the crypto
module — the same plaintext always seals to the same ciphertext, which is what
lets equality queries hit:

```python
CryptoDepsModule(
    kms=...,
    directory=...,
    deterministic_root=load_secret("search-root"),          # >= 32 bytes
    deterministic_previous_root=load_secret("search-root-prev"),  # rotation only
)
```

Rotating the root is a two-phase overlap: set `deterministic_previous_root` to
the old value, and reads match values written under *either* root while new
writes use the current one. Run `reencrypt_documents` to re-index every
searchable value under the new root, then drop the previous one.

!!! note "Searchable fields trade secrecy for queryability"

    Deterministic encryption leaks equality — identical plaintexts are visible as
    identical ciphertexts. Mark a field `searchable` only when you must query it
    by exact value; otherwise leave it randomized in `FieldEncryption.encrypted`.

## Declaring a minimum

As with tenant isolation, coverage can be **prescriptive**. Set
`required_encryption` on a deps module and wiring refuses to assemble any surface
whose derived tier is weaker — a fail-closed floor checked once, at startup,
never per request:

```python
PostgresDepsModule(
    client=...,
    required_encryption="field",  # every document route must seal something
)
```

A spec that forgot to mark a field, or a storage route left in the clear, fails
to wire instead of quietly persisting plaintext. Leave it unset (the default)
and nothing is enforced — coverage stays opt-in per spec.

## Per-tenant keys (BYOK)

Stronger isolation gives each tenant its own KEK, so one tenant's data is
unreadable with another's key — and a tenant can supply or revoke their own.
Swap the static directory for a per-tenant one:

```python
from forze.application.contracts.crypto import TenantTemplateKeyDirectory

directory = TenantTemplateKeyDirectory(
    template="tenant/{tenant_id}/kek",
    default_key_id="shared-kek",  # used when no tenant is bound
)

CryptoDepsModule(kms=VaultTransitKeyManagement(client=vault), directory=directory)
```

The KEK itself is provisioned per tenant through the same
[`TenantProvisionerPort`](multi-tenancy.md#provisioning-per-tenant-infrastructure)
seam onboarding uses for schemas and buckets. `forze_vault` ships
`VaultTransitTenantProvisioner`, which resolves the tenant through that same
directory and creates its Transit key on onboarding (teardown opt-in via
`allow_deletion`, since deleting a KEK is irreversible data loss):

```python
from forze_vault.adapters import VaultTransitTenantProvisioner

TenancyDepsModule(
    tenant_management={"main"},
    tenant_provisioner=VaultTransitTenantProvisioner(
        client=vault, directory=directory  # same directory the keyring resolves with
    ),
)
```

Compose it with other provisioners (a schema, a bucket, a key) via
`CompositeTenantProvisioner` so onboarding a tenant readies every backend at
once.

## Observability

The keyring exports the same pull-based metrics as the rest of the engine. Pass
your keyrings to `instrument_crypto` by label —
`instrument_crypto({"default": keyring}, meter=meter)` — to see DEK generation,
unwrap calls, cache hits, and cold misses, the signal for whether
`max_dek_messages` is sized right for your traffic. See
[Observability](observability.md) for the meter setup.
