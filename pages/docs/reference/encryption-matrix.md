---
title: Encryption matrix
icon: lucide/lock
summary: Every encryptable surface — coverage, reach, searchability, AAD binding, per-tenant keys, and the fail-closed code
---

The narrative is in [Encryption](../identity-tenancy-enc/encryption.md); this is the
exhaustive surface. Forze uses envelope encryption — a KMS-held KEK wraps short-lived
DEKs; every ciphertext is an `EncryptedEnvelope` bound to associated data (AAD) that
includes at least the tenant. Coverage is **opt-in per spec** and **fail-closed**: a
surface that marks something for encryption but finds no keyring refuses to persist
plaintext.

## Field-level surfaces (one shared policy)

A single `FieldEncryption` policy — `encrypted` (randomized AEAD) and `searchable`
(deterministic, equality-only) field sets, plus `binds_record_id` — is declared once on
the document spec and **carried to search, analytics, and graph** by pointing their spec
at the same policy, so the planes can't drift.

| Surface | Backends | `searchable` | `binds_record_id` | Fail-closed code |
|---------|----------|--------------|-------------------|------------------|
| Document fields | Postgres, Mongo, Firestore, mock | yes | yes (randomized fields only) | `core.document.encryption_wiring` |
| Search | Postgres, Mongo, Meilisearch | yes (rewrites equality filters) | inherited | `core.search.encryption_wiring` |
| Analytics / warehouse | Postgres, ClickHouse, BigQuery, DuckDB, mock | yes (equality only) | **rejected** — warehouse rows have no id | `core.analytics.encryption_wiring` |
| Graph | Neo4j | yes | nodes + key-addressed edges; **rejected** for endpoint-identity edges | `core.graph.encryption_wiring` |

- **Reach:** at rest. Fields are sealed on write and decrypted out of *every* read path
  — search results, warehouse offset/cursor/chunked/projection reads, graph
  get/neighbors/walk/shortest-path. A search route's result snapshots (kept for stable
  re-pagination) are sealed at rest too, automatically.
- **Confidential by physics:** an `encrypted` (randomized) field is never
  content-searchable, aggregatable, or matchable in a predicate. Use `searchable`
  (deterministic) for equality lookups — it trades secrecy for queryability (identical
  plaintexts share a ciphertext), so mark a field `searchable` only when you must query
  it by exact value.
- **`searchable` needs a stable root** (`deterministic_root` on the crypto module);
  rotate with `deterministic_previous_root` overlap + `reencrypt_documents`.

## Whole-payload & object surfaces

| Surface | Coverage | Reach | Backends |
|---------|----------|-------|----------|
| Object storage | whole object (`encrypt=True`) | client-side — backend stores only the envelope | S3, GCS, mock |
| Outbox | whole payload (`OutboxEncryptionTier`) | `none` / `at_rest` (relay decrypts before publish) / `end_to_end` (consumer decrypts after dedup) | Postgres / Mongo store → any transport |
| Direct queue / stream / pub-sub | whole payload | `none` / `end_to_end` only (no store, so no `at_rest`) | SQS, RabbitMQ, Redis, Inngest |
| Idempotency result | whole cached result (`encrypt_result=True`) | at rest (sealed on commit, opened on replay; metadata stays plaintext) | Redis, Postgres |

- **Object storage caveat:** multipart / presigned uploads are **blocked** when a route
  encrypts — the client would write bytes the app never sees, landing as plaintext. Use
  a single-shot `upload()`.
- **Outbox ↔ direct messaging:** both bind the payload AAD to `(tenant, event/message
  id)` reconstructable from envelope headers, and share a payload domain — so an
  `end_to_end` message decrypts identically whether it was relayed from the outbox or
  published directly, across every transport. Legacy plaintext rows still relay.
- **Idempotency AAD:** `(tenant, op:key)` with a length-prefixed id so `(op, key)`
  boundaries can't collide.

## Not encrypted

| Surface | State | Note |
|---------|-------|------|
| Inbox | dedup records (id + tenant) stored plaintext | dedup keys on the id, not the payload; works across encrypted and plaintext messages |
| General cache | plaintext | the `CachePort` / L1 cache have no encryption flag; only **idempotency results** and **search result-snapshots** are sealed |

## Keys & enforcement

- **Per-tenant keys (BYOK):** swap `StaticKeyDirectory` for `TenantTemplateKeyDirectory`
  (`template="tenant/{tenant_id}/kek"`) so each tenant's data is unreadable with
  another's key. The KEK is provisioned through the same `TenantProvisionerPort` as
  schemas and buckets — every backend ships one (`VaultTransitTenantProvisioner`,
  `AwsKmsTenantProvisioner`, `GcpKmsTenantProvisioner`, `YcKmsTenantProvisioner`), and
  teardown is opt-in (`allow_deletion`). Yandex Cloud mints its key ids, so it pairs with
  `YcKmsKeyDirectory` (name lookup) rather than a template directory.
- **KMS backends:** every one holds the KEK outside the app and self-describes the key
  version in the envelope, so rotation never orphans data.

    | Backend | Package | `KeyManagementPort` |
    |---------|---------|---------------------|
    | Vault Transit | `forze[vault]` | `VaultTransitKeyManagement` |
    | AWS KMS | `forze[kms-aws]` | `AwsKmsKeyManagement` |
    | Google Cloud KMS | `forze[kms-gcp]` | `GcpKmsKeyManagement` |
    | Yandex Cloud KMS | `forze[kms-yc]` | `YcKmsKeyManagement` |

    `MockKeyManagement` is **dev/test only** (it protects nothing). Any other KMS — Azure,
    an HSM — is a custom `KeyManagementPort`. See [Cloud KMS](../integrations/kms.md).
- **`required_encryption` floor:** set it on a deps module and wiring refuses to assemble
  any surface whose derived coverage is weaker — a fail-closed floor checked once at
  startup. See [Encryption → Declaring a minimum](../identity-tenancy-enc/encryption.md#declaring-a-minimum).
- **Observability:** `instrument_crypto({"default": keyring}, meter=…)` exports DEK
  generation, unwraps, and cache hit/miss.
