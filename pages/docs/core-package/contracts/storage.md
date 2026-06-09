# Storage contracts

Storage contracts provide S3-style object storage for binary data and metadata.

## `StorageSpec`

| Section | Details |
|---------|---------|
| Purpose | Names a logical object storage backend. |
| Import path | `from forze.application.contracts.storage import StorageSpec` |
| Type parameters | None. |
| Required fields | `name`. |
| Returned values | Passed to `ctx.storage.query(spec)` / `ctx.storage.command(spec)` to resolve the storage ports. |
| Common implementations | Mock storage adapter, S3-compatible adapter, GCS adapter. |
| Related dependency keys | `StorageQueryDepKey`, `StorageCommandDepKey`. |
| Minimal example | `attachments = StorageSpec(name="attachments")` |
| Related pages | [S3-Compatible Storage](../../integrations/s3.md), [Google Cloud Storage](../../integrations/gcs.md). |

## `StorageQueryPort` / `StorageCommandPort`

Storage follows the same CQRS split as the document, search, and outbox contracts:
reads live on `StorageQueryPort`, writes on `StorageCommandPort`, each with its own
dependency key. A single adapter (S3/GCS/Mock) satisfies both ports, so the split is
free at the adapter level while letting you wire a read-only query side (e.g. read-only
credentials or a read-replica/CDN bucket) independently.

| Section | Details |
|---------|---------|
| Purpose | `StorageQueryPort` downloads and lists; `StorageCommandPort` uploads and deletes. |
| Import path | `from forze.application.contracts.storage import StorageQueryPort, StorageCommandPort` |
| Type parameters | None. |
| Required methods | Query: `download`, `list`. Command: `upload`, `delete`. |
| Returned values | `StoredObject`, `DownloadedObject`, `None`, or `(list[StoredObject], int)`. |
| Common implementations | Mock, S3-compatible storage, GCS. |
| Related dependency keys | `StorageQueryDepKey` (resolve with `ctx.storage.query(spec)`), `StorageCommandDepKey` (resolve with `ctx.storage.command(spec)`). |
| Minimal example | See below. |
| Related pages | [Contracts overview](../contracts.md), [Mock integration](../../integrations/mock.md). |

Required methods:

| Port | Method | Parameters | Returns |
|------|--------|------------|---------|
| `StorageCommandPort` | `upload` | `UploadedObject` | `StoredObject` metadata. |
| `StorageCommandPort` | `delete` | `key` | `None`. |
| `StorageQueryPort` | `download` | `key` | `DownloadedObject` with bytes and headers. |
| `StorageQueryPort` | `list` | `limit`, `offset`, optional `prefix` | `(objects, total_count)`. |

## Storage value types

| Type | Import path | Required fields |
|------|-------------|-----------------|
| `StoredObject` | `from forze.application.contracts.storage import StoredObject` | `key`, `filename`, `description`, `content_type`, `size`, `created_at` |
| `DownloadedObject` | `from forze.application.contracts.storage import DownloadedObject` | `data`, `content_type`, `filename` |
| `ObjectMetadata` | `from forze.application.contracts.storage.types import ObjectMetadata` | `filename`, `created_at`, `size`; optional `description` |

    :::python
    from forze.application.contracts.storage import StorageSpec

    attachments = StorageSpec(name="attachments")
    stored = await ctx.storage.command(attachments).upload(
        UploadedObject(filename="report.txt", data=b"hello", prefix="reports")
    )
    downloaded = await ctx.storage.query(attachments).download(stored.key)
