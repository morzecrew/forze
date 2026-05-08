# Storage contracts

Storage contracts provide S3-style object storage for binary data and metadata.

## `StorageSpec`

| Section | Details |
|---------|---------|
| Purpose | Names a logical object storage backend. |
| Import path | `from forze.application.contracts.storage import StorageSpec` |
| Type parameters | None. |
| Required fields | `name`. |
| Returned values | Passed to `ctx.storage(spec)` to resolve `StoragePort`. |
| Common implementations | Mock storage adapter, S3-compatible adapter. |
| Related dependency keys | `StorageDepKey`. |
| Minimal example | `attachments = StorageSpec(name="attachments")` |
| Related pages | [S3-Compatible Storage](../../integrations/s3.md). |

## `StoragePort`

| Section | Details |
|---------|---------|
| Purpose | Uploads, downloads, deletes, and lists binary objects. |
| Import path | `from forze.application.contracts.storage import StoragePort` |
| Type parameters | None. |
| Required methods | `upload`, `download`, `delete`, `list`. |
| Returned values | `StoredObject`, `DownloadedObject`, `None`, or `(list[StoredObject], int)`. |
| Common implementations | Mock, S3-compatible storage. |
| Related dependency keys | `StorageDepKey`; resolve with `ctx.storage(spec)`. |
| Minimal example | See below. |
| Related pages | [Contracts overview](../contracts.md), [Mock integration](../../integrations/mock.md). |

Required methods:

| Method | Parameters | Returns |
|--------|------------|---------|
| `upload` | `filename`, `data`, optional `description`, `prefix` | `StoredObject` metadata. |
| `download` | `key` | `DownloadedObject` with bytes and headers. |
| `delete` | `key` | `None`. |
| `list` | `limit`, `offset`, optional `prefix` | `(objects, total_count)`. |

## Storage value types

| Type | Import path | Required fields |
|------|-------------|-----------------|
| `StoredObject` | `from forze.application.contracts.storage import StoredObject` | `key`, `filename`, `description`, `content_type`, `size`, `created_at` |
| `DownloadedObject` | `from forze.application.contracts.storage import DownloadedObject` | `data`, `content_type`, `filename` |
| `ObjectMetadata` | `from forze.application.contracts.storage.types import ObjectMetadata` | `filename`, `created_at`, `size`; optional `description` |

    :::python
    from forze.application.contracts.storage import StorageSpec

    attachments = StorageSpec(name="attachments")
    storage = ctx.storage(attachments)
    stored = await storage.upload("report.txt", b"hello", prefix="reports")
    downloaded = await storage.download(stored["key"])
