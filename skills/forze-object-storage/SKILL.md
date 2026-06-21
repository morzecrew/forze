---
name: forze-object-storage
description: >-
  Wires and consumes Forze object storage with StorageSpec, the StorageFacade /
  build_storage_registry kit, StorageQueryPort / StorageCommandPort, the S3
  (S3DepsModule) and GCS (GCSDepsModule) backends, tenant-aware buckets,
  lifecycle, upload/download/list/delete and presigned/multipart uploads, and
  MockStorageAdapter tests. Use when adding blob/file storage on S3-compatible
  or Google Cloud Storage backends.
---

# Forze object storage (S3 & GCS)

Use when adding blob/file storage to a Forze app. The contract is one `StorageSpec` and one port surface; only the deps module and lifecycle step differ per backend (S3-compatible vs Google Cloud Storage). For general handler patterns, see [`forze-framework-usage`](../forze-framework-usage/SKILL.md).

## Spec and deps route

`StorageSpec.name` is the logical route — prefer a shared `StrEnum` and register the **same** route under the backend module's `storages` map. The spec carries no bucket name; the deps config does.

```python
from enum import StrEnum

from forze.application.contracts.storage import StorageSpec


class ResourceName(StrEnum):
    PROJECT_ATTACHMENTS = "project-attachments"


attachments_spec = StorageSpec(name=ResourceName.PROJECT_ATTACHMENTS)
```

The module's `client=` alone registers only the client key; `ctx.storage.query/command(spec)` need a matching `storages` route. Use a secrets/env layer for real credentials — never hard-code production keys or service-account JSON.

### S3 / S3-compatible

```python
from forze_s3 import S3Client, S3Config, S3DepsModule, s3_lifecycle_step
from forze.application.execution import LifecyclePlan

s3_module = S3DepsModule(
    client=S3Client(),
    storages={
        ResourceName.PROJECT_ATTACHMENTS: {"bucket": "project-files", "tenant_aware": True},
    },
)
lifecycle = LifecyclePlan.from_steps(
    s3_lifecycle_step(
        endpoint="http://localhost:9000",     # MinIO/LocalStack for local dev
        access_key_id="minioadmin",
        secret_access_key="minioadmin",
        config=S3Config(max_pool_connections=20),
    )
)
```

### Google Cloud Storage

```python
from forze_gcs import GCSClient, GCSDepsModule, gcs_lifecycle_step
from forze.application.execution import LifecyclePlan

gcs_module = GCSDepsModule(
    client=GCSClient(),
    storages={
        ResourceName.PROJECT_ATTACHMENTS: {"bucket": "project-files", "tenant_aware": True},
    },
)
lifecycle = LifecyclePlan.from_steps(
    gcs_lifecycle_step(project_id="my-gcp-project"),  # ADC, or service_file=... for explicit JSON
)
```

For local `fake-gcs-server`, set `STORAGE_EMULATOR_HOST=http://localhost:4443` before startup.

## Consuming storage

Storage spreads across three ports:

- **`StorageQueryPort`** — `ctx.storage.query(spec)` — `download`, `download_range` (ranged), `download_if_changed` (conditional), `head` (metadata, no body), `list`, `presign_download`.
- **`StorageCommandPort`** — `ctx.storage.command(spec)` — `upload`, `delete`, `copy`, `move`, `put_object_tags`, `presign_upload`.
- **`StorageUploadSessionPort`** — `ctx.storage.uploads(spec)` — the multipart session ops `begin_upload`, `presign_part`, `list_parts`, `complete_upload`, `abort_upload`.

After a presigned/direct upload (where the app never sees the bytes), use `head` to confirm the object actually landed before recording it.

**Standalone object operations (driving code)** — drive a frozen storage registry through a **`StorageFacade`**, or project it onto FastAPI with `attach_storage_routes` (see [`forze-fastapi-interface`](../forze-fastapi-interface/SKILL.md)):

```python
from forze_kits.aggregates.storage import StorageFacade, build_storage_registry

storage_registry = build_storage_registry(attachments_spec).freeze()
files = StorageFacade(ctx=ctx, registry=storage_registry, namespace=attachments_spec.default_namespace)
# files.upload(...) / files.download(...) / files.list(...) / files.delete(...)
# direct & resumable uploads: presign_download / presign_upload / begin_upload /
#   presign_part / list_parts (resume) / complete_upload / abort_upload (cleanup)
```

**Inside a custom handler** — when an upload is one step of a domain operation, resolve the port directly in the factory:

```python
import attrs

from forze.application.contracts.document import DocumentQueryPort
from forze.application.contracts.execution import Handler
from forze.application.contracts.storage import StorageCommandPort, StoredObject, UploadedObject


@attrs.define(slots=True, kw_only=True, frozen=True)
class UploadAttachment(Handler[UploadAttachmentCmd, StoredObject]):
    doc: DocumentQueryPort[ProjectRead]
    storage: StorageCommandPort

    async def __call__(self, cmd: UploadAttachmentCmd) -> StoredObject:
        # confirm the parent exists (raises not_found) — and gate authz on it — before
        # writing object storage, or an invalid id leaves an orphaned project-scoped blob
        await self.doc.get(cmd.project_id)
        return await self.storage.upload(
            UploadedObject(filename=cmd.filename, data=cmd.data, prefix=f"projects/{cmd.project_id}"),
        )
# factory: lambda ctx: UploadAttachment(
#     doc=ctx.document.query(project_spec), storage=ctx.storage.command(attachments_spec))
```

The adapter generates collision-resistant object keys and detects content type.

## Tenant-aware storage

With `tenant_aware=True`, the adapter derives the tenant from `ExecutionContext`. Bind `TenantIdentity` at the HTTP/worker boundary before calling storage; do not thread tenant ids through domain DTOs solely for storage routing.

## Testing

`MockDepsModule` registers the storage keys with `MockStorageAdapter` (`forze_mock`), so unit tests use the facade or `ctx.storage.query/command(StorageSpec(...))` with no S3/GCS. For integration checks, use MinIO/LocalStack (S3) or `fake-gcs-server` (GCS).

## Anti-patterns

1. **Putting bucket names in `StorageSpec`** — specs carry logical names; deps config carries buckets.
2. **Skipping the module's `storages` route** — no storage port is registered, resolution fails.
3. **Using object storage as transactional state** — write document metadata in a transaction, then run storage side effects after commit when consistency matters.
4. **Hard-coding cloud credentials / service-account JSON** — use a secrets layer, ADC, or workload identity.
5. **Assuming Forze creates buckets/IAM/CORS** — manage provider resources with infrastructure tooling.

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version (e.g. `.../forze/0.3/...`) or use the version selector on the site.

- [S3 integration](https://morzecrew.github.io/forze/latest/integrations/s3/)
- [GCS integration](https://morzecrew.github.io/forze/latest/integrations/gcs/)
- [Storage contracts](https://morzecrew.github.io/forze/latest/reference/contracts/stores/)
- [FastAPI route generators](https://morzecrew.github.io/forze/latest/reference/fastapi-routes/)
- Sibling skill: [`forze-fastapi-interface`](../forze-fastapi-interface/SKILL.md)
