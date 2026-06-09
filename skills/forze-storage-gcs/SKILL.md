---
name: forze-storage-gcs
description: >-
  Wires and consumes Forze object storage with StorageSpec, StorageQueryPort, StorageCommandPort,
  GCSDepsModule, tenant-aware buckets, lifecycle, upload/download/list/delete,
  and tests with MockStorageAdapter. Use when adding GCS blob/file storage.
---

# Forze storage and GCS

Use when adding blob storage on Google Cloud Storage (`gcloud-aio-storage` async client). For S3-compatible backends, see [`forze-storage-s3`](../forze-storage-s3/SKILL.md). For general handler patterns, see [`forze-framework-usage`](../forze-framework-usage/SKILL.md).

## Spec and deps route

`StorageSpec.name` is the logical route. Prefer a shared `StrEnum` and register the same route in `GCSDepsModule.storages`.

```python
from enum import StrEnum

from forze.application.contracts.storage import StorageSpec
from forze_gcs import GCSClient, GCSDepsModule


class ResourceName(StrEnum):
    PROJECT_ATTACHMENTS = "project-attachments"


attachments_spec = StorageSpec(name=ResourceName.PROJECT_ATTACHMENTS)

gcs_client = GCSClient()
gcs_module = GCSDepsModule(
    client=gcs_client,
    storages={
        ResourceName.PROJECT_ATTACHMENTS: {
            "bucket": "project-files",
            "tenant_aware": True,
        }
    },
)
```

## Lifecycle

```python
from forze.application.execution import LifecyclePlan
from forze_gcs import gcs_lifecycle_step

lifecycle = LifecyclePlan.from_steps(
    gcs_lifecycle_step(project_id="my-gcp-project"),
)
```

For local fake-gcs-server, set `STORAGE_EMULATOR_HOST=http://localhost:4443` before startup (mapped port). Optional `service_file` on lifecycle for explicit service account JSON (ADC otherwise).

## Handler pattern

```python
from forze.application.contracts.execution import Handler
from forze.application.contracts.storage import StorageCommandPort, StoredObject, UploadedObject


class UploadAttachment(Handler[UploadAttachmentCmd, StoredObject]):
    storage: StorageCommandPort

    async def __call__(self, cmd: UploadAttachmentCmd) -> StoredObject:
        return await self.storage.upload(
            UploadedObject(
                filename=cmd.filename,
                data=cmd.data,
                prefix=f"attachments/{cmd.project_id}",
            ),
        )
```

Storage is CQRS-split. Resolve the command side in the factory for writes:
`storage=ctx.storage.command(attachments_spec)`; use `ctx.storage.query(spec)` for
`download` / `list`.

## Tenant-aware storage

When `tenant_aware=True`, bind `TenantIdentity` at the HTTP/worker boundary before calling storage; do not pass tenant ids through domain DTOs solely for storage routing.

## Testing

Use `MockStorageAdapter` from `forze_mock` for unit tests without GCS. For integration-style checks, use fake-gcs-server as described in the [GCS integration](https://morzecrew.github.io/forze/docs/integrations/gcs/) doc.

## Anti-patterns

1. **Putting bucket names in `StorageSpec`** — specs carry logical names; deps config carries bucket names.
2. **Skipping `GCSDepsModule.storages`** — no storage route is registered.
3. **Using object storage as transactional state** — write document metadata in a transaction, then run storage side effects after commit when consistency matters.
4. **Hard-coding service account JSON in application code** — use ADC, workload identity, or a secrets layer.
5. **Assuming Forze creates buckets or IAM** — manage GCP resources with your infrastructure tooling.

## Reference

- [GCS integration](https://morzecrew.github.io/forze/docs/integrations/gcs/)
- [Storage contracts](https://morzecrew.github.io/forze/docs/core-package/contracts/storage/)
- [`forze-storage-s3`](../forze-storage-s3/SKILL.md)
- [`forze-framework-usage`](../forze-framework-usage/SKILL.md)
