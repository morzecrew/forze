---
name: forze-storage-gcs
description: >-
  Wires and consumes Forze object storage with StorageSpec, StoragePort,
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
from forze.application.contracts.storage import StoragePort, StoredObject, UploadedObject


class UploadAttachment(Handler[UploadAttachmentCmd, StoredObject]):
    storage: StoragePort

    async def __call__(self, cmd: UploadAttachmentCmd) -> StoredObject:
        return await self.storage.upload(
            UploadedObject(
                filename=cmd.filename,
                data=cmd.data,
                prefix=f"attachments/{cmd.project_id}",
            ),
        )
```

Resolve storage in the factory: `storage=ctx.storage(attachments_spec)`.

## Testing

Use `MockStorageAdapter` from `forze_mock` for unit tests without GCS. Integration tests use fake-gcs-server via Docker testcontainers.
