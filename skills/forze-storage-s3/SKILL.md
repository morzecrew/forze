---
name: forze-storage-s3
description: >-
  Wires and consumes Forze object storage with StorageSpec, StorageQueryPort,
  StorageCommandPort, S3DepsModule, tenant-aware buckets, lifecycle,
  upload/download/list/delete, and tests with MockStorageAdapter. Use when
  adding blob/file storage.
---

# Forze storage and S3

Use when adding blob storage to handlers or wiring S3-compatible infrastructure. For general handler patterns, see [`forze-framework-usage`](../forze-framework-usage/SKILL.md).

## Spec and deps route

`StorageSpec.name` is the logical route. Prefer a shared `StrEnum` and register the same route in `S3DepsModule.storages`.

```python
from enum import StrEnum

from forze.application.contracts.storage import StorageSpec
from forze_s3 import S3Client, S3DepsModule


class ResourceName(StrEnum):
    PROJECT_ATTACHMENTS = "project-attachments"


attachments_spec = StorageSpec(name=ResourceName.PROJECT_ATTACHMENTS)

s3_client = S3Client()
s3_module = S3DepsModule(
    client=s3_client,
    storages={
        ResourceName.PROJECT_ATTACHMENTS: {
            "bucket": "project-files",
            "tenant_aware": True,
        }
    },
)
```

`S3DepsModule(client=...)` alone registers only `S3ClientDepKey`; `ctx.storage.query(spec)` / `ctx.storage.command(spec)` need a matching `storages` route.

## Lifecycle

```python
from forze.application.execution import LifecyclePlan
from forze_s3 import S3Config, s3_lifecycle_step

lifecycle = LifecyclePlan.from_steps(
    s3_lifecycle_step(
        endpoint="http://localhost:9000",
        access_key_id="minioadmin",
        secret_access_key="minioadmin",
        config=S3Config(max_pool_connections=20),
    )
)
```

Use a secrets-backed or environment-backed configuration layer for real credentials; do not hard-code production keys.

## Handler pattern

```python
from forze.application.contracts.execution import Handler
from forze.application.contracts.storage import StorageCommandPort, StoredObject, UploadedObject

class UploadAttachment(Handler[UploadAttachmentCmd, StoredObject]):
    doc: DocumentQueryPort[ProjectRead]
    storage: StorageCommandPort

    async def __call__(self, args: UploadAttachmentCmd) -> StoredObject:
        await self.doc.get(args.project_id)
        return await self.storage.upload(
            UploadedObject(
                filename=args.filename,
                data=args.data,
                description=args.description,
                prefix=f"projects/{args.project_id}",
            )
        )
```

Storage is CQRS-split: `upload` / `delete` live on `StorageCommandPort` (resolve via `ctx.storage.command(spec)`); `download` / `list` live on `StorageQueryPort` (resolve via `ctx.storage.query(spec)`). The S3 adapter generates collision-resistant object keys and detects content type.

## Tenant-aware storage

When `tenant_aware=True`, the adapter derives tenant information from `ExecutionContext`. Bind `TenantIdentity` at the HTTP/worker boundary before calling storage; do not pass tenant ids through domain DTOs solely for storage routing.

## Testing

`MockDepsModule` registers `StorageQueryDepKey` and `StorageCommandDepKey` with `MockStorageAdapter`, so unit tests can use `ctx.storage.query(StorageSpec(...))` / `ctx.storage.command(StorageSpec(...))` without S3 or MinIO.

## Anti-patterns

1. **Putting bucket names in `StorageSpec`** — specs carry logical names; deps config carries bucket names.
2. **Skipping `S3DepsModule.storages`** — no storage route is registered.
3. **Using object storage as transactional state** — write document metadata in a transaction, then run storage side effects after commit when consistency matters.
4. **Hard-coding cloud credentials in code or examples** — use secrets/config outside application specs.
5. **Assuming Forze creates buckets/IAM/CORS** — manage provider resources with infrastructure tooling.

## Reference

- [S3 integration](https://morzecrew.github.io/forze/docs/integrations/s3/)
- [Storage contracts](https://morzecrew.github.io/forze/docs/core-package/contracts/storage/)
- [`forze-framework-usage`](../forze-framework-usage/SKILL.md)
