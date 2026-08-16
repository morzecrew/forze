# Object storage

Blobs through a `StorageSpec`: S3 and GCS wiring, tenant-aware buckets, presigned and multipart uploads, and the mock. Object semantics only — never transactional state.

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
import os

from forze_s3 import S3Client, S3Config, S3DepsModule, S3StorageConfig, s3_lifecycle_step
from forze.application.execution import LifecyclePlan

s3_module = S3DepsModule(
    client=S3Client(),
    storages={
        ResourceName.PROJECT_ATTACHMENTS: S3StorageConfig(bucket="project-files", tenant_aware=True),
    },
)
lifecycle = LifecyclePlan.from_steps(
    s3_lifecycle_step(
        # read credentials from env/secrets — never commit literal keys
        endpoint=os.environ["S3_ENDPOINT"],          # e.g. http://localhost:9000 for MinIO or another S3-compatible endpoint
        access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        config=S3Config(max_pool_connections=20),
    )
)
```

### Google Cloud Storage

```python
from forze_gcs import GCSClient, GCSDepsModule, GCSStorageConfig, gcs_lifecycle_step
from forze.application.execution import LifecyclePlan

gcs_module = GCSDepsModule(
    client=GCSClient(),
    storages={
        ResourceName.PROJECT_ATTACHMENTS: GCSStorageConfig(bucket="project-files", tenant_aware=True),
    },
)
lifecycle = LifecyclePlan.from_steps(
    gcs_lifecycle_step(project_id="my-gcp-project"),  # ADC, or service_file=... for explicit JSON
)
```

For local `fake-gcs-server`, set `STORAGE_EMULATOR_HOST=http://localhost:4443` before startup.

## Consuming storage

Storage spreads across three ports:

- **`StorageQueryPort`** — `ctx.storage.query(spec)` — `download`, `download_stream` (bounded-memory chunked download), `download_range` (ranged), `download_if_changed` (conditional), `head` (metadata, no body), `list`, `presign_download`.
- **`StorageCommandPort`** — `ctx.storage.command(spec)` — `upload`, `upload_stream` / `overwrite_stream` (bounded-memory streaming writes), `delete`, `copy`, `move`, `put_object_tags`, `presign_upload`.

Streaming works with client-side encryption: an encrypting route seals/decrypts **chunk-by-chunk** (chunked-AEAD), so large encrypted blobs — including ranged reads over them — never sit whole in memory.
- **`StorageUploadSessionPort`** — `ctx.storage.uploads(spec)` — the multipart session ops `begin_upload`, `presign_part`, `list_parts`, `complete_upload`, `abort_upload`.

After a presigned/direct upload (where the app never sees the bytes), confirm the object landed with `ctx.storage.query(spec).head(...)` before recording it — `head` is a port call, not a facade method.

**Standalone object operations (driving code)** — drive a frozen storage registry through a **`StorageFacade`**, or project it onto FastAPI with `attach_storage_routes` (see [FastAPI routes](fastapi-generated-routes.md)):

```python
from forze_kits.aggregates.storage import (
    BeginUploadRequestDTO,
    CompleteUploadRequestDTO,
    ListObjectsRequestDTO,
    PresignDownloadRequestDTO,
    PresignPartRequestDTO,
    PresignUploadRequestDTO,
    StorageFacade,
    UploadObjectRequestDTO,
    UploadSessionRequestDTO,
    build_storage_registry,
)

storage_registry = build_storage_registry(attachments_spec).freeze()
files = StorageFacade(ctx=ctx, registry=storage_registry, namespace=attachments_spec.default_namespace)
# download(key) and delete(key) take the raw object-key string; every other
# method takes its request DTO from forze_kits.aggregates.storage, e.g.
# files.upload(UploadObjectRequestDTO(...)) / files.list(ListObjectsRequestDTO(...))
# presign: presign_download(PresignDownloadRequestDTO) / presign_upload(PresignUploadRequestDTO)
# multipart: begin_upload(BeginUploadRequestDTO) / presign_part(PresignPartRequestDTO) /
#   list_parts + abort_upload(UploadSessionRequestDTO) / complete_upload(CompleteUploadRequestDTO)
```

The facade stops there. The remaining port operations — `head`, `download_stream`, `download_range`, `download_if_changed` (query) and `upload_stream`, `overwrite_stream`, `copy`, `move`, `put_object_tags` (command) — have no facade method; reach them through `ctx.storage.query(spec)` / `ctx.storage.command(spec)`.

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

## Behavior worth knowing

- **`list` returns a `StoredObjectPage`** — `objects`, `total`, and `container_missing` — not a `(objects, total)` tuple.
- **Listing a bucket that does not exist raises** by default (a missing bucket is a provisioning fault, not a caller miss, and it is classified `configuration` so a retry loop stops instead of asking forever). Pass `missing_ok=True` when a not-yet-provisioned bucket should read as an empty listing — the page then comes back flagged `container_missing`, so tolerating the absence does not mean being unable to see it. Under per-tenant buckets that flag is the difference between a tenant nobody provisioned and one that has uploaded nothing. Object listing also bounds its per-object HEAD fan-out, so a large prefix does not turn one call into thousands of concurrent requests.
- **Tags on a streamed (multipart) upload** are applied after completion — multipart completion itself carries no tagging. Tag-dependent lifecycle rules therefore see the object a moment after the last part lands.
- **An unconditional `overwrite_stream` creates the object** when it does not exist, on the mock exactly as on S3 and GCS.

## Testing

`MockDepsModule` registers the storage keys with `MockStorageAdapter` (`forze_mock`), so unit tests use the facade or `ctx.storage.query/command(StorageSpec(...))` with no S3/GCS. For integration checks, use MinIO or floci (S3) or `fake-gcs-server` (GCS).

## Anti-patterns

- **Putting bucket names in `StorageSpec`** — specs carry logical names; deps config carries buckets.
- **Skipping the module's `storages` route** — no storage port is registered, resolution fails.
- **Using object storage as transactional state** — write document metadata in a transaction, then run storage side effects after commit when consistency matters.
- **Hard-coding cloud credentials / service-account JSON** — use a secrets layer, ADC, or workload identity.
- **Assuming Forze creates buckets/IAM/CORS** — manage provider resources with infrastructure tooling.

## Reference

- [S3 integration](https://morzecrew.github.io/forze/latest/integrations/s3/)
- [GCS integration](https://morzecrew.github.io/forze/latest/integrations/gcs/)
- [Storage contracts](https://morzecrew.github.io/forze/latest/reference/contracts/stores/)
- [FastAPI route generators](https://morzecrew.github.io/forze/latest/reference/fastapi-routes/)
