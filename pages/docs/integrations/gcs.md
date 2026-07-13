---
title: Google Cloud Storage
icon: lucide/cloud
summary: Object upload, download, list, and delete on Google Cloud Storage
---

`forze[gcs]` implements object storage on Google Cloud Storage — the same
storage contracts as [S3](s3.md), on GCS buckets.

## Install

```bash
uv add 'forze[gcs]'
```

Needs GCS (or `fake-gcs-server` via `STORAGE_EMULATOR_HOST`).

## The client

```python
from forze_gcs import GCSClient

gcs = GCSClient()
```

`RoutedGCSClient` (with `GCSRoutingCredentials`) resolves per-tenant
projects/credentials.

## Wire it

Each storage route names a **bucket**, keyed by `StorageSpec.name`:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_gcs import GCSClient, GCSDepsModule, GCSStorageConfig, gcs_lifecycle_step

deps = DepsRegistry.from_modules(
    GCSDepsModule(client=gcs, storages={"assets": GCSStorageConfig(bucket="my-assets")}),
)
lifecycle = LifecyclePlan.from_steps(gcs_lifecycle_step(project_id="my-project"))
```

## What it provides

| Contract | Operations | Keyed by |
|----------|-----------|----------|
| Storage query | `download`, `download_stream`, `download_range`, `download_if_changed`, `head`, `presign_download`, `list` | `StorageSpec.name` (`storages`) |
| Storage command | `upload`, `upload_stream`, `overwrite_stream`, `presign_upload`, `delete`, `copy`, `move`, `put_object_tags` | `StorageSpec.name` (`storages`) |
| Upload sessions (resumable multipart) | `begin` … `complete` / `abort` | `StorageSpec.name` (`storages`) |

The full method surface is the
[storage contract](../reference/contracts/stores.md#storage); the streamed and
ranged downloads are what the generated
[download route](../reference/fastapi-routes.md#downloads-streaming-ranges-caching)
serves by default.

## Notes

- **You provision buckets, IAM, and lifecycle rules.** Forze only does
  query/command (with content-type detection).
- Credentials default to Application Default Credentials; pass `service_file` for
  an explicit key, or `RoutedGCSClient` for per-tenant projects.
- With `tenant_aware`, object keys are prefixed per tenant.
