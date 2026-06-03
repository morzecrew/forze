# Google Cloud Storage Integration

## What this integration provides

Store and retrieve binary objects behind Forze storage contracts without coupling handlers to the GCS SDK.

## When to use it

Use this when you run on GCP (or local fake-gcs-server) and want native GCS buckets with Application Default Credentials instead of S3 interoperability.

Use `RoutedGCSClient` when tenant identity selects GCP project and credentials (`tenant_aware` on storage routes still prefixes object keys).

## Standard setup checklist

1. Install the matching optional extra.
2. Create the integration client or module configuration.
3. Register the module in `DepsRegistry` with routes that match your specs.
4. Add lifecycle steps when the integration opens network connections.
5. Resolve ports from `ExecutionContext`; do not import adapters in handlers.

`forze_gcs` implements `StoragePort` using native async [`gcloud-aio-storage`](https://pypi.org/project/gcloud-aio-storage/).

## Installation

    :::bash
    uv add 'forze[gcs]'

## Runtime wiring

    :::python
    from forze.application.execution import DepsRegistry, ExecutionRuntime, LifecyclePlan
    from forze_gcs import GCSClient, GCSDepsModule, GCSStorageConfig, gcs_lifecycle_step

    client = GCSClient()
    module = GCSDepsModule(
        client=client,
        storages={"app-assets": GCSStorageConfig(bucket="my-project-assets")},
    )

    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(module).freeze(),
        lifecycle=LifecyclePlan.from_steps(
            gcs_lifecycle_step(project_id="my-gcp-project"),
        ).freeze(),
    )

### Emulator (fake-gcs-server)

For local development and integration tests, set `STORAGE_EMULATOR_HOST` to the emulator base URL **before** starting the runtime (for example `http://localhost:4443`). The client reads this environment variable at initialization; lifecycle and application code do not take an emulator URL parameter.

Start [fake-gcs-server](https://github.com/fsouza/fake-gcs-server) (mapped port), then wire lifecycle as usual:

    :::python
    gcs_lifecycle_step(project_id="local-dev")

### Service account credentials

By default the client uses Application Default Credentials. To use an explicit key file:

    :::python
    gcs_lifecycle_step(
        project_id="my-gcp-project",
        service_file="/path/to/service-account.json",
    )

### Routed client

Register `RoutedGCSClient` under `GCSClientDepKey` and use `routed_gcs_lifecycle_step(client=routed_gcs)`. Per-tenant secrets use `GCSRoutingCredentials` (same shape as BigQuery: `project_id`, optional `service_file` or `service_account_json`).

### What gets registered

| Key | Capability |
|-----|-----------|
| `GCSClientDepKey` | Raw GCS client for direct bucket/blob operations |
| `StorageDepKey` | Storage port adapter factory |

## Using the storage port

    :::python
    from forze.application.contracts.storage import StorageSpec, UploadedObject

    storage = ctx.storage(StorageSpec(name="app-assets"))

### Upload

    :::python
    stored = await storage.upload(
        UploadedObject(
            filename="invoice.pdf",
            data=pdf_bytes,
            description="Invoice #42",
            prefix="invoices/2026/03",
        ),
    )

The adapter generates a unique key from the prefix and a UUID v7 segment. Content type is detected with `python-magic`.

### Download

    :::python
    downloaded = await storage.download(stored.key)

### Delete

    :::python
    await storage.delete(stored.key)

### List

    :::python
    objects, total = await storage.list(
        limit=20,
        offset=0,
        prefix="invoices/2026",
    )

## Operation reference

| Method | Returns | Purpose |
|--------|---------|---------|
| `upload(UploadedObject)` | `StoredObject` | Upload bytes and return metadata |
| `download(key)` | `DownloadedObject` | Download previously stored object |
| `delete(key)` | `None` | Delete an object by key |
| `list(limit, offset, *, prefix?)` | `(list[StoredObject], int)` | Paginated listing with optional prefix filter |

## Multi-tenant behavior

When `ExecutionContext` has a bound `TenantIdentity` and the storage route config sets `tenant_aware=True`, object keys are prefixed with `tenant_{tenant_id}/`.

## Scope of the integration

Forze handles resolving `StoragePort`, upload/download/delete/list, content-type detection, metadata in custom blob metadata, and optional tenant key prefixes.

Forze does **not** manage IAM, bucket lifecycle rules, CORS, encryption defaults, or signed URLs — configure those in GCP or IaC.

For framework tests or advanced wiring, prefer `from forze_gcs.execution.deps import ConfigurableGCSStorage` rather than removed `forze_gcs.execution.deps.deps` paths.
