# Cloud Firestore Integration

## What this integration provides

Persist documents and transactions in Google Cloud Firestore behind Forze document contracts.

## When to use it

Use this when Firestore is your document store and you want the same `DocumentSpec` / query DSL surface as Postgres or Mongo, with explicit errors for unsupported query shapes.

Use `RoutedFirestoreClient` when tenant identity selects Firestore `project_id` and `database` (row-level isolation can still use `tenant_aware` on document routes). Per-tenant service account overrides are not supported in v1 (ADC only).

## Standard setup checklist

1. Install the matching optional extra.
2. Create the integration client or module configuration.
3. Register the module in `DepsRegistry` with routes that match your specs.
4. Add lifecycle steps when the integration opens network connections.
5. Resolve ports from `ExecutionContext`; do not import adapters in handlers.

`forze_firestore` implements `DocumentQueryPort`, `DocumentCommandPort`, and `TxManagerPort` using `google.cloud.firestore.AsyncClient`.

Kernel `DocumentSpec` names must match keys in `FirestoreDepsModule.rw_documents` / `ro_documents`. See [Specs and infrastructure wiring](../concepts/specs-and-wiring.md).

## Installation

    :::bash
    uv add 'forze[firestore]'

## Runtime wiring

    :::python
    from forze.application.execution import DepsRegistry, ExecutionRuntime, LifecyclePlan
    from forze_firestore import (
        FirestoreClient,
        FirestoreConfig,
        FirestoreDepsModule,
        FirestoreDocumentConfig,
        firestore_lifecycle_step,
    )

    client = FirestoreClient()
    module = FirestoreDepsModule(
        client=client,
        rw_documents={
            "projects": FirestoreDocumentConfig(
                read=("(default)", "projects"),
                write=("(default)", "projects"),
                history=("(default)", "projects_history"),
            ),
        },
        tx={"default"},
    )

    # Local emulator: set before startup (or use firestore_lifecycle_step(emulator_host=...)).
    os.environ["FIRESTORE_EMULATOR_HOST"] = "127.0.0.1:19280"

    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(module).freeze(),
        lifecycle=LifecyclePlan.from_steps(
            firestore_lifecycle_step(
                project_id="my-gcp-project",
                database="(default)",
                config=FirestoreConfig(),
            )
        ).freeze(),
    )

### FirestoreConfig options

| Option | Type | Default | Purpose |
|--------|------|---------|---------|
| (reserved) | — | — | Placeholder for future client tuning |

#### `read_validation` (read throughput)

`FirestoreReadOnlyDocumentConfig` and `FirestoreDocumentConfig` accept `read_validation`:

| Value | Behavior |
|-------|----------|
| `"strict"` (default) | Full Pydantic validation on every document returned from reads. |
| `"trusted"` | Build read models with `model_construct` when stored fields match the read model (no validator run). |

Use `"trusted"` only when Firestore field shapes match `DocumentSpec.read` and decoded values already match expected Python types. Extra fields not on the read model raise a precondition error. History blobs, cache payloads, and write paths stay strict.

### Routed client

Register `RoutedFirestoreClient` under `FirestoreClientDepKey` and use `routed_firestore_lifecycle_step(client=routed_fs)`. Per-tenant JSON: `FirestoreRoutingCredentials` with `project_id` and `database`.

### What gets registered

| Key | Capability |
|-----|------------|
| `FirestoreClientDepKey` | Firestore client |
| `DocumentQueryDepKey` | Routed document query factories |
| `DocumentCommandDepKey` | Routed document command factories |
| `TxManagerDepKey` | Transaction managers per route in `tx` |

## DocumentSpec and Firestore config

`DocumentSpec` carries model types, `history_enabled`, and optional `CacheSpec`. Per-database mapping uses `FirestoreDocumentConfig`:

| Field | Purpose |
|-------|---------|
| `read` | `(database_id, collection_id)` for reads |
| `write` | `(database_id, collection_id)` for writes |
| `history` | Optional `(database_id, collection_id)` for snapshots |
| `batch_size` | Optional write batch size |
| `tenant_aware` | Optional tenant field handling |

Document primary keys are stored as **string** Firestore document IDs (domain `id` as string).

## Document operations

    :::python
    doc_q = ctx.document.query(project_spec)
    doc_c = ctx.document.command(project_spec)

    created = await doc_c.create(CreateProjectCmd(title="Alpha"))
    fetched = await doc_q.get(created.id)
    updated = await doc_c.update(
        created.id,
        created.rev,
        UpdateProjectCmd(title="Beta"),
    )

## Query and filter behavior

The Firestore adapter supports a **subset** of the shared query DSL. Unsupported shapes raise `CoreError` or `InvalidOperationError` with a hint (no silent fallback).

Supported in MVP:

- `$values` equality, ordering, membership, `$null`, `$empty`
- `$and`, limited `$or` (when expressible as one Firestore-legal disjunction)
- Sorts and `count`
- `find_many`, `find_page` (counted; **offset must be 0**)
- **Cursor** pagination (`find_cursor`) with id-based keyset

Not supported in MVP (explicit errors):

| Feature | Error |
|---------|--------|
| `$fields`, `$not` | `CoreError` |
| `$any` / `$all` / `$none` | `CoreError` |
| `$like` / `$ilike` / `$regex` | `CoreError` |
| Aggregates / `aggregate_page` / `select_page_aggregated` | `CoreError` |
| Large offset pagination | `InvalidOperationError` (use cursor pagination) |
| `update_matching` | `CoreError` |
| `for_update` | No row lock (documented no-op at gateway) |

See [Query Syntax](../reference/query-syntax.md).

### Firestore-specific behavior

- Compound queries may require composite indexes in production; the integration surfaces `CoreError` when a filter cannot be expressed.
- The SDK connects to the host in `FIRESTORE_EMULATOR_HOST` (insecure gRPC on that host:port). Integration tests publish the emulator on **19280** so it is not confused with a local HTTP proxy often bound to **1081**; tests clear proxy env vars for the session only.

## Transactions

Use `ctx.tx_ctx.scope("firestore")` (or your configured tx route). Firestore requires **all reads before writes** in a transaction. The write gateway materializes results after writes when inside a transaction; `FirestoreDocumentAdapter.create` avoids a post-create read in that case.

Keep transaction scope small (operation count and contention limits apply on the real service).

During development, enable runtime tracing (`FORZE_RUNTIME_TRACE` or `DepsRegistry(...).with_tracing(runtime=True).freeze()`) and run `validate_runtime_trace(deps.runtime_trace(), validator=validate_reads_before_writes_in_tx)` (from `forze_firestore.execution.trace_validation`) to catch handlers that call `document_query` reads after `document_command` writes in the same transaction segment. See [Execution reference](../reference/execution.md#runtime-tracing-development).

    :::python
    async with ctx.tx_ctx.scope("firestore"):
        existing = await doc_q.get(existing_id)
        await doc_c.update(
            existing.id,
            existing.rev,
            UpdateProjectCmd(title="Also in tx"),
        )

## Revision and history

The adapter manages `rev` in application space. When `history_enabled` and a `history` collection are configured, snapshots are stored after writes (separate collection, keyed by document id and revision).

## Local emulator (tests)

Integration tests start the gcloud emulator via `testcontainers` (`DockerContainer` + `gcloud beta emulators firestore start`). Python `testcontainers.google` does not ship a dedicated Firestore container class.

    :::bash
    export FIRESTORE_EMULATOR_HOST=127.0.0.1:19280
    just test tests/integration/test_forze_firestore

## Differences from Mongo

| Aspect | MongoDB | Firestore |
|--------|---------|-----------|
| Client | Async Motor | `AsyncClient` |
| Config | `(database, collection)` | `(database_id, collection_id)` |
| Transactions | Replica set; read after write OK | Reads before writes; smaller limits |
| Query surface | Broader DSL | MVP subset with explicit errors |
| Offset pages | Supported | Only `offset=0`; use cursor for paging |

For framework tests or advanced wiring, prefer `from forze_firestore.execution.deps import ConfigurableFirestoreDocument`, `ConfigurableFirestoreReadOnlyDocument`, and `firestore_txmanager` rather than removed `forze_firestore.execution.deps.deps` paths.
