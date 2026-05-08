# Mock Integration

`forze_mock` provides in-memory adapters for all Forze contracts. It is designed for development and testing — no external infrastructure required.

## Installation

`forze_mock` is bundled with the core `forze` package and requires no extra dependencies:

    :::bash
    uv add forze

## Overview

The package supplies a single `MockDepsModule` that registers in-memory adapters for every Forze contract. All adapters share a `MockState` instance so that, for example, documents written through `DocumentCommandPort` are visible through `DocumentQueryPort` and `SearchQueryPort`.

### Available adapters

| Adapter | Implements |
|---------|-----------|
| `MockDocumentAdapter` | `DocumentQueryPort`, `DocumentCommandPort` |
| `MockSearchAdapter` | `SearchQueryPort` |
| `MockCounterAdapter` | `CounterPort` |
| `MockCacheAdapter` | `CachePort` |
| `MockIdempotencyAdapter` | `IdempotencyPort` |
| `MockStorageAdapter` | `StoragePort` |
| `MockTxManagerAdapter` | `TxManagerPort` |
| `MockQueueAdapter` | `QueueReadPort`, `QueueWritePort` |
| `MockPubSubAdapter` | `PubSubCommandPort`, `PubSubQueryPort` |
| `MockStreamAdapter` | `StreamQueryPort`, `StreamCommandPort` |
| `MockStreamGroupAdapter` | `StreamGroupQueryPort` |

## Runtime wiring

Create a module and build a runtime exactly as you would with real infrastructure:

    :::python
    from forze.application.execution import DepsPlan, ExecutionRuntime
    from forze_mock import MockDepsModule

    module = MockDepsModule()

    runtime = ExecutionRuntime(
        deps=DepsPlan.from_modules(module),
    )

No lifecycle plan is needed — mock adapters have no connections to manage.

### What gets registered

`MockDepsModule` registers adapters for all standard dependency keys:

| Key | Capability |
|-----|------------|
| `MockStateDepKey` | Shared in-memory state |
| `DocumentQueryDepKey` | Document query adapter |
| `DocumentCommandDepKey` | Document command adapter |
| `SearchQueryDepKey` | Search adapter |
| `CounterDepKey` | Counter adapter |
| `CacheDepKey` | Cache adapter |
| `IdempotencyDepKey` | Idempotency adapter |
| `StorageDepKey` | Storage adapter |
| `TxManagerDepKey` | Transaction manager (no-op) |
| `QueueReadDepKey` | Queue read adapter |
| `QueueWriteDepKey` | Queue write adapter |
| `PubSubCommandDepKey` | Pub/sub command adapter |
| `PubSubQueryDepKey` | Pub/sub query adapter |
| `StreamQueryDepKey` | Stream query adapter |
| `StreamCommandDepKey` | Stream command adapter |
| `StreamGroupQueryDepKey` | Stream group query adapter |

## Shared state

`MockState` holds all in-memory data across adapters. Documents, cache entries, counters, queues, and streams all live in the same state object. This means:

- Creating a document via `DocumentCommandPort` makes it immediately visible through `DocumentQueryPort`
- Search results reflect the current state of stored documents
- Queue messages persist until acknowledged

Access the state directly for test assertions:

    :::python
    from forze_mock import MockState, MockStateDepKey

    state = ctx.dep(MockStateDepKey)

## Using in tests

    :::python
    from forze.application.execution import DepsPlan, ExecutionContext
    from forze_mock import MockDepsModule

    module = MockDepsModule()
    deps = DepsPlan.from_modules(module).build()
    ctx = ExecutionContext(deps=deps)

    doc = ctx.doc_command(project_spec)
    created = await doc.create(CreateProjectCmd(title="Test"))

    fetched = await ctx.doc_query(project_spec).get(created.id)
    assert fetched.title == "Test"

## Using with FastAPI

Replace real infrastructure modules with mock for local development or testing:

    :::python
    from forze.application.composition.document import (
        DocumentDTOs,
        build_document_registry,
    )
    from fastapi import APIRouter

    from forze.application.execution import DepsPlan, ExecutionRuntime
    from forze_fastapi.endpoints.document import attach_document_endpoints
    from forze_mock import MockDepsModule

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsPlan.from_modules(module))

    project_dtos = DocumentDTOs(
        read=ProjectReadModel,
        create=CreateProjectCmd,
        update=UpdateProjectCmd,
    )

    registry = build_document_registry(project_spec, project_dtos)

    projects_router = APIRouter(prefix="/projects", tags=["projects"])
    attach_document_endpoints(
        projects_router,
        document=project_spec,
        dtos=project_dtos,
        registry=registry,
        ctx_dep=lambda: runtime.get_context(),
    )

    app.include_router(projects_router)

## Custom state

You can pre-seed state by passing an existing `MockState`:

    :::python
    from forze_mock import MockState, MockDepsModule

    state = MockState()
    module = MockDepsModule(state=state)

## Combining with real modules

Mix mock and real adapters by merging dependency containers:

    :::python
    from forze.application.execution import Deps, DepsPlan
    from forze_mock import MockDepsModule
    from forze_postgres import PostgresDepsModule

    mock_module = MockDepsModule()
    pg_module = PostgresDepsModule(
        client=pg,
        rw_documents={
            "projects": {
                "read": ("public", "projects"),
                "write": ("public", "projects"),
                "bookkeeping_strategy": "database",
            },
        },
    )

    deps_plan = DepsPlan.from_modules(
        lambda: Deps.merge(pg_module(), mock_module()),
    )

Note that `Deps.merge()` raises `CoreError` on key conflicts, so only combine modules that register non-overlapping keys.
