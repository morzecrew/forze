# Testing with the mock

Running the whole application against in-memory adapters — every port, no containers. The fastest feedback loop Forze offers, and the substrate [DST simulation](dst-simulation.md) builds on.

## Testing with Mock

In-memory adapters — no external services:

```python
from uuid import uuid4

from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_kits.aggregates.document import DocumentFacade, DocumentIdDTO
from forze_mock import MockDepsModule

mock_module = MockDepsModule()
runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(mock_module).freeze())

async with runtime.scope():
    ctx = runtime.get_context()
    # project_spec + registry as built in "Document composition" above
    facade = DocumentFacade(ctx=ctx, registry=registry, namespace=project_spec.default_namespace)
    some_uuid = uuid4()  # in a real test, the id you created via facade.create(...)
    result = await facade.get(DocumentIdDTO(id=some_uuid))
```

**Hybrid contexts** — pass `MockDepsModule` *alongside* real modules to get "real Postgres, mock everything else" in one list: everything the mock registers is a **fallback**, so a real registration of the same key or route wins instead of conflicting (order irrelevant; two real — or two mock — modules still raise). Caveat: an unregistered route then falls back to the mock instead of failing, so a spec-name typo resolves silently. Freeze logs that hazard set at INFO (`catch-all behind real routes: …`), also available as `check_wiring(...).fallbacks.catch_all`; to prove a test hit the real adapter, assert `"orders" in ctx.deps.store.routed_deps[DocumentQueryDepKey]`.

```python
from forze_postgres import PostgresDepsModule

DepsRegistry.from_modules(PostgresDepsModule(...), MockDepsModule(state=shared_state))
```

## Running the mock as a server

In-process wiring covers your own tests. When something *outside* the process needs to talk to the app — a frontend in development, a contract test in another language — `forze_mock.server` serves the same mock-backed app over HTTP, with the same fallback rule:

```python
from forze_mock.server import MockApp

mock_app = MockApp(build_app=build_app, deps=(), seed=seed_plan)
```

`seed` is applied once the runtime scope opens and re-applied by `POST /_mock/reset`, which is what lets a consumer's suite start each run from the same fixtures. Declare it: an unseeded plane still answers every read — successfully, with nothing in it — so a caller can pass against a backend that holds no data at all.

## The one shape the mock cannot hold

The mock stores what was written and reads it back through the read model, so an
aggregate whose read model **requires a field no write produces** cannot round-trip
through it — the ordinary shape of a read model assembled by a SQL view.
`lenient_read_fields` does not help: it rehydrates from the model default and so refuses a
required field.

Declare those fields instead, and the mock performs the join:

```python
from forze.application.contracts.conformity import DerivedReadField
from forze.application.contracts.document import DocumentSpec

ORDERS = DocumentSpec(
    name="orders",
    read=OrderRead,  # carries `supplier: str`; no write produces it
    derived_read_fields={
        "supplier": DerivedReadField(source="suppliers", via="supplier_id", field="name"),
    },
)
```

One hop, by primary key. A derived field is not filterable, sortable or sealable — this
aggregate holds no column for it — and a key resolving to no row is refused rather than
silently `None`, because in a store that holds every row that is a seeding bug. Real
backends read the view's column as before.

## Surviving a restart

An MVP running on the mock can keep its data across restarts without a container.
`MockStatePersistence` snapshots the whole `MockState` at shutdown and loads it at startup:

```python
from datetime import timedelta
from pathlib import Path

from forze.application.execution import LifecyclePlan
from forze_mock import MockStatePersistence, mock_state_lifecycle_step

persistence = MockStatePersistence(
    path=Path(".forze/mvp.state"),
    flush_every=timedelta(minutes=5),  # optional; shutdown-only without it
)

lifecycle = LifecyclePlan.from_steps(
    mock_state_lifecycle_step(state=state, persistence=persistence),
)
```

`state` is the same `MockState` the deps module was built with. A missing file is a first
run, not an error. Periodic flushing is off by default, since it needs a background task
a serverless host cannot keep running between invocations.

Everything the mock implements comes back: documents, counters, outbox and inbox rows,
stored objects, identity, durable runs. What does not is anything whose meaning is local to
the process that wrote it — held locks and in-flight transactions restore as none held,
none active, because a lock expiry measured against one process's clock means nothing
against another's. Persistence alongside a per-tenant routed state registry is refused at
startup rather than writing a snapshot that would silently miss every tenant's data.

It is a snapshot, not a database: one process, all in RAM, no crash durability between
flushes.

## Reference

- [Mock integration](https://morzecrew.github.io/forze/latest/integrations/)
