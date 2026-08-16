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

## Reference

- [Mock integration](https://morzecrew.github.io/forze/latest/integrations/)
