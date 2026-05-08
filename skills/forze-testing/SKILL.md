---
name: forze-testing
description: >-
  Tests Forze applications and integrations with pytest, MockDepsModule,
  fake ports, ExecutionContext, runtime scopes, testcontainers-backed
  integrations, and focused quality checks. Use when writing or debugging tests.
---

# Forze testing

Use when testing Forze usecases, dependency wiring, adapters, or integrations. For this repository, default to `justfile` commands.

## Unit-test usecases with mock deps

`MockDepsModule` registers in-memory document, search, cache, counter, storage, idempotency, queue, pub/sub, stream, and transaction adapters.

```python
from forze.application.execution import DepsPlan, ExecutionContext
from forze_mock import MockDepsModule

deps = DepsPlan.from_modules(MockDepsModule()).build()
ctx = ExecutionContext(deps=deps)

created = await ctx.doc_command(project_spec).create(CreateProjectCmd(title="Demo"))
fetched = await ctx.doc_query(project_spec).get(created.id)
assert fetched.title == "Demo"
```

Use a shared `MockState` when multiple contexts need to observe the same data.

## Fake one port when behavior is narrow

For small usecase tests, register only the dependency keys the usecase needs.

```python
from forze.application.execution import Deps, ExecutionContext
from forze.application.contracts.workflow import WorkflowCommandDepKey

deps = Deps.routed(
    {
        WorkflowCommandDepKey: {
            workflow_spec.name: lambda ctx, spec: FakeWorkflowCommandPort()
        }
    }
)
ctx = ExecutionContext(deps=deps)
```

This keeps tests fast and avoids unrelated mock behavior.

## Test runtime/lifecycle wiring

Use `ExecutionRuntime` when the code under test calls `runtime.get_context()` or depends on lifecycle.

```python
runtime = ExecutionRuntime(deps=DepsPlan.from_modules(MockDepsModule()))

async with runtime.scope():
    ctx = runtime.get_context()
    await usecase_factory(ctx)(args)
```

Do not call `runtime.get_context()` outside `runtime.scope()`.

## Integration tests

Integration tests under `tests/integration` use real infrastructure via testcontainers and require Docker. Unit tests should not depend on Docker, Postgres, Redis, MinIO, MongoDB, RabbitMQ, SQS, or Temporal services.

Useful commands:

| Task | Command |
|------|---------|
| Unit path | `just test-fast tests/unit/path/to/test.py` |
| All non-perf tests | `just test-fast` |
| Perf tests | `just test-perf` |
| Quality checks | `just quality` |

## Assertions that catch wiring bugs

- Assert `DepsPlan.from_modules(...).build()` succeeds for intended module combinations.
- Assert `ctx.dep(Key, route=spec.name)` resolves for every spec exposed by the module.
- Use `StrEnum` spec names in tests when adding new deps modules.
- Test both query and command routes when a module registers split ports.
- For transaction-aware behavior, test `UsecasePlan` or `ctx.transaction(route)` with the same route enum used by deps.

## Anti-patterns

1. **Using real infrastructure in unit tests** — use `MockDepsModule` or fake ports.
2. **Testing adapter internals through usecases** — use adapter tests for adapter behavior and usecase tests for application behavior.
3. **Skipping route assertions for deps modules** — missing routed keys fail later in usecases.
4. **Creating contexts by hand for runtime-scope behavior** — use `ExecutionRuntime.scope()`.
5. **Running perf/integration tests without Docker** — expect testcontainers failures when Docker is unavailable.

## Reference

- [`pages/docs/integrations/mock.md`](../../pages/docs/integrations/mock.md)
- [`tests/unit`](../../tests/unit)
- [`tests/integration`](../../tests/integration)
- [`justfile`](../../justfile)
