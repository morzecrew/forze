---
title: Testing
icon: lucide/flask-conical
summary: Unit and integration testing patterns for Forze applications
---

Forze's port-based architecture makes testing straightforward: handlers see only ports, so tests swap real adapters for in-memory fakes. No Docker, no database setup, no external services.

## Unit testing with MockDepsModule

`MockDepsModule` provides in-memory adapters for every contract. To run a handler against it, build a context with `context_from_modules` (shipped in `forze.testing`) — no runtime, no transport, just the context your ports resolve from:

```python
from forze.testing import context_from_modules
from forze_mock import MockDepsModule

async def test_create_user():
    ctx = context_from_modules(MockDepsModule())

    user = await ctx.document.command(user_spec).create(CreateUser(name="Ada"))

    assert user.name == "Ada"
    assert user.id is not None
```

Every port — documents, search, cache, queues, streams, storage — works against shared in-memory state. Write a user in one test, query it in the same test, and the data is there. (`command(...)` is the write side — `create` / `update`; `query(...)` is the read side — `get` / `find`.)

## Transaction rollback in tests

Mock transactions are **faithful by default**: a write inside a transaction that rolls back is undone, so a "forgot to run it in the same transaction" bug fails in your unit test exactly as it would in production. No flag needed.

- **Rolls back** — documents, outbox rows, inbox marks, and document-backed identity stores. A handler that stages an outbox event and then fails leaves no rows behind.
- **Survives rollback** — queues, streams, storage blobs, caches, counters, idempotency keys, locks, search and analytics state (these aren't transactional in production either).

The default journal mode is atomic *without* serializing, so concurrent transactions still interleave. Two modes are opt-in — `MockDepsModule(transactions="strict")` (a serializing global-snapshot manager) and `transactions="none"` (the legacy no-op). See [Transactions](../writing-operation/transactions.md#transactions-under-the-mock) for the full picture, and [Deterministic simulation](../dst/overview.md) for exploring concurrency and isolation under the faithful default.

## Testing with identity context

A handler reads `ctx.authn` / `ctx.tenancy` from the identity plane, resolved during operation execution — not from a value you set by hand. To test such a handler, wire the mock's identity stubs and drive the authn flow (seed an account, authenticate) rather than constructing an identity directly. See [Identity](../identity-tenancy-enc/identity.md) and the [authn, authz & tenancy recipe](../recipes/authn-authz-tenancy-fastapi.md) for the wiring.

## What the mock can't model

The mock is faithful at the **port** boundary, but logic that lives *below* a port is not in it. If your app leans on database triggers, generated columns, `CHECK` constraints, cascade deletes, or `LISTEN`/`NOTIFY` — efficient, native, but invisible to the app — a mock-based test can't run it, and an invariant a trigger maintains will even *false-pass or false-fail* because the mock writes the rows but never fires the trigger. That logic belongs in an integration test against the real database (below). Keeping invariant logic above the port is what keeps it portable across adapters and testable without one.

## Integration testing with testcontainers

For tests that need real infrastructure — or to exercise database-level logic the mock can't model — use testcontainers to spin up ephemeral databases:

```python
import pytest
from testcontainers.postgres import PostgresContainer
from forze.application.execution import build_runtime
from forze_postgres import PostgresClient, PostgresDepsModule, PostgresLifecycleModule

@pytest.fixture(scope="session")
def postgres_dsn():
    with PostgresContainer("postgres:18", driver=None) as pg:
        yield pg.get_connection_url()

async def test_postgres_integration(postgres_dsn):
    client = PostgresClient()
    runtime = build_runtime(
        PostgresDepsModule(client=client, rw_documents={"users": users_pg}),
        lifecycle_modules=[PostgresLifecycleModule(client=client, dsn=postgres_dsn)],
    )

    async with runtime.scope():          # starts the pool, runs lifecycle
        ctx = runtime.get_context()
        await ctx.document.command(user_spec).create(CreateUser(name="Ada"))
```

Integration tests are slower and require Docker, but they catch issues that mock adapters miss — schema migrations, constraint violations, connection handling.

## Hybrid contexts: one real backend, mock for the rest

Most integration tests only need *one* plane to be real. Pass `MockDepsModule` alongside the real modules and you get exactly that — real Postgres for documents and transactions, the mock for the queue the relay publishes into, the cache, the locks, everything else:

```python
runtime = ExecutionRuntime(
    deps=DepsRegistry.from_modules(
        PostgresDepsModule(client=pg, tx={"default"}, outboxes={"events": events_pg}),
        MockDepsModule(state=shared_state),
    ).freeze(),
)
```

Everything `MockDepsModule` registers is marked a **fallback**: a background environment rather than a claim on a key. Where a real module registers the same contract, the real one wins; where none does, the mock answers. Module order does not matter — provenance decides, not position. Two real modules registering the same route still raise at build time, exactly as before, and so do two mock modules in one context.

You can mark your own registrations the same way — `Deps.plain(deps, fallback=True)` / `Deps.routed(routes, fallback=True)` — if you ship an environment module of your own. Real backend modules never set it.

!!! warning "A route the real module never registered reaches the mock"

    In a context that includes a fallback, an unregistered route does not fail — it falls back to the plain catch-all. A typo in a spec name therefore resolves to the mock instead of raising, and the mock is the capability *superset*, so it may accept a call the real backend would refuse.

    Freeze names that hazard set at `INFO` (`Hybrid deps wiring: … catch-all behind real routes: document_query, …`) — the keys your real module routes *and* the mock still backs; the full fallback-served list is at `DEBUG`. The same report is `check_wiring(...).fallbacks`, and `report.fallbacks.catch_all` is the set to look at. To prove a test really hit the real adapter, assert your route is registered rather than inferred:

    ```python
    assert "orders" in ctx.deps.store.routed_deps[DocumentQueryDepKey]
    ```

    Production is unaffected: it has no fallback module, so every overlap is still fail-loud.

## Testing operations directly

To exercise a *registered* operation — with its stage hooks and transaction plan, not just a raw port — run it with `run_operation` against a mock context. No HTTP, no transport:

```python
import pytest
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException
from forze.testing import context_from_modules
from forze_mock import MockDepsModule

async def test_pay_order():
    ctx = context_from_modules(MockDepsModule())
    order_id = await run_operation(registry, "create_order", None, ctx)

    await run_operation(registry, "pay_order", PayCmd(order_id=order_id), ctx)

    order = await ctx.document.query(order_spec).get(order_id)
    assert order.paid

    # a domain failure surfaces as a CoreException (with a `.code`), the same as in production
    with pytest.raises(CoreException):
        await run_operation(registry, "pay_order", PayCmd(order_id=order_id), ctx)  # already paid
```

This tests domain logic and the operation's hooks without touching FastAPI or HTTP serialization.

## Testing sagas and events

A handler that emits a domain event stages it to the outbox inside the same transaction, and a saga reacts to it. To test that arc, wire the event handlers into the mock — `MockDepsModule(domain_events=...)` — run the operation, then inspect what was staged:

```python
events = await ctx.outbox.query(order_events_spec).claim_pending()
assert len(events) == 1
```

See [Events & sagas](../data-events/events-sagas.md) for the full model and the runnable order-fulfillment walkthrough that drives the whole aggregate → event → saga → outbox → relay → inbox flow in-process.

## Reflection gates: test a property, not a case

Some guarantees are about a *whole surface* — every import in a module tree, every
port signature, every operation id — and a per-case test can't keep up with a
surface that grows. `forze.testing` ships three gates for that; each discovers its
set by reflection, checks the property over everything found, and **refuses an
empty discovery**, so a rename can't turn the gate into a test that quietly checks
nothing:

```python
from uuid import UUID
from forze.testing import (
    assert_operation_namespaces,
    assert_pure_module,
    assert_scope_first,
)

def test_engine_is_pure():
    # allowlist catches the module nobody thought to name; the forbidden list is a
    # named refusal that widening the allowlist can never step over
    assert_pure_module(
        "app.core.engine",
        allowed=["math", "decimal", "attrs"],
        forbidden=["time", "random", "uuid", "os"],
    )

def test_ports_take_the_tenant_first():
    # positional-only is the mechanism: a keyword parameter can be omitted and
    # filled by a default — a caller must be physically unable to leave the key out
    assert_scope_first("app.ports.documents", name="tenant_id", annotation=UUID)

def test_edges_stay_disjoint():
    assert_operation_namespaces({
        "product": product_registry.operation_ids(),
        "operator": operator_registry.operation_ids(),
    })
```

Failures list every violation at once with its location. `assert_scope_first`
takes `exclude=["Port.method"]` for deliberate exceptions — and an exclusion that
matches nothing fails, so the list can only shrink.

## Test organization

A typical test structure for a Forze application:

```
tests/
├── unit/
│   ├── test_users.py      # domain logic, MockDepsModule
│   ├── test_orders.py
│   └── test_sagas.py
├── integration/
│   ├── test_postgres.py   # real DB, testcontainers
│   └── test_redis.py
└── conftest.py            # shared fixtures
```

Keep unit tests fast and parallelizable; run integration tests in CI or before deploy.

## See also

- [Concurrency & isolation](concurrency.md) — force a deterministic interleaving; verify an adapter's isolation
- [Deterministic simulation](../dst/overview.md) — seed-driven exploration of concurrency, faults, and crashes
- [Contracts](../core-concepts/contracts.md) — ports and adapters overview
- [Transactions](../writing-operation/transactions.md) — strict mode details
