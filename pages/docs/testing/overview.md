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

## Integration testing with testcontainers

For tests that need real infrastructure, use testcontainers to spin up ephemeral databases:

```python
import pytest
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="session")
def postgres_url():
    with PostgresContainer("postgres:16") as pg:
        yield pg.get_connection_url()

async def test_postgres_integration(postgres_url):
    runtime = build_runtime(PostgresDepsModule(dsn=postgres_url))

    async with runtime.scope():          # starts the pool, runs lifecycle
        ctx = runtime.get_context()
        await ctx.document.command(user_spec).create(CreateUser(name="Ada"))
```

Integration tests are slower and require Docker, but they catch issues that mock adapters miss — schema migrations, constraint violations, connection handling.

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
