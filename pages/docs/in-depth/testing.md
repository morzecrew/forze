---
title: Testing
icon: lucide/flask-conical
summary: Unit and integration testing patterns for Forze applications
---

Forze's port-based architecture makes testing straightforward: handlers see only ports, so tests swap real adapters for in-memory fakes. No Docker, no database setup, no external services.

## Unit testing with MockDepsModule

`MockDepsModule` provides in-memory adapters for every contract. Wire it instead of real integration modules:

```python
from forze_mock import MockDepsModule

async def test_create_user():
    module = MockDepsModule()
    runtime = build_runtime(registry, module)

    async with runtime:
        ctx = runtime.get_context()
        facade = ctx.document.query(user_spec)

        result = await facade.create(CreateUser(name="Ada"))

        assert result.name == "Ada"
        assert result.id is not None
```

Every port — documents, search, cache, queues, streams, storage — works against shared in-memory state. Write a user in one test, query it in the same test, and the data is there.

## Strict transaction mode

By default, mock transactions are no-ops: a write inside a transaction that rolls back still persists. This hides bugs where you forget to run operations in the same transaction.

Enable strict mode to get real rollback semantics:

```python
module = MockDepsModule(strict_tx=True)
```

Strict mode rolls back exactly what a database transaction would:

- **Rolls back** — documents, outbox rows, inbox marks, and document-backed identity stores
- **Survives rollback** — queues, streams, storage blobs, caches, counters, idempotency keys, locks, search and analytics state (these aren't transactional in production either)

Strict mode catches transaction bugs in unit tests before they reach production.

!!! warning "Strict roots serialize"

    Strict mode restores a global snapshot on rollback, so concurrent root
    transactions on one `MockState` are serialized. Real databases serialize
    conflicting writers anyway, but this can slow down test parallelization.

## Testing with identity context

For handlers that depend on `AuthnIdentity` or `TenantIdentity`, mock the identity plane:

```python
from forze_identity import AuthnIdentity
from forze_mock import MockDepsModule

module = MockDepsModule()

async with runtime:
    ctx = runtime.get_context()

    # bind an identity before calling handlers that need one
    identity = AuthnIdentity(subject="user-123", claims={"role": "admin"})
    ctx = ctx.with_identity(identity)

    # now handlers can access ctx.authn
    result = await some_handler(ctx, ...)
```

For tenant-scoped operations:

```python
from forze_identity import TenantIdentity

tenant = TenantIdentity(tenant_id="acme-corp")
ctx = ctx.with_tenant(tenant)
```

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
    module = PostgresDepsModule(dsn=postgres_url)
    runtime = build_runtime(registry, module)

    async with runtime:
        ctx = runtime.get_context()
        # test against real Postgres
```

Integration tests are slower and require Docker, but they catch issues that mock adapters miss — schema migrations, constraint violations, connection handling.

## Testing operations directly

Test handlers without HTTP by calling operations through the facade:

```python
async def test_user_validation():
    module = MockDepsModule()
    runtime = build_runtime(registry, module)

    async with runtime:
        ctx = runtime.get_context()
        facade = ctx.document.query(user_spec)

        # test validation error
        with pytest.raises(ValidationError):
            await facade.create(CreateUser(name=""))  # empty name

        # test business rule
        await facade.create(CreateUser(name="Ada"))
        with pytest.raises(ConflictError):
            await facade.create(CreateUser(name="Ada"))  # duplicate
```

This tests domain logic without touching FastAPI or HTTP serialization.

## Testing sagas and events

For handlers that emit domain events or run sagas, check the outbox:

```python
async def test_order_emits_event():
    module = MockDepsModule()
    runtime = build_runtime(registry, module)

    async with runtime:
        ctx = runtime.get_context()
        facade = ctx.document.query(order_spec)

        await facade.create(CreateOrder(product="widget", qty=5))

        # check the outbox for the expected event
        outbox = ctx.outbox.query(order_events_spec)
        events = await outbox.claim_pending()

        assert len(events) == 1
        assert events[0].payload["product"] == "widget"
```

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

- [Contracts](../core-concepts/contracts.md) — ports and adapters overview
- [Transactions](transactions.md) — strict mode details
- [Identity](identity.md) — testing with authn/authz context
