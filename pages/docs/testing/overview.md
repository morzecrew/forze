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

A `command(...)` port is handed out guarded, so a write inside a read-only operation is refused wherever the port came from. `isinstance` and attribute assignment on the guard itself do not work. To ask whether it implements a protocol, use `implements(port, SomeAware)` from `forze.application.contracts.deps`, which also sees through port spans and port logging, as in production. To patch one of its methods, reach the real one with `forze.testing.unguarded(port)`.

One thing to know before you rely on that: the mock stores what was written and reads it back through the read model, so an aggregate whose read model requires a field **no write produces** cannot round-trip through it. That is the ordinary shape of a read model assembled by a SQL view — a joined reference, an aggregate total, a computed flag.

Declare those fields on the spec (`derived_read_fields={"supplier": None}`) and supply their values with the seed:

```python
SpecSeed(
    spec=ORDERS,  # needs a `write` spec: seeding goes through its create command
    count=20,
    derived={"supplier": {"id": ..., "rev": 1, "name": "Acme", "number_id": 7}},
)
```

The values are fixture data — nothing recomputes them when a source row changes, because the mock does not implement the view. What the test then exercises is your handler around the read, which is what a unit test is for; the derivation itself is the database's, and belongs in an integration test against the real view. A required field declared derived and left unsupplied is refused by name, rather than surfacing as a pydantic error about a missing key.

A seed cannot reach a row your **code** creates, though — and under [deterministic simulation](../dst/overview.md) the workload creates every row there is. Register what the relation would have produced instead, and the mock consults it per row:

```python
MockDepsModule(
    derived_values=MockDerivedRegistry().on(
        "orders",
        lambda row: {"supplier": {"id": row["supplier_id"], "rev": 1, "name": "Acme"}},
    ),
)
```

Keep it a function of the row: a source that reads a clock or a counter makes two runs of one seed disagree, and a simulation you cannot replay is one you cannot minimize. A seeded or persisted value still wins, so a test that wrote a specific row keeps it. Registering nothing keeps the refusal above, which is the posture that makes a missing value discoverable.

Where the value is one row reached by one key — a joined column, or an expanded reference like `warehouse: {id, name}` — declare the join instead (`DerivedReadField(source=..., via=..., fields=("id", "name"))`, see [reading data](../data-events/reading-data.md#read-fields-storage-doesnt-hold)) and the mock resolves it with no callback at all.

### Checking derived fields against the real view

A declared join or a registered callback is the mock's copy of your view, and it can drift from the real one while every mock-backed test stays green. `run_derived_fidelity` checks one spec against a real backend: it loads one seed into both stores, reads the same ids through both, and compares only the derived fields:

```python
from forze.testing import context_from_modules
from forze_dst.conformance import run_derived_fidelity
from forze_mock import MockDepsModule
from forze_mock.seeding import apply_seed


async def test_orders_match_the_view(postgres_ctx, plan) -> None:
    report = await run_derived_fidelity(
        spec=ORDERS,
        seed=lambda ctx: apply_seed(ctx, plan),  # one SeedPlan, loaded into both stores
        real=postgres_ctx,  # reads ORDERS from the view
        mock=context_from_modules(MockDepsModule()),  # plus derived_values=... for marked fields
    )

    assert report.agrees, report.render_markdown()
    assert all(tally.compared for tally in report.tallies), report.render_markdown()
```

Each disagreement is one `DerivedMismatch` for one field on one row, holding both values. A read that raises on one side is recorded as a `ReadRefused` value, so one bad row does not hide the rest. A value that JSON serialization refuses is recorded as `Unserializable`, which never agrees with anything. A derived field the read model excludes from serialization is refused up front, since it could only ever agree as `None`.

`agrees` means only that nothing mismatched. A field that is `None` on both sides, or a row both sides refused, agrees without testing the join. So also check the per-field `tallies`: each one counts the rows actually `compared`, the rows `null_on_both` and the rows `refused_on_both`, and `render_markdown()` prints them.

Keep the plan's `instant` pinned (the default), so both stores create the same ids. Leave `SpecSeed.derived` out of the plan: the real store has no column to write those values to, and the point is to test the join or callback your tests use, not a value you supplied. For a tenant-aware spec, pass `tenant=TenantIdentity(...)`; it is bound on both contexts for the seed and the reads.

## Analytics answered from current state

By default the mock answers an [analytics](../data-events/analytics.md) query from rows you seed into `MockState.analytics_query_hits`. Those rows never change. A handler that reads a query to decide what to do sees the same numbers after every write, so under simulation it never takes the branch the real numbers would send it down.

Register a resolver instead, and the mock computes the query on each call from the documents it holds:

```python
from forze_mock import MockAnalyticsRegistry, MockDepsModule


def totals_by_customer(view, params):
    sums = defaultdict(int)
    for row in view.rows(ORDERS):
        sums[row["customer"]] += row["amount"]
    return [{"customer": c, "total": t} for c, t in sums.items() if t >= params.min_total]


MockDepsModule(
    analytics=MockAnalyticsRegistry().on(TOTALS, "by_customer", totals_by_customer),
)
```

`view.rows(spec)` returns a document spec's stored rows in storage form: the columns a SQL query over the table would see. You get the rows the calling transaction sees. Inside a transaction, its own uncommitted writes are included and a concurrent transaction's are not. Outside one, you get the committed rows. A field the document spec seals arrives encrypted, exactly as SQL sees the column. It is decrypted on the way out only if the analytics spec declares it encrypted too.

On a tenant-aware analytics route, a call with no tenant bound is refused with `tenant_required`. Otherwise `view.rows` returns only rows whose `tenant_id` names the bound tenant, even when the document route keeps every tenant in one namespace. A tenant-aware document route narrows `view.rows` the way a document read does, whatever the analytics route declares: the bound tenant's partition, without rows another tenant owns or rows with no owner.

The resolver receives the call's validated params. Its rows then go through the same `max_rows` cap and read-model decoding as seeded rows. A query with no resolver keeps its seeded rows. `on()` refuses a query key the spec does not declare, and a second resolver for the same query.

A resolver is a second implementation of the query, and only a test keeps it in step with the SQL. `forze_dst.conformance.compare_analytics_query` is that test. It loads one seed into the mock and into the real engine, runs the query on both, and diffs the rows:

```python
from forze_dst.conformance import compare_analytics_query
from forze_mock.seeding import apply_seed

comparison = await compare_analytics_query(
    TOTALS,
    "by_customer",
    MinTotal(min_total=10),
    mock=mock_ctx,
    real=postgres_ctx,
    seed=lambda ctx: apply_seed(ctx, plan),
)
assert comparison.agrees, comparison.render()
```

Rows are compared as a multiset after the read model, so a `Decimal` from the engine and an `int` from the resolver agree when the read model types the field. In an untyped field, `1` and `1.0` agree and `true` never matches `1`, in either mode. Pass `ordered=True` only for a query with an `ORDER BY`. `render()` prints one line for each row that only one side returned, or, in ordered mode, for each position that differs. Two empty answers do not agree: a seed that produced no rows the query selects would pass any resolver. Pass `allow_empty=True` only for a query whose correct answer is empty. To use the same query as a simulation invariant, see [`query_invariant`](../dst/invariants.md#what-must-always-hold).

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
