---
title: Transactions
icon: lucide/git-merge
summary: Scopes, nesting, and what actually commits together
---

When several writes must succeed or fail as a unit, wrap them in a **transaction
scope**. Forze owns the boundary — begin on entry, commit on a clean exit, roll
back on an exception — while the adapter does the real database work.

## A transaction scope

Open a scope by **route**, the same logical name the deps wiring registered
under `tx={...}`. Everything inside commits together:

```python
async with ctx.tx_ctx.scope("orders"):
    order = await ctx.document.command(order_spec).create(cmd)
    await ctx.counter(order_counter).incr()
# committed here; an exception inside would have rolled it all back
```

The route must be registered for transactions when you wire the module —
`PostgresDepsModule(client=pg, ..., tx={"orders"})` — otherwise the scope can't
resolve a transaction manager.

!!! tip "A scope holds a connection only from the first query"

    With `lazy_transaction` enabled (the default) on the Postgres, Mongo, and
    Firestore clients, opening a scope acquires no pooled connection (and issues
    no `BEGIN` / `startTransaction` / `_begin`) until the **first query** inside
    it. Parsing, computing, or calling an
    external service before you touch the database no longer parks a connection
    idle-in-transaction — so keep cheap-but-slow work *before* the first query and
    the transaction stays short. A scope that runs no query holds nothing and
    commits nothing. One consequence: a connection-acquire or connect failure now
    surfaces at the first query rather than at scope entry (retries that wrap the
    whole operation are unaffected).

## What commits together

A scope has a **scope key** — the *kind* of transaction, such as a database
versus a cache. A port joins the active transaction only if its scope key
matches; a port of a different kind runs outside it.

!!! warning "Atomicity is bounded to one manager"

    A transaction coordinates only the operations that share its scope key and
    client. Two Postgres writes against the same connection commit atomically; a
    Postgres write and a Redis write do **not**. When you need consistency
    *across* systems, you don't reach for a bigger transaction — you stage the
    cross-system effect and apply it after commit (see [below](#after-the-commit)
    and [Events & sagas](events-sagas.md)).

## Two-phase handlers: work before the write

Lazy acquisition keeps the transaction off the connection until your first query —
but only for work that runs *before* that query. When an external call has to sit
*between* a read and a write, split the handler into two phases instead:

```python
class QuoteAndCreate(TwoPhaseDocumentHandler[QuoteRequest, int, WidgetRead, WidgetCreate]):
    enrich: PricingService

    async def prepare(self, args):                 # outside the transaction
        return await self.enrich.quote(args.sku)   # external call — no tx held

    async def apply(self, args, price):            # inside the transaction
        return await self.writer().create(WidgetCreate(price=price))
```

Register it with `.two_phase()`:

```python
registry.bind("quote").two_phase().bind_tx().set_route("orders").finish()
```

The engine runs `prepare` in the outer scope — **before** the transaction opens —
and threads its return value into `apply`, which runs inside the transaction. So
the transaction wraps only the writes, never the external call.

!!! note "What `prepare` can and can't do"

    `prepare` runs under the read-only flag, so it cannot acquire a write port
    (use `self.reader()` for reads, `self.writer()` only in `apply`). Its reads run
    *outside* `apply`'s transaction, so there's no read/write atomicity between the
    phases — validate on write in `apply` (an optimistic-concurrency `rev` check).
    `prepare` runs **exactly once** per invocation: if a retry or hedge wrap
    re-runs the operation, only `apply` repeats — with the payload `prepare`
    already produced.

## Nesting

Scopes nest naturally. A nested scope of the **same kind** joins the outer
transaction rather than starting a new one — with a savepoint where the backend
supports it, so an inner failure can roll back without losing the outer work:

```python
async with ctx.tx_ctx.scope("orders"):
    await ctx.document.command(order_spec).create(cmd)

    async with ctx.tx_ctx.scope("orders"):   # joins the same transaction
        await ctx.document.command(line_spec).create(line_cmd)
```

A nested scope whose kind **doesn't** match the active transaction is a
programming error and is rejected immediately — you can't open a cache
transaction inside a database one and expect them to commit together.

## Read-only transactions

`QUERY` operations open their scope `read_only=True`, so the backend begins a
read-only transaction where it supports one (Postgres `BEGIN … READ ONLY`) and
rejects accidental writes. You rarely set this by hand — the operation kind does
it for you.

## Isolation

Most operations are fine at the backend's default isolation. When one needs a
stronger guarantee — no lost update, no write skew — declare it on the operation
and the kernel holds the backend to it:

```python
plan = (
    OperationPlan()
    .bind_tx()
    .set_route("orders")
    .set_isolation(IsolationLevel.SERIALIZABLE)
    .finish()
)
```

The level is **fail-closed**. When the root scope is first entered, the kernel
checks that the route's transaction manager actually supports it (a manager opts
in by reporting its `TxCapabilities`); if it can't — or reports nothing — the
operation raises `exc.configuration` (`code="tx_isolation_unsupported"`) rather
than silently running weaker isolation. Declaring isolation without a transaction
route is rejected at registry freeze for the same reason: there is no transaction
to carry it.

`IsolationLevel` is intent-named and ordered — `READ_COMMITTED < SNAPSHOT <
SERIALIZABLE` — and each adapter maps it to its backend's spelling. The mock
honors all three through an in-memory MVCC overlay (it rejects the write-write,
write-skew, and phantom conflicts the level forbids), so an isolation-dependent
bug is catchable in a unit test or under [simulation](deterministic-simulation.md).

## After the commit

Some work must happen **only if** the transaction commits — publishing an event,
sending a notification, enqueuing a job. Doing it inside the scope risks acting
on a change that later rolls back. Defer it instead:

```python
await ctx.tx_ctx.run_or_defer(send_confirmation)
```

Inside a transaction, the callback is queued and runs after the **root** scope
commits successfully. Outside any transaction, it runs immediately. This single
mechanism is the foundation of the transactional outbox — covered next.

Deferred work is also **cancellation-protected**: a client disconnect or a
[deadline](deadlines.md) expiring between the commit and the deferred
callbacks can't skip them — they run to completion, then the cancellation
re-raises. A committed transaction is never left half-announced.

## Transactions under the mock

The mock transaction manager is **faithful by default** (`transactions="journal"`).
Each write records an undo, and an aborted transaction replays its journal in
reverse — undoing *only its own* writes. So a "forgot to run it in the same
transaction" bug fails in tests exactly as it would in production, and because
nothing restores a global snapshot, concurrent transactions still interleave
freely — the basis [simulation](deterministic-simulation.md) needs.

Rollback covers exactly what a database transaction would:

- **Rolls back** — documents, outbox rows, inbox marks, and the document-backed
  identity stores. A handler that stages an outbox event and then fails leaves
  *no* rows behind, same as Postgres.
- **Survives rollback, on purpose** — queues, streams, storage blobs, caches,
  counters, idempotency keys, locks, search and analytics state. Those backends
  are not transactional in production; rolling them back would hide the very
  cross-system consistency gaps the [outbox pattern](#after-the-commit) exists to
  close.

Nested scopes behave as savepoints (an inner rollback reverts only the inner
writes); a `QUERY` root enforces `read_only` (a write to a participating store
raises a precondition error, `code="read_only_tx"`); and the manager honors the
declared [isolation](#isolation) level through its MVCC overlay.

Two other modes are opt-in when wiring `MockDepsModule`:

=== "Strict (serializing)"

    `transactions="strict"` (or `strict_tx=True`) restores a global snapshot on
    rollback — simpler, but concurrent root transactions on one `MockState`
    **serialize** (real databases serialize conflicting writers anyway). Reach
    for it only when you specifically want that behavior.

=== "None (legacy)"

    `transactions="none"` is the old no-op: writes persist through a rollback. It
    hides transaction bugs — kept only for comparison.

!!! note "Only mock stores roll back"

    Rollback reverts the mock stores, not arbitrary in-process side effects — a
    handler that mutates a Python object it captured cannot be rolled back.
