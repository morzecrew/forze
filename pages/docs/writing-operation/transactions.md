---
title: Transactions
icon: lucide/git-merge
summary: Scopes, nesting, and what actually commits together
---

When several writes must succeed or fail as a unit, wrap them in a **transaction
scope**. Forze owns the boundary — begin on entry, commit on a clean exit, roll
back on an exception — while the adapter does the real database work.

![Read and external calls run before the scope; the transaction begins at the first query, its writes commit together, then deferred work runs only after a successful commit](../_diagrams/light/transaction-scope.svg#only-light){ data-src="../_diagrams/light/transaction-scope.svg#only-light" }
![Read and external calls run before the scope; the transaction begins at the first query, its writes commit together, then deferred work runs only after a successful commit](../_diagrams/dark/transaction-scope.svg#only-dark){ data-src="../_diagrams/dark/transaction-scope.svg#only-dark" }

## A transaction scope

Open a scope by **route**, the same logical name the deps wiring registered
under `tx={...}`. Everything inside commits together:

```python
async with ctx.tx_ctx.scope("orders"):
    order = await ctx.document.command(order_spec).create(cmd)
    await ctx.document.command(line_spec).create(line_cmd)
# committed here; an exception inside would have rolled it all back
```

Both writes land on a transactional backend, so both roll back together. Not
everything you can call inside a scope does — see [What commits
together](#what-commits-together).

The route must be registered for transactions when you wire the module —
`PostgresDepsModule(client=pg, ..., tx={"orders"})` — otherwise the scope can't
resolve a transaction manager.

!!! tip "A scope holds a connection only from the first query"

    With `lazy_transaction` enabled (the default) on the Postgres, Mongo, and
    Firestore clients, opening a scope acquires no pooled connection until the
    **first query** inside it — so parsing or calling an external service before you
    touch the database doesn't park a connection idle-in-transaction, and a scope
    that runs no query holds and commits nothing.

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
    and [Events & sagas](../data-events/events-sagas.md)).

Queues, streams, object storage, caches, locks, and search/analytics writes are
not transactional in production, so they never roll back with the scope.

**Counters are the case worth calling out**, because they look like an ordinary
Postgres write and are not one: a counter allocation runs on its own connection,
outside the caller's transaction, on *every* backend — Redis, Postgres, Mongo and
Firestore alike. A rolled-back transaction does not give the number back. That is
deliberate: a sequence you could roll back would reissue an invoice number already
handed out. Treat an allocated number as spent and let gaps happen.

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
        return await self.writer.create(WidgetCreate(price=price))
```

Register it with `.two_phase()`:

```python
registry.bind("quote").two_phase().bind_tx().set_route("orders").finish()
```

The engine runs `prepare` in the outer scope — **before** the transaction opens —
and threads its return value into `apply`, which runs inside the transaction. So
the transaction wraps only the writes, never the external call. Worked end to end in
the [two-phase handler](../recipes/two-phase-handler.md) recipe; for CPU-heavy `prepare`
work, [offload it off the event loop](../recipes/offload-cpu-work.md).

!!! note "What `prepare` can and can't do"

    `prepare` runs under the read-only flag, so it cannot write: a write port refuses
    there, the handler's own `self.writer` included (use `self.reader` for reads,
    `self.writer` only in `apply`). Its reads run
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

A manager whose store refused every write in the transaction it opened says so
through `ReadOnlyAware.read_only_enforced()`. Postgres and the mock's `journal`
and `strict` managers do, but not for a scope nested in a transaction opened
outside forze, which becomes a savepoint. A nested scope on another route
resolves another manager and opens a transaction of its own, so the read-only
root counts only while it is the only route a finished scope entered. After-commit
work queued with `run_or_defer` may write, so it gives up the skip too. A query
interrupted after
such a read is safe to re-run rather than
[`commit_ambiguous`](../running-in-prod/deadlines.md#a-late-cancel-is-never-reported-as-safe).

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
bug is catchable in a unit test or under [simulation](../dst/overview.md).

## Serializing an operation

Some operations must not run twice at once for the same input. A recompute of an
order's totals reads every line and writes the sum. Two of them in parallel can
each read a different set of lines, and the second to commit wins with a stale
sum. Optimistic concurrency does not help when the conflict is between two
*reads* of rows that neither write touches.

`SerializeOperation` makes invocations that share a key take turns. The lock is
taken inside the operation's transaction and held until the root transaction
commits or rolls back:

```python
from forze.application.hooks.locking import SerializeOperation

registry = (
    registry.bind("orders.recompute")
    .bind_tx()
    .set_route("orders")
    .before(SerializeOperation(key=lambda args: args.order_id).to_step())
    .finish(deep=True)
)
```

A second `orders.recompute` for the same order waits at that step until the
first one's transaction ends. Different orders, and different tenants, never
wait on each other. The lock is keyed by the operation's registered name, the
bound tenant and the key, so another operation never shares it. Under a
[two-phase handler](#two-phase-handlers-work-before-the-write) it covers `apply`
only: `prepare` runs before the transaction opens.

The key function returns one value or a tuple of values. Each must be a `str`,
an `int`, a `UUID` or a timezone-aware `datetime`. A datetime locks on its
instant, so two offsets of one moment take the same lock. Any other type
(`None`, an enum member, a `bool`, a float) is refused as `configuration`
(`operation_lock_key_unsupported`); pass an enum's `.value`. A naive datetime is
refused as `validation` (`operation_lock_key_naive_datetime`). What the key
function returns is known only when the operation runs, so these refuse that
call, and the error names the operation.

An operation that runs nested in another one's transaction takes the lock in a
savepoint. If the nested operation succeeds, its lock stays until the root
transaction ends, as its writes do. If it fails, Postgres rolls the savepoint
back and releases the lock with the writes it guarded, while the outer
transaction carries on. The mock keeps the lock until the root ends either way,
which is stricter.

Wiring problems surface before the first request:

- Placed in the outer scope, which runs before the transaction opens, it is
  refused at registry freeze (`tx_lock_outside_transaction_scope`).
- An operation with no transaction route is refused there too
  (`tx_lock_without_route`).
- A route whose backend has no transaction-scoped lock is refused when the
  operation is resolved, on its first call or under `check_wiring`
  (`tx_lock_unsupported`). Postgres has one; Mongo and Firestore do not. It
  never falls back to a Redis lease.

On Postgres the lock is `pg_advisory_xact_lock` on the transaction's own
connection. Nothing expires mid-transaction and nothing needs a fencing token,
because only a commit or a rollback releases it. A wait counts against the
operation's [deadline](../running-in-prod/deadlines.md). The mock takes the same
per-owner write locks a `SerializedBy` write takes, under the `journal` and
`strict` managers. `transactions="none"` opens no transaction, so it binds no
lock and the operation is refused at resolve.

To take a lock by hand inside a scope, call the port the step uses:

```python
async with ctx.tx_ctx.scope("orders"):
    await ctx.tx_locks("orders").hold(key)  # key: a 64-bit int, e.g. advisory_lock_key(...)
```

Outside a transaction it is refused (`tx_lock_outside_transaction`), since the
lock would end with its own statement. One `hold` call takes its keys in sorted
order. Two calls, or two transactions taking keys in opposite orders, can
deadlock; one side is refused as a retryable `concurrency`, on Postgres and the
mock alike.

!!! note "Which lock to reach for"

    The [`SerializedBy`](../reference/contracts/document.md) guarantee and
    `command.hold(...)` lock a *document owner*: every write for that owner takes
    the lock, whoever wrote the code. `SerializeOperation` locks an *operation
    input*: only invocations of that operation take it, and the key need not be
    any document's field. A [distributed lock](../reference/contracts/coordination.md#distributed-lock)
    is a lease for work outside any transaction; its release is not tied to a
    commit.

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
[deadline](../running-in-prod/deadlines.md) expiring between the commit and the deferred
callbacks can't skip them — they run to completion, then the cancellation
re-raises. A committed transaction is never left half-announced.

A deferred callback that **fails** never takes the committed result down with
it: the failure is logged and the operation still returns normally. To surface
those failures for alerting instead of relying on logs, install a handler when
building the runtime — `build_runtime(..., after_commit_error_handler=...)` —
and it is notified out-of-band with an `AfterCommitError` describing each failed
callback. The handler must not raise.

## Transactions under the mock

The mock transaction manager is **faithful by default** (`transactions="journal"`):
an aborted transaction undoes *only its own* writes, so a "forgot to run it in the
same transaction" bug fails in tests exactly as it would in production, and
concurrent transactions still interleave — the basis [simulation](../dst/overview.md)
needs. It rolls back what a database would — documents, outbox rows, inbox marks, the
document-backed identity stores — and deliberately **not** the non-transactional
backends (queues, streams, storage, caches, counters, locks, search/analytics):
rolling those back would hide the cross-system gaps the [outbox](#after-the-commit)
exists to close. Nested scopes act as savepoints, a `QUERY` root enforces `read_only`
(a participating write raises `code="read_only_tx"`), and the manager honors the
declared [isolation](#isolation) level through its MVCC overlay.

Two other modes are opt-in when wiring `MockDepsModule`: `transactions="strict"`
restores a global snapshot on rollback, so concurrent root transactions serialize;
`transactions="none"` is the legacy no-op (writes persist through a rollback) kept
only for comparison.

Row locks — a `for_update` read, and the lock a write takes on the row it changes —
are claims by default: nobody waits, and of two transactions over a locked row the
one that commits second is aborted with a conflict. That is safe, but it cannot tell
a correct lock order from a deadlock-prone one, since both abort under contention.
`MockDepsModule(row_locks="block")` makes the contender wait instead, as Postgres
does: it parks until the holder commits or rolls back, then carries on against the
committed row. At `SNAPSHOT` or `SERIALIZABLE` isolation, a row changed since the
transaction's snapshot fails with `exc.concurrency(code="serialization_failure")`
instead, as Postgres refuses it. A wait that would close a cycle — two transactions
taking the same rows in opposite orders — aborts the waiter with `exc.concurrency`
(`code="deadlock_detected"`, the rows in its details), and a
[simulation](../dst/invariants.md#transactional-isolation) reports it as a
`no_lock_order_deadlock` violation. The mock only blocks on the simulation loop,
which reports a wait that never ends instead of hanging; on any other loop it falls
back to claims. It needs the default `transactions="journal"`.

`"nowait"` and `"skip_locked"` are a mock divergence in either mode. On Postgres, `NOWAIT`
raises instead of waiting and `SKIP LOCKED` passes over a held row; the mock gives both
plain `for_update` behaviour, as the `RowLockMode` contract allows for backends without
them, so in blocking mode they wait. The mechanism divergences on
[Fidelity](../dst/fidelity.md) list `skip-locked-degrades-to-for-update`.

A transaction rolls back when an operation raises; what those failures are, and how a
raised error becomes a response, is [Errors](errors.md).
