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
</content>
