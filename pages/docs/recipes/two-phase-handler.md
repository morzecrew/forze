---
title: Call a service before the write
icon: lucide/split
summary: Run an external call outside the transaction, then write inside it — a two-phase handler
---

A handler needs to price an order from a remote service, then persist the priced
order. Doing both in one handler holds the database transaction open across the
network call. A **two-phase handler** splits the work: `prepare` runs the call
*outside* the transaction and `apply` writes *inside* it, so the connection is
held only for the write. The concept is covered in
[Transactions → Two-phase handlers](../in-depth/transactions.md#two-phase-handlers-work-before-the-write);
this is the wiring.

The runnable version lives at `examples/recipes/two_phase_pricing/` and runs on
the in-memory mock store — no infrastructure needed.

## Split the handler

Subclass `TwoPhaseDocumentHandler`: `prepare` does the external/compute work and
returns a payload; `apply` writes it via the handler's write port. The base holds
the read and write ports (not the execution context), so each phase declares what
it needs:

```python
--8<-- "recipes/two_phase_pricing/app.py:handler"
```

`prepare` runs under the read-only flag — use the read port there, the write port
only in `apply`. It runs **exactly once** per invocation: a retry or hedge re-runs
only `apply`, with the payload `prepare` already produced.

## Register it

`.two_phase()` marks the operation; the transaction route scopes `apply`'s
transaction (required — enforced at freeze). `TwoPhaseDocumentBuilder` resolves
the read/write ports from the context and hands them to the handler:

```python
--8<-- "recipes/two_phase_pricing/app.py:registry"
```

## Call it

`prepare` runs outside the transaction, the engine opens it, `apply` writes, and
the row commits:

```python
--8<-- "recipes/two_phase_pricing/app.py:scenario"
```

## Notes

- **When to reach for it.** Lazy transaction acquisition already keeps the
  connection off the pool until your first query, so pure compute or an external
  call *before* the first query needs nothing special. Use a two-phase handler
  when the call must sit *between* a read and the write that depends on it.
- **No read/write atomicity across phases.** A read in `prepare` runs outside
  `apply`'s transaction. If `apply` depends on it, re-validate on write — an
  optimistic-concurrency `rev` check — rather than assuming the read still holds.
- **One transaction.** Two-phase wraps a single transaction around `apply`. For
  multiple transactions with compensation between external steps, reach for a
  [saga](../in-depth/events-sagas.md), not more phases.
