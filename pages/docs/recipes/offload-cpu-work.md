---
title: Offload heavy CPU work
icon: lucide/cpu
summary: Run a heavy synchronous parse or validation off the event loop in a two-phase prepare, without blocking the runtime
---

A bulk import parses a large blob, validates it into models, then writes it. The
parsing and validation are **synchronous** and CPU-bound — and on the event loop a
single long call blocks *every* other task for its whole duration: other invocations,
the outbox relay, heartbeats, deadlines, all frozen until it returns.

`run_cpu` moves that work to a bounded worker pool. Put it in a two-phase `prepare`
(which runs outside the transaction) and the parse can't stall the runtime and holds no
connection; `apply` then writes the result inside the transaction.

The runnable version of this recipe lives at `examples/recipes/cpu_offload/`.

## The synchronous work

The parse stands in for a large payload run through pydantic — ordinary blocking code:

```python
--8<-- "recipes/cpu_offload/app.py:parse"
```

## Offload it in `prepare`

`prepare` is the place for pre-write work; wrapping the parse in `run_cpu` takes it off
the loop. `apply` does only the write:

```python
--8<-- "recipes/cpu_offload/app.py:handler"
```

That is the whole change — `await run_cpu(fn, *args)` instead of calling `fn(*args)`
directly. `run_cpu` honors the invocation deadline, carries the tenant and tracing
context into the worker so logs stay correlated, and runs **inline and deterministically
under simulation**, so this handler is still testable under Deterministic Simulation
Testing. A bare `asyncio.to_thread` would raise `RealIOForbidden` under the simulator.

For mapping a function over a large collection, `run_cpu_map(items, fn, chunk_size=…)`
offloads in chunks and checkpoints cancellation at each boundary.

## What a thread buys you

A worker thread keeps the event loop **responsive**, not faster: pure-Python work holds
the GIL, so `run_cpu` won't speed it up — you get real parallelism only for GIL-releasing
C extensions (database drivers, `orjson`, `numpy`). Reach for it to stop one long
synchronous call from monopolizing the loop, not to parse faster.

## When to use it

- **Use it** for a heavy synchronous parse, validation, serialization, or crypto inside
  a handler — anything that would otherwise hold the event loop for a noticeable time.
- **You don't need it** for pure compute *before the first query* in a plain handler:
  lazy transaction acquisition already keeps the connection unheld there. `run_cpu` is
  about the event loop, not the connection.

## Sizing the pool

By default `run_cpu` uses a process-wide bounded pool — zero configuration. To size and
scope-manage it, pass a `ThreadPoolCpuExecutor` to the runtime; it is bound for the scope
and drained on shutdown:

```python
from forze.base.primitives import ThreadPoolCpuExecutor

runtime = build_runtime(MyModule(), cpu_executor=ThreadPoolCpuExecutor(max_workers=8))
```
