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

## When the GIL itself is the problem

A thread keeps the loop running, but the loop still shares the GIL with it. While a long
pure-Python call runs, a concurrent request waits for the GIL each time its I/O completes,
and a request that makes a few dozen such handoffs slows down noticeably. Pass
`isolated=True` to run the call in a worker process that has its own GIL:

```python
from forze.base.primitives import run_cpu


async def score(rows: list[tuple[int, float]]) -> float:
    return await run_cpu(fit_model, rows, isolated=True)
```

The runtime binds a process pool for this beside its thread pool. The pool starts no
process until the first isolated call, and the runtime terminates its workers when the
scope exits. `isolated_cpu_workers=` sizes it.

**What suits it** is a large, self-contained computation over plain data: scoring a batch,
fitting a model, diffing two documents. The function takes plain values and returns plain
values. **What does not**:

- small offloads, such as pydantic validation or decoding: they finish quickly on a
  thread, and the trip across would cost more than it saves;
- a per-row callback: every call pays for the trip, so send the whole batch in one call;
- work that needs the request: its tenant, its tracing, a database connection.

What crosses the boundary, and what does not:

- **Inputs are pickled on the event loop** before submission, and the loop waits while
  they are. On a typical core, 10,000 small dicts (about 400 KiB) take about 2 ms; a
  `bytes` payload costs far less per megabyte. Pass a module-level function and only the
  data the call needs, or a path to read it from. A lambda, a local function or an
  argument that cannot be pickled is refused with a `configuration` error that names the
  call.
- **The result or exception is pickled back.** One that cannot make the trip fails its
  own call with a `configuration` error (`cpu_offload_result_not_picklable`), carrying the
  worker's traceback.
- **The calling context stays behind.** The worker has no tenant, trace, log fields,
  deadline or cancellation token, so `checkpoint()` there does nothing.
- **The deadline and cancellation end the wait, not the work.** The caller gets its
  timeout or cancellation at once. The worker cannot be signalled, so its call runs to
  completion and the result is discarded. Killing one worker would fail every other call
  on the pool. For work you need to kill, or do not fully trust, use the
  [sandbox plane](../data-events/sandbox.md).
- **An abandoned call keeps its worker until it returns.** The pool has a fixed number
  of workers, so calls that never return starve every isolated call after them. Bound the
  work itself, and always run isolated calls under a deadline so no caller waits forever.
  Each time a running call is abandoned, forze logs a warning naming the function.
- **A worker that dies** (killed for memory, crashed) fails its calls with a retryable
  `unavailable` error (`cpu_offload_worker_lost`), and the next call starts a fresh pool.
- **Workers start with `forkserver`**, or `spawn` where the platform lacks it, never with
  `fork`. Each worker imports your main module, so keep its startup code under
  `if __name__ == "__main__":`.
- **Nothing bound means a refusal.** With no runtime scope open, `isolated=True` raises a
  `configuration` error instead of falling back to threads or running inline.
- **Test with the pickle round trip.** In a unit test, bind
  `bind_isolated_cpu_executor(InlineCpuExecutor(pickled=True))`. A plain
  `InlineCpuExecutor()` runs the call without pickling it, so a lambda or a local function
  passes the test and fails in production. Under simulation the call runs inline and takes
  the round trip too.

## When to use it

- **Use it** for a heavy synchronous parse, validation, serialization, or crypto inside
  a handler — anything that would otherwise hold the event loop for a noticeable time.
- **You don't need it** for pure compute *before the first query* in a plain handler:
  lazy transaction acquisition already keeps the connection unheld there. `run_cpu` is
  about the event loop, not the connection.

## Sizing the pool

By default the runtime binds a bounded worker pool for its scope and closes it on exit —
zero configuration (`cpu_workers=` sizes it). To manage the pool yourself, pass a
`ThreadPoolCpuExecutor`; the runtime binds it for the scope but you own its lifetime:

```python
from forze.base.primitives import ThreadPoolCpuExecutor

runtime = build_runtime(MyModule(), cpu_executor=ThreadPoolCpuExecutor(max_workers=8))
```

The scope also holds its pool for the whole process while it is open. An ASGI server
runs the lifespan holding the scope (`runtime_lifespan`, or your own
`async with runtime.scope()`) in a task of its own, and starts every request from
somewhere else; a request handler's `run_cpu` still lands on the runtime's pool. With
several runtime scopes open in one process, code that none of them started uses the
oldest one's pool. Only with no scope open at all does `run_cpu` run inline. An
executor bound around the scope, which it defers to, stays in the scope's own task.
Once its scope exits, a closed pool refuses further work with a retryable
`unavailable` error.

The isolated pool works the same way, held for the process too: `isolated_cpu_workers=`
sizes the runtime's own, and `isolated_cpu_executor=` injects yours. On Python 3.14 you can
inject an `InterpreterPoolCpuExecutor`, whose workers are subinterpreters with a GIL each:
cheaper to start and lighter than processes. Each worker interpreter imports the function's module,
and many C extensions refuse to load there, `orjson` among them, which forze itself
imports. Keep the function in a module that imports only the standard library and
pure-Python code. On Python 3.13 the constructor refuses with a `configuration` error.

```python
from forze.base.primitives import InterpreterPoolCpuExecutor

runtime = build_runtime(MyModule(), isolated_cpu_executor=InterpreterPoolCpuExecutor())
```
