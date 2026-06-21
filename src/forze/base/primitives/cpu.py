"""CPU / blocking-work offload seam.

Run a blocking or CPU-bound callable off the event loop via the **active**
:class:`CpuExecutor`, bound per context exactly like :class:`TimeSource` and
:class:`EntropySource`:

- production binds :class:`ThreadPoolCpuExecutor` (a bounded, dedicated pool);
- plain unit tests use :class:`InlineCpuExecutor` (synchronous, no threads);
- Deterministic Simulation Testing binds an inline executor so the work stays
  deterministic and never trips ``RealIOForbidden`` (DST swaps it in itself).

The handler-facing entry points are the free functions :func:`run_cpu` /
:func:`run_cpu_map` (reachable anywhere — a two-phase ``prepare`` has no
execution context but does have ambient context) and :func:`checkpoint` (the
cooperative cancellation point offloaded code calls between chunks/rows).

Honesty about the GIL: a worker thread keeps the event loop **responsive** for
GIL-bound work (parsing, pydantic validation) and gives real **parallelism**
only for GIL-releasing C extensions (argon2, db drivers, orjson, numpy). It is
not a speedup for pure-Python compute; it stops one long call from freezing the
whole loop.
"""

from __future__ import annotations

import asyncio
import functools
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from itertools import batched
from typing import Callable, Generator, Iterable, Protocol, final, runtime_checkable

import attrs

from forze.base.exceptions import exc

from .deadline import clear_deadline, remaining_time

# ----------------------- #

_DEFAULT_CPU_WORKERS = min(32, (os.cpu_count() or 1) + 4)
_DEFAULT_CHUNK_SIZE = 256

# ----------------------- #


@runtime_checkable
class CpuExecutor(Protocol):
    """Runs a blocking / CPU-bound thunk off the event loop."""

    async def run[T](self, fn: Callable[[], T], *, label: str | None = None) -> T:
        """Run *fn* (a zero-arg thunk) and return its result.

        *label* identifies the call site — the offloaded callable's qualified
        name — so a simulation executor can model that site's cost independently.
        Real executors ignore it.
        """

        ...


# ----------------------- #
# Cancellation


@final
@attrs.define(slots=True, eq=False)
class CancelToken:
    """A thread-safe one-shot cancellation flag shared into an offload worker.

    :func:`run_cpu` flips it when its await is cancelled or the deadline fires;
    offloaded code observes it through :func:`checkpoint`.
    """

    _event: threading.Event = attrs.field(factory=threading.Event, init=False)

    def cancel(self) -> None:
        """Request cancellation (idempotent)."""

        self._event.set()

    @property
    def cancelled(self) -> bool:
        """Whether cancellation has been requested."""

        return self._event.is_set()


_CANCEL: ContextVar[CancelToken | None] = ContextVar("forze_cpu_cancel", default=None)

# ....................... #


def checkpoint() -> None:
    """Cooperative cancellation point for offloaded code.

    Raises if the call was cancelled (e.g. client disconnect) or the invocation
    deadline has passed. Cheap — call it at chunk/row boundaries in long work so
    cancellation aborts promptly instead of after the whole function returns.
    Outside an offload (no active token, no deadline) it is a no-op.
    """

    token = _CANCEL.get()

    if token is not None and token.cancelled:
        raise exc.timeout("CPU offload cancelled.", code="cpu_offload_cancelled")

    remaining = remaining_time()

    if remaining is not None and remaining <= 0:
        raise exc.timeout(
            "CPU offload exceeded the invocation deadline.",
            code="cpu_offload_deadline",
        )


# ----------------------- #
# Executors


@final
@attrs.define(slots=True)
class ThreadPoolCpuExecutor:
    """Offload to a bounded, dedicated thread pool (the production default).

    The pool is created lazily on first use (so importing this module spawns no
    threads) and lives for the process, like the Argon2 hashing pool. Size it
    via :attr:`max_workers`; close it on shutdown via :meth:`close`.
    """

    max_workers: int = _DEFAULT_CPU_WORKERS
    thread_name_prefix: str = "forze-cpu"
    _pool: ThreadPoolExecutor | None = attrs.field(default=None, init=False)

    def _require_pool(self) -> ThreadPoolExecutor:
        # Lazy and unguarded: callers sit on one event loop and there is no await
        # between the check and the assignment (mirrors PasswordService).
        if self._pool is None:
            self._pool = ThreadPoolExecutor(
                max_workers=self.max_workers,
                thread_name_prefix=self.thread_name_prefix,
            )

        return self._pool

    async def run[T](self, fn: Callable[[], T], *, label: str | None = None) -> T:
        loop = asyncio.get_running_loop()

        return await loop.run_in_executor(self._require_pool(), fn)

    def close(self) -> None:
        """Shut the pool down, cancelling work that has not started."""

        if self._pool is not None:
            self._pool.shutdown(cancel_futures=True)
            self._pool = None


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class InlineCpuExecutor:
    """Run the thunk inline (synchronously). For plain unit tests and as the base
    for the deterministic simulation executor — no threads, fully reproducible."""

    async def run[T](self, fn: Callable[[], T], *, label: str | None = None) -> T:
        return fn()


# ....................... #

_DEFAULT_EXECUTOR: CpuExecutor = ThreadPoolCpuExecutor()

_CPU_EXECUTOR: ContextVar[CpuExecutor] = ContextVar(
    "forze_cpu_executor",
    default=_DEFAULT_EXECUTOR,
)

# ....................... #


def current_cpu_executor() -> CpuExecutor:
    """Return the CPU executor active in the current context."""

    return _CPU_EXECUTOR.get()


# ....................... #


@contextmanager
def bind_cpu_executor(executor: CpuExecutor) -> Generator[None]:
    """Bind *executor* as the active CPU executor for the duration of the block."""

    token = _CPU_EXECUTOR.set(executor)

    try:
        yield

    finally:
        _CPU_EXECUTOR.reset(token)


# ----------------------- #
# Public API


async def run_cpu[T](
    fn: Callable[..., T],
    /,
    *args: object,
    deadline: bool = True,
    **kwargs: object,
) -> T:
    """Run a blocking / CPU-bound callable off the event loop via the active executor.

    Honors the current invocation deadline and copies the calling context (tenant,
    tracing, log fields) into the worker so emitted logs stay correlated. On
    deadline expiry raises :func:`exc.timeout`; on cancellation propagates
    :class:`asyncio.CancelledError`. In both cases the running worker is *signalled*
    to abort at its next :func:`checkpoint` — a running thread cannot be force-killed,
    so a function with no checkpoints is abandoned (its result discarded), never kept
    on the critical path. Work that has not yet started is dropped outright.

    Set *deadline* to ``False`` for best-effort plumbing that must not be killed by the
    invocation deadline (e.g. decoding a cache hit, where a deadline-driven failure would
    only force a redundant fallback fetch). The bounded pool, context propagation, and
    outer-cancellation token still apply — only deadline enforcement is skipped, and the
    deadline is also cleared in the worker context so a :func:`checkpoint` inside the
    offloaded code does not re-impose it. (``deadline`` is a reserved keyword of this
    function; pass any same-named argument to *fn* via :func:`functools.partial`.)
    """

    bound = functools.partial(fn, *args, **kwargs)

    # Call-site id for simulation cost models; unwrap partials so a partial-wrapped
    # callable still resolves to its underlying function name.
    target: object = fn
    while isinstance(target, functools.partial):
        target = target.func
    label = getattr(target, "__qualname__", None)
    token = CancelToken()

    ctx = copy_context()
    ctx.run(_CANCEL.set, token)  # the token lives only in the worker's context copy

    if not deadline:
        # Best-effort: drop the deadline in the worker context too, so a checkpoint()
        # inside the offloaded code doesn't re-impose the budget the caller opted out of.
        ctx.run(clear_deadline)

    thunk = functools.partial(ctx.run, bound)

    remaining = remaining_time() if deadline else None

    if remaining is not None and remaining <= 0:
        raise exc.timeout(
            "CPU offload skipped: invocation deadline already passed.",
            code="cpu_offload_deadline",
        )

    executor = current_cpu_executor()

    try:
        if remaining is None:
            return await executor.run(thunk, label=label)

        async with asyncio.timeout(remaining):
            return await executor.run(thunk, label=label)

    except TimeoutError as e:
        token.cancel()
        raise exc.timeout(
            "CPU offload exceeded the invocation deadline.",
            code="cpu_offload_deadline",
        ) from e

    except asyncio.CancelledError:
        token.cancel()
        raise


# ....................... #


async def run_cpu_map[I, R](
    items: Iterable[I],
    fn: Callable[[I], R],
    *,
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
) -> list[R]:
    """Map *fn* over *items* in chunks on the active executor.

    Auto-checkpoints at every chunk boundary: the per-chunk await re-checks the
    deadline and is a cancellation point, so a long mapping aborts promptly without
    the caller writing :func:`checkpoint` calls. ``chunk_size`` is the cancellation
    granularity (and the interleaving / virtual-time point under simulation).

    *items* is consumed lazily one chunk at a time, so an unbounded or very large
    iterable neither materializes up front nor blocks the loop before chunking starts.
    """

    if chunk_size < 1:
        raise exc.precondition("run_cpu_map chunk_size must be at least 1.")

    out: list[R] = []

    for chunk in batched(items, chunk_size):
        out.extend(await run_cpu(_map_chunk, fn, chunk))

    return out


def _map_chunk[I, R](fn: Callable[[I], R], chunk: tuple[I, ...]) -> list[R]:
    return [fn(item) for item in chunk]
