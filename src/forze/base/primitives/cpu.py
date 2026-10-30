"""CPU / blocking-work offload seam.

Run a blocking or CPU-bound callable off the event loop via the **active**
:class:`CpuExecutor`, bound per context exactly like :class:`TimeSource` and
:class:`EntropySource`:

- an :class:`ExecutionRuntime` scope binds a bounded :class:`ThreadPoolCpuExecutor`
  it owns and closes on exit (production), unless one is already bound or injected,
  and holds it process-wide for contexts that bind none (an ASGI request's);
- with nothing bound or held (no runtime, no explicit bind) the work runs inline via
  :class:`InlineCpuExecutor` — no hidden process-global pool is ever created;
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

For pure-Python compute that should not share the loop's GIL at all,
``run_cpu(..., isolated=True)`` runs the call on the **isolated** executor — a
second binding beside the default one, normally a :class:`ProcessPoolCpuExecutor`
(or an :class:`InterpreterPoolCpuExecutor` on Python 3.14). The callable and its
arguments then cross a process or interpreter boundary, so they must be picklable,
and the caller's context does not travel with them.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import os
import pickle  # nosec B403 - only this process's own payloads, sent to its own workers
import threading
import traceback
from collections.abc import Callable, Generator, Iterable, Sequence
from concurrent.futures import BrokenExecutor, ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from itertools import batched
from typing import TYPE_CHECKING, Protocol, cast, final, runtime_checkable

import attrs

from forze.base.exceptions import CoreException, exc

from .._logger import logger
from .deadline import clear_deadline, remaining_time
from .runtime import ScopeFallback

if TYPE_CHECKING:
    from concurrent.futures import ProcessPoolExecutor
    from multiprocessing.process import BaseProcess

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
    threads), once even when first used from several threads, and lives for the
    process, like the Argon2 hashing pool. Size it via :attr:`max_workers`; close
    it on shutdown via :meth:`close`, which is final.
    """

    max_workers: int = _DEFAULT_CPU_WORKERS
    thread_name_prefix: str = "forze-cpu"
    _pool: ThreadPoolExecutor | None = attrs.field(default=None, init=False)
    _lock: threading.Lock = attrs.field(factory=threading.Lock, init=False, eq=False, repr=False)
    _closed: bool = attrs.field(default=False, init=False, eq=False, repr=False)

    def _require_pool(self) -> ThreadPoolExecutor:
        # Double-checked under a lock: a runtime scope holds its pool process-wide, so event
        # loops on other threads can reach the first use too.
        pool = self._pool

        if pool is None:
            with self._lock:
                if self._closed:
                    # Refused, as the drain gate refuses new work: inline would block the loop
                    # unseen, and a new pool would outlive its runtime with nothing to close it.
                    raise exc.unavailable(
                        "CPU executor is closed; its runtime scope has exited.",
                        code="cpu_executor_closed",
                    )

                pool = self._pool

                if pool is None:
                    pool = self._pool = ThreadPoolExecutor(
                        max_workers=self.max_workers,
                        thread_name_prefix=self.thread_name_prefix,
                    )

        return pool

    async def run[T](self, fn: Callable[[], T], *, label: str | None = None) -> T:
        loop = asyncio.get_running_loop()
        pool = self._require_pool()

        try:
            future = loop.run_in_executor(pool, fn)

        except RuntimeError:
            # A close that ran between ``_require_pool`` and the submit shut the pool down.
            if self._closed:
                raise exc.unavailable(
                    "CPU executor is closed; its runtime scope has exited.",
                    code="cpu_executor_closed",
                ) from None

            raise

        return await future

    def close(self) -> None:
        """Shut the pool down, cancelling work that has not started.

        Final: a later offload is refused with a retryable ``unavailable``
        (``cpu_executor_closed``) instead of building a pool nothing would close.
        """

        with self._lock:
            self._closed = True
            pool, self._pool = self._pool, None

        # Outside the lock: shutdown waits for running work, which must not stall a first use.
        if pool is not None:
            pool.shutdown(cancel_futures=True)


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class InlineCpuExecutor:
    """Run the thunk inline (synchronously). For plain unit tests and as the base
    for the deterministic simulation executor — no threads, fully reproducible.

    With :attr:`pickled`, the thunk goes through the same pickle round trip as on a
    :class:`ProcessPoolCpuExecutor`, inline: bind ``InlineCpuExecutor(pickled=True)`` for
    ``run_cpu(..., isolated=True)`` in a unit test, and a lambda, a local function or an
    argument that cannot be pickled is refused there as it would be in production.
    """

    pickled: bool = False

    async def run[T](self, fn: Callable[[], T], *, label: str | None = None) -> T:
        if not self.pickled:
            return fn()

        return cast("T", _unpack_outcome(_call_pickled(_pickle_call(fn, label)), label))


# ....................... #
# Isolated executors (run_cpu(..., isolated=True))


class _RemoteTraceback(Exception):
    """The traceback a worker formatted, chained as ``__cause__`` of what its call raised."""

    def __init__(self, text: str) -> None:
        super().__init__(text)
        self.text = text

    def __str__(self) -> str:
        return self.text


# ....................... #


def _closed_executor() -> CoreException:
    # Refused like ThreadPoolCpuExecutor after close: a new pool would outlive its runtime
    # with nothing to close it.
    return exc.unavailable(
        "Isolated CPU executor is closed; its runtime scope has exited.",
        code="cpu_executor_closed",
    )


def _lost_worker(label: str | None) -> CoreException:
    return exc.unavailable(
        f"Isolated CPU offload of {label or 'a call'} lost its worker, which was killed or "
        "crashed.",
        code="cpu_offload_worker_lost",
    )


# ....................... #


def _pickle_call(fn: Callable[[], object], label: str | None) -> bytes:
    """Pickle the thunk on the calling thread, refusing one that cannot cross to a worker."""

    try:
        return pickle.dumps(fn, protocol=pickle.HIGHEST_PROTOCOL)

    except Exception as e:
        raise exc.configuration(
            f"Isolated CPU offload of {label or fn!r} cannot be sent to a worker process: "
            f"the callable and its arguments must be picklable ({e}).",
            code="cpu_offload_not_picklable",
        ) from e


# ....................... #

_Outcome = tuple[bool, bytes | None, str | None]
"""What a worker sends back: ``(ok, pickled result or exception, its traceback text)``."""


def _call_pickled(payload: bytes) -> _Outcome:
    """Worker-side half of :class:`ProcessPoolCpuExecutor`: run the thunk, never raise.

    The result or exception is pickled here, not by the pool: an outcome the parent cannot
    unpickle would otherwise read as a broken pool and fail every call in flight. A bytes
    payload and a string unpickle anywhere, so only this call can fail.
    """

    tb: str | None = None

    try:
        ok, value = True, pickle.loads(payload)()  # nosec B301 - this process's own payload

    except Exception as e:
        ok, value, tb = False, e, "".join(traceback.format_exception(e))

    try:
        return ok, pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL), tb

    except Exception as e:
        return ok, None, "".join(traceback.format_exception(e)) + (tb or "")


def _unpack_outcome(outcome: _Outcome, label: str | None) -> object:
    """Parent-side half: return the result or raise the exception, refusing on this call
    alone an outcome that cannot come back."""

    ok, blob, tb = outcome
    cause = _RemoteTraceback(tb) if tb else None
    what = "result" if ok else "exception"
    stuck = exc.configuration(
        f"Isolated CPU offload of {label or 'a call'}: its {what} cannot come back from the "
        "worker; "
        "it must pickle there and unpickle here.",
        code="cpu_offload_result_not_picklable",
    )

    if blob is None:  # the worker could not pickle it; tb holds why
        raise stuck from cause

    try:
        value = pickle.loads(blob)  # nosec B301 - pickled by our own worker

    except Exception as e:
        raise stuck from (cause or e)

    if ok:
        return value

    raise cast("BaseException", value) from cause


# ....................... #


async def _await_worker[R](future: concurrent.futures.Future[R], label: str | None) -> R:
    try:
        return await asyncio.wrap_future(future)

    except asyncio.CancelledError:
        # Queued work is dropped; a call already running keeps its worker until it returns.
        if not future.cancel() and not future.done():
            logger.warning(
                "Isolated CPU offload of %s abandoned while running; its worker stays busy "
                "until the call returns.",
                label,
            )

        raise


# ....................... #


def _reap(processes: Sequence[BaseProcess]) -> None:
    """Give terminated workers a moment, then kill any that ignore ``SIGTERM``."""

    for process in processes:
        process.join(0.5 / len(processes))  # bounded: 0.5 s in all

    for process in processes:
        if process.is_alive():
            process.kill()


# ....................... #


@final
@attrs.define(slots=True)
class ProcessPoolCpuExecutor:
    """Offload to a bounded pool of worker processes, each with its own GIL.

    The isolated executor behind ``run_cpu(..., isolated=True)``: pure-Python compute runs
    in parallel with the event loop instead of contending for its GIL. The thunk is pickled
    on the calling thread before submission, so a callable or argument that cannot be
    pickled is refused at once with a ``configuration`` error naming the call. The result
    or exception comes back pickled; an exception keeps the worker's traceback as
    ``__cause__``, and one that cannot come back fails only its own call
    (``cpu_offload_result_not_picklable``).

    Bind it in the isolated slot (:func:`bind_isolated_cpu_executor`), not as the default
    executor: the default ``run_cpu`` path wraps the call in the caller's context, which
    cannot be pickled.

    Workers start with ``forkserver`` where the platform has it and ``spawn`` elsewhere,
    never ``fork``, which would copy a process running an event loop and threads. Either way
    a worker imports the application's main module, so it must be importable without side
    effects. The pool is created on first use, once even from several threads, and starts
    workers on demand. A worker that dies (killed for memory, crashed) breaks the stdlib
    pool: the calls in flight fail with a retryable ``unavailable``
    (``cpu_offload_worker_lost``) and the next call starts a fresh pool. :meth:`close` is
    final.
    """

    max_workers: int = attrs.field(factory=lambda: os.process_cpu_count() or 1)
    _pool: ProcessPoolExecutor | None = attrs.field(default=None, init=False)
    _lock: threading.Lock = attrs.field(factory=threading.Lock, init=False, eq=False, repr=False)
    _closed: bool = attrs.field(default=False, init=False, eq=False, repr=False)

    def _require_pool(self) -> ProcessPoolExecutor:
        # Double-checked under a lock, like ThreadPoolCpuExecutor. Imported here: the process
        # pool and multiprocessing cost an import most applications never use.
        pool = self._pool

        if pool is None:
            with self._lock:
                if self._closed:
                    raise _closed_executor()

                pool = self._pool

                if pool is None:
                    import multiprocessing
                    from concurrent.futures import ProcessPoolExecutor

                    methods = multiprocessing.get_all_start_methods()
                    method = "forkserver" if "forkserver" in methods else "spawn"
                    pool = self._pool = ProcessPoolExecutor(
                        max_workers=self.max_workers,
                        mp_context=multiprocessing.get_context(method),
                    )

        return pool

    def _broken(self, pool: ProcessPoolExecutor, label: str | None) -> CoreException:
        # A dead worker leaves the stdlib pool refusing all further work: drop it so the next
        # call starts a fresh one (other calls failing on the same pool skip this).
        with self._lock:
            closed, current = self._closed, self._pool is pool

            if current:
                self._pool = None

        if current:
            pool.shutdown(wait=False, cancel_futures=True)

        return _closed_executor() if closed else _lost_worker(label)

    async def run[T](self, fn: Callable[[], T], *, label: str | None = None) -> T:
        payload = _pickle_call(fn, label)
        pool = self._require_pool()

        try:
            future = pool.submit(_call_pickled, payload)

        except BrokenExecutor as e:
            raise self._broken(pool, label) from e

        except RuntimeError:
            # A close, or a call dropping the pool a dead worker broke, ran between
            # ``_require_pool`` and the submit and shut the pool down.
            if self._closed:
                raise _closed_executor() from None

            if self._pool is not pool:
                raise _lost_worker(label) from None

            raise

        try:
            outcome = await _await_worker(future, label)

        except BrokenExecutor as e:
            raise self._broken(pool, label) from e

        return cast("T", _unpack_outcome(outcome, label))

    def close(self) -> None:
        """Shut the pool down without waiting: drop queued work and stop the workers.

        A call still running here was abandoned by its caller (``run_cpu`` stopped waiting
        at the deadline or on cancellation), so its worker is terminated, not waited for, and
        killed if it is still alive half a second later. Final: a later call is refused with
        a retryable ``unavailable`` (``cpu_executor_closed``), as
        :meth:`ThreadPoolCpuExecutor.close` does.
        """

        with self._lock:
            self._closed = True
            pool, self._pool = self._pool, None

        if pool is None:
            return

        # Under the pool's own lock, which orders the snapshot against it starting a worker.
        with pool._shutdown_lock:
            processes = list((pool._processes or {}).values())

        terminate = getattr(pool, "terminate_workers", None)

        if terminate is not None:
            terminate()  # Python 3.14+: also stops a worker started since the snapshot

        else:
            # shortcut: Python 3.13 has no terminate_workers(), so it relies on the private
            # process table above; drop this branch once 3.14 is the floor.
            pool.shutdown(wait=False, cancel_futures=True)

            for process in processes:
                if process.is_alive():
                    process.terminate()

        if processes:
            _reap(processes)


# ....................... #


def _interpreter_pool_type() -> type[ThreadPoolExecutor] | None:
    """The stdlib ``InterpreterPoolExecutor`` (Python 3.14+), or ``None`` before it existed."""

    return getattr(concurrent.futures, "InterpreterPoolExecutor", None)


# ....................... #


@final
@attrs.define(slots=True)
class InterpreterPoolCpuExecutor:
    """Offload to a bounded pool of subinterpreters, each with its own GIL (Python 3.14+).

    An isolated executor like :class:`ProcessPoolCpuExecutor`, without the processes: lower
    start-up and memory cost, but each worker interpreter imports the callable's module, and
    many C extensions refuse to load in a subinterpreter (``orjson``, which forze itself
    imports, among them). Keep the callable in a module that imports only the standard
    library and pure-Python code. A callable, argument or result that cannot be shared is
    refused with a ``configuration`` error naming the call.

    Construction refuses with a ``configuration`` error on a Python without
    ``concurrent.futures.InterpreterPoolExecutor``. A worker interpreter cannot be stopped
    from outside, so an abandoned call runs to completion; :meth:`close` does not wait for
    it, but interpreter exit does. :meth:`close` is final.
    """

    max_workers: int = attrs.field(factory=lambda: os.process_cpu_count() or 1)
    _pool: ThreadPoolExecutor | None = attrs.field(default=None, init=False)
    _lock: threading.Lock = attrs.field(factory=threading.Lock, init=False, eq=False, repr=False)
    _closed: bool = attrs.field(default=False, init=False, eq=False, repr=False)

    def __attrs_post_init__(self) -> None:
        if _interpreter_pool_type() is None:
            raise exc.configuration(
                "InterpreterPoolCpuExecutor needs Python 3.14 or newer "
                "(concurrent.futures.InterpreterPoolExecutor); use ProcessPoolCpuExecutor.",
                code="cpu_offload_interpreters_unavailable",
            )

    def _require_pool(self) -> ThreadPoolExecutor:
        pool = self._pool

        if pool is None:
            with self._lock:
                if self._closed:
                    raise _closed_executor()

                pool = self._pool

                if pool is None:
                    pool_type = cast("type[ThreadPoolExecutor]", _interpreter_pool_type())
                    pool = self._pool = pool_type(max_workers=self.max_workers)

        return pool

    async def run[T](self, fn: Callable[[], T], *, label: str | None = None) -> T:
        from concurrent.interpreters import ExecutionFailed, NotShareableError

        pool = self._require_pool()

        try:
            future = pool.submit(fn)

        except BrokenExecutor as e:  # a worker interpreter failed to start
            raise _lost_worker(label) from e

        except RuntimeError:
            # A close that ran between ``_require_pool`` and the submit shut the pool down.
            if self._closed:
                raise _closed_executor() from None

            raise

        try:
            return await _await_worker(future, label)

        except (NotShareableError, pickle.PicklingError) as e:
            if isinstance(e.__cause__, ExecutionFailed):
                raise  # raised by the call itself, inside the worker

            raise exc.configuration(
                f"Isolated CPU offload of {label or fn!r} cannot cross into a worker "
                f"interpreter: the callable, its arguments and its result must be shareable, "
                f"and its module must import in a subinterpreter ({e}).",
                code="cpu_offload_not_shareable",
            ) from e

    def close(self) -> None:
        """Shut the pool down without waiting, cancelling work that has not started.

        Final, like :meth:`ProcessPoolCpuExecutor.close`.
        """

        with self._lock:
            self._closed = True
            pool, self._pool = self._pool, None

        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)


# ....................... #

# The fallback when nothing is bound or held (no runtime scope, no explicit bind): run inline.
# It is stateless, immutable, and holds no resources, so there is no hidden process-global
# thread pool created at import — a real offload comes from a runtime scope (which owns a
# scope-lifetime ThreadPoolCpuExecutor) or an explicit bind_cpu_executor.
_FALLBACK_EXECUTOR: CpuExecutor = InlineCpuExecutor()

_CPU_EXECUTOR: ContextVar[CpuExecutor | None] = ContextVar(
    "forze_cpu_executor",
    default=None,
)

_HELD_EXECUTORS: ScopeFallback[CpuExecutor] = ScopeFallback()
"""The executors open runtime scopes hold, for a context that binds none (an ASGI request)."""

# ....................... #


def current_cpu_executor() -> CpuExecutor:
    """Return the CPU executor active in the current context.

    One bound in this context wins. Otherwise the executor of the oldest runtime scope open in
    the process — a request an ASGI server runs outside the lifespan's task still offloads to
    the runtime's pool — and with no scope open, the inline fallback.
    """

    bound = _CPU_EXECUTOR.get()

    if bound is not None:
        return bound

    held = _HELD_EXECUTORS.held()

    return held[0] if held else _FALLBACK_EXECUTOR


# ....................... #


def cpu_executor_bound() -> bool:
    """Whether an executor is explicitly bound in this context (vs the inline fallback).

    A runtime scope uses this to defer to an executor an outer context already bound
    (e.g. a simulation's inline executor) instead of overriding it with its own pool.
    """

    return _CPU_EXECUTOR.get() is not None


# ....................... #


@contextmanager
def bind_cpu_executor(executor: CpuExecutor) -> Generator[None]:
    """Bind *executor* as the active CPU executor for the duration of the block."""

    token = _CPU_EXECUTOR.set(executor)

    try:
        yield

    finally:
        _CPU_EXECUTOR.reset(token)


# ....................... #


@contextmanager
def hold_cpu_executor(executor: CpuExecutor) -> Generator[None]:
    """Bind *executor* for the block and hold it process-wide — an execution runtime's scope.

    Held, it is what :func:`current_cpu_executor` returns in a context that binds none, such as
    a request task an ASGI server creates outside the lifespan's task; with several scopes open,
    the oldest one's. Released on exit.
    """

    with bind_cpu_executor(executor), _HELD_EXECUTORS.hold(executor):
        yield


# ....................... #

# The isolated slot, for run_cpu(..., isolated=True). No inline fallback: refusing beats running
# work meant to leave the caller's GIL inline on the loop, or back on the GIL-sharing thread pool.
_ISOLATED_CPU_EXECUTOR: ContextVar[CpuExecutor | None] = ContextVar(
    "forze_isolated_cpu_executor",
    default=None,
)

_HELD_ISOLATED_EXECUTORS: ScopeFallback[CpuExecutor] = ScopeFallback()
"""The isolated executors open runtime scopes hold, for a context that binds none."""

# ....................... #


def current_isolated_cpu_executor() -> CpuExecutor | None:
    """Return the isolated CPU executor active in the current context, or ``None``.

    One bound in this context wins, then the one the oldest open runtime scope holds, as for
    :func:`current_cpu_executor`. There is no inline fallback: with neither,
    ``run_cpu(..., isolated=True)`` refuses.
    """

    bound = _ISOLATED_CPU_EXECUTOR.get()

    if bound is not None:
        return bound

    held = _HELD_ISOLATED_EXECUTORS.held()

    return held[0] if held else None


# ....................... #


def isolated_cpu_executor_bound() -> bool:
    """Whether an isolated executor is explicitly bound in this context (vs held or none).

    A runtime scope uses this to defer to one an outer context already bound, as with
    :func:`cpu_executor_bound`.
    """

    return _ISOLATED_CPU_EXECUTOR.get() is not None


# ....................... #


@contextmanager
def bind_isolated_cpu_executor(executor: CpuExecutor) -> Generator[None]:
    """Bind *executor* as the isolated CPU executor for the duration of the block."""

    token = _ISOLATED_CPU_EXECUTOR.set(executor)

    try:
        yield

    finally:
        _ISOLATED_CPU_EXECUTOR.reset(token)


# ....................... #


@contextmanager
def hold_isolated_cpu_executor(executor: CpuExecutor) -> Generator[None]:
    """Bind *executor* as the isolated executor and hold it process-wide — a runtime's scope.

    Like :func:`hold_cpu_executor`: held, it is what :func:`current_isolated_cpu_executor`
    returns in a context that binds none; with several scopes open, the oldest one's.
    """

    with bind_isolated_cpu_executor(executor), _HELD_ISOLATED_EXECUTORS.hold(executor):
        yield


# ----------------------- #
# Public API


async def run_cpu[T](
    fn: Callable[..., T],
    /,
    *args: object,
    deadline: bool = True,
    isolated: bool = False,
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
    offloaded code does not re-impose it.

    Set *isolated* to ``True`` to run on the isolated executor instead (see
    :func:`bind_isolated_cpu_executor`): pure-Python compute then runs outside the caller's
    GIL. *fn* and its arguments must be picklable and cross the boundary as they are — the
    calling context, the cancellation token and the deadline stay behind, so a
    :func:`checkpoint` in the worker is a no-op. The deadline and cancellation still end the
    *wait*; the worker cannot be signalled, so its call is abandoned, keeping its worker
    busy until it returns (a warning is logged). With no isolated executor bound or held,
    raises a ``configuration`` error rather than fall back to threads or inline. (``deadline`` and ``isolated`` are reserved keywords of this function; pass any
    same-named argument to *fn* via :func:`functools.partial`.)
    """

    bound = functools.partial(fn, *args, **kwargs)

    # Call-site id for simulation cost models; unwrap partials so a partial-wrapped
    # callable still resolves to its underlying function name.
    target: object = fn
    while isinstance(target, functools.partial):
        target = target.func
    label = getattr(target, "__qualname__", None)
    token = CancelToken()
    executor: CpuExecutor | None
    thunk: Callable[[], T]

    if isolated:
        executor = current_isolated_cpu_executor()

        if executor is None:
            raise exc.configuration(
                f"run_cpu({label or fn!r}, isolated=True) found no isolated CPU executor: run "
                "it inside an ExecutionRuntime scope or bind one with bind_isolated_cpu_executor.",
                code="cpu_offload_isolated_unbound",
            )

        # Crosses the process / interpreter boundary as it is: a context copy and the token
        # cannot be pickled, so neither travels.
        thunk = bound

    else:
        executor = current_cpu_executor()
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

    for chunk in batched(items, chunk_size, strict=False):
        out.extend(await run_cpu(_map_chunk, fn, chunk))

    return out


def _map_chunk[I, R](fn: Callable[[I], R], chunk: tuple[I, ...]) -> list[R]:
    return [fn(item) for item in chunk]
