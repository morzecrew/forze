import asyncio
import inspect
from collections.abc import Awaitable, Coroutine

# ----------------------- #


async def maybe_await[T](value: T | Awaitable[T]) -> T:
    if inspect.isawaitable(value):
        return await value

    return value


# ....................... #


async def run_to_completion[T](coro: Coroutine[object, object, T]) -> T:
    """Await *coro* to completion even if the surrounding task is cancelled.

    Runs *coro* as a task and keeps waiting through cancellations of the
    surrounding task; once the inner task finishes, a pending cancellation is
    re-raised so it propagates normally (``asyncio.timeout`` blocks and task
    groups observe it as usual). Use this to protect a critical section that
    must not be torn by cancellation (e.g. post-commit work).

    When the surrounding task was cancelled *and* *coro* itself failed, the
    cancellation wins and the inner error is attached as its ``__cause__``.

    The inner task runs in a copy of the current context: ContextVar writes
    made inside *coro* do not propagate back to the caller, and tokens created
    before the call must not be reset inside it.
    """

    task = asyncio.ensure_future(coro)
    cancelled: asyncio.CancelledError | None = None

    while True:
        try:
            result = await asyncio.shield(task)
            break

        except asyncio.CancelledError as e:
            if task.cancelled():
                # The inner task itself was cancelled (not the surrounding
                # task): nothing left to wait for, propagate.
                raise

            cancelled = e

        except BaseException as e:
            if cancelled is None:
                raise

            raise cancelled from e

    if cancelled is not None:
        raise cancelled

    return result
