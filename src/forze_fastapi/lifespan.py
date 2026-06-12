"""Run an :class:`ExecutionRuntime` for the lifetime of a FastAPI app."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import AsyncGenerator, Callable

from fastapi import FastAPI

from forze.application.execution import ExecutionRuntime

# ----------------------- #


def runtime_lifespan(
    runtime: ExecutionRuntime,
) -> Callable[[FastAPI], AbstractAsyncContextManager[None]]:
    """Return a lifespan callable that holds ``runtime.scope()`` open for the app.

    Pass the result to ``FastAPI(lifespan=...)``: on startup the runtime creates
    its execution context and runs lifecycle startup; on shutdown lifecycle
    shutdown runs and the context is reset — even when the app's lifetime ends
    with an error. Per-request access goes through
    :meth:`ExecutionRuntime.get_context`, which also serves as the ``ctx_dep``
    factory for middlewares and generated routes::

        runtime = build_runtime(...)
        app = FastAPI(lifespan=runtime_lifespan(runtime))
        attach_document_routes(router, ..., ctx_dep=runtime.get_context)

    :param runtime: Runtime to run for the app's lifetime.
    :returns: Lifespan callable for ``FastAPI(lifespan=...)``.
    """

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
        async with runtime.scope():
            yield

    return lifespan
