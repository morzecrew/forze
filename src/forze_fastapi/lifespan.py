"""Run an :class:`ExecutionRuntime` for the lifetime of a FastAPI app."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

from fastapi import FastAPI

from forze.application.execution import ExecutionRuntime

from .middlewares.authn_cookies import check_authn_cookie_paths
from .middlewares.bypass import check_bypass_paths
from .middlewares.raw_websocket import check_websocket_allowlist

# ----------------------- #


def run_startup_checks(app: Any) -> None:
    """Run every startup check the gating middlewares rely on, on *app* and every Starlette
    or FastAPI app mounted beneath it.

    Fails the boot (``configuration``) on a websocket allowlist that does not serve
    exactly one governed route per path
    (:func:`~forze_fastapi.middlewares.check_websocket_allowlist`), on a bypass that names
    a generated operation route, differs between the middlewares or matches no route
    (:func:`~forze_fastapi.middlewares.check_bypass_paths`), and on a cookie-mode authn
    route a security middleware would 401 or a refresh cookie would miss
    (:func:`~forze_fastapi.middlewares.check_authn_cookie_paths`). A no-op for an app
    without those middlewares or routes.

    ``runtime_lifespan`` calls it at startup. An app with its own lifespan calls it there,
    on the outermost app — Starlette never sends lifespan events to a mounted app, so a
    mounted app's own lifespan never runs::

        @asynccontextmanager
        async def lifespan(app: FastAPI) -> AsyncIterator[None]:
            run_startup_checks(app)

            async with build_runtime(...).scope():
                yield

    :param app: The outermost application.
    :raises CoreException: ``configuration`` on the first check that fails.
    """

    # Fail the boot on a websocket-allowlist/route mismatch rather than serving
    # (or refusing) connections wrongly at runtime; a no-op without allowlists.
    check_websocket_allowlist(app)

    # And on a bypass that names a governed operation route, disagrees between the
    # middlewares, or matches no route at all; a no-op without bypass_paths.
    check_bypass_paths(app)

    # And on a cookie-mode logout route its access cookie never reaches.
    check_authn_cookie_paths(app)


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

    Before the scope opens it runs :func:`run_startup_checks` on the app. Give it to the
    outermost app: Starlette never sends lifespan events to a mounted app, so on a
    mounted app neither the runtime nor the checks would ever start.

    :param runtime: Runtime to run for the app's lifetime.
    :returns: Lifespan callable for ``FastAPI(lifespan=...)``.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        run_startup_checks(app)

        async with runtime.scope():
            yield

    return lifespan
