"""Run an :class:`ExecutionRuntime` for the lifetime of a FastAPI app."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractContextManager, asynccontextmanager, nullcontext
from datetime import timedelta
from typing import Any, final

from fastapi import FastAPI

from forze.application.execution import ExecutionContext, ExecutionRuntime
from forze.base.exceptions import exc

from .middlewares.authn_cookies import check_authn_cookie_paths
from .middlewares.bypass import check_bypass_paths
from .middlewares.raw_websocket import check_websocket_allowlist
from .serving import drain_ahead_of_uvicorn

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


@final
class RuntimeLifespan:
    """The lifespan :func:`runtime_lifespan` returns: pass it to ``FastAPI(lifespan=...)``.

    It also hands the runtime to whatever the app wires before that runtime exists:
    :meth:`get_context` is the ``ctx_dep`` for middlewares and generated routes, and
    :meth:`runtime` the provider ``attach_readiness_route`` takes.
    """

    def __init__(
        self,
        source: ExecutionRuntime | Callable[[], ExecutionRuntime],
        *,
        drain_budget: timedelta | None,
        announce_delay: timedelta,
    ) -> None:
        self._source = source
        self._runtime = source if isinstance(source, ExecutionRuntime) else None
        self._drain_budget = drain_budget
        self._announce_delay = announce_delay
        self._running = False

    # ....................... #

    def runtime(self) -> ExecutionRuntime | None:
        """The app's runtime; with ``build``, ``None`` until the lifespan built it and again
        once it tore it down."""

        return self._runtime

    # ....................... #

    def get_context(self) -> ExecutionContext:
        """The runtime's execution context, for the scope the lifespan holds.

        :raises CoreException: ``internal`` — no runtime has been built, or no scope is open.
        """

        if self._runtime is None:
            raise exc.internal("No runtime yet: the app's lifespan builds it at startup")

        return self._runtime.get_context()

    # ....................... #

    @asynccontextmanager
    async def __call__(self, app: FastAPI) -> AsyncGenerator[None]:
        if self._running:
            # One runtime per lifespan: a second app, or a nested entry, would build another
            # one with its own pools, or trip over the scope already open.
            raise exc.configuration(
                "runtime_lifespan is already running; give each app its own runtime_lifespan"
            )

        self._running = True

        try:
            run_startup_checks(app)

            runtime = self._source if isinstance(self._source, ExecutionRuntime) else self._source()
            self._runtime = runtime

            drain: AbstractContextManager[None] = (
                nullcontext()
                if self._drain_budget is None
                else drain_ahead_of_uvicorn(
                    runtime,
                    drain_budget=self._drain_budget,
                    announce_delay=self._announce_delay,
                )
            )

            with drain:
                async with runtime.scope():
                    yield

        finally:
            self._running = False

            if not isinstance(self._source, ExecutionRuntime):
                self._runtime = None


# ....................... #


def runtime_lifespan(
    runtime: ExecutionRuntime | None = None,
    *,
    build: Callable[[], ExecutionRuntime] | None = None,
    drain_budget: timedelta | None = None,
    announce_delay: timedelta = timedelta(0),
) -> RuntimeLifespan:
    """Return a lifespan that holds a runtime's scope open for the app.

    Pass the result to ``FastAPI(lifespan=...)``: on startup the runtime creates
    its execution context and runs lifecycle startup; on shutdown lifecycle
    shutdown runs and the context is reset — even when the app's lifetime ends
    with an error. Per-request access goes through
    :meth:`ExecutionRuntime.get_context`, which also serves as the ``ctx_dep``
    factory for middlewares and generated routes::

        runtime = build_runtime(...)
        app = FastAPI(lifespan=runtime_lifespan(runtime))
        attach_document_routes(router, ..., ctx_dep=runtime.get_context)

    With *build* instead, the lifespan builds the runtime at startup, so an app factory
    imports and runs without the environment the runtime needs, and every worker process
    builds its own. Wire the routes to the lifespan, which hands them the runtime once it
    exists::

        def create_app() -> FastAPI:
            lifespan = runtime_lifespan(build=build_my_runtime)
            app = FastAPI(lifespan=lifespan)
            attach_document_routes(router, ..., ctx_dep=lifespan.get_context)
            attach_readiness_route(router, lifespan.runtime)
            ...

    A *build* that raises fails the startup, and the server does not serve. The lifespan
    holds one runtime at a time: entered again while it runs, it refuses rather than build
    a second; entered after it tore the first down, it builds a new one.

    With *drain_budget*, the first SIGTERM or SIGINT drains the runtime before uvicorn stops
    — :func:`~forze_fastapi.serving.drain_ahead_of_uvicorn`. It covers what
    :func:`~forze_fastapi.serving.serve_uvicorn` cannot: a runtime built by *build*, and
    each worker of ``uvicorn --workers N`` or of gunicorn's uvicorn worker.

    Before the scope opens — and before *build* runs — it runs :func:`run_startup_checks`
    on the app. Give it to the outermost app: Starlette never sends lifespan events to a
    mounted app, so on a mounted app neither the runtime nor the checks would ever start.

    :param runtime: Runtime to run for the app's lifetime.
    :param build: Builds the runtime at startup, instead of *runtime*.
    :param drain_budget: Shared by the drain phases on the first signal; ``None`` (default)
        leaves the signals to the server.
    :param announce_delay: How long readiness fails before the first drain phase starts.
    :returns: Lifespan callable for ``FastAPI(lifespan=...)``.
    :raises CoreException: ``configuration`` — not exactly one of *runtime* and *build*.
    """

    source: ExecutionRuntime | Callable[[], ExecutionRuntime]

    if runtime is not None and build is None:
        source = runtime

    elif build is not None and runtime is None:
        source = build

    else:
        raise exc.configuration("runtime_lifespan takes a runtime or build=, not both or neither")

    return RuntimeLifespan(source, drain_budget=drain_budget, announce_delay=announce_delay)
