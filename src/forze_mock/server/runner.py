"""Compose a declared :class:`MockApp` into a served ASGI app — and refuse to serve a real one."""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from starlette.applications import Starlette
from starlette.routing import Mount

from forze.application.execution import DepsRegistry, ExecutionRuntime, LifecyclePlan
from forze.base.exceptions import exc
from forze.base.logging import get_logger
from forze.base.primitives import bind_time_source
from forze_mock.adapters import MockState
from forze_mock.execution.module import MockDepsModule

from .clock import ClockMiddleware, ControlledTimeSource
from .control import build_control_app
from .declaration import MockApp
from .faults import ControlInterceptor, FaultBoard
from .session import MockSession, issue_session

# ----------------------- #

SERVE_ENV_GATE = "FORZE_MOCK_SERVER"
"""Must be truthy for :func:`serve` to bind a port."""

_TRUTHY = frozenset({"1", "true", "yes"})

logger = get_logger("forze_mock.server")

# ....................... #


def _runtime_for(mock_app: MockApp, board: FaultBoard) -> tuple[ExecutionRuntime, MockState]:
    """Compose the declared modules with the mock underneath them."""

    mock = mock_app.mock or MockDepsModule(state=mock_app.state or MockState())
    state: MockState = mock.state

    registry = DepsRegistry.from_modules(*mock_app.modules, mock)

    if mock_app.deps:
        registry = registry.with_deps(*mock_app.deps)

    # Deps-scoped, so every configurable port the app resolves passes through the board and
    # neither the app nor its handlers know: fault injection stays at the seam.
    registry = registry.with_interceptors(ControlInterceptor(board=board))

    lifecycle = (
        LifecyclePlan.from_steps(*mock_app.lifecycle).freeze() if mock_app.lifecycle else None
    )
    frozen = registry.freeze()

    _refuse_without_a_fallback(frozen)

    runtime = (
        ExecutionRuntime(deps=frozen, lifecycle=lifecycle)
        if lifecycle is not None
        else ExecutionRuntime(deps=frozen)
    )

    return runtime, state


# ....................... #


def _refuse_without_a_fallback(frozen: Any) -> None:
    """Refuse a composition with no mock in it.

    The structural half of the production-leak guard: rather than trusting a name or a flag,
    ask the provider store whether anything is *fallback*-marked — which only a mock module
    is. A runtime wired entirely to real backends can therefore never be served here, and so
    can never grow a reset/fault/state API.
    """

    if not frozen.store.fallback_plain:
        raise exc.configuration(
            "Refusing to build a mock server: the composed deps contain no fallback-marked "
            "mock module, so this is a real runtime"
        )


# ....................... #


def build_mock_server(mock_app: MockApp) -> Any:
    """Build the app *mock_app* declares, on in-memory backends, with the control plane.

    The app's own factory runs unchanged — its routes, middleware, exception handlers and
    identity ingress are the ones served. Only the ``DepsRegistry`` differs, and the seed and
    ``/_mock`` routes are added around it.
    """

    board = FaultBoard()
    runtime, state = _runtime_for(mock_app, board)
    clock = ControlledTimeSource()

    session = issue_session(
        runtime=runtime,
        state=state,
        board=board,
        clock=clock,
        seed=mock_app.seed,
        on_emit=mock_app.on_emit,
    )

    app = mock_app.build_app(runtime)

    if not hasattr(getattr(app, "router", None), "lifespan_context"):
        raise exc.configuration(
            "MockApp.build_app must return a Starlette/FastAPI app: its lifespan is what "
            f"opens the runtime scope the served routes resolve from (got {type(app)})"
        )

    control = (
        [Mount(mock_app.control.prefix, app=build_control_app(session))]
        if mock_app.control.enabled
        else []
    )

    # The control plane goes *beside* the app, not inside it. Mounted onto the app it would
    # sit behind the app's own middleware — and a dev tool that 401s because the app requires
    # a credential is a tool you cannot use to reset the app.
    return Starlette(
        routes=[*control, Mount("", app=ClockMiddleware(app=app, clock=clock))],
        lifespan=_delegating_lifespan(app, session, clock),
    )


# ....................... #


def _delegating_lifespan(app: Any, session: MockSession, clock: ControlledTimeSource) -> Any:
    """Run the app's own lifespan from the wrapper, then seed.

    A mounted ASGI app never receives lifespan events — only the outermost app does — so the
    wrapper has to drive the app's lifespan explicitly, or the runtime scope its routes
    resolve from is never opened. The clock is bound outside it, so everything the app does
    at startup already sees the source ``POST /_mock/time`` drives.
    """

    @asynccontextmanager
    async def lifespan(_wrapper: Any) -> AsyncGenerator[None]:
        with bind_time_source(clock):
            async with app.router.lifespan_context(app):
                await _seed_once(session)
                yield

    return lifespan


# ....................... #


async def _seed_once(session: MockSession) -> None:
    if session.seed is None:
        return

    from forze_mock.seeding import apply_seed

    result = await apply_seed(session.runtime.get_context(), session.seed)

    logger.info("Mock server seeded %s document(s)", result.total)


# ....................... #


def serve(
    mock_app: MockApp,
    *,
    host: str = "127.0.0.1",
    port: int = 8000,
    log_level: str = "info",
) -> None:
    """Serve *mock_app* with uvicorn — refusing unless this is explicitly a mock environment.

    The gate is here, at the entry point, and not at import time: an import-time environment
    check poisons every test that merely imports the module, and a gate people learn to work
    around is worse than none.
    """

    import uvicorn

    if os.environ.get(SERVE_ENV_GATE, "").strip().lower() not in _TRUTHY:
        raise exc.configuration(
            f"Refusing to serve the mock: set {SERVE_ENV_GATE}=1 to confirm this is a "
            "development environment. This server keeps all data in memory and enforces "
            "none of the guarantees a real backend does"
        )

    app = build_mock_server(mock_app)

    logger.warning(
        "SERVING AN IN-MEMORY MOCK on http://%s:%s — not a production backend "
        "(seed=%s, control plane=%s)",
        host,
        port,
        "declared" if mock_app.seed is not None else "none",
        mock_app.control.prefix if mock_app.control.enabled else "disabled",
    )

    uvicorn.run(app, host=host, port=port, log_level=log_level)
