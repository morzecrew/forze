"""Shared fixtures for forze_socketio unit tests."""

import io
from collections.abc import AsyncIterator

import pytest

from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.logging import configure_logging
from forze_mock import MockDepsModule

# ----------------------- #


@pytest.fixture
def error_log_buf() -> io.StringIO:
    """Capture ``socketio.errors`` logger output as JSON records."""

    buf = io.StringIO()
    configure_logging(
        level="info",
        logger_names=["socketio.errors"],
        stream=buf,
        render_mode="json",
    )
    return buf


@pytest.fixture(autouse=True)
def no_turn_away_hold(monkeypatch: pytest.MonkeyPatch) -> None:
    """A draining replica holds a turned-away connect for 1–5 s before closing it; tests
    that are not about the hold close at once."""

    from forze_socketio import connection

    monkeypatch.setattr(connection, "_TURN_AWAY_HOLD_SECONDS", (0.0, 0.0), raising=False)


@pytest.fixture
async def serving_runtime() -> AsyncIterator[ExecutionRuntime]:
    """The serving runtime, its scope held open as an app's lifespan holds it: offline replay
    and acks run on its live context."""

    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        yield runtime
