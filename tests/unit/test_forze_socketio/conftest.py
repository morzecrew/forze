"""Shared fixtures for forze_socketio unit tests."""

import io

import pytest

from forze.base.logging import configure_logging

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
