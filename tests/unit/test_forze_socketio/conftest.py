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
