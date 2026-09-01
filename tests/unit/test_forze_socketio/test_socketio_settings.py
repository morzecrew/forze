"""Unit tests for :class:`forze_socketio.settings.SocketIOSettings`."""

import pytest
from pydantic import SecretStr

pytest.importorskip("socketio")

from forze_socketio.server import build_socketio_server
from forze_socketio.settings import SocketIOSettings

# ----------------------- #


class TestSettings:
    def test_defaults_to_no_backplane(self) -> None:
        """Correct for one replica, and silently wrong for two — hence the docstring."""

        assert SocketIOSettings().redis_url is None
        assert SocketIOSettings().redis_channel == "socketio"

    # ....................... #

    def test_the_backplane_url_is_a_secret(self) -> None:
        """It carries the Redis password, so it must not reach a log by accident."""

        settings = SocketIOSettings(redis_url=SecretStr("redis://:pw@cache:6379"))

        assert "pw" not in repr(settings)
        assert settings.redis_url is not None
        assert settings.redis_url.get_secret_value() == "redis://:pw@cache:6379"

    # ....................... #

    def test_the_secret_url_feeds_the_server_directly(self) -> None:
        """No `.get_secret_value()` at the call site — the builder unwraps it."""

        settings = SocketIOSettings(redis_url=SecretStr("redis://localhost:6379"))
        server = build_socketio_server(
            redis_url=settings.redis_url,
            redis_channel=settings.redis_channel,
            redis_write_only=settings.redis_write_only,
        )

        assert server.manager is not None
