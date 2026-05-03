"""Unit tests for :mod:`forze_socketio.server`."""

import pytest

from forze.base.errors import CoreError
from forze_socketio import server as server_module

# ----------------------- #


class StubAsyncServer:
    """AsyncServer stub capturing constructor args."""

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        self.args = args
        self.kwargs = kwargs


class StubRedisManager:
    """Redis manager stub capturing constructor args."""

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        self.args = args
        self.kwargs = kwargs


class StubASGIApp:
    """ASGI app stub capturing constructor args."""

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        self.args = args
        self.kwargs = kwargs


class TestSocketIOServerBuilders:
    """Tests for Socket.IO server helper builders."""

    def test_build_server_configures_redis_manager(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(server_module, "AsyncRedisManager", StubRedisManager)
        monkeypatch.setattr(server_module, "AsyncServer", StubAsyncServer)

        server = server_module.build_socketio_server(
            redis_url="redis://localhost:6379/0",
            cors_allowed_origins="*",
        )

        manager = server.kwargs["client_manager"]
        assert isinstance(manager, StubRedisManager)
        assert manager.args == ("redis://localhost:6379/0",)
        assert manager.kwargs["channel"] == "socketio"
        assert manager.kwargs["write_only"] is False
        assert server.kwargs["cors_allowed_origins"] == "*"

    def test_build_server_rejects_conflicting_manager_config(self) -> None:
        with pytest.raises(CoreError, match="either `redis_url` or `client_manager`"):
            server_module.build_socketio_server(
                redis_url="redis://localhost:6379/0",
                client_manager=object(),  # type: ignore[arg-type]
            )

    def test_build_asgi_app_wraps_server(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(server_module, "ASGIApp", StubASGIApp)

        server = object()
        app = server_module.build_socketio_asgi_app(
            server,  # type: ignore[arg-type]
            other_asgi_app="app",
            socketio_path="ws",
        )

        assert isinstance(app, StubASGIApp)
        assert app.args == (server,)
        assert app.kwargs == {"other_asgi_app": "app", "socketio_path": "ws"}

    def test_build_server_without_redis_url(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(server_module, "AsyncServer", StubAsyncServer)
        server = server_module.build_socketio_server()
        assert server.kwargs.get("client_manager") is None
