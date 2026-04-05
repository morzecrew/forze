"""Unit tests for :class:`forze_fastapi.middlewares.logging.LoggingMiddleware`."""

from unittest.mock import AsyncMock

import pytest
from starlette.testclient import TestClient

from forze.base.errors import CoreError

from forze_fastapi.middlewares.logging import LoggingMiddleware

# ----------------------- #


class TestLoggingMiddleware:
    """Tests for access logging and process-time header."""

    @staticmethod
    async def _ok_app(scope: object, receive: object, send: object) -> None:
        await send(  # type: ignore[misc]
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [],
            }
        )
        await send({"type": "http.response.body", "body": b"ok"})  # type: ignore[misc]

    def test_adds_process_time_header(self) -> None:
        """Successful responses include the configured process time header."""
        mw = LoggingMiddleware(self._ok_app, process_time_header="X-Process-Time")
        client = TestClient(mw)
        response = client.get("/")

        assert response.status_code == 200
        assert "x-process-time" in response.headers

    @pytest.mark.asyncio
    async def test_non_http_passthrough(self) -> None:
        """Non-HTTP scopes are forwarded without logging wrapper logic."""
        app = AsyncMock()
        mw = LoggingMiddleware(app)

        await mw({"type": "lifespan"}, AsyncMock(), AsyncMock())

        app.assert_awaited_once()

    def test_core_error_propagates(self) -> None:
        """CoreError is not converted to a 500 JSON response."""

        async def core_app(scope: object, receive: object, send: object) -> None:
            raise CoreError(message="boundary", code="test")

        mw = LoggingMiddleware(core_app)
        client = TestClient(mw, raise_server_exceptions=True)

        with pytest.raises(CoreError, match="boundary"):
            client.get("/")

    def test_unhandled_exception_returns_500_json(self) -> None:
        """Non-CoreError exceptions yield a generic 500 JSON body."""

        async def boom(scope: object, receive: object, send: object) -> None:
            raise RuntimeError("boom")

        mw = LoggingMiddleware(boom)
        client = TestClient(mw, raise_server_exceptions=False)
        response = client.get("/")

        assert response.status_code == 500
        assert response.json() == {"detail": "Internal server error"}
