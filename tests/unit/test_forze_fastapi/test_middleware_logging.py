"""Unit tests for :class:`forze_fastapi.middlewares.logging.LoggingMiddleware`."""

import io
import json
from unittest.mock import AsyncMock

import pytest
from starlette.testclient import TestClient

from forze.base.exceptions import exc
from forze.base.logging import configure_logging
from forze_fastapi._logging import ForzeFastAPILogger
from forze_fastapi.exceptions import ERROR_CODE_HEADER
from forze_fastapi.middlewares.logging import LoggingMiddleware

# ----------------------- #


def _json_records(stream: io.StringIO) -> list[dict]:
    out: list[dict] = []
    for line in stream.getvalue().strip().split("\n"):
        line = line.strip()
        if line.startswith("{"):
            out.append(json.loads(line))
    return out


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

    def test_core_error_converted_to_standard_response(self) -> None:
        """CoreException raised below is converted to the standard JSON response."""

        async def core_app(scope: object, receive: object, send: object) -> None:
            raise exc.internal("boundary", code="test")

        mw = LoggingMiddleware(core_app)
        client = TestClient(mw, raise_server_exceptions=True)

        response = client.get("/")

        assert response.status_code == 500
        # internal summaries must not leak to clients
        assert response.json() == {"detail": "Internal server error"}
        assert response.headers.get(ERROR_CODE_HEADER) == "test"
        assert "x-process-time" in response.headers

    def test_unhandled_exception_returns_500_json(self) -> None:
        """Non-exc.internal exceptions yield a generic 500 JSON body."""

        async def boom(scope: object, receive: object, send: object) -> None:
            raise RuntimeError("boom")

        mw = LoggingMiddleware(boom)
        client = TestClient(mw, raise_server_exceptions=False)
        response = client.get("/")

        assert response.status_code == 500
        assert response.json() == {"detail": "Internal server error"}

    def test_unhandled_exception_logs_traceback(self) -> None:
        """Fallback path logs the exception with a traceback."""

        buf = io.StringIO()
        configure_logging(
            level="info",
            logger_names=[str(ForzeFastAPILogger.ACCESS)],
            stream=buf,
            render_mode="json",
        )

        async def boom(scope: object, receive: object, send: object) -> None:
            raise RuntimeError("boom")

        mw = LoggingMiddleware(boom)
        client = TestClient(mw, raise_server_exceptions=False)
        client.get("/")

        records = _json_records(buf)
        assert len(records) == 1
        row = records[0]
        assert row["level"] == "critical"
        assert row["event"] == "Unhandled exception"
        assert row["error.type"] == "RuntimeError"
        assert "RuntimeError" in row["error.stack"]
