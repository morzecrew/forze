"""Unit tests for forze_fastapi.exceptions."""

import io
import json

import pytest
from fastapi import FastAPI
from starlette.requests import Request
from starlette.testclient import TestClient

from forze.base.exceptions import CoreException, exc
from forze.base.logging import configure_logging
from forze.base.scrubbing import SECRET_PLACEHOLDER
from forze_fastapi._logging import ForzeFastAPILogger
from forze_fastapi.exceptions import (
    ERROR_CODE_HEADER,
    _forze_exception_handler,
    register_exception_handlers,
)

# ----------------------- #


def _json_records(stream: io.StringIO) -> list[dict]:
    out: list[dict] = []
    for line in stream.getvalue().strip().split("\n"):
        line = line.strip()
        if line.startswith("{"):
            out.append(json.loads(line))
    return out


@pytest.fixture
def error_log_buf() -> io.StringIO:
    buf = io.StringIO()
    configure_logging(
        level="info",
        logger_names=[str(ForzeFastAPILogger.ERRORS)],
        stream=buf,
        render_mode="json",
    )
    return buf


# ----------------------- #


class TestForzeExceptionHandler:
    @pytest.mark.asyncio
    async def test_not_found_returns_404(self) -> None:
        err = exc.not_found("Document not found")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)
        assert response.status_code == 404
        assert response.body == b'{"detail":"Document not found"}'
        assert response.headers.get(ERROR_CODE_HEADER) == "core.not_found"

    @pytest.mark.asyncio
    async def test_not_found_does_not_log(self, error_log_buf: io.StringIO) -> None:
        err = exc.not_found("Document not found")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        await _forze_exception_handler(request, err)
        assert _json_records(error_log_buf) == []

    @pytest.mark.asyncio
    async def test_conflict_returns_409(self) -> None:
        err = exc.conflict("Revision mismatch")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)
        assert response.status_code == 409
        assert response.headers.get(ERROR_CODE_HEADER) == "core.conflict"

    @pytest.mark.asyncio
    async def test_validation_returns_422(self) -> None:
        err = exc.validation("Invalid input")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)
        assert response.status_code == 422
        assert response.headers.get(ERROR_CODE_HEADER) == "core.validation"

    @pytest.mark.asyncio
    async def test_internal_returns_500(self) -> None:
        err = exc.internal("Something went wrong", code="internal")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)
        assert response.status_code == 500
        assert response.headers.get(ERROR_CODE_HEADER) == "internal"

    @pytest.mark.asyncio
    async def test_internal_without_cause_logs_error_without_stack(
        self,
        error_log_buf: io.StringIO,
    ) -> None:
        err = exc.internal("Something went wrong", code="internal")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        await _forze_exception_handler(request, err)

        records = _json_records(error_log_buf)
        assert len(records) == 1
        row = records[0]
        assert row["level"] == "error"
        assert row["event"] == "Server error"
        assert row["error_code"] == "internal"
        assert row["error_kind"] == "internal"
        assert "error.stack" not in row

    @pytest.mark.asyncio
    async def test_internal_with_cause_logs_critical_with_stack(
        self,
        error_log_buf: io.StringIO,
    ) -> None:
        try:
            raise ValueError("password=hunter2")
        except ValueError as cause:
            err = exc.internal("Something went wrong", code="internal")
            err.__cause__ = cause

        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        await _forze_exception_handler(request, err)

        records = _json_records(error_log_buf)
        assert len(records) == 1
        row = records[0]
        assert row["level"] == "critical"
        assert row["event"] == "Server error"
        assert row["error_code"] == "internal"
        assert "ValueError" in row["error.stack"]
        assert SECRET_PLACEHOLDER in row["error.message"]
        assert "hunter2" not in row["error.message"]
        assert "hunter2" not in row["error.stack"]

    @pytest.mark.asyncio
    async def test_includes_context_when_error_has_details(self) -> None:
        err = exc.not_found(
            "Document not found",
            details={"table": "users", "value": "a57cf97f-a50f-42eb-bdc6-502f8c7f18af"},
        )
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)

        assert response.status_code == 404
        assert response.headers.get(ERROR_CODE_HEADER) == "core.not_found"
        assert json.loads(response.body) == {
            "detail": "Document not found",
            "context": {
                "table": "users",
                "value": "a57cf97f-a50f-42eb-bdc6-502f8c7f18af",
            },
        }

    @pytest.mark.asyncio
    async def test_redacts_sensitive_keys_in_context(self) -> None:
        err = exc.validation(
            "Invalid input",
            details={"password": "hunter2", "field": "email"},
        )
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)

        assert response.status_code == 422
        body = json.loads(response.body)
        assert body["context"]["password"] == SECRET_PLACEHOLDER
        assert body["context"]["field"] == "email"

    @pytest.mark.asyncio
    async def test_omits_context_on_500(self) -> None:
        err = exc.infrastructure(
            "Database down",
            details={"dsn": "postgres://user:pass@localhost/db"},
        )
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await _forze_exception_handler(request, err)

        assert response.status_code == 500
        assert json.loads(response.body) == {"detail": "Database down"}


class TestRegisterExceptionHandlers:
    def test_registers_handler(self) -> None:
        app = FastAPI()

        @app.get("/raise")
        def raise_core_error() -> None:
            raise exc.not_found("Not found")

        register_exception_handlers(app)

        client = TestClient(app)
        response = client.get("/raise")
        assert response.status_code == 404
        assert response.json() == {"detail": "Not found"}
        assert response.headers.get(ERROR_CODE_HEADER) == "core.not_found"

    def test_unhandled_exception_returns_500_json(
        self,
        error_log_buf: io.StringIO,
    ) -> None:
        app = FastAPI()

        @app.get("/raise")
        def raise_unhandled() -> None:
            raise ValueError("Something broke")

        register_exception_handlers(app)

        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/raise")
        assert response.status_code == 500
        assert response.json() == {"detail": "Internal server error"}

        records = _json_records(error_log_buf)
        assert len(records) == 1
        row = records[0]
        assert row["level"] == "critical"
        assert row["event"] == "Unhandled exception"
        assert row["error.type"] == "ValueError"
        assert "ValueError" in row["error.stack"]
