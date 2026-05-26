"""Unit tests for forze_fastapi.exceptions."""

import json

import pytest
from fastapi import FastAPI
from starlette.requests import Request
from starlette.testclient import TestClient

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.scrubbing import SECRET_PLACEHOLDER
from forze_fastapi.exceptions import (
    ERROR_CODE_HEADER,
    _forze_exception_handler,
    register_exception_handlers,
)

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

    def test_unhandled_exception_returns_500(self) -> None:
        app = FastAPI()

        @app.get("/raise")
        def raise_unhandled() -> None:
            raise ValueError("Something broke")

        register_exception_handlers(app)

        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/raise")
        assert response.status_code == 500
        assert response.headers.get("content-type", "").startswith("text/plain")
        assert response.text == "Internal Server Error"
