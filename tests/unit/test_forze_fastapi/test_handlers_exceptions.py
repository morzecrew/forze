"""Unit tests for forze_fastapi.handlers.exceptions."""

import json

import pytest

from fastapi import FastAPI
from starlette.requests import Request
from starlette.testclient import TestClient

from forze.base.errors import (
    ConflictError,
    CoreError,
    NotFoundError,
    ValidationError,
)
from forze_fastapi.constants import ERROR_CODE_HEADER
from forze_fastapi.handlers.exceptions import (
    forze_exception_handler,
    register_exception_handlers,
)


# ----------------------- #


class TestForzeExceptionHandler:
    """Tests for forze_exception_handler."""

    @pytest.mark.asyncio
    async def test_not_found_returns_404(self) -> None:
        """NotFoundError maps to 404."""
        exc = NotFoundError(message="Document not found")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await forze_exception_handler(request, exc)
        assert response.status_code == 404
        assert response.body == b'{"detail":"Document not found"}'
        assert response.headers.get(ERROR_CODE_HEADER) == "not_found"

    @pytest.mark.asyncio
    async def test_conflict_returns_409(self) -> None:
        """ConflictError maps to 409."""
        exc = ConflictError(message="Revision mismatch")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await forze_exception_handler(request, exc)
        assert response.status_code == 409
        assert response.headers.get(ERROR_CODE_HEADER) == "conflict"

    @pytest.mark.asyncio
    async def test_validation_returns_422(self) -> None:
        """ValidationError maps to 422."""
        exc = ValidationError(message="Invalid input")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await forze_exception_handler(request, exc)
        assert response.status_code == 422
        assert response.headers.get(ERROR_CODE_HEADER) == "validation_error"

    @pytest.mark.asyncio
    async def test_unknown_core_error_returns_500(self) -> None:
        """Unmapped CoreError maps to 500."""
        exc = CoreError(message="Something went wrong", code="internal")
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await forze_exception_handler(request, exc)
        assert response.status_code == 500
        assert response.headers.get(ERROR_CODE_HEADER) == "internal"

    @pytest.mark.asyncio
    async def test_includes_context_when_error_has_details(self) -> None:
        """Responses include context payload when CoreError details are present."""
        exc = NotFoundError(
            message="Document not found",
            details={"table": "users", "value": "a57cf97f-a50f-42eb-bdc6-502f8c7f18af"},
        )
        request = Request(scope={"type": "http", "path": "/", "method": "GET"})
        response = await forze_exception_handler(request, exc)

        assert response.status_code == 404
        assert response.headers.get(ERROR_CODE_HEADER) == "not_found"
        assert json.loads(response.body) == {
            "detail": "Document not found",
            "context": {
                "table": "users",
                "value": "a57cf97f-a50f-42eb-bdc6-502f8c7f18af",
            },
        }


class TestRegisterExceptionHandlers:
    """Tests for register_exception_handlers."""

    def test_registers_handler(self) -> None:
        """register_exception_handlers wires CoreError to the app."""
        app = FastAPI()

        @app.get("/raise")
        def raise_core_error() -> None:
            raise NotFoundError(message="Not found")

        register_exception_handlers(app)

        client = TestClient(app)
        response = client.get("/raise")
        assert response.status_code == 404
        assert response.json() == {"detail": "Not found"}
        assert response.headers.get(ERROR_CODE_HEADER) == "not_found"
