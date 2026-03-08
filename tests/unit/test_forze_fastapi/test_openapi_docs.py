"""Unit tests for forze_fastapi.openapi.docs."""

from fastapi import FastAPI
from starlette.testclient import TestClient

from forze_fastapi.openapi import docs


# ----------------------- #


class TestIsValidDns:
    """Tests for _is_valid_dns."""

    def test_valid_dns_returns_true(self) -> None:
        """Valid DNS-like address returns True."""
        assert docs._is_valid_dns("api.example.com") is True
        assert docs._is_valid_dns("sub.domain.co.uk") is True

    def test_invalid_returns_false(self) -> None:
        """Invalid address returns False."""
        assert docs._is_valid_dns("localhost") is False
        assert docs._is_valid_dns("192.168.1.1") is False
        assert docs._is_valid_dns("") is False


class TestScalarDocs:
    """Tests for scalar_docs."""

    def test_returns_html_response(self) -> None:
        """scalar_docs returns HTML content when called via a route."""
        from starlette.requests import Request

        app = FastAPI(title="Test API")

        @app.get("/docs-internal", include_in_schema=False)
        def get_docs(request: Request):
            return docs.scalar_docs(request, title="Test")

        client = TestClient(app)
        response = client.get("/docs-internal")
        assert response.status_code == 200
        assert "text/html" in response.headers.get("content-type", "")


class TestRegisterScalarDocs:
    """Tests for register_scalar_docs."""

    def test_registers_docs_route(self) -> None:
        """register_scalar_docs adds a GET route at the given path."""
        app = FastAPI(title="Test API")
        docs.register_scalar_docs(app, path="/custom-docs")

        client = TestClient(app)
        response = client.get("/custom-docs")
        assert response.status_code == 200
        assert "text/html" in response.headers.get("content-type", "")

    def test_default_path_is_docs(self) -> None:
        """register_scalar_docs uses /docs by default."""
        app = FastAPI(title="Test API")
        docs.register_scalar_docs(app)

        client = TestClient(app)
        response = client.get("/docs")
        assert response.status_code == 200
