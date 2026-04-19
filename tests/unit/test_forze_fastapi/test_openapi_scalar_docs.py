"""Tests for :mod:`forze_fastapi.openapi.docs`."""

from unittest.mock import MagicMock

from starlette.requests import Request

from forze_fastapi.openapi.docs import _is_valid_dns, register_scalar_docs, scalar_docs


def test_is_valid_dns() -> None:
    assert _is_valid_dns("api.example.com") is True
    assert _is_valid_dns("not a dns") is False


def test_scalar_docs_without_forwarded_host() -> None:
    req = MagicMock(spec=Request)
    req.scope = {"root_path": "/api"}
    req.headers = {}
    html = scalar_docs(req, title="T")
    assert html.status_code == 200


def test_scalar_docs_with_forwarded_host_uses_https_for_dns_name() -> None:
    req = MagicMock(spec=Request)
    req.scope = {"root_path": "/v1"}
    req.headers = {"x-forwarded-host": "api.example.org"}
    html = scalar_docs(req)
    assert html.status_code == 200


def test_scalar_docs_relative_favicon_gets_prefixed() -> None:
    req = MagicMock(spec=Request)
    req.scope = {"root_path": "/app"}
    req.headers = {}
    html = scalar_docs(req, favicon_url="static/icon.svg")
    assert html.status_code == 200


def test_register_scalar_docs_adds_route() -> None:
    app = MagicMock()
    app.title = "App"
    register_scalar_docs(app, path="/reference")
    app.get.assert_called_once()
