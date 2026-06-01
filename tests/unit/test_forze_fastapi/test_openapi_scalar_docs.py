"""Tests for :mod:`forze_fastapi.docs`."""

from unittest.mock import MagicMock, patch

from starlette.requests import Request

from forze_fastapi.docs import (
    _is_valid_dns,
    _scalar_servers_from_request,
    register_scalar_docs,
    scalar_docs,
)


def test_is_valid_dns() -> None:
    assert _is_valid_dns("api.example.com") is True
    assert _is_valid_dns("not a dns") is False


def test_scalar_docs_without_forwarded_host() -> None:
    req = MagicMock(spec=Request)
    req.scope = {"root_path": "/api"}
    req.headers = {}
    html = scalar_docs(req, title="T")
    assert html.status_code == 200


@patch("forze_fastapi.docs.get_scalar_api_reference")
def test_scalar_docs_ignores_forwarded_host_by_default(mock_scalar) -> None:
    mock_scalar.return_value = MagicMock(status_code=200)
    req = MagicMock(spec=Request)
    req.scope = {"root_path": "/v1"}
    req.headers = {"x-forwarded-host": "api.example.org"}

    scalar_docs(req)

    assert mock_scalar.call_args.kwargs["servers"] == []
    assert mock_scalar.call_args.kwargs["persist_auth"] is False


@patch("forze_fastapi.docs.get_scalar_api_reference")
def test_scalar_docs_trust_forwarded_host_uses_https_for_dns_name(mock_scalar) -> None:
    mock_scalar.return_value = MagicMock(status_code=200)
    req = MagicMock(spec=Request)
    req.scope = {"root_path": "/v1"}
    req.headers = {"x-forwarded-host": "api.example.org"}

    scalar_docs(req, trust_forwarded_host=True)

    assert mock_scalar.call_args.kwargs["servers"] == [
        {"url": "https://api.example.org/v1"}
    ]


@patch("forze_fastapi.docs.get_scalar_api_reference")
def test_scalar_docs_trust_forwarded_proto_header(mock_scalar) -> None:
    mock_scalar.return_value = MagicMock(status_code=200)
    req = MagicMock(spec=Request)
    req.scope = {"root_path": ""}
    req.headers = {
        "x-forwarded-host": "api.example.org",
        "x-forwarded-proto": "https",
    }

    scalar_docs(req, trust_forwarded_host=True)

    assert mock_scalar.call_args.kwargs["servers"] == [
        {"url": "https://api.example.org"}
    ]


@patch("forze_fastapi.docs.get_scalar_api_reference")
def test_scalar_docs_persist_auth_opt_in(mock_scalar) -> None:
    mock_scalar.return_value = MagicMock(status_code=200)
    req = MagicMock(spec=Request)
    req.scope = {"root_path": ""}
    req.headers = {}

    scalar_docs(req, persist_auth=True)

    assert mock_scalar.call_args.kwargs["persist_auth"] is True


def test_scalar_servers_from_request_untrusted_returns_empty() -> None:
    req = MagicMock(spec=Request)
    req.scope = {"root_path": "/api"}
    req.headers = {"x-forwarded-host": "evil.example.org"}

    assert _scalar_servers_from_request(req, trust_forwarded_host=False) == []


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
