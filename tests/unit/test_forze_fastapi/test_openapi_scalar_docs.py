"""Tests for :mod:`forze_fastapi.docs`."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request

from forze.base.exceptions import CoreException
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


@patch("forze_fastapi.docs.get_scalar_api_reference")
def test_scalar_docs_prefixes_a_document_path_with_root_path(mock_scalar) -> None:
    mock_scalar.return_value = MagicMock(status_code=200)
    req = MagicMock(spec=Request)
    req.scope = {"root_path": "/api"}
    req.headers = {}

    scalar_docs(req, openapi_url="/v1/openapi.json")

    assert mock_scalar.call_args.kwargs["openapi_url"] == "/api/v1/openapi.json"


@patch("forze_fastapi.docs.get_scalar_api_reference")
def test_scalar_docs_uses_an_absolute_document_url_as_given(mock_scalar) -> None:
    mock_scalar.return_value = MagicMock(status_code=200)
    req = MagicMock(spec=Request)
    req.scope = {"root_path": "/api"}
    req.headers = {}

    scalar_docs(req, openapi_url="https://specs.example.org/api.json")

    assert mock_scalar.call_args.kwargs["openapi_url"] == "https://specs.example.org/api.json"


def test_register_scalar_docs_points_at_the_apps_openapi_url() -> None:
    # The page fetched /openapi.json whatever the app served, so it rendered nothing.
    app = FastAPI(openapi_url="/v1/openapi.json", docs_url=None)
    register_scalar_docs(app)

    page = TestClient(app).get("/docs").text

    # docs_url=None: FastAPI's own Swagger page would otherwise answer /docs.
    assert "scalar" in page.lower()
    assert "/v1/openapi.json" in page
    assert '"/openapi.json"' not in page


def test_register_scalar_docs_takes_an_explicit_openapi_url() -> None:
    app = FastAPI(openapi_url=None)
    register_scalar_docs(app, openapi_url="/specs/public.json")

    assert "/specs/public.json" in TestClient(app).get("/docs").text


def test_register_scalar_docs_refuses_an_app_serving_no_document() -> None:
    with pytest.raises(CoreException) as ei:
        register_scalar_docs(FastAPI(openapi_url=None))

    assert ei.value.code == "scalar_docs_no_openapi"


def test_register_scalar_docs_keeps_the_default_document_url() -> None:
    app = FastAPI(docs_url=None)
    register_scalar_docs(app)

    assert '"/openapi.json"' in TestClient(app).get("/docs").text


def test_register_scalar_docs_prefixes_root_path_end_to_end() -> None:
    app = FastAPI(openapi_url="/v1/openapi.json", root_path="/api", docs_url=None)
    register_scalar_docs(app)

    assert '"/api/v1/openapi.json"' in TestClient(app).get("/docs").text


def test_register_scalar_docs_refuses_an_empty_openapi_url() -> None:
    # FastAPI serves no document for a falsy openapi_url, so the page would be blank.
    with pytest.raises(CoreException):
        register_scalar_docs(FastAPI(openapi_url=""))


@pytest.mark.parametrize(
    ("openapi_url", "root_path", "expected"),
    [
        ("/openapi.json?src=https://x", "/api", "/api/openapi.json?src=https://x"),
        ("//specs.example.org/api.json", "/api", "//specs.example.org/api.json"),
        ("openapi.json", "/api/", "/api/openapi.json"),
        ('/x"</script><script>alert(1)//', "", "/x%22%3C/script%3E%3Cscript%3Ealert(1)//"),
    ],
)
@patch("forze_fastapi.docs.get_scalar_api_reference")
def test_scalar_docs_document_url_rules(
    mock_scalar, openapi_url: str, root_path: str, expected: str
) -> None:
    mock_scalar.return_value = MagicMock(status_code=200)
    req = MagicMock(spec=Request)
    req.scope = {"root_path": root_path}
    req.headers = {}

    scalar_docs(req, openapi_url=openapi_url)

    assert mock_scalar.call_args.kwargs["openapi_url"] == expected
