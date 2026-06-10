"""Unit tests for the GCS / aiohttp error handler."""

import pytest

pytest.importorskip("aiohttp")

from aiohttp import ClientResponseError, RequestInfo
from yarl import URL

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_gcs.kernel.client.errors import _gcs_eh

# ----------------------- #


def _client_error(status: int) -> ClientResponseError:
    request_info = RequestInfo(
        url=URL("http://example.com"),
        method="GET",
        headers={},
        real_url=URL("http://example.com"),
    )
    return ClientResponseError(
        request_info=request_info,
        history=(),
        status=status,
        message="error",
        headers={},
    )


class TestGCSErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = exc.internal("x")
        assert _gcs_eh(original, site="op") is original

    def test_not_found(self) -> None:
        r = _gcs_eh(_client_error(404), site="get")
        assert r is not None
        assert r.kind == ExceptionKind.INFRASTRUCTURE
        assert "not found" in r.summary.lower()

    def test_forbidden(self) -> None:
        r = _gcs_eh(_client_error(403), site="get")
        assert r is not None
        assert "access denied" in r.summary.lower()

    def test_unauthorized(self) -> None:
        r = _gcs_eh(_client_error(401), site="get")
        assert r is not None
        assert "access denied" in r.summary.lower()

    @pytest.mark.parametrize(
        ("status", "needle"),
        [
            (429, "throttl"),
            (500, "internal error"),
            (503, "internal error"),
        ],
    )
    def test_api_errors(self, status: int, needle: str) -> None:
        r = _gcs_eh(_client_error(status), site="op")
        assert r is not None
        assert needle in r.summary.lower()

    def test_unknown_status(self) -> None:
        r = _gcs_eh(_client_error(418), site="op")
        assert r is not None
        assert "418" in r.summary

    def test_generic_fallback(self) -> None:
        r = _gcs_eh(ValueError("nope"), site="gcs_op")
        assert r is not None
        assert "gcs_op" in r.summary
        # raw driver text must not leak into the summary, only into details
        assert "nope" not in r.summary
        assert r.details is not None
        assert r.details["error"] == "nope"
