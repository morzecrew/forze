"""Unit tests for the GCS / aiohttp error handler."""

import pytest

pytest.importorskip("aiohttp")

from aiohttp import ClientResponseError, RequestInfo
from yarl import URL

from forze.base.exceptions import InfrastructureError
from forze_gcs.kernel.platform.errors import _gcs_eh


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
        assert _gcs_eh(original, "op") is original

    def test_not_found(self) -> None:
        r = _gcs_eh(_client_error(404), "get")
        assert isinstance(r, InfrastructureError)
        assert "not found" in r.message.lower()

    def test_forbidden(self) -> None:
        r = _gcs_eh(_client_error(403), "get")
        assert isinstance(r, InfrastructureError)
        assert "access denied" in r.message.lower()

    def test_unauthorized(self) -> None:
        r = _gcs_eh(_client_error(401), "get")
        assert isinstance(r, InfrastructureError)
        assert "access denied" in r.message.lower()

    @pytest.mark.parametrize(
        ("status", "needle"),
        [
            (429, "throttl"),
            (500, "internal error"),
            (503, "internal error"),
        ],
    )
    def test_api_errors(self, status: int, needle: str) -> None:
        r = _gcs_eh(_client_error(status), "op")
        assert isinstance(r, InfrastructureError)
        assert needle in r.message.lower()

    def test_unknown_status(self) -> None:
        r = _gcs_eh(_client_error(418), "op")
        assert isinstance(r, InfrastructureError)
        assert "418" in r.message

    def test_generic_fallback(self) -> None:
        r = _gcs_eh(ValueError("nope"), "gcs_op")
        assert isinstance(r, InfrastructureError)
        assert "gcs_op" in r.message
