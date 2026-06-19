"""Tests for ClickHouse error normalization."""

from __future__ import annotations

from forze.base.exceptions import CoreException, ExceptionKind, exc
import pytest

pytest.importorskip("aiohttp")

from aiohttp import ClientResponseError, RequestInfo
from yarl import URL

from forze_clickhouse.kernel.client.errors import _clickhouse_eh

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

class TestClickHouseErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = exc.internal("boom")
        assert _clickhouse_eh(original, site="op") is original

    def test_not_found(self) -> None:
        r = _clickhouse_eh(_client_error(404), site="get")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "not found" in r.summary.lower()

    def test_access_denied_status(self) -> None:
        r = _clickhouse_eh(_client_error(403), site="query")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "access denied" in r.summary.lower()

    def test_throttled(self) -> None:
        r = _clickhouse_eh(_client_error(429), site="query")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "throttled" in r.summary.lower()

    def test_generic_client_error_status(self) -> None:
        r = _clickhouse_eh(_client_error(500), site="query")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "500" in r.summary

    def test_authentication_code_maps_to_access_denied(self) -> None:
        # Real ClickHouse auth errors carry a numeric code: 516 / 497.
        err = RuntimeError("Code: 516. DB::Exception: default: Authentication failed")
        r = _clickhouse_eh(err, site="query")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "access denied" in r.summary.lower()

    def test_access_denied_code_maps_to_access_denied(self) -> None:
        err = RuntimeError("Code: 497. DB::Exception: user is not allowed")
        r = _clickhouse_eh(err, site="query")
        assert "access denied" in r.summary.lower()

    def test_message_mentioning_password_without_code_does_not_misfire(self) -> None:
        # A non-auth error whose text merely contains "password" must not be
        # classified as access denied.
        err = RuntimeError("Code: 47. DB::Exception: Unknown column 'password'")
        r = _clickhouse_eh(err, site="query")
        assert "access denied" not in r.summary.lower()


class TestAssembledChain:
    """Regression: the package mapper must be reachable through the chain
    wired into ``exc_interceptor`` (nested default chain used to shadow it)."""

    def test_http_404_through_assembled_chain(self) -> None:
        from forze_clickhouse.kernel.client.errors import exc_interceptor

        out = exc_interceptor.mapper(_client_error(404), site="query")
        assert out is not None
        assert out.kind == ExceptionKind.INFRASTRUCTURE
        assert out.code != "core.unhandled"
        assert "not found" in out.summary.lower()
