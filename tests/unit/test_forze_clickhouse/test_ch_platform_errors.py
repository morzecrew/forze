"""Tests for ClickHouse error normalization."""

from __future__ import annotations

from forze.base.exceptions import CoreException, ExceptionKind, exc
import pytest

pytest.importorskip("aiohttp")

from aiohttp import ClientResponseError, RequestInfo
from yarl import URL

from forze_clickhouse.kernel.platform.errors import _clickhouse_eh

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

    def test_authentication_message_fallback(self) -> None:
        r = _clickhouse_eh(RuntimeError("authentication failed"), site="query")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "access denied" in r.summary.lower()
