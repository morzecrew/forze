"""Tests for ClickHouse error normalization."""

from __future__ import annotations

import pytest

pytest.importorskip("aiohttp")

from aiohttp import ClientResponseError, RequestInfo
from yarl import URL

from forze.base.errors import CoreError, InfrastructureError
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
        original = CoreError("boom")
        assert _clickhouse_eh(original, "op") is original

    def test_not_found(self) -> None:
        r = _clickhouse_eh(_client_error(404), "get")
        assert isinstance(r, InfrastructureError)
        assert "not found" in r.message.lower()
