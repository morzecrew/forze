"""Tests for BigQuery error normalization."""

from __future__ import annotations

from forze.base.exceptions import CoreException, ExceptionKind, exc
import pytest

pytest.importorskip("aiohttp")

from aiohttp import ClientResponseError, RequestInfo
from yarl import URL

from forze_bigquery.kernel.platform.errors import _bigquery_eh

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

class TestBigQueryErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = exc.internal("boom")
        assert _bigquery_eh(original, site="op") is original

    def test_not_found(self) -> None:
        r = _bigquery_eh(_client_error(404), site="get")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "not found" in r.summary.lower()
