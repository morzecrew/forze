"""Unit tests for HTTP feature helpers (serialization)."""

from unittest.mock import MagicMock

import pytest
from fastapi import Response

from forze_fastapi.endpoints.http.features.utils import (
    response_from_endpoint_result,
    serialize_endpoint_result,
)


def test_serialize_endpoint_result_from_response_with_bytes_body() -> None:
    resp = Response(content=b"abc", media_type="application/pdf")
    body, ct = serialize_endpoint_result(resp, None)

    assert body == b"abc"
    assert ct == "application/pdf"


def test_serialize_endpoint_result_from_response_without_body() -> None:
    resp = MagicMock(spec=Response)
    resp.body = None
    resp.media_type = None
    resp.headers = {"content-type": "text/plain"}

    body, ct = serialize_endpoint_result(resp, None)

    assert body == b""
    assert ct == "text/plain"


def test_serialize_none_returns_empty_json() -> None:
    body, ct = serialize_endpoint_result(None, None)

    assert body == b""
    assert ct == "application/json"


def test_serialize_str_utf8() -> None:
    body, ct = serialize_endpoint_result("hi", None)

    assert body == b"hi"
    assert "text/plain" in ct


def test_response_from_endpoint_result_merges_extra_headers() -> None:
    inner = Response(content=b"x", media_type="application/octet-stream")
    out = response_from_endpoint_result(
        inner,
        response_model=None,
        status_code=200,
        extra_headers={"X-Trace": "1"},
    )

    assert out.headers["X-Trace"] == "1"
    assert out.body == b"x"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (b"bin", b"bin"),
        (bytearray(b"buf"), b"buf"),
    ],
)
def test_serialize_bytes_like(raw: bytes | bytearray, expected: bytes) -> None:
    body, ct = serialize_endpoint_result(raw, None)

    assert body == expected
    assert ct == "application/octet-stream"
