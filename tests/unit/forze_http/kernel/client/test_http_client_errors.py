"""Tests for httpx error mapping."""

import httpx

from forze_http.kernel.client.errors import _httpx_eh

# ----------------------- #


def test_maps_404_to_not_found() -> None:
    request = httpx.Request("GET", "https://example.com/x")
    response = httpx.Response(404, request=request)
    err = httpx.HTTPStatusError("not found", request=request, response=response)

    mapped = _httpx_eh(err, site="http.test")

    assert mapped is not None
    assert mapped.code is not None
