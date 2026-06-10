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


def test_catch_all_keeps_driver_error_out_of_summary() -> None:
    mapped = _httpx_eh(
        RuntimeError("driver internals: token=hunter2"),
        site="http.test",
    )

    assert mapped is not None
    assert mapped.summary == "An error occurred during HTTP operation http.test."
    assert "driver internals" not in mapped.summary
    assert mapped.details is not None
    assert mapped.details["error"] == "driver internals: token=hunter2"


def test_catch_all_preserves_existing_details() -> None:
    mapped = _httpx_eh(
        RuntimeError("boom"),
        site="http.test",
        details={"endpoint": "billing"},
    )

    assert mapped is not None
    assert mapped.details is not None
    assert mapped.details["endpoint"] == "billing"
    assert mapped.details["error"] == "boom"
