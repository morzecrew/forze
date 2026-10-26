"""Tests for httpx error mapping."""

from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest

from forze.base.exceptions import (
    ExceptionKind,
    exception_egress_policy,
    make_http_exception_mapper,
)
from forze_http.kernel.client.errors import _httpx_eh, exc_interceptor

# ----------------------- #


def _status_error(
    status: int, headers: dict[str, str | bytes] | None = None
) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://example.com/x")
    response = httpx.Response(status, request=request, headers=headers)

    return httpx.HTTPStatusError("rejected", request=request, response=response)


@pytest.mark.parametrize(
    ("status", "kind", "retryable"),
    [
        (302, ExceptionKind.INTERNAL, False),
        (400, ExceptionKind.INTERNAL, False),
        (401, ExceptionKind.INFRASTRUCTURE, True),
        (402, ExceptionKind.INTERNAL, False),
        (403, ExceptionKind.INFRASTRUCTURE, True),
        (404, ExceptionKind.NOT_FOUND, False),
        (408, ExceptionKind.INFRASTRUCTURE, True),
        (409, ExceptionKind.INTERNAL, False),
        (413, ExceptionKind.INTERNAL, False),
        (422, ExceptionKind.INTERNAL, False),
        (429, ExceptionKind.THROTTLED, True),
        (500, ExceptionKind.INFRASTRUCTURE, True),
        (503, ExceptionKind.INFRASTRUCTURE, True),
    ],
)
def test_a_status_maps_to_its_kind(status: int, kind: ExceptionKind, retryable: bool) -> None:
    mapped = exc_interceptor.mapper(_status_error(status), site="http.test")

    assert mapped is not None
    assert mapped.kind == kind
    assert exception_egress_policy(mapped.kind).retryable is retryable


def test_a_redirect_says_it_was_not_followed() -> None:
    mapped = exc_interceptor.mapper(_status_error(302), site="http.test")

    assert mapped is not None
    assert mapped.summary == "HTTP redirect not followed (302)."


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("7", 7.0),
        ("not a date", None),
        (None, None),
        # Read back as "²": `str.isdigit` accepts it, `float` does not.
        (b"\xb2", None),
        ("9" * 400, 3600.0),
        ("Fri, 31 Dec 9999 23:59:59 GMT", 3600.0),
        ("Thu, 01 Jan 1970 00:00:00 GMT", 0.0),
    ],
)
def test_a_throttle_carries_the_upstreams_retry_after(
    header: str | bytes | None, expected: float | None
) -> None:
    mapped = _httpx_eh(
        _status_error(429, {"Retry-After": header} if header is not None else None),
        site="http.test",
        details={"op": "list"},
    )

    assert mapped is not None
    assert mapped.details is not None
    assert mapped.details["op"] == "list"
    assert mapped.details.get("retry_after") == expected


def test_a_retry_after_date_becomes_seconds_from_now() -> None:
    when = format_datetime(datetime.now(UTC) + timedelta(seconds=60), usegmt=True)

    mapped = _httpx_eh(_status_error(429, {"Retry-After": when}), site="http.test")

    assert mapped is not None and mapped.details is not None
    assert 50 <= mapped.details["retry_after"] <= 60


@pytest.mark.parametrize(
    ("status", "kind"),
    [
        (401, ExceptionKind.INFRASTRUCTURE),
        (403, ExceptionKind.INFRASTRUCTURE),
        (404, ExceptionKind.INFRASTRUCTURE),
        (429, ExceptionKind.THROTTLED),
        (500, ExceptionKind.INFRASTRUCTURE),
    ],
)
def test_the_shared_http_mapper_maps_a_status_to_its_kind(status: int, kind: ExceptionKind) -> None:
    class ResponseError(Exception):
        def __init__(self, status: int) -> None:
            self.status = status

    mapper = make_http_exception_mapper(
        label="Store",
        response_error_type=ResponseError,
        http_status_message=lambda s: f"Store failed ({s}).",
    )

    mapped = mapper(ResponseError(status), site="store.get")

    assert mapped is not None
    assert mapped.kind == kind


def test_maps_404_to_not_found() -> None:
    request = httpx.Request("GET", "https://example.com/x")
    response = httpx.Response(404, request=request)
    err = httpx.HTTPStatusError("not found", request=request, response=response)

    mapped = _httpx_eh(err, site="http.test")

    assert mapped is not None
    assert mapped.code is not None


def test_catch_all_keeps_driver_error_out_of_summary() -> None:
    mapped = exc_interceptor.mapper(
        RuntimeError("driver internals: token=fixture-7chr"),
        site="http.test",
    )

    assert mapped is not None
    assert mapped.summary == "An error occurred during HTTP operation http.test."
    assert "driver internals" not in mapped.summary
    assert mapped.details is not None
    assert mapped.details["error"] == "driver internals: token=fixture-7chr"


def test_catch_all_preserves_existing_details() -> None:
    mapped = exc_interceptor.mapper(
        RuntimeError("boom"),
        site="http.test",
        details={"endpoint": "billing"},
    )

    assert mapped is not None
    assert mapped.details is not None
    assert mapped.details["endpoint"] == "billing"
    assert mapped.details["error"] == "boom"


def test_assembled_chain_reaches_httpx_mapper() -> None:
    """Regression: the package mapper must be reachable through the chain
    wired into ``exc_interceptor`` (nested default chain used to shadow it)."""

    from forze.base.exceptions import ExceptionKind
    from forze_http.kernel.client.errors import exc_interceptor

    request = httpx.Request("GET", "https://example.com/x")
    response = httpx.Response(404, request=request)
    err = httpx.HTTPStatusError("not found", request=request, response=response)

    out = exc_interceptor.mapper(err, site="http.test")

    assert out is not None
    assert out.kind == ExceptionKind.NOT_FOUND
    assert out.code != "core.unhandled"
