"""Unit tests for the GCS / aiohttp error handler."""

import pytest

pytest.importorskip("aiohttp")

from aiohttp import ClientResponseError, RequestInfo
from yarl import URL

from forze.base.exceptions import ExceptionKind, exc
from forze_gcs.kernel.client.errors import _gcs_eh, exc_interceptor

# ----------------------- #


def _client_error(
    status: int,
    url: str = "http://example.com/storage/v1/b/bkt/o/key",
) -> ClientResponseError:
    request_info = RequestInfo(
        url=URL(url),
        method="GET",
        headers={},
        real_url=URL(url),
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
        assert exc_interceptor.mapper(original, site="op") is original

    def test_object_404_is_a_caller_miss(self) -> None:
        # A caller miss, not downstream ill health: no retries, no breaker
        # failure, a 404 at the edge — and the mock/real kinds agree.
        r = _gcs_eh(_client_error(404), site="get")
        assert r is not None
        assert r.kind == ExceptionKind.NOT_FOUND
        assert "not found" in r.summary.lower()

    @pytest.mark.parametrize(
        "url",
        [
            # Bucket-level and upload URLs 404 on a missing/unavailable
            # *bucket* — a deployment fault, never a deleted-object race.
            "http://example.com/storage/v1/b/bkt",
            "http://example.com/upload/storage/v1/b/bkt/o?uploadType=media",
            "http://example.com",
        ],
    )
    def test_bucket_level_404_stays_infrastructure(self, url: str) -> None:
        r = _gcs_eh(_client_error(404, url=url), site="ensure")
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
        r = exc_interceptor.mapper(ValueError("nope"), site="gcs_op")
        assert r is not None
        assert "gcs_op" in r.summary
        # raw driver text must not leak into the summary, only into details
        assert "nope" not in r.summary
        assert r.details is not None
        assert r.details["error"] == "nope"


class TestAssembledChain:
    """Regression: the package mapper must be reachable through the chain
    wired into ``exc_interceptor`` (nested default chain used to shadow it)."""

    def test_http_404_through_assembled_chain(self) -> None:
        from forze_gcs.kernel.client.errors import exc_interceptor

        out = exc_interceptor.mapper(_client_error(404), site="get")
        assert out is not None
        assert out.kind == ExceptionKind.NOT_FOUND
        assert out.code != "core.unhandled"
        assert "not found" in out.summary.lower()
