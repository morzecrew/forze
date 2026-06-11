"""Unit tests for :mod:`forze_redis.kernel.client.errors`."""

from forze.base.exceptions import CoreException, ExceptionKind, exc
import pytest

pytest.importorskip("redis")

from redis import exceptions as redis_errors

from forze_redis.kernel.client.errors import _redis_eh

class TestRedisErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = exc.internal("x")
        assert _redis_eh(original, site="op") is original

    @pytest.mark.parametrize(
        ("error", "needle"),
        [
            (redis_errors.ConnectionError(), "connection"),
            (redis_errors.TimeoutError(), "timeout"),
            (redis_errors.AuthenticationError(), "authentication"),
            (redis_errors.BusyLoadingError(), "loading"),
            (redis_errors.ReadOnlyError(), "read-only"),
            (redis_errors.DataError(), "arguments"),
        ],
    )
    def test_infrastructure_errors(self, error: BaseException, needle: str) -> None:
        r = _redis_eh(error, site="op")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert needle in r.summary

    @pytest.mark.parametrize(
        ("message", "needle"),
        [
            ("WRONGTYPE ...", "wrong type"),
            ("BUSY ...", "busy"),
            ("something else", "response error"),
        ],
    )
    def test_response_errors(self, message: str, needle: str) -> None:
        r = _redis_eh(redis_errors.ResponseError(message), site="op")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert needle in r.summary

    def test_unknown_exception_fallback(self) -> None:
        r = _redis_eh(RuntimeError("boom"), site="my_op")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "my_op" in r.summary


class TestAssembledChain:
    """Regression: the package mapper must be reachable through the chain
    wired into ``exc_interceptor`` (nested default chain used to shadow it)."""

    def test_connection_error_through_assembled_chain(self) -> None:
        from forze_redis.kernel.client.errors import exc_interceptor

        out = exc_interceptor.mapper(redis_errors.ConnectionError(), site="get")
        assert out is not None
        assert out.kind == ExceptionKind.INFRASTRUCTURE
        assert out.code != "core.unhandled"
        assert "connection" in out.summary.lower()
