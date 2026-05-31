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

    def test_connection_error(self) -> None:
        r = _redis_eh(redis_errors.ConnectionError(), site="get")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "connection" in r.summary

    def test_timeout_error(self) -> None:
        r = _redis_eh(redis_errors.TimeoutError(), site="get")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "timeout" in r.summary

    def test_authentication_error(self) -> None:
        r = _redis_eh(redis_errors.AuthenticationError(), site="auth")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "authentication" in r.summary

    def test_busy_loading_error(self) -> None:
        r = _redis_eh(redis_errors.BusyLoadingError(), site="get")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "loading" in r.summary

    def test_read_only_error(self) -> None:
        r = _redis_eh(redis_errors.ReadOnlyError(), site="set")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "read-only" in r.summary

    def test_data_error(self) -> None:
        r = _redis_eh(redis_errors.DataError(), site="cmd")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "arguments" in r.summary

    def test_response_error_wrongtype(self) -> None:
        r = _redis_eh(redis_errors.ResponseError("WRONGTYPE ..."), site="get")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "wrong type" in r.summary

    def test_response_error_busy(self) -> None:
        r = _redis_eh(redis_errors.ResponseError("BUSY ..."), site="x")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "busy" in r.summary

    def test_response_error_generic(self) -> None:
        r = _redis_eh(redis_errors.ResponseError("something else"), site="x")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "response error" in r.summary

    def test_unknown_exception_fallback(self) -> None:
        r = _redis_eh(RuntimeError("boom"), site="my_op")
        assert isinstance(r, CoreException) and r.kind == ExceptionKind.INFRASTRUCTURE
        assert "my_op" in r.summary
