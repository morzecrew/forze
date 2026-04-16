"""Unit tests for the Redis error handler."""

import pytest

pytest.importorskip("redis")

from redis import exceptions as redis_errors

from forze.base.errors import CoreError, InfrastructureError
from forze_redis.kernel.platform.errors import _redis_eh


class TestRedisErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = CoreError("x")
        assert _redis_eh(original, "op") is original

    def test_connection_error(self) -> None:
        r = _redis_eh(redis_errors.ConnectionError(), "get")
        assert isinstance(r, InfrastructureError)
        assert "connection" in r.code.lower()

    def test_timeout_error(self) -> None:
        r = _redis_eh(redis_errors.TimeoutError(), "get")
        assert isinstance(r, InfrastructureError)
        assert "timeout" in r.code.lower()

    def test_authentication_error(self) -> None:
        r = _redis_eh(redis_errors.AuthenticationError(), "auth")
        assert isinstance(r, InfrastructureError)
        assert "authentication" in r.code.lower()

    def test_busy_loading_error(self) -> None:
        r = _redis_eh(redis_errors.BusyLoadingError(), "get")
        assert isinstance(r, InfrastructureError)
        assert "loading" in r.code.lower()

    def test_read_only_error(self) -> None:
        r = _redis_eh(redis_errors.ReadOnlyError(), "set")
        assert isinstance(r, InfrastructureError)
        assert "read-only" in r.code.lower()

    def test_data_error(self) -> None:
        r = _redis_eh(redis_errors.DataError(), "cmd")
        assert isinstance(r, InfrastructureError)
        assert "arguments" in r.code.lower()

    def test_response_error_wrongtype(self) -> None:
        r = _redis_eh(redis_errors.ResponseError("WRONGTYPE ..."), "get")
        assert isinstance(r, InfrastructureError)
        assert "wrong type" in r.code.lower()

    def test_response_error_busy(self) -> None:
        r = _redis_eh(redis_errors.ResponseError("BUSY ..."), "x")
        assert isinstance(r, InfrastructureError)
        assert "busy" in r.code.lower()

    def test_response_error_generic(self) -> None:
        r = _redis_eh(redis_errors.ResponseError("something else"), "x")
        assert isinstance(r, InfrastructureError)
        assert "response error" in r.code.lower()

    def test_unknown_exception_fallback(self) -> None:
        r = _redis_eh(RuntimeError("boom"), "my_op")
        assert isinstance(r, InfrastructureError)
        assert "my_op" in r.code
