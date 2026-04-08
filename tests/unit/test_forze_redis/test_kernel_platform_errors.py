"""Unit tests for Redis platform error mapping."""

import pytest
from redis import exceptions as redis_errors

from forze.base.errors import CoreError, InfrastructureError

from forze_redis.kernel.platform.errors import _redis_eh


def test_redis_eh_passes_through_core_error() -> None:
    err = CoreError("x")
    assert _redis_eh(err, "op") is err


@pytest.mark.parametrize(
    ("exc", "msg"),
    [
        (redis_errors.ConnectionError(), "Redis connection error."),
        (redis_errors.TimeoutError(), "Redis timeout."),
        (redis_errors.AuthenticationError(), "Redis authentication failed."),
        (redis_errors.BusyLoadingError(), "Redis is loading data, try again later."),
        (redis_errors.ReadOnlyError(), "Redis instance is read-only."),
        (redis_errors.DataError(), "Invalid Redis command arguments."),
    ],
)
def test_redis_eh_infra_cases(exc: Exception, msg: str) -> None:
    out = _redis_eh(exc, "ping")
    assert isinstance(out, InfrastructureError)
    assert out.code == msg


def test_redis_eh_response_error_wrongtype() -> None:
    exc = redis_errors.ResponseError("WRONGTYPE")
    out = _redis_eh(exc, "get")
    assert out.code == "Redis key has wrong type."


def test_redis_eh_response_error_busy() -> None:
    exc = redis_errors.ResponseError("BUSY")
    out = _redis_eh(exc, "x")
    assert out.code == "Redis resource is busy."


def test_redis_eh_response_error_generic() -> None:
    exc = redis_errors.ResponseError("other")
    out = _redis_eh(exc, "x")
    assert out.code == "Redis response error: other"


def test_redis_eh_unknown_exception() -> None:
    out = _redis_eh(RuntimeError("boom"), "custom_op")
    assert isinstance(out, InfrastructureError)
    assert "custom_op" in out.code
    assert "boom" in out.code
