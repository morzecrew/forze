"""Unit tests for RabbitMQ platform error mapping."""

import pytest
from aio_pika import exceptions as aio_pika_errors

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_rabbitmq.kernel.platform.errors import _rabbitmq_eh

# ----------------------- #


class TestRabbitmqErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = exc.internal("x")
        assert _rabbitmq_eh(original, site="op") is original

    def test_authentication_error(self) -> None:
        r = _rabbitmq_eh(aio_pika_errors.AuthenticationError("auth"), site="connect")
        assert r is not None
        assert r.kind == ExceptionKind.INFRASTRUCTURE
        assert "authentication" in r.summary.lower()

    def test_connection_error(self) -> None:
        r = _rabbitmq_eh(aio_pika_errors.AMQPConnectionError("conn"), site="connect")
        assert r is not None
        assert "connection" in r.summary.lower()

    def test_protocol_mismatch(self) -> None:
        r = _rabbitmq_eh(
            aio_pika_errors.IncompatibleProtocolError("proto"),
            site="connect",
        )
        assert r is not None
        assert "protocol" in r.summary.lower()

    def test_channel_invalid_state(self) -> None:
        r = _rabbitmq_eh(
            aio_pika_errors.ChannelInvalidStateError("state"),
            site="channel",
        )
        assert r is not None
        assert "invalid state" in r.summary.lower()

    def test_channel_error(self) -> None:
        r = _rabbitmq_eh(aio_pika_errors.AMQPChannelError("channel"), site="channel")
        assert r is not None
        assert "channel error" in r.summary.lower()

    def test_timeout(self) -> None:
        r = _rabbitmq_eh(TimeoutError("timeout"), site="op")
        assert r is not None
        assert "timed out" in r.summary.lower()

    def test_unknown_exception_fallback(self) -> None:
        r = _rabbitmq_eh(RuntimeError("boom"), site="rabbitmq.test")
        assert r is not None
        assert "rabbitmq.test" in r.summary.lower()
