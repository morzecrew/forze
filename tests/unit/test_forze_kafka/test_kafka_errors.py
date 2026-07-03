"""Kafka platform error mapping — the pure mapper against real aiokafka errors."""

from aiokafka import errors as kafka_errors

from forze.base.exceptions import ExceptionKind, exc
from forze_kafka.kernel.client.errors import _kafka_eh, exc_interceptor

# ----------------------- #


class TestKafkaErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = exc.internal("x")
        assert exc_interceptor.mapper(original, site="op") is original

    def test_topic_authorization(self) -> None:
        r = _kafka_eh(
            kafka_errors.TopicAuthorizationFailedError("nope"), site="produce"
        )
        assert r is not None
        assert r.kind == ExceptionKind.INFRASTRUCTURE
        assert "authorization" in r.summary.lower()

    def test_group_authorization(self) -> None:
        r = _kafka_eh(
            kafka_errors.GroupAuthorizationFailedError("nope"), site="consume"
        )
        assert r is not None
        assert "authorization" in r.summary.lower()

    def test_connection_error(self) -> None:
        r = _kafka_eh(kafka_errors.KafkaConnectionError("conn"), site="connect")
        assert r is not None
        assert r.kind == ExceptionKind.INFRASTRUCTURE
        assert "connection" in r.summary.lower()

    def test_node_not_ready(self) -> None:
        r = _kafka_eh(kafka_errors.NodeNotReadyError("wait"), site="connect")
        assert r is not None
        assert "not ready" in r.summary.lower()

    def test_coordinator_not_available(self) -> None:
        r = _kafka_eh(
            kafka_errors.GroupCoordinatorNotAvailableError("wait"), site="commit"
        )
        assert r is not None
        assert "not ready" in r.summary.lower()

    def test_producer_closed(self) -> None:
        r = _kafka_eh(kafka_errors.ProducerClosed("closed"), site="produce")
        assert r is not None
        assert "closed" in r.summary.lower()

    def test_consumer_stopped(self) -> None:
        r = _kafka_eh(kafka_errors.ConsumerStoppedError("stopped"), site="consume")
        assert r is not None
        assert "closed" in r.summary.lower()

    def test_kafka_timeout(self) -> None:
        r = _kafka_eh(kafka_errors.KafkaTimeoutError("slow"), site="produce")
        assert r is not None
        assert "timed out" in r.summary.lower()

    def test_builtin_timeout(self) -> None:
        r = _kafka_eh(TimeoutError("slow"), site="produce")
        assert r is not None
        assert "timed out" in r.summary.lower()

    def test_generic_kafka_error(self) -> None:
        r = _kafka_eh(kafka_errors.KafkaError("weird"), site="op")
        assert r is not None
        assert r.kind == ExceptionKind.INFRASTRUCTURE

    def test_non_kafka_defers(self) -> None:
        # The arm returns None so the chain falls through to the fallback.
        assert _kafka_eh(ValueError("not kafka"), site="op") is None

    def test_unknown_exception_fallback_via_chain(self) -> None:
        r = exc_interceptor.mapper(RuntimeError("boom"), site="kafka.test")
        assert r is not None
        assert r.kind == ExceptionKind.INFRASTRUCTURE
        assert "kafka.test" in r.summary.lower()
        # raw driver text must not leak into the summary, only into details
        assert "boom" not in r.summary
        assert r.details is not None
        assert r.details["error"] == "boom"

    def test_connection_error_through_assembled_chain(self) -> None:
        out = exc_interceptor.mapper(
            kafka_errors.KafkaConnectionError("conn"), site="connect"
        )
        assert out is not None
        assert out.kind == ExceptionKind.INFRASTRUCTURE
        assert out.code != "core.unhandled"
        assert "connection" in out.summary.lower()
