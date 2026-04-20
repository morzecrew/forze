"""Unit tests for :class:`~forze_rabbitmq.kernel.platform.client.RabbitMQClient` helpers (no broker)."""

from datetime import UTC, datetime, timezone

import pytest

pytest.importorskip("aio_pika")

from forze_rabbitmq.kernel.platform.client import RabbitMQClient


# ----------------------- #


class TestRabbitMQClientExtractors:
    def test_extract_key_none_headers(self) -> None:
        assert RabbitMQClient._RabbitMQClient__extract_key(None) is None

    def test_extract_key_bytes_and_str(self) -> None:
        assert (
            RabbitMQClient._RabbitMQClient__extract_key({"forze_key": b"k"}) == "k"
        )
        assert RabbitMQClient._RabbitMQClient__extract_key({"forze_key": "k"}) == "k"

    def test_extract_key_unsupported_type(self) -> None:
        assert (
            RabbitMQClient._RabbitMQClient__extract_key({"forze_key": 99}) is None
        )

    def test_extract_timestamp_datetime(self) -> None:
        dt = datetime(2024, 1, 1, tzinfo=UTC)
        assert RabbitMQClient._RabbitMQClient__extract_timestamp(dt) is dt

    def test_extract_timestamp_numeric_unix(self) -> None:
        got = RabbitMQClient._RabbitMQClient__extract_timestamp(1_700_000_000.0)
        assert got == datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)

    def test_extract_timestamp_other(self) -> None:
        assert RabbitMQClient._RabbitMQClient__extract_timestamp("nope") is None
