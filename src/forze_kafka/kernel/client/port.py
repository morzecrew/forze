"""Structural protocol for Kafka clients (single bootstrap or tenant-routed)."""

from collections.abc import Awaitable, Mapping, Sequence
from typing import Protocol

from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import RecordMetadata

# ----------------------- #


class KafkaClientPort(Protocol):
    """Operations implemented by :class:`KafkaClient` and routed variants.

    Backend-neutral in intent (an alternate high-throughput driver could
    implement it), though the handles it hands back are ``aiokafka`` types today
    — the same pattern as the RabbitMQ client exposing an ``aio_pika`` channel.
    """

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]: ...  # pragma: no cover

    def send(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | None = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
        timestamp_ms: int | None = None,
    ) -> Awaitable[RecordMetadata]: ...  # pragma: no cover

    def get_consumer(
        self,
        *,
        group: str,
        member: str,
        topics: Sequence[str],
        auto_offset_reset: str | None = None,
        max_poll_records: int | None = None,
        listener: ConsumerRebalanceListener | None = None,
    ) -> Awaitable[AIOKafkaConsumer]:
        """Return a started, pooled consumer for ``(group, member, topics)``.

        When a new consumer is created, *listener* is registered on its
        subscription so partition revocation / assignment during a rebalance is
        observed (offset-log consumers rewind to committed on assign and drop
        stale routing on revoke). A pooled cache hit ignores *listener* — the
        consumer keeps the listener it was first subscribed with.
        """
        ...  # pragma: no cover

    def new_transient_consumer(
        self,
        *,
        group: str | None = None,
    ) -> Awaitable[AIOKafkaConsumer]:
        """Return a fresh, started consumer the caller must ``stop()`` (admin/replay)."""
        ...  # pragma: no cover

    def admin(self) -> Awaitable[AIOKafkaAdminClient]: ...  # pragma: no cover

    def group_config(self) -> Mapping[str, object]:
        """Return default consumer knobs (e.g. ``auto_offset_reset``)."""
        ...  # pragma: no cover
