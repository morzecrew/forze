from datetime import timedelta
from typing import final

import attrs
from pydantic import SecretStr

from forze.base.exceptions import exc
from forze.base.serialization.pydantic import pydantic_secret_converter

# ----------------------- #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class KafkaConfig:
    """Connection and producer/consumer tuning shared by a Kafka client.

    Security is plaintext by default; set ``security_protocol`` (and the
    ``sasl_*`` fields for ``SASL_*`` protocols) for authenticated clusters.
    ``enable_auto_commit`` is deliberately absent — the offset-log consumer
    commits only after the inbox mark (see the commit-stream runner), so
    auto-commit is never used.
    """

    security_protocol: str = "PLAINTEXT"
    """``PLAINTEXT`` / ``SSL`` / ``SASL_PLAINTEXT`` / ``SASL_SSL``."""

    sasl_mechanism: str | None = None
    """SASL mechanism (e.g. ``PLAIN``, ``SCRAM-SHA-256``) for ``SASL_*`` protocols."""

    sasl_plain_username: str | None = None
    """SASL username for ``PLAIN`` / ``SCRAM`` mechanisms."""

    sasl_plain_password: SecretStr | None = attrs.field(
        default=None,
        converter=attrs.converters.optional(pydantic_secret_converter),
        repr=False,
    )
    """SASL password for ``PLAIN`` / ``SCRAM`` mechanisms (never logged)."""

    acks: str | int = "all"
    """Producer durability (``"all"`` = every in-sync replica). ``enable_idempotence``
    requires ``"all"``."""

    enable_idempotence: bool = True
    """Idempotent producer — de-duplicates retried produces at the broker."""

    compression_type: str | None = None
    """Producer compression (``gzip`` / ``snappy`` / ``lz4`` / ``zstd``), or ``None``."""

    linger_ms: int = 0
    """Producer batching linger, in milliseconds."""

    request_timeout: timedelta = timedelta(seconds=40)
    """Per-request timeout for produce / fetch / admin calls."""

    max_poll_records: int | None = None
    """Default cap on records returned by one consumer ``getmany`` (``None`` = adapter default)."""

    auto_offset_reset: str = "latest"
    """First-consume position when a group has no committed offset (``latest`` / ``earliest``)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.request_timeout.total_seconds() <= 0:
            raise exc.configuration("Kafka request_timeout must be positive")

        if self.linger_ms < 0:
            raise exc.configuration("Kafka linger_ms must be non-negative")

        if self.security_protocol.startswith("SASL_") and self.sasl_mechanism is None:
            raise exc.configuration("Kafka sasl_mechanism is required for SASL security protocols")

        if self.enable_idempotence and self.acks not in ("all", -1):
            raise exc.configuration("Kafka enable_idempotence=True requires acks='all' (or -1)")
