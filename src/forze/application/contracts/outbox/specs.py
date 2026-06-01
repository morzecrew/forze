"""Outbox and integration-event specifications."""

from typing import Any, final

import attrs

from forze.base.primitives import StrKey
from forze.base.serialization import RecordMappingCodec

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OutboxDestination:
    """Relay target for staged integration events."""

    queue_route: StrKey
    """Route name of the registered :class:`~forze.application.contracts.queue.QueueSpec`."""

    queue: str
    """Logical queue channel passed to :meth:`~forze.application.contracts.queue.QueueCommandPort.enqueue`."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OutboxSpec[M](BaseSpec):
    """Specification binding an outbox route to its integration-event payload codec."""

    codec: RecordMappingCodec[M, Any]
    """Payload record codec for staged integration events."""

    destination: OutboxDestination | None = None
    """Optional default relay target for :func:`~forze_kits.outbox.relay_outbox_to_queue`."""

    # ....................... #

    @property
    def model_type(self) -> type[M]:
        """Payload model type carried by :attr:`codec`."""

        return self.codec.model_type
