"""Background wiring for durable realtime signals — relay the outbox to the stream.

Durable signals staged via :meth:`RealtimePublisher.stage` land in the outbox.
This step relays them to the realtime stream after commit, where the gateway
consumes them. A thin convenience over the generic
:func:`outbox_relay_background_lifecycle_step`.
"""

from datetime import timedelta
from typing import Any

from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.stream import StreamSpec
from forze.base.primitives import StrKey

from forze_kits.integrations.outbox import outbox_relay_background_lifecycle_step

# ----------------------- #


def realtime_relay_lifecycle_step(
    *,
    outbox_spec: OutboxSpec[Any],
    stream_spec: StreamSpec[Any],
    interval: timedelta = timedelta(seconds=2),
    step_id: StrKey = "realtime_relay",
) -> LifecycleStep:
    """Relay durable realtime signals from the outbox to the realtime stream.

    Run alongside the gateway: ``stage`` → outbox → (this relay) → stream →
    gateway → emit. The signals carry the outbox event id, so the gateway dedupes
    them for exactly-once delivery.
    """

    return outbox_relay_background_lifecycle_step(
        outbox_spec=outbox_spec,
        transport="stream",
        stream_spec=stream_spec,
        interval=interval,
        step_id=step_id,
    )
