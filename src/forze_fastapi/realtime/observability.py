"""OpenTelemetry export for the SSE/WebSocket live-signal hub (opt-in).

:func:`instrument_realtime_hub` exports what :class:`~forze_fastapi.realtime.RealtimeSseHub`
already counts: the streams subscribed on this node, and the live signals dropped on full
subscriber queues. OpenTelemetry is imported lazily, so an uninstrumented app pays nothing.

The names are the hub's own rather than Socket.IO's ``forze.realtime.connections`` with a
transport label. A hub subscriber is a stream on the live tail, not an accepted connection:
a catch-up-only SSE stream or a replay-only WebSocket is never one. And the SDK hands a
second registration of a name the first instrument, dropping its callbacks, so a process
serving both transports would lose one of them.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Iterable
from typing import TYPE_CHECKING

from .hub import RealtimeSseHub

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Meter, Observation

# ----------------------- #

__all__ = [
    "REALTIME_HUB_DROPPED_COUNTER",
    "REALTIME_HUB_SUBSCRIBERS_COUNTER",
    "instrument_realtime_hub",
]

REALTIME_HUB_SUBSCRIBERS_COUNTER = "forze.realtime.hub.subscribers"
REALTIME_HUB_DROPPED_COUNTER = "forze.realtime.hub.dropped"

# ....................... #


def instrument_realtime_hub(
    hub: RealtimeSseHub,
    *,
    channel: str = "realtime",
    meter: "Meter | None" = None,
) -> None:
    """Export the hub's counts as OTel observable instruments.

    - ``forze.realtime.hub.subscribers`` — an up-down counter of the SSE and WebSocket
      streams subscribed to the live tail on this node.
    - ``forze.realtime.hub.dropped`` — live signals dropped because a subscriber's queue
      was full. The live leg is at-most-once, so every increment is a lost live frame; a
      durable one reaches the client through the mailbox replay on its next reconnect.

    Both carry ``forze.realtime.channel``. The callbacks read two in-memory counts, so a
    scrape queries no store. Emits via the global OTel meter unless *meter* is supplied.
    Call once per meter at assembly time, with the hub handed to the routes: the SDK keeps
    a name's first instrument and drops a later call's callbacks, so a second hub
    instrumented on the same meter exports nothing.

    :param hub: The hub the SSE and WebSocket routes share.
    :param channel: The ``forze.realtime.channel`` label.
    :param meter: The meter to register on; the global ``forze`` meter when ``None``.
    """

    from opentelemetry import metrics

    meter = meter or metrics.get_meter("forze")
    attributes = {"forze.realtime.channel": channel}

    def subscribers(_options: "CallbackOptions") -> "Iterable[Observation]":
        return [metrics.Observation(hub.subscribers, attributes)]

    def dropped(_options: "CallbackOptions") -> "Iterable[Observation]":
        return [metrics.Observation(hub.dropped, attributes)]

    meter.create_observable_up_down_counter(
        REALTIME_HUB_SUBSCRIBERS_COUNTER,
        callbacks=[subscribers],
        unit="1",
        description="SSE and WebSocket streams subscribed to the live tail on this node.",
    )
    meter.create_observable_counter(
        REALTIME_HUB_DROPPED_COUNTER,
        callbacks=[dropped],
        unit="1",
        description="Live signals dropped on full subscriber queues (at-most-once leg).",
    )
