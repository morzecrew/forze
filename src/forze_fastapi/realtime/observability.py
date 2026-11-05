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

from typing import TYPE_CHECKING

from forze.application.execution.realtime.observability import (
    ChannelInstrument,
    observe_channel,
)

from .hub import RealtimeSseHub

if TYPE_CHECKING:
    from opentelemetry.metrics import Meter

# ----------------------- #

__all__ = [
    "REALTIME_HUB_DROPPED_COUNTER",
    "REALTIME_HUB_SUBSCRIBERS_COUNTER",
    "instrument_realtime_hub",
]

REALTIME_HUB_SUBSCRIBERS_COUNTER = "forze.realtime.hub.subscribers"
REALTIME_HUB_DROPPED_COUNTER = "forze.realtime.hub.dropped"

_HUB_INSTRUMENTS: tuple[ChannelInstrument[RealtimeSseHub], ...] = (
    ChannelInstrument(
        REALTIME_HUB_SUBSCRIBERS_COUNTER,
        "up_down_counter",
        lambda hub: hub.subscribers,
        "SSE and WebSocket streams subscribed to the live tail on this node.",
    ),
    ChannelInstrument(
        REALTIME_HUB_DROPPED_COUNTER,
        "counter",
        lambda hub: hub.dropped,
        "Live signals dropped on full subscriber queues (at-most-once leg).",
    ),
)

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
    Call once per channel at assembly time, with the hub handed to the routes. Each channel
    exports its own series; a repeat call for the same hub and channel adds nothing.

    :param hub: The hub the SSE and WebSocket routes share.
    :param channel: The ``forze.realtime.channel`` label.
    :param meter: The meter to register on; the global ``forze`` meter when ``None``.
    """

    observe_channel(hub, _HUB_INSTRUMENTS, channel=channel, meter=meter)
