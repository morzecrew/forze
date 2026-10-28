"""Delivery counters for the realtime stream's consumers, and their OTel export.

One :class:`RealtimeGatewayStats` is shared by a consumer and its signal source: the
Socket.IO gateway counts its emit outcomes on it, the
:class:`~forze.application.execution.realtime.RealtimeMailboxWriter` its stores, and either
one's source its bridge failures and poison drops. :func:`instrument_realtime_gateway` exports
them; OpenTelemetry is imported lazily, so an uninstrumented app pays nothing.
"""

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, final

import attrs

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Meter, Observation

# ----------------------- #

GATEWAY_EMITTED_COUNTER = "forze.realtime.gateway.emitted"
GATEWAY_EMIT_FAILED_COUNTER = "forze.realtime.gateway.emit_failed"
GATEWAY_PRESENCE_SKIPPED_COUNTER = "forze.realtime.gateway.presence_skipped"
GATEWAY_DEDUP_SKIPPED_COUNTER = "forze.realtime.gateway.dedup_skipped"
GATEWAY_ADMISSION_REJECTED_COUNTER = "forze.realtime.gateway.admission_rejected"
GATEWAY_UNTENANTED_DROPPED_COUNTER = "forze.realtime.gateway.untenanted_dropped"
GATEWAY_MAILBOXED_COUNTER = "forze.realtime.gateway.mailboxed"
GATEWAY_BRIDGE_FAILED_COUNTER = "forze.realtime.gateway.bridge_failed"
GATEWAY_POISONED_COUNTER = "forze.realtime.gateway.poisoned"

# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class RealtimeGatewayStats:
    """Mutable delivery counters for one gateway (share one instance gateway ↔ source).

    Deliberately **one** object for both wiring points: the gateway counts emit outcomes,
    its signal source counts bridge failures and poison drops — splitting them would mean
    two instruments that can drift. Plain ints mutated on the event loop (no lock needed);
    the instrument's callbacks read them at scrape time.
    """

    emitted: int = 0
    """Frames delivered to Socket.IO (``sio.emit`` returned)."""

    emit_failed: int = 0
    """``sio.emit`` calls that raised — including ``emit_timeout`` expiries."""

    presence_skipped: int = 0
    """Live emits skipped because the principal room was empty (mailbox recovers)."""

    dedup_skipped: int = 0
    """Durable signals skipped as already-seen (relay retries, reclaim duplicates)."""

    admission_rejected: int = 0
    """Signals dropped at the catalog gate (undeclared event, audience, payload shape)."""

    untenanted_dropped: int = 0
    """Signals dropped for resolving no tenant on a ``require_tenant`` gateway."""

    mailboxed: int = 0
    """Durable principal signals stored for offline replay."""

    bridge_failed: int = 0
    """Bridge attempts that failed and were left for redelivery (or acked, if ephemeral)."""

    poisoned: int = 0
    """Durable signals dropped at the delivery ceiling — every one is a bounded loss."""


# ----------------------- #


def instrument_realtime_gateway(
    stats: RealtimeGatewayStats,
    *,
    channel: str = "realtime",
    meter: "Meter | None" = None,
) -> None:
    """Export the gateway's delivery counters as OTel observable counters.

    Emits via the global OTel meter unless *meter* is supplied; labelled
    ``forze.realtime.channel``. Call once at assembly time, with the same *stats*
    instance handed to the gateway and its source.

    The two to alarm on: ``forze.realtime.gateway.poisoned`` (every increment is a
    dropped durable delivery) and ``forze.realtime.gateway.emit_failed`` climbing while
    ``emitted`` is flat (the Socket.IO side — or its Redis backplane — stopped taking
    frames).
    """

    from opentelemetry import metrics

    meter = meter or metrics.get_meter("forze")
    attributes = {"forze.realtime.channel": channel}

    def _counter(name: str, pick: Callable[[], int], description: str) -> None:
        def callback(_options: "CallbackOptions") -> "Iterable[Observation]":
            return [metrics.Observation(pick(), attributes)]

        meter.create_observable_counter(
            name, callbacks=[callback], unit="1", description=description
        )

    _counter(GATEWAY_EMITTED_COUNTER, lambda: stats.emitted, "Frames delivered to Socket.IO.")
    _counter(
        GATEWAY_EMIT_FAILED_COUNTER,
        lambda: stats.emit_failed,
        "sio.emit calls that raised (including emit-timeout expiries).",
    )
    _counter(
        GATEWAY_PRESENCE_SKIPPED_COUNTER,
        lambda: stats.presence_skipped,
        "Live emits skipped for an empty principal room (recoverable via mailbox).",
    )
    _counter(
        GATEWAY_DEDUP_SKIPPED_COUNTER,
        lambda: stats.dedup_skipped,
        "Durable signals skipped as already seen.",
    )
    _counter(
        GATEWAY_ADMISSION_REJECTED_COUNTER,
        lambda: stats.admission_rejected,
        "Signals rejected at the catalog admission gate.",
    )
    _counter(
        GATEWAY_UNTENANTED_DROPPED_COUNTER,
        lambda: stats.untenanted_dropped,
        "Signals dropped for resolving no tenant on a require_tenant gateway.",
    )
    _counter(
        GATEWAY_MAILBOXED_COUNTER,
        lambda: stats.mailboxed,
        "Durable principal signals stored for offline replay.",
    )
    _counter(
        GATEWAY_BRIDGE_FAILED_COUNTER,
        lambda: stats.bridge_failed,
        "Bridge attempts that failed (redelivered if durable, dropped if ephemeral).",
    )
    _counter(
        GATEWAY_POISONED_COUNTER,
        lambda: stats.poisoned,
        "Durable signals dropped at the delivery ceiling (bounded loss).",
    )
