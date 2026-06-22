"""OpenTelemetry instrumentation for the offline mailbox.

``instrument_realtime_mailbox`` exports a channel's :class:`MailboxStats` as always-on
observable counters, so a deployment can watch offline store-and-forward rates — how
much is being stored for later, replayed on reconnect, trimmed, and acked. Mirrors the
identity plane's ``instrument_signing``. OpenTelemetry is imported lazily, so importing
this module does not pull ``opentelemetry`` into an uninstrumented app's import path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Iterable

from .mailbox import MailboxStats

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Meter, Observation  # noqa: F401

# ----------------------- #

MAILBOX_STORED_COUNTER = "forze.realtime.mailbox.stored"
MAILBOX_REPLAYED_COUNTER = "forze.realtime.mailbox.replayed"
MAILBOX_TRIMMED_COUNTER = "forze.realtime.mailbox.trimmed"
MAILBOX_ACKED_COUNTER = "forze.realtime.mailbox.acked"

# ....................... #


def instrument_realtime_mailbox(
    stats: MailboxStats,
    *,
    channel: str = "realtime",
    meter: "Meter | None" = None,
) -> None:
    """Export *stats* as OTel observable counters labelled ``forze.realtime.channel``.

    Pass one :class:`MailboxStats` shared by the channel's ``DocumentRealtimeMailbox``
    and ``DocumentMailboxCursors``. Emits via the global OTel meter unless *meter* is
    supplied. Call once at assembly time.

    - ``forze.realtime.mailbox.stored`` — durable principal signals stored for replay.
    - ``forze.realtime.mailbox.replayed`` — entries fetched on connect-time replay.
    - ``forze.realtime.mailbox.trimmed`` — entries dropped by retention/ack trimming.
    - ``forze.realtime.mailbox.acked`` — cursor advances (devices confirming receipt).
    """

    from opentelemetry import metrics
    from opentelemetry.metrics import Observation

    meter = meter or metrics.get_meter("forze")
    attributes = {"forze.realtime.channel": channel}

    def _observe(
        pick: Callable[[MailboxStats], int],
    ) -> Callable[["CallbackOptions"], Iterable["Observation"]]:
        def callback(_options: "CallbackOptions") -> Iterable["Observation"]:
            return [Observation(pick(stats), attributes)]

        return callback

    meter.create_observable_counter(
        MAILBOX_STORED_COUNTER,
        callbacks=[_observe(lambda s: s.stored)],
        unit="1",
        description="Durable principal signals stored for offline replay.",
    )
    meter.create_observable_counter(
        MAILBOX_REPLAYED_COUNTER,
        callbacks=[_observe(lambda s: s.replayed)],
        unit="1",
        description="Mailbox entries fetched for connect-time replay.",
    )
    meter.create_observable_counter(
        MAILBOX_TRIMMED_COUNTER,
        callbacks=[_observe(lambda s: s.trimmed)],
        unit="1",
        description="Mailbox entries dropped by retention/ack trimming.",
    )
    meter.create_observable_counter(
        MAILBOX_ACKED_COUNTER,
        callbacks=[_observe(lambda s: s.acked)],
        unit="1",
        description="Per-device cursor advances (acked deliveries).",
    )
