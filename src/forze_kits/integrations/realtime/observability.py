"""OpenTelemetry instrumentation for the offline mailbox.

``instrument_realtime_mailbox`` exports a channel's offline store-and-forward counters as
always-on observable counters — how much is stored for later, replayed on reconnect,
trimmed, and acked. Mirrors the identity plane's ``instrument_signing``: the mailbox /
cursors keep mutable counters and snapshot a frozen :class:`MailboxStats` on read.
OpenTelemetry is imported lazily, so importing this module pulls nothing into an
uninstrumented app's import path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Iterable

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Meter, Observation  # noqa: F401

    from .mailbox import DocumentMailboxCursors, DocumentRealtimeMailbox

# ----------------------- #

MAILBOX_STORED_COUNTER = "forze.realtime.mailbox.stored"
MAILBOX_REPLAYED_COUNTER = "forze.realtime.mailbox.replayed"
MAILBOX_TRIMMED_COUNTER = "forze.realtime.mailbox.trimmed"
MAILBOX_ACKED_COUNTER = "forze.realtime.mailbox.acked"

# ....................... #


def instrument_realtime_mailbox(
    mailbox: "DocumentRealtimeMailbox",
    cursors: "DocumentMailboxCursors",
    *,
    channel: str = "realtime",
    meter: "Meter | None" = None,
) -> None:
    """Export the mailbox + cursor counters as OTel observable counters.

    Each callback snapshots the live counters (``mailbox.stats()`` / ``cursors.stats()``)
    at scrape time. Emits via the global OTel meter unless *meter* is supplied; labelled
    ``forze.realtime.channel``. Call once at assembly time.

    - ``forze.realtime.mailbox.stored`` — durable principal signals stored for replay.
    - ``forze.realtime.mailbox.replayed`` — entries fetched on connect-time replay.
    - ``forze.realtime.mailbox.trimmed`` — entries dropped by retention/ack trimming.
    - ``forze.realtime.mailbox.acked`` — cursor advances (devices confirming receipt).
    """

    from opentelemetry import metrics
    from opentelemetry.metrics import Observation

    meter = meter or metrics.get_meter("forze")
    attributes = {"forze.realtime.channel": channel}

    def _counter(name: str, pick: Callable[[], int], description: str) -> None:
        def callback(_options: "CallbackOptions") -> Iterable["Observation"]:
            return [Observation(pick(), attributes)]

        meter.create_observable_counter(
            name, callbacks=[callback], unit="1", description=description
        )

    _counter(MAILBOX_STORED_COUNTER, lambda: mailbox.stats().stored,
             "Durable principal signals stored for offline replay.")
    _counter(MAILBOX_REPLAYED_COUNTER, lambda: mailbox.stats().replayed,
             "Mailbox entries fetched for connect-time replay.")
    _counter(MAILBOX_TRIMMED_COUNTER, lambda: mailbox.stats().trimmed,
             "Mailbox entries dropped by retention/ack trimming.")
    _counter(MAILBOX_ACKED_COUNTER, lambda: cursors.stats().acked,
             "Per-device cursor advances (acked deliveries).")
