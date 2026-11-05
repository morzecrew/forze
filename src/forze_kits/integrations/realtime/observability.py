"""OpenTelemetry instrumentation for the offline mailbox.

``instrument_realtime_mailbox`` exports a channel's offline store-and-forward counters as
always-on observable counters — how much is stored for later, replayed on reconnect,
trimmed, and acked — plus a histogram of the entries each connect's replay yielded, the
backlog trend. Mirrors the identity plane's ``instrument_signing``: the mailbox /
cursors keep mutable counters and snapshot a frozen :class:`MailboxStats` on read.
OpenTelemetry is imported lazily, so importing this module pulls nothing into an
uninstrumented app's import path.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Meter, Observation

    from .mailbox import DocumentMailboxCursors, DocumentRealtimeMailbox

# ----------------------- #

MAILBOX_STORED_COUNTER = "forze.realtime.mailbox.stored"
MAILBOX_REPLAYED_COUNTER = "forze.realtime.mailbox.replayed"
MAILBOX_TRIMMED_COUNTER = "forze.realtime.mailbox.trimmed"
MAILBOX_ACKED_COUNTER = "forze.realtime.mailbox.acked"
MAILBOX_OVERFLOWED_COUNTER = "forze.realtime.mailbox.overflowed"
MAILBOX_REPLAY_SIZE_HISTOGRAM = "forze.realtime.mailbox.replay_size"

# ....................... #


def instrument_realtime_mailbox(
    mailbox: DocumentRealtimeMailbox,
    cursors: DocumentMailboxCursors,
    *,
    channel: str = "realtime",
    meter: Meter | None = None,
) -> None:
    """Export the mailbox + cursor counters as OTel observable counters, and the
    per-connect replay size as a histogram.

    Each callback snapshots the live counters (``mailbox.stats()`` / ``cursors.stats()``)
    at scrape time; the histogram is recorded by the replay drain as each connect's
    replay finishes, so neither queries the store. Emits via the global OTel meter unless
    *meter* is supplied; labelled ``forze.realtime.channel``. Call once at assembly time.

    The counters and the replay hook live on *mailbox* itself, so instrument the instance
    the transports' ``mailbox_factory`` returns: a factory that builds a new mailbox per
    connect records nothing.

    - ``forze.realtime.mailbox.stored`` — durable principal signals stored for replay.
    - ``forze.realtime.mailbox.replayed`` — entries fetched on connect-time replay.
    - ``forze.realtime.mailbox.trimmed`` — entries dropped by retention/ack trimming.
    - ``forze.realtime.mailbox.acked`` — cursor advances (devices confirming receipt).
    - ``forze.realtime.mailbox.overflowed`` — replays that lost their oldest backlog to
      the retention cap; every increment is a device that fell more than ``cap``
      entries behind (alarm on it).
    - ``forze.realtime.mailbox.replay_size`` — per connect, the entries past the device's
      acked cursor that its replay read: the unacked backlog, not the frames sent (a
      resume skips what it delivered before the tear). Only a replay that finishes
      records. A tail that keeps growing means devices fall behind on acks before
      ``overflowed`` counts a loss.
    """

    from opentelemetry import metrics

    meter = meter or metrics.get_meter("forze")
    attributes = {"forze.realtime.channel": channel}

    def _counter(name: str, pick: Callable[[], int], description: str) -> None:
        def callback(_options: CallbackOptions) -> Iterable[Observation]:
            return [metrics.Observation(pick(), attributes)]

        meter.create_observable_counter(
            name, callbacks=[callback], unit="1", description=description
        )

    _counter(
        MAILBOX_STORED_COUNTER,
        lambda: mailbox.stats().stored,
        "Durable principal signals stored for offline replay.",
    )
    _counter(
        MAILBOX_REPLAYED_COUNTER,
        lambda: mailbox.stats().replayed,
        "Mailbox entries fetched for connect-time replay.",
    )
    _counter(
        MAILBOX_TRIMMED_COUNTER,
        lambda: mailbox.stats().trimmed,
        "Mailbox entries dropped by retention/ack trimming.",
    )
    _counter(
        MAILBOX_ACKED_COUNTER,
        lambda: cursors.stats().acked,
        "Per-device cursor advances (acked deliveries).",
    )
    _counter(
        MAILBOX_OVERFLOWED_COUNTER,
        lambda: mailbox.stats().overflowed,
        "Replays that lost their oldest backlog to the retention cap (bounded loss).",
    )

    replay_size = meter.create_histogram(
        MAILBOX_REPLAY_SIZE_HISTOGRAM,
        unit="1",
        description="Unacked mailbox entries one connect's replay read.",
    )
    mailbox.on_replay = lambda count: replay_size.record(count, attributes)
