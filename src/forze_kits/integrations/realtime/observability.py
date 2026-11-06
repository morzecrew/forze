"""OpenTelemetry instrumentation for the offline mailbox.

``instrument_realtime_mailbox`` exports the offline store-and-forward counters as always-on
observable counters — how much is stored for later, replayed on reconnect, trimmed, and
acked — plus a histogram of the entries each connect's replay yielded, the backlog trend.

The counts are kept per document spec, here, for the life of the process. The transports
build a mailbox on every connect and the gateway and writer build theirs inside their own
step, so counts held on an instance would reach no exporter. Every
:class:`~.mailbox.DocumentRealtimeMailbox` and :class:`~.mailbox.DocumentMailboxCursors`
resolves its spec's record once, at build, and adds to it. The records are plain ints:
nothing reaches OpenTelemetry, which is imported lazily, until the instrument call.
"""

from __future__ import annotations

import threading
import weakref
from collections.abc import Callable
from typing import TYPE_CHECKING, final

import attrs

from forze.application.execution.realtime.observability import (
    ChannelInstrument,
    ChannelReading,
    observe_channel,
)

if TYPE_CHECKING:
    from opentelemetry.metrics import Histogram, Meter

# ----------------------- #

MAILBOX_STORED_COUNTER = "forze.realtime.mailbox.stored"
MAILBOX_REPLAYED_COUNTER = "forze.realtime.mailbox.replayed"
MAILBOX_TRIMMED_COUNTER = "forze.realtime.mailbox.trimmed"
MAILBOX_ACKED_COUNTER = "forze.realtime.mailbox.acked"
MAILBOX_OVERFLOWED_COUNTER = "forze.realtime.mailbox.overflowed"
MAILBOX_REPLAY_SIZE_HISTOGRAM = "forze.realtime.mailbox.replay_size"

_SPEC_LABEL = "forze.document"
"""Every series' one label: the mailbox or cursor spec's name."""

# ....................... #


@final
@attrs.define(slots=True)
class _MailboxTotals:
    """One mailbox spec's counts, summed over every instance built for it."""

    spec: str
    stored: int = 0
    replayed: int = 0
    trimmed: int = 0
    overflowed: int = 0

    def add(
        self, *, stored: int = 0, replayed: int = 0, trimmed: int = 0, overflowed: int = 0
    ) -> None:
        """Add to the counts under the registry lock: event loops on other threads share them."""

        with _LOCK:
            self.stored += stored
            self.replayed += replayed
            self.trimmed += trimmed
            self.overflowed += overflowed


@final
@attrs.define(slots=True)
class _CursorTotals:
    """One cursor spec's counts, summed over every instance built for it."""

    spec: str
    acked: int = 0

    def add(self, *, acked: int) -> None:
        """Add to the count under the registry lock: event loops on other threads share it."""

        with _LOCK:
            self.acked += acked


# ....................... #

_LOCK = threading.Lock()
"""Guards the registries below and every count in them: one spec's record is shared by the
event loops of every thread, and read by scrapes on any thread. Never held across an await
or an SDK call. Uncontended, an update costs about 150 ns, against a store round trip."""

_MAILBOX_TOTALS: dict[str, _MailboxTotals] = {}
_CURSOR_TOTALS: dict[str, _CursorTotals] = {}
"""Held strongly: a cumulative counter must outlive the per-connect instances adding to it."""

_REPLAY_HISTOGRAMS: weakref.WeakKeyDictionary[Meter, Histogram] = weakref.WeakKeyDictionary()
"""The replay-size histogram of every meter instrumented, empty until the first call."""


def mailbox_totals(spec: str) -> _MailboxTotals:
    """The record every mailbox built for *spec* adds to."""

    with _LOCK:
        return _MAILBOX_TOTALS.setdefault(spec, _MailboxTotals(spec))


def cursor_totals(spec: str) -> _CursorTotals:
    """The record every cursor store built for *spec* adds to."""

    with _LOCK:
        return _CURSOR_TOTALS.setdefault(spec, _CursorTotals(spec))


def record_replay(spec: str, count: int) -> None:
    """Record one connect's finished replay on every instrumented meter."""

    with _LOCK:
        histograms = list(_REPLAY_HISTOGRAMS.values())

    for histogram in histograms:
        histogram.record(count, {_SPEC_LABEL: spec})


# ....................... #


def _counter[T: (_MailboxTotals, _CursorTotals)](
    name: str, pick: Callable[[T], int], description: str
) -> ChannelInstrument[dict[str, T]]:
    def read(registry: dict[str, T]) -> ChannelReading:
        with _LOCK:
            return [(pick(one), {_SPEC_LABEL: one.spec}) for one in registry.values()]

    return ChannelInstrument(name, "counter", read, description)


_MAILBOX_INSTRUMENTS: tuple[ChannelInstrument[dict[str, _MailboxTotals]], ...] = (
    _counter(
        MAILBOX_STORED_COUNTER,
        lambda totals: totals.stored,
        "Durable principal signals stored for offline replay.",
    ),
    _counter(
        MAILBOX_REPLAYED_COUNTER,
        lambda totals: totals.replayed,
        "Mailbox entries fetched for connect-time replay.",
    ),
    _counter(
        MAILBOX_TRIMMED_COUNTER,
        lambda totals: totals.trimmed,
        "Mailbox entries dropped by retention/ack trimming.",
    ),
    _counter(
        MAILBOX_OVERFLOWED_COUNTER,
        lambda totals: totals.overflowed,
        "Replays that lost their oldest backlog to the retention cap (bounded loss).",
    ),
)

_CURSOR_INSTRUMENTS: tuple[ChannelInstrument[dict[str, _CursorTotals]], ...] = (
    _counter(
        MAILBOX_ACKED_COUNTER,
        lambda totals: totals.acked,
        "Per-device cursor advances (acked deliveries).",
    ),
)

# ....................... #


def instrument_realtime_mailbox(*, meter: Meter | None = None) -> None:
    """Export every mailbox and cursor spec's counters as OTel observable counters, and the
    per-connect replay size as a histogram.

    Covers every mailbox built in the process, whoever built it: a per-connect
    ``mailbox_factory``, the Socket.IO gateway, the mailbox writer, the retention sweep.
    The counters are process totals, so a scrape after the call reports what happened
    before it too; the histogram records only the replays that finish after it. A callback
    reads in-memory counts only, so a scrape never queries the store. Emits via the global
    OTel meter unless *meter* is supplied. Call once at assembly time; a repeat call for
    the same meter adds nothing.

    Every series is labelled ``forze.document``: the mailbox spec's name, or the cursor
    spec's for ``acked``. One series per spec, never per principal, device or tenant.

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

    :param meter: The meter to register on; the global ``forze`` meter when ``None``.
    """

    observe_channel(_MAILBOX_TOTALS, _MAILBOX_INSTRUMENTS, channel=None, meter=meter)
    meter = observe_channel(_CURSOR_TOTALS, _CURSOR_INSTRUMENTS, channel=None, meter=meter)

    if meter in _REPLAY_HISTOGRAMS:
        return

    # Outside the lock: the SDK holds its own while a scrape's callbacks take ours.
    histogram = meter.create_histogram(
        MAILBOX_REPLAY_SIZE_HISTOGRAM,
        unit="1",
        description="Unacked mailbox entries one connect's replay read.",
    )

    with _LOCK:
        _REPLAY_HISTOGRAMS.setdefault(meter, histogram)
