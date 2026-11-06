"""Delivery counters for the realtime stream's consumers, and their OTel export.

One :class:`RealtimeGatewayStats` is shared by a consumer and its signal source: the
Socket.IO gateway counts its emit outcomes on it, the
:class:`~forze.application.execution.realtime.RealtimeMailboxWriter` its stores, and either
one's source its bridge failures and poison drops. :func:`instrument_realtime_gateway` exports
them; OpenTelemetry is imported lazily, so an uninstrumented app pays nothing.

Every channel-labelled realtime instrument call, here and in the transport packages,
registers through :func:`observe_channel`: one set of instruments per meter, whose callbacks
walk every channel registered on it. The SDK keeps a name's first instrument and drops a
later registration's callbacks, so a second channel registering its own instruments would
export nothing.
"""

import threading
import weakref
from collections.abc import Callable, Iterable, Mapping, Sequence
from functools import cache
from typing import TYPE_CHECKING, Any, Literal, final

import attrs

from forze.application._logger import logger as _logger

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

ChannelReading = float | Iterable[tuple[float, Mapping[str, str]]]
"""One source's observations: a bare value, or ``(value, labels)`` pairs whose labels join
the channel's."""


@final
@attrs.frozen(slots=True)
class ChannelInstrument[S]:
    """One observable instrument a channel-labelled realtime source exports.

    The meter's first registration of :attr:`name` keeps its :attr:`read` for every channel
    registered later, so *read* takes everything from its source argument.
    """

    name: str
    """Instrument name, unique per meter."""

    kind: Literal["counter", "up_down_counter", "gauge"]
    """The observable instrument it registers as."""

    read: Callable[[S], ChannelReading]
    """The source's observations, read at scrape time from in-memory counts only."""

    description: str
    """Instrument description."""

    unit: str = "1"
    """Instrument unit."""


# ....................... #

_ENTRIES_LOCK = threading.Lock()
"""Guards every entry list: appended at assembly, copied by scrapes on any thread."""

_ENTRIES: "weakref.WeakKeyDictionary[Meter, dict[str, list[tuple[Any, dict[str, str]]]]]" = (
    weakref.WeakKeyDictionary()
)
"""Per meter, per instrument name, the ``(source, attributes)`` pairs its callback walks.
Weak on the meter only: a meter's instruments hold their callbacks, and those the sources."""


_METER_LOCK = threading.Lock()
"""Serializes the first default-meter fetch: ``cache`` alone lets two threads both compute it."""


@cache
def _forze_meter() -> "Meter":
    """The global ``forze`` meter, fetched once.

    Before an SDK is configured, every ``get_meter`` returns a new proxy, and two proxies'
    instruments would collide on the real meter once one is set.
    """

    from opentelemetry import metrics

    return metrics.get_meter("forze")


# ....................... #


def observe_channel[S](
    source: S,
    instruments: Sequence[ChannelInstrument[S]],
    *,
    channel: str | None,
    meter: "Meter | None",
) -> "Meter":
    """Export *source* as one ``forze.realtime.channel`` series of each instrument.

    The first call on a meter registers each instrument once; every later call appends
    *source* to what its callbacks walk, so each channel exports its own series. A repeat
    call for the same source and channel adds nothing. Sources are held strongly, so a
    cumulative count never drops while its meter lives. Give every call the same *meter*, or
    ``None``: a proxy fetched before the SDK is set and the SDK's own ``forze`` meter keep
    separate entries, and the SDK drops the later one's callbacks.

    :param source: The in-memory counts the instruments read.
    :param instruments: What to export from *source*.
    :param channel: The ``forze.realtime.channel`` label, or ``None`` for a source whose
        readings carry every label they need.
    :param meter: The meter to register on; the global ``forze`` meter when ``None``.
    :returns: The meter registered on, for a caller that adds a synchronous instrument to
        the same one.
    """

    if meter is None:
        with _METER_LOCK:
            meter = _forze_meter()

    attributes = {"forze.realtime.channel": channel} if channel is not None else {}
    by_name, fresh = _enlist(meter, instruments, source, attributes)

    create = {
        "counter": meter.create_observable_counter,
        "up_down_counter": meter.create_observable_up_down_counter,
        "gauge": meter.create_observable_gauge,
    }

    # Outside the lock: the SDK calls these callbacks, which take it, while holding its own.
    for instrument in fresh:
        create[instrument.kind](
            instrument.name,
            callbacks=[_channel_callback(by_name, instrument)],
            unit=instrument.unit,
            description=instrument.description,
        )

    return meter


def _enlist[S](
    meter: "Meter",
    instruments: Sequence[ChannelInstrument[S]],
    source: S,
    attributes: dict[str, str],
) -> tuple[dict[str, list[tuple[Any, dict[str, str]]]], list[ChannelInstrument[S]]]:
    """Add *source* to each instrument's entries on *meter*, once; return the meter's entries
    and the instruments not registered on it yet."""

    with _ENTRIES_LOCK:
        by_name = _ENTRIES.setdefault(meter, {})
        fresh = [instrument for instrument in instruments if instrument.name not in by_name]

        for instrument in instruments:
            entries = by_name.setdefault(instrument.name, [])

            if not any(known is source and labels == attributes for known, labels in entries):
                entries.append((source, attributes))

    return by_name, fresh


def _channel_callback[S](
    by_name: dict[str, list[tuple[Any, dict[str, str]]]],
    instrument: ChannelInstrument[S],
) -> "Callable[[CallbackOptions], Iterable[Observation]]":
    """Read every source enlisted for *instrument*, copying the list under the lock first."""

    from opentelemetry import metrics

    def callback(_options: "CallbackOptions") -> "Iterable[Observation]":
        with _ENTRIES_LOCK:
            entries = list(by_name[instrument.name])

        observations: list[Observation] = []

        for known, labels in entries:
            # One failing source must not silence the other channels sharing this callback.
            try:
                reading = instrument.read(known)

            except Exception:
                _logger.warning(
                    "Realtime metric %s failed to read a source; other channels still export",
                    instrument.name,
                    exc_info=True,
                    _dedup_key=f"realtime.metric_read_failed.{instrument.name}",
                )
                continue

            if isinstance(reading, int | float):
                reading = [(reading, {})]

            # The registered channel label wins over anything a reading supplies.
            observations.extend(
                metrics.Observation(value, {**extra, **labels}) for value, extra in reading
            )

        return observations

    return callback


# ----------------------- #


def _gateway_counter(
    name: str, read: Callable[[RealtimeGatewayStats], int], description: str
) -> ChannelInstrument[RealtimeGatewayStats]:
    return ChannelInstrument(name, "counter", read, description)


_GATEWAY_INSTRUMENTS = (
    _gateway_counter(
        GATEWAY_EMITTED_COUNTER, lambda stats: stats.emitted, "Frames delivered to Socket.IO."
    ),
    _gateway_counter(
        GATEWAY_EMIT_FAILED_COUNTER,
        lambda stats: stats.emit_failed,
        "sio.emit calls that raised (including emit-timeout expiries).",
    ),
    _gateway_counter(
        GATEWAY_PRESENCE_SKIPPED_COUNTER,
        lambda stats: stats.presence_skipped,
        "Live emits skipped for an empty principal room (recoverable via mailbox).",
    ),
    _gateway_counter(
        GATEWAY_DEDUP_SKIPPED_COUNTER,
        lambda stats: stats.dedup_skipped,
        "Durable signals skipped as already seen.",
    ),
    _gateway_counter(
        GATEWAY_ADMISSION_REJECTED_COUNTER,
        lambda stats: stats.admission_rejected,
        "Signals rejected at the catalog admission gate.",
    ),
    _gateway_counter(
        GATEWAY_UNTENANTED_DROPPED_COUNTER,
        lambda stats: stats.untenanted_dropped,
        "Signals dropped for resolving no tenant on a require_tenant gateway.",
    ),
    _gateway_counter(
        GATEWAY_MAILBOXED_COUNTER,
        lambda stats: stats.mailboxed,
        "Durable principal signals stored for offline replay.",
    ),
    _gateway_counter(
        GATEWAY_BRIDGE_FAILED_COUNTER,
        lambda stats: stats.bridge_failed,
        "Bridge attempts that failed (redelivered if durable, dropped if ephemeral).",
    ),
    _gateway_counter(
        GATEWAY_POISONED_COUNTER,
        lambda stats: stats.poisoned,
        "Durable signals dropped at the delivery ceiling (bounded loss).",
    ),
)

# ....................... #


def instrument_realtime_gateway(
    stats: RealtimeGatewayStats,
    *,
    channel: str = "realtime",
    meter: "Meter | None" = None,
) -> None:
    """Export the gateway's delivery counters as OTel observable counters.

    Emits via the global OTel meter unless *meter* is supplied; labelled
    ``forze.realtime.channel``. Call once per channel at assembly time, with the same
    *stats* instance handed to the gateway and its source. Each channel exports its own
    series; a repeat call for the same *stats* and channel adds nothing.

    The two to alarm on: ``forze.realtime.gateway.poisoned`` (every increment is a
    dropped durable delivery) and ``forze.realtime.gateway.emit_failed`` climbing while
    ``emitted`` is flat (the Socket.IO side — or its Redis backplane — stopped taking
    frames).
    """

    observe_channel(stats, _GATEWAY_INSTRUMENTS, channel=channel, meter=meter)
