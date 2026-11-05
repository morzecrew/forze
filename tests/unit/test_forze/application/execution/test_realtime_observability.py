"""One registry per meter for the channel-labelled realtime instruments.

# covers: forze.application.execution.realtime.observability (observe_channel,
#         instrument_realtime_gateway)

The SDK keeps a name's first instrument and drops a later registration's callbacks, so
these run against a real ``MeterProvider``: a second channel that vanished there would pass
against a stub meter.
"""

from __future__ import annotations

from typing import Any

import pytest
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.execution.realtime import (
    RealtimeGatewayStats,
    instrument_realtime_gateway,
)
from forze.application.execution.realtime.observability import (
    GATEWAY_EMITTED_COUNTER,
    GATEWAY_POISONED_COUNTER,
    ChannelInstrument,
    _forze_meter,  # pyright: ignore[reportPrivateUsage]
    observe_channel,
)

# ----------------------- #


def _reader() -> tuple[InMemoryMetricReader, Any]:
    reader = InMemoryMetricReader()

    return reader, MeterProvider(metric_readers=[reader]).get_meter("test")


def _series(reader: InMemoryMetricReader) -> dict[str, dict[str, Any]]:
    """Every exported point, by metric name, then by its channel label."""

    data = reader.get_metrics_data()
    assert data is not None
    found: dict[str, dict[str, Any]] = {}

    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                for point in metric.data.data_points:
                    channel = point.attributes["forze.realtime.channel"]
                    found.setdefault(metric.name, {})[channel] = point.value

    return found


# ----------------------- #


def test_two_channels_on_one_meter_export_two_series_per_instrument() -> None:
    reader, meter = _reader()
    orders, chat = RealtimeGatewayStats(emitted=3), RealtimeGatewayStats(emitted=5, poisoned=1)

    instrument_realtime_gateway(orders, channel="orders", meter=meter)
    instrument_realtime_gateway(chat, channel="chat", meter=meter)

    series = _series(reader)
    assert len(series) == 9
    assert all(set(by_channel) == {"orders", "chat"} for by_channel in series.values())
    assert series[GATEWAY_EMITTED_COUNTER] == {"orders": 3, "chat": 5}
    assert series[GATEWAY_POISONED_COUNTER] == {"orders": 0, "chat": 1}


def test_a_repeat_call_for_the_same_stats_and_channel_adds_nothing() -> None:
    reader, meter = _reader()
    stats = RealtimeGatewayStats(emitted=3)

    instrument_realtime_gateway(stats, channel="orders", meter=meter)
    instrument_realtime_gateway(stats, channel="orders", meter=meter)

    # The SDK sums same-labelled observations, so a duplicate entry would read 6.
    assert _series(reader)[GATEWAY_EMITTED_COUNTER] == {"orders": 3}


def test_channels_on_the_default_meter_share_it_before_an_sdk_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Before an SDK is set, every ``get_meter`` returns a new proxy, whose instruments
    would collide on the real meter later; the default meter is fetched once."""

    readers: list[InMemoryMetricReader] = []

    def proxy(_name: str, *_args: Any, **_kwargs: Any) -> Any:
        reader, meter = _reader()
        readers.append(reader)

        return meter

    _forze_meter.cache_clear()
    monkeypatch.setattr(metrics, "get_meter", proxy)

    try:
        instrument_realtime_gateway(RealtimeGatewayStats(emitted=1), channel="orders")
        instrument_realtime_gateway(RealtimeGatewayStats(emitted=2), channel="chat")

        [reader] = readers
        assert _series(reader)[GATEWAY_EMITTED_COUNTER] == {"orders": 1, "chat": 2}

    finally:
        _forze_meter.cache_clear()


def test_nothing_is_registered_without_the_call(monkeypatch: pytest.MonkeyPatch) -> None:
    asked: list[str] = []
    real = metrics.get_meter

    def spy(name: str, *args: Any, **kwargs: Any) -> Any:
        asked.append(name)
        return real(name, *args, **kwargs)

    _forze_meter.cache_clear()
    monkeypatch.setattr(metrics, "get_meter", spy)
    stats = RealtimeGatewayStats()
    stats.emitted += 1

    assert asked == []

    instrument_realtime_gateway(stats)
    assert asked == ["forze"]  # the global meter, only once asked to


def test_a_failing_source_leaves_the_other_channels_exporting() -> None:
    reader, meter = _reader()

    def read(source: dict[str, int]) -> int:
        return source["count"]

    instrument = ChannelInstrument("test.reads", "counter", read, "Reads.")
    observe_channel({"count": 3}, [instrument], channel="orders", meter=meter)
    observe_channel({}, [instrument], channel="broken", meter=meter)

    assert _series(reader)["test.reads"] == {"orders": 3}


def test_a_reading_cannot_relabel_its_channel() -> None:
    reader, meter = _reader()

    def read(source: int) -> list[tuple[int, dict[str, str]]]:
        return [(source, {"forze.realtime.channel": "alerts"})]

    instrument = ChannelInstrument("test.relabel", "counter", read, "Relabel.")
    observe_channel(5, [instrument], channel="orders", meter=meter)

    assert _series(reader)["test.relabel"] == {"orders": 5}
