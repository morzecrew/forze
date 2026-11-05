"""The SSE/WebSocket hub's OTel instrument — subscribers and dropped live signals.

# covers: forze_fastapi.realtime.observability (instrument_realtime_hub)
"""

from __future__ import annotations

from typing import Any

import pytest
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.execution.realtime.observability import (
    _forze_meter,  # pyright: ignore[reportPrivateUsage]
)
from forze_fastapi.realtime import RealtimeSseHub, instrument_realtime_hub

# ----------------------- #


def _signal(n: int) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal("p1"), "e", {"n": n})


def _instrumented(hub: RealtimeSseHub) -> InMemoryMetricReader:
    reader = InMemoryMetricReader()
    instrument_realtime_hub(
        hub, channel="c1", meter=MeterProvider(metric_readers=[reader]).get_meter("test")
    )

    return reader


def _value(reader: InMemoryMetricReader, name: str) -> Any:
    data = reader.get_metrics_data()
    assert data is not None
    [point] = [
        point
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if metric.name == name
        for point in metric.data.data_points
    ]
    assert dict(point.attributes) == {"forze.realtime.channel": "c1"}

    return point.value


# ----------------------- #


def test_subscribers_rise_and_fall_with_streams() -> None:
    hub = RealtimeSseHub()
    reader = _instrumented(hub)

    assert _value(reader, "forze.realtime.hub.subscribers") == 0

    first = hub.subscribe(principal="p1", tenant=None)
    hub.subscribe(principal="p2", tenant=None)
    assert _value(reader, "forze.realtime.hub.subscribers") == 2

    hub.unsubscribe(first)
    assert _value(reader, "forze.realtime.hub.subscribers") == 1


def test_a_full_subscriber_queue_counts_a_drop() -> None:
    hub = RealtimeSseHub(queue_size=1)
    reader = _instrumented(hub)
    hub.subscribe(principal="p1", tenant=None)

    hub.publish(_signal(1), None, event_id=None)
    assert _value(reader, "forze.realtime.hub.dropped") == 0

    hub.publish(_signal(2), None, event_id=None)  # the queue holds one: dropped
    assert _value(reader, "forze.realtime.hub.dropped") == 1


def _by_channel(reader: InMemoryMetricReader) -> dict[str, dict[str, Any]]:
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


def test_two_hubs_on_one_meter_export_a_series_each() -> None:
    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter("test")
    orders, chat = RealtimeSseHub(), RealtimeSseHub()
    chat.subscribe(principal="p1", tenant=None)

    instrument_realtime_hub(orders, channel="orders", meter=meter)
    instrument_realtime_hub(chat, channel="chat", meter=meter)
    instrument_realtime_hub(chat, channel="chat", meter=meter)  # a repeat adds nothing

    assert _by_channel(reader) == {
        "forze.realtime.hub.subscribers": {"orders": 0, "chat": 1},
        "forze.realtime.hub.dropped": {"orders": 0, "chat": 0},
    }


def test_nothing_is_registered_without_the_call(monkeypatch: pytest.MonkeyPatch) -> None:
    asked: list[str] = []
    real = metrics.get_meter

    def spy(name: str, *args: Any, **kwargs: Any) -> Any:
        asked.append(name)
        return real(name, *args, **kwargs)

    _forze_meter.cache_clear()  # the default meter is fetched once per process
    monkeypatch.setattr(metrics, "get_meter", spy)
    hub = RealtimeSseHub(queue_size=1)
    subscription = hub.subscribe(principal="p1", tenant=None)
    hub.publish(_signal(1), None, event_id=None)
    hub.publish(_signal(2), None, event_id=None)
    hub.unsubscribe(subscription)

    assert asked == []

    instrument_realtime_hub(hub)
    assert asked == ["forze"]  # the global meter, only once asked to
