"""Gateway/backplane observability — counters, gauges, and the health snapshot.

# covers: forze_socketio.observability

The instruments register observable callbacks against a meter; a stub meter captures them
so each callback can be invoked directly and asserted to reflect the live stats/health —
the same scrape path OTel takes, minus the SDK.
"""

from __future__ import annotations

import threading
from typing import Any

from forze.application.execution.background import SpreadOutcome
from forze_socketio import (
    BackplaneHealth,
    RealtimeConnectionStats,
    RealtimeGatewayStats,
    instrument_realtime_backplane,
    instrument_realtime_connections,
    instrument_realtime_gateway,
)

# ----------------------- #


class _StubMeter:
    """Captures observable-instrument registrations: name → callback."""

    def __init__(self) -> None:
        self.callbacks: dict[str, Any] = {}

    def create_observable_counter(self, name: str, *, callbacks: Any, **_: Any) -> None:
        self.callbacks[name] = callbacks[0]

    def create_observable_gauge(self, name: str, *, callbacks: Any, **_: Any) -> None:
        self.callbacks[name] = callbacks[0]

    def create_observable_up_down_counter(self, name: str, *, callbacks: Any, **_: Any) -> None:
        self.callbacks[name] = callbacks[0]


def _scrape(meter: _StubMeter, name: str) -> float:
    [observation] = meter.callbacks[name](None)
    return observation.value


# ----------------------- #


def test_backplane_health_snapshot() -> None:
    health = BackplaneHealth()

    assert health.seconds_since_ok == -1.0  # never succeeded ≠ stale — a wiring signal

    health.failed()
    health.failed()
    assert health.consecutive_failures == 2
    assert health.seconds_since_ok == -1.0

    health.ok()
    assert health.consecutive_failures == 0
    assert 0.0 <= health.seconds_since_ok < 60.0


def test_gateway_instrument_reflects_live_stats() -> None:
    stats = RealtimeGatewayStats()
    meter = _StubMeter()

    instrument_realtime_gateway(stats, meter=meter)  # pyright: ignore[reportArgumentType]

    assert len(meter.callbacks) == 9  # every delivery outcome registered

    stats.emitted = 7
    stats.emit_failed = 2
    stats.presence_skipped = 1
    stats.dedup_skipped = 3
    stats.admission_rejected = 4
    stats.untenanted_dropped = 2
    stats.mailboxed = 5
    stats.bridge_failed = 6
    stats.poisoned = 1

    assert _scrape(meter, "forze.realtime.gateway.emitted") == 7
    assert _scrape(meter, "forze.realtime.gateway.emit_failed") == 2
    assert _scrape(meter, "forze.realtime.gateway.presence_skipped") == 1
    assert _scrape(meter, "forze.realtime.gateway.dedup_skipped") == 3
    assert _scrape(meter, "forze.realtime.gateway.admission_rejected") == 4
    assert _scrape(meter, "forze.realtime.gateway.untenanted_dropped") == 2
    assert _scrape(meter, "forze.realtime.gateway.mailboxed") == 5
    assert _scrape(meter, "forze.realtime.gateway.bridge_failed") == 6
    assert _scrape(meter, "forze.realtime.gateway.poisoned") == 1


def test_backplane_instrument_reflects_health() -> None:
    health = BackplaneHealth()
    meter = _StubMeter()

    instrument_realtime_backplane(health, meter=meter)  # pyright: ignore[reportArgumentType]

    assert _scrape(meter, "forze.realtime.backplane.seconds_since_ok") == -1.0
    assert _scrape(meter, "forze.realtime.backplane.consecutive_failures") == 0.0

    health.failed()
    health.ok()
    health.failed()

    assert _scrape(meter, "forze.realtime.backplane.seconds_since_ok") >= 0.0
    assert _scrape(meter, "forze.realtime.backplane.consecutive_failures") == 1.0


def _series(meter: _StubMeter, name: str) -> dict[tuple[str, ...], float]:
    """Every observation of *name*, keyed by its labels other than the channel."""

    return {
        tuple(
            value
            for key, value in sorted(observation.attributes.items())
            if key != "forze.realtime.channel"
        ): observation.value
        for observation in meter.callbacks[name](None)
    }


def test_connection_instruments_reflect_the_stats_by_label() -> None:
    stats = RealtimeConnectionStats()
    meter = _StubMeter()

    instrument_realtime_connections(stats, meter=meter)  # pyright: ignore[reportArgumentType]

    stats.connected("/")
    stats.connected("/")
    stats.connected("/chat")
    stats.disconnected("/")
    stats.refused("/", "draining")
    stats.refused("/", "draining")
    stats.refused("/chat", "auth")
    stats.drained(SpreadOutcome(closed=3, failed=1, timed_out=0))

    assert _series(meter, "forze.realtime.connections") == {("/",): 1, ("/chat",): 1}
    assert _series(meter, "forze.realtime.connect_refused") == {
        ("/", "draining"): 2,
        ("/chat", "auth"): 1,
    }
    assert _series(meter, "forze.realtime.drain_closed") == {
        ("closed",): 3,
        ("failed",): 1,
        ("timed_out",): 0,
    }


def test_a_scrape_from_another_thread_never_sees_a_dictionary_change_under_it() -> None:
    """The Prometheus route collects in a worker thread while connects mutate the stats."""

    stats = RealtimeConnectionStats()
    meter = _StubMeter()
    instrument_realtime_connections(stats, meter=meter)  # pyright: ignore[reportArgumentType]

    for index in range(20_000):
        stats.connected(f"/ns-{index}")

    errors: list[BaseException] = []
    stop = threading.Event()

    def scrape() -> None:
        while not stop.is_set():
            for name in (
                "forze.realtime.connections",
                "forze.realtime.connect_refused",
                "forze.realtime.drain_closed",
            ):
                try:
                    list(meter.callbacks[name](None))

                except RuntimeError as error:  # "dictionary changed size during iteration"
                    errors.append(error)

    scraper = threading.Thread(target=scrape)
    scraper.start()

    try:
        for index in range(20_000, 120_000):
            stats.connected(f"/ns-{index}")
            stats.refused(f"/ns-{index}", "draining")

    finally:
        stop.set()
        scraper.join(timeout=10)

    assert errors == []
