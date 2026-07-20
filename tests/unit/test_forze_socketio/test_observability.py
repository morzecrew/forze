"""Gateway/backplane observability — counters, gauges, and the health snapshot.

# covers: forze_socketio.observability

The instruments register observable callbacks against a meter; a stub meter captures them
so each callback can be invoked directly and asserted to reflect the live stats/health —
the same scrape path OTel takes, minus the SDK.
"""

from __future__ import annotations

from typing import Any

from forze_socketio import (
    BackplaneHealth,
    RealtimeGatewayStats,
    instrument_realtime_backplane,
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
