"""The offline-mailbox OTel instrument — callbacks reflect the live store counters, and
the replay drain records one replay-size observation per connect.

# covers: forze_kits.integrations.realtime.observability.instrument_realtime_mailbox
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.application.integrations.realtime import BacklogDrain, iter_backlog
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import (
    build_realtime_cursors,
    build_realtime_mailbox,
    instrument_realtime_mailbox,
    realtime_cursor_spec,
    realtime_mailbox_spec,
)
from forze_mock import MockDepsModule
from tests.support.realtime_retention import UNSWEPT

# ----------------------- #


class _StubMeter:
    def __init__(self) -> None:
        self.callbacks: dict[str, Any] = {}

    def create_observable_counter(self, name: str, *, callbacks: Any, **_: Any) -> None:
        self.callbacks[name] = callbacks[0]

    def create_histogram(self, name: str, **_: Any) -> Any:
        return None  # recorded only by a replay, which these counter tests never run


def _scrape(meter: _StubMeter, name: str) -> float:
    [observation] = meter.callbacks[name](None)
    return observation.value


# ----------------------- #


async def test_instrument_reflects_store_and_cursor_counters() -> None:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        # specs referenced for completeness — the mock serves any route plainly
        _ = realtime_mailbox_spec(), realtime_cursor_spec()

        mailbox = build_realtime_mailbox(ctx, retention=UNSWEPT)
        cursors = build_realtime_cursors(ctx)

        meter = _StubMeter()
        instrument_realtime_mailbox(mailbox, cursors, meter=meter)  # pyright: ignore[reportArgumentType]

        assert _scrape(meter, "forze.realtime.mailbox.stored") == 0

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=UUID(int=1))):
            signal = RealtimeSignal.of(Audience.principal("u1"), "e", {"n": 1})
            await mailbox.store(
                principal="u1",
                event_id="00000000-0000-0000-0000-000000000001",
                hlc=HlcTimestamp(physical_ms=1, logical=0),
                signal=signal,
            )
            await mailbox.read_since(principal="u1", since=None)

        assert _scrape(meter, "forze.realtime.mailbox.stored") == 1
        assert _scrape(meter, "forze.realtime.mailbox.replayed") == 1
        assert _scrape(meter, "forze.realtime.mailbox.trimmed") == 0
        assert _scrape(meter, "forze.realtime.mailbox.acked") == 0


# ....................... #


def _replay_sizes(reader: InMemoryMetricReader) -> list[Any]:
    data = reader.get_metrics_data()

    if data is None:
        return []

    return [
        point
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if metric.name == "forze.realtime.mailbox.replay_size"
        for point in metric.data.data_points
    ]


async def _connect(mailbox: Any, since: HlcTimestamp | None) -> int:
    """One connect's replay, as every transport drains it."""

    return len(
        [
            entry
            async for entry in iter_backlog(
                mailbox, principal="u1", since=since, outcome=BacklogDrain()
            )
        ]
    )


async def test_each_connect_records_one_replay_size() -> None:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        mailbox = build_realtime_mailbox(ctx, retention=UNSWEPT)
        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter("test")

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=UUID(int=1))):
            for n in range(1, 4):
                await mailbox.store(
                    principal="u1",
                    event_id=f"00000000-0000-0000-0000-00000000000{n}",
                    hlc=HlcTimestamp(physical_ms=n, logical=0),
                    signal=RealtimeSignal.of(Audience.principal("u1"), "e", {"n": n}),
                )

            await _connect(mailbox, None)  # not instrumented yet: nothing recorded
            assert mailbox.on_replay is None
            assert _replay_sizes(reader) == []

            instrument_realtime_mailbox(mailbox, build_realtime_cursors(ctx), channel="c1", meter=meter)

            assert await _connect(mailbox, None) == 3  # a device three entries behind
            assert await _connect(mailbox, HlcTimestamp(physical_ms=3, logical=0)) == 0

        [point] = _replay_sizes(reader)
        assert dict(point.attributes) == {"forze.realtime.channel": "c1"}
        assert (point.count, point.sum, point.min, point.max) == (2, 3, 0, 3)
