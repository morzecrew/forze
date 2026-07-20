"""The offline-mailbox OTel instrument — callbacks reflect the live store counters.

# covers: forze_kits.integrations.realtime.observability.instrument_realtime_mailbox
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import (
    build_realtime_cursors,
    build_realtime_mailbox,
    instrument_realtime_mailbox,
    realtime_cursor_spec,
    realtime_mailbox_spec,
)
from forze_mock import MockDepsModule

# ----------------------- #


class _StubMeter:
    def __init__(self) -> None:
        self.callbacks: dict[str, Any] = {}

    def create_observable_counter(self, name: str, *, callbacks: Any, **_: Any) -> None:
        self.callbacks[name] = callbacks[0]


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

        mailbox = build_realtime_mailbox(ctx)
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
