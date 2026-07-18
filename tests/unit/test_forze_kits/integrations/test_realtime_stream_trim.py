"""The realtime stream-trim lifecycle step — periodic, supervised, drainable.

# covers: forze_kits.integrations.realtime.realtime_stream_trim_lifecycle_step

The trim semantics themselves are the port's (``test_mock_stream_retention.py`` /
the Redis integration leg); this proves the step drives them on an interval, registers as a
drainable, and stops between ticks instead of being cancelled mid-sweep.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.stream import (
    AckStreamGroupAdminDepKey,
    AckStreamGroupQueryDepKey,
    StreamCommandDepKey,
    StreamQueryDepKey,
)
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_kits.integrations.realtime import (
    realtime_stream_trim_lifecycle_step,
    realtime_stream_spec,
)
from forze_mock import MockDepsModule

# ----------------------- #


async def test_step_trims_on_an_interval_and_stops_cleanly() -> None:
    spec = realtime_stream_spec()
    step = realtime_stream_trim_lifecycle_step(
        stream_spec=spec, interval=timedelta(milliseconds=10), jitter=0.0
    )

    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()

        admin = ctx.deps.resolve_configurable(ctx, AckStreamGroupAdminDepKey, spec, route=spec.name)
        command = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, spec, route=spec.name)
        group = ctx.deps.resolve_configurable(ctx, AckStreamGroupQueryDepKey, spec, route=spec.name)
        query = ctx.deps.resolve_configurable(ctx, StreamQueryDepKey, spec, route=spec.name)

        await admin.ensure_group("gw", str(spec.name), start_id="0")

        signal = RealtimeSignal.of(Audience.topic("t"), "e", {})
        for _ in range(3):
            await command.append(str(spec.name), signal)

        delivered = await group.read("gw", "c1", {str(spec.name): ">"})
        await group.ack(group="gw", stream=str(spec.name), ids=[m.id for m in delivered])

        await step.startup(ctx)
        assert step.startup in ctx.drainables.loops  # type: ignore[comparison-overlap]

        waited = 0.0
        while await query.read({str(spec.name): "0"}) and waited < 5.0:
            await asyncio.sleep(0.01)
            waited += 0.01

        assert await query.read({str(spec.name): "0"}) == []  # the sweep trimmed the acked prefix

        await step.shutdown(ctx)

        task = step.startup.task  # type: ignore[attr-defined]
        assert task is not None and task.done() and not task.cancelled()  # stopped between ticks
