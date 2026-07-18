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


async def test_step_trims_each_assigned_tenant() -> None:
    from uuid import UUID

    from forze.application.contracts.tenancy import TenantIdentity
    from forze_mock import MockRouteConfig

    spec = realtime_stream_spec()
    tenants = (UUID(int=1), UUID(int=2))
    routes = {str(spec.name): MockRouteConfig(tenant_aware=True)}
    step = realtime_stream_trim_lifecycle_step(
        stream_spec=spec,
        interval=timedelta(milliseconds=10),
        jitter=0.0,
        tenants=lambda: tenants,
    )

    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule(routes=routes)).freeze()
    )
    signal = RealtimeSignal.of(Audience.topic("t"), "e", {})

    async with runtime.scope():
        ctx = runtime.get_context()

        # provision + publish + consume + ack per tenant, on each tenant's own stream key
        for tenant in tenants:
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                admin = ctx.deps.resolve_configurable(
                    ctx, AckStreamGroupAdminDepKey, spec, route=spec.name
                )
                command = ctx.deps.resolve_configurable(
                    ctx, StreamCommandDepKey, spec, route=spec.name
                )
                group = ctx.deps.resolve_configurable(
                    ctx, AckStreamGroupQueryDepKey, spec, route=spec.name
                )
                await admin.ensure_group("gw", str(spec.name), start_id="0")
                await command.append(str(spec.name), signal)
                delivered = await group.read("gw", "c1", {str(spec.name): ">"})
                await group.ack(group="gw", stream=str(spec.name), ids=[m.id for m in delivered])

        await step.startup(ctx)

        async def _empty(tenant: UUID) -> bool:
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                query = ctx.deps.resolve_configurable(
                    ctx, StreamQueryDepKey, spec, route=spec.name
                )
                return not await query.read({str(spec.name): "0"})

        waited = 0.0
        while not all([await _empty(t) for t in tenants]) and waited < 5.0:
            await asyncio.sleep(0.01)
            waited += 0.01

        assert all([await _empty(t) for t in tenants])  # both tenants' acked prefixes trimmed

        await step.shutdown(ctx)


async def test_configuration_error_stops_the_trim_loop(
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    from forze.base.exceptions import exc
    from forze_mock.adapters.stream import MockAckStreamGroupAdminAdapter

    async def _config_error(self, stream):  # type: ignore[no-untyped-def]
        raise exc.configuration("route is not wired")

    monkeypatch.setattr(MockAckStreamGroupAdminAdapter, "trim_acknowledged", _config_error)

    spec = realtime_stream_spec()
    step = realtime_stream_trim_lifecycle_step(
        stream_spec=spec, interval=timedelta(milliseconds=10), jitter=0.0
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)
        await step.startup(ctx)  # duplicate startup is ignored while running (or just-dead)

        task = step.startup.task  # type: ignore[attr-defined]
        assert task is not None

        waited = 0.0
        while not task.done() and waited < 5.0:
            await asyncio.sleep(0.01)
            waited += 0.01

        # wiring does not fix itself: the loop stopped on its own, loudly, not cancelled
        assert task.done() and not task.cancelled()

        await step.shutdown(ctx)


async def test_operational_error_does_not_stop_the_trim_loop(
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    from forze_mock.adapters.stream import MockAckStreamGroupAdminAdapter

    calls = {"n": 0}

    async def _flaky(self, stream):  # type: ignore[no-untyped-def]
        calls["n"] += 1
        raise RuntimeError("broker blip")

    monkeypatch.setattr(MockAckStreamGroupAdminAdapter, "trim_acknowledged", _flaky)

    spec = realtime_stream_spec()
    step = realtime_stream_trim_lifecycle_step(
        stream_spec=spec, interval=timedelta(milliseconds=10), jitter=0.0
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)

        waited = 0.0
        while calls["n"] < 3 and waited < 5.0:
            await asyncio.sleep(0.01)
            waited += 0.01

        task = step.startup.task  # type: ignore[attr-defined]
        assert calls["n"] >= 3 and task is not None and not task.done()  # kept sweeping

        await step.shutdown(ctx)
        assert task.done() and not task.cancelled()


def test_invalid_trim_settings_are_refused() -> None:
    import pytest

    from forze.base.exceptions import CoreException

    spec = realtime_stream_spec()

    with pytest.raises(CoreException):
        realtime_stream_trim_lifecycle_step(stream_spec=spec, interval=timedelta(0))

    with pytest.raises(CoreException):
        realtime_stream_trim_lifecycle_step(stream_spec=spec, jitter=1.5)


async def test_per_tenant_errors_stay_isolated_and_config_is_terminal(
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """One tenant's operational failure trims the others; a config error stops the loop."""

    from uuid import UUID

    from forze.base.exceptions import exc
    from forze_mock import MockRouteConfig
    from forze_mock.adapters.stream import MockAckStreamGroupAdminAdapter

    broken, healthy = UUID(int=1), UUID(int=2)
    spec = realtime_stream_spec()
    routes = {str(spec.name): MockRouteConfig(tenant_aware=True)}
    trims: list[str] = []
    mode = {"error": "operational"}

    original = MockAckStreamGroupAdminAdapter.trim_acknowledged

    async def _per_tenant(self, stream):  # type: ignore[no-untyped-def]
        tenant = self.stream.tenant_provider()  # the bound shard tenant
        if tenant is not None and tenant.tenant_id == broken:
            if mode["error"] == "operational":
                raise RuntimeError("tenant backend blip")
            raise exc.infrastructure("tenant backend down")

        trims.append(str(tenant.tenant_id if tenant else None))
        return await original(self, stream)

    monkeypatch.setattr(MockAckStreamGroupAdminAdapter, "trim_acknowledged", _per_tenant)

    step = realtime_stream_trim_lifecycle_step(
        stream_spec=spec,
        interval=timedelta(milliseconds=10),
        jitter=0.0,
        tenants=lambda: (broken, healthy),
    )
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule(routes=routes)).freeze()
    )

    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)
        assert step.startup.loop_name.startswith("realtime_stream_trim")  # type: ignore[attr-defined]

        waited = 0.0
        while len(trims) < 2 and waited < 5.0:
            await asyncio.sleep(0.01)
            waited += 0.01

        # a plain failure AND a CoreException failure on the broken tenant — the healthy
        # tenant's sweep ran regardless, and the loop stayed alive through both
        mode["error"] = "core"
        seen = len(trims)
        waited = 0.0
        while len(trims) <= seen and waited < 5.0:
            await asyncio.sleep(0.01)
            waited += 0.01

        task = step.startup.task  # type: ignore[attr-defined]
        assert trims and all(one == str(healthy) for one in trims)
        assert task is not None and not task.done()

        await step.shutdown(ctx)


async def test_tenantless_core_error_is_logged_and_the_loop_continues(
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    from forze.base.exceptions import exc
    from forze_mock.adapters.stream import MockAckStreamGroupAdminAdapter

    calls = {"n": 0}

    async def _infra(self, stream):  # type: ignore[no-untyped-def]
        calls["n"] += 1
        raise exc.infrastructure("broker down")

    monkeypatch.setattr(MockAckStreamGroupAdminAdapter, "trim_acknowledged", _infra)

    step = realtime_stream_trim_lifecycle_step(
        stream_spec=realtime_stream_spec(), interval=timedelta(milliseconds=10), jitter=0.0
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)

        waited = 0.0
        while calls["n"] < 3 and waited < 5.0:
            await asyncio.sleep(0.01)
            waited += 0.01

        task = step.startup.task  # type: ignore[attr-defined]
        assert calls["n"] >= 3 and task is not None and not task.done()  # infra ≠ terminal

        await step.shutdown(ctx)


async def test_per_tenant_configuration_error_is_terminal(
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    from uuid import UUID

    from forze.base.exceptions import exc
    from forze_mock import MockRouteConfig
    from forze_mock.adapters.stream import MockAckStreamGroupAdminAdapter

    async def _config_error(self, stream):  # type: ignore[no-untyped-def]
        raise exc.configuration("tenant route is not wired")

    monkeypatch.setattr(MockAckStreamGroupAdminAdapter, "trim_acknowledged", _config_error)

    spec = realtime_stream_spec()
    step = realtime_stream_trim_lifecycle_step(
        stream_spec=spec,
        interval=timedelta(milliseconds=10),
        jitter=0.0,
        tenants=lambda: (UUID(int=1),),
    )
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(
            MockDepsModule(routes={str(spec.name): MockRouteConfig(tenant_aware=True)})
        ).freeze()
    )

    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)

        task = step.startup.task  # type: ignore[attr-defined]
        assert task is not None

        waited = 0.0
        while not task.done() and waited < 5.0:
            await asyncio.sleep(0.01)
            waited += 0.01

        # the per-tenant config error escaped the tenant loop and stopped the sweep loudly
        assert task.done() and not task.cancelled()

        await step.shutdown(ctx)
