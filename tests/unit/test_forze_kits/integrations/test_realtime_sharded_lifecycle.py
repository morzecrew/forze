"""The two sharded lifecycle steps read the `RealtimeShard` correctly.

`realtime_tenant_group_ensure_lifecycle_step` and `realtime_tenant_relay_lifecycle_step` are
thin wrappers that pull `stream_spec` / `tenants` / `group` off the shard. These tests pin
that wiring: the ensure step **behaviourally** creates a consumer group on *each assigned
tenant's* stream partition (so a swapped/forgotten shard field would surface), and the relay
step **structurally** forwards the shard's stream + tenants to the background relay (whose
per-tenant drain behaviour is already covered by the relay tests).
"""

from __future__ import annotations

from uuid import UUID

from forze.application.contracts.realtime import (
    Audience,
    RealtimeShard,
    RealtimeSignal,
)
from forze.application.contracts.stream import AckStreamGroupQueryDepKey, StreamCommandDepKey
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_kits.integrations.outbox.lifecycle import _OutboxRelayBackgroundStartup
from forze_kits.integrations.realtime import (
    realtime_outbox_spec,
    realtime_stream_spec,
    realtime_tenant_group_ensure_lifecycle_step,
    realtime_tenant_relay_lifecycle_step,
)
from forze_mock import MockDepsModule, MockRouteConfig

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")

_STREAM = realtime_stream_spec()


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal("u1"), "e", {"text": text})


def _runtime() -> ExecutionRuntime:
    module = MockDepsModule(routes={str(_STREAM.name): MockRouteConfig(tenant_aware=True)})
    return ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())


async def _append(ctx, tenant: UUID, text: str) -> None:  # type: ignore[no-untyped-def]
    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
        cmd = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, _STREAM, route=_STREAM.name)
        await cmd.append(str(_STREAM.name), _signal(text), type="e")


async def _group_read(ctx, tenant: UUID, group: str) -> list[str]:  # type: ignore[no-untyped-def]
    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
        group_q = ctx.deps.resolve_configurable(ctx, AckStreamGroupQueryDepKey, _STREAM, route=_STREAM.name)
        messages = await group_q.read(group, "c", {str(_STREAM.name): ">"})
    return [m.payload.payload["text"] for m in messages]


# ----------------------- #


async def test_tenant_group_ensure_creates_a_group_per_assigned_tenant() -> None:
    shard = RealtimeShard(stream_spec=_STREAM, tenants=[_T1, _T2], group="gw")
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        # one pre-existing entry per tenant, BEFORE any group exists
        await _append(ctx, _T1, "old")
        await _append(ctx, _T2, "old")

        # the step ensures group "gw" at "$" on EACH assigned tenant's partition
        await realtime_tenant_group_ensure_lifecycle_step(shard=shard).startup(ctx)

        # the group exists at "$" per tenant: a read skips the pre-existing "old" (returns []),
        # then delivers a post-ensure entry. Had the group NOT been created on this tenant's
        # partition, the read would auto-create at 0 and surface "old" — which would fail here.
        for tenant, label in ((_T1, "n1"), (_T2, "n2")):
            assert await _group_read(ctx, tenant, "gw") == []  # "old" is before the group
            await _append(ctx, tenant, label)
            assert await _group_read(ctx, tenant, "gw") == [label]  # only the new entry


def test_tenant_relay_step_forwards_the_shard_to_the_background_relay() -> None:
    outbox = realtime_outbox_spec(name="realtime-outbox", stream=str(_STREAM.name))
    shard = RealtimeShard(stream_spec=_STREAM, tenants=[_T1, _T2])

    step = realtime_tenant_relay_lifecycle_step(shard=shard, outbox_spec=outbox)
    startup = step.startup

    assert isinstance(startup, _OutboxRelayBackgroundStartup)
    # the relay's provider returns the shard's fixed snapshot — same tenant set, no drift
    assert startup.tenants is not None
    assert tuple(startup.tenants()) == shard.tenants == (_T1, _T2)
    assert startup.stream_spec is shard.stream_spec
    assert startup.outbox_spec is outbox
    assert startup.transport == "stream"
