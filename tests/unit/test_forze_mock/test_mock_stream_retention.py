"""Stream retention cap — the mock mirrors ``XADD MAXLEN ~`` eviction.

# covers: forze_mock.adapters.stream.MockStreamAdapter.max_entries

Retention is a route-config concern (``MockRouteConfig.stream_retention_max_entries`` /
``RedisStreamConfig.retention_max_entries``); the adapter applies it at every append by
evicting the oldest entries. The second test pins the deliberate sharp edge: an entry
trimmed before its consumer group ever read it is gone — the same loss surface real Redis
has — so the cap must be sized against the delivery horizon, not treated as harmless.
"""

from __future__ import annotations

from pydantic import BaseModel

from forze.application.contracts.stream import (
    AckStreamGroupAdminDepKey,
    AckStreamGroupQueryDepKey,
    StreamCommandDepKey,
    StreamQueryDepKey,
    StreamSpec,
)
from forze.application.execution import ExecutionRuntime
from forze.application.execution.deps import DepsRegistry
from forze.base.serialization import PydanticModelCodec
from forze_mock import MockDepsModule, MockRouteConfig

# ----------------------- #


class _Event(BaseModel):
    seq: int


_SPEC = StreamSpec(name="capped", codec=PydanticModelCodec(model_type=_Event))
_ROUTES = {"capped": MockRouteConfig(stream_retention_max_entries=3)}


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule(routes=_ROUTES)).freeze()
    )


# ----------------------- #


async def test_append_evicts_oldest_beyond_cap() -> None:
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        command = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, _SPEC, route="capped")
        query = ctx.deps.resolve_configurable(ctx, StreamQueryDepKey, _SPEC, route="capped")

        for seq in range(5):
            await command.append("capped", _Event(seq=seq))

        messages = await query.read({"capped": "0"})

        assert [m.payload.seq for m in messages] == [2, 3, 4]  # 0 and 1 evicted


# ....................... #


async def test_trimmed_undelivered_entry_is_lost_to_the_group() -> None:
    """The documented sharp edge: trim outruns an idle consumer group → oldest are gone."""

    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        command = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, _SPEC, route="capped")
        admin = ctx.deps.resolve_configurable(
            ctx, AckStreamGroupAdminDepKey, _SPEC, route="capped"
        )
        group = ctx.deps.resolve_configurable(
            ctx, AckStreamGroupQueryDepKey, _SPEC, route="capped"
        )

        await admin.ensure_group("g", "capped", start_id="0")

        for seq in range(5):  # all appended AFTER group creation, none delivered yet
            await command.append("capped", _Event(seq=seq))

        messages = await group.read("g", "consumer", {"capped": ">"})

        assert [m.payload.seq for m in messages] == [2, 3, 4]  # 0 and 1 lost to the trim


# ----------------------- #
# trim_acknowledged — the precise (group-floor) companion to the blunt cap


_UNCAPPED = StreamSpec(name="floor", codec=PydanticModelCodec(model_type=_Event))


def _floor_runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


async def _floor_ports(ctx):  # type: ignore[no-untyped-def]
    command = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, _UNCAPPED, route="floor")
    query = ctx.deps.resolve_configurable(ctx, StreamQueryDepKey, _UNCAPPED, route="floor")
    group = ctx.deps.resolve_configurable(
        ctx, AckStreamGroupQueryDepKey, _UNCAPPED, route="floor"
    )
    admin = ctx.deps.resolve_configurable(
        ctx, AckStreamGroupAdminDepKey, _UNCAPPED, route="floor"
    )
    return command, query, group, admin


async def test_trim_acknowledged_removes_only_the_acked_prefix() -> None:
    runtime = _floor_runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        command, query, group, admin = await _floor_ports(ctx)

        await admin.ensure_group("g", "floor", start_id="0")
        for seq in range(5):
            await command.append("floor", _Event(seq=seq))

        delivered = await group.read("g", "c1", {"floor": ">"})
        await group.ack(group="g", stream="floor", ids=[m.id for m in delivered[:2]])

        trimmed = await admin.trim_acknowledged("floor")
        survivors = [m.payload.seq for m in await query.read({"floor": "0"})]

        # the two acked entries went; the three pending ones hold the floor
        assert trimmed == 2
        assert survivors == [2, 3, 4]

        # acking the rest moves the floor past everything delivered
        await group.ack(group="g", stream="floor", ids=[m.id for m in delivered[2:]])
        assert await admin.trim_acknowledged("floor") == 3
        assert await query.read({"floor": "0"}) == []


async def test_trim_acknowledged_never_touches_undelivered_backlog() -> None:
    runtime = _floor_runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        command, query, group, admin = await _floor_ports(ctx)

        await admin.ensure_group("g", "floor", start_id="0")
        for seq in range(3):
            await command.append("floor", _Event(seq=seq))

        # nothing delivered yet — the whole stream is backlog, nothing may go
        assert await admin.trim_acknowledged("floor") == 0
        assert len(await query.read({"floor": "0"})) == 3


async def test_trim_acknowledged_without_groups_is_a_refusal() -> None:
    runtime = _floor_runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        command, query, _group, admin = await _floor_ports(ctx)

        for seq in range(3):
            await command.append("floor", _Event(seq=seq))

        # no group = no horizon to trust — a group-less stream is never trimmed
        assert await admin.trim_acknowledged("floor") == 0
        assert len(await query.read({"floor": "0"})) == 3


async def test_slowest_group_holds_the_floor() -> None:
    runtime = _floor_runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        command, query, group, admin = await _floor_ports(ctx)

        await admin.ensure_group("fast", "floor", start_id="0")
        await admin.ensure_group("slow", "floor", start_id="0")
        for seq in range(4):
            await command.append("floor", _Event(seq=seq))

        # the fast group consumes and acks everything; the slow one never reads
        delivered = await group.read("fast", "c1", {"floor": ">"})
        await group.ack(group="fast", stream="floor", ids=[m.id for m in delivered])

        assert await admin.trim_acknowledged("floor") == 0  # slow's backlog holds the floor
        assert len(await query.read({"floor": "0"})) == 4
