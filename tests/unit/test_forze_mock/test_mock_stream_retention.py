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
