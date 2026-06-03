"""Unit tests for :func:`~forze_kits.integrations.outbox.relay_outbox_to_stream`."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.stream import StreamSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import relay_outbox_to_stream
from forze_mock import MockDepsModule, MockStateDepKey


class _EventPayload(BaseModel):
    n: int


@pytest.mark.asyncio
async def test_relay_appends_to_stream() -> None:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.stream(route="audit", channel="audit"),
    )
    stream_spec = StreamSpec(name="audit", codec=codec)

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)
        await ctx.outbox.command(outbox_spec).stage(
            "project.created", _EventPayload(n=3)
        )
        await ctx.outbox.command(outbox_spec).flush()

        result = await relay_outbox_to_stream(
            ctx,
            outbox_spec=outbox_spec,
            stream_spec=stream_spec,
            reclaim_stale_after=None,
        )

        assert result.published == 1
        messages = state.streams["audit"]["audit"]
        assert len(messages) == 1
        assert messages[0].payload.n == 3
        assert messages[0].type == "project.created"
        assert messages[0].key is not None


@pytest.mark.asyncio
async def test_relay_stream_wrong_spec_route_raises() -> None:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.stream(route="audit", channel="audit"),
    )
    stream_spec = StreamSpec(name="other", codec=codec)

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(Exception, match="spec.name must match"):
            await relay_outbox_to_stream(
                ctx,
                outbox_spec=outbox_spec,
                stream_spec=stream_spec,
                reclaim_stale_after=None,
            )
