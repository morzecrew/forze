"""Unit tests for :func:`~forze_kits.integrations.outbox.relay_outbox_to_pubsub`."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.pubsub import PubSubSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import relay_outbox_to_pubsub
from forze_mock import MockDepsModule, MockStateDepKey


class _EventPayload(BaseModel):
    n: int


@pytest.mark.asyncio
async def test_relay_publishes_to_topic() -> None:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.pubsub(route="live", channel="projects"),
    )
    pubsub_spec = PubSubSpec(name="live", codec=codec)

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)
        await ctx.outbox.command(outbox_spec).stage(
            "project.created", _EventPayload(n=5)
        )
        await ctx.outbox.command(outbox_spec).flush()

        result = await relay_outbox_to_pubsub(
            ctx,
            outbox_spec=outbox_spec,
            pubsub_spec=pubsub_spec,
            reclaim_stale_after=None,
        )

        assert result.published == 1
        messages = state.pubsub_logs["live"]["projects"]
        assert len(messages) == 1
        assert messages[0].payload.n == 5
        assert messages[0].type == "project.created"


@pytest.mark.asyncio
async def test_relay_pubsub_missing_destination_raises() -> None:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(name="events", codec=codec)
    pubsub_spec = PubSubSpec(name="live", codec=codec)

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(Exception, match="destination is required"):
            await relay_outbox_to_pubsub(
                ctx,
                outbox_spec=outbox_spec,
                pubsub_spec=pubsub_spec,
                reclaim_stale_after=None,
            )
