"""Unit tests for outbox flush operation-plan factory."""

from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import outbox_flush_tx_on_success_factory
from forze_mock import MockDepsModule, MockStateDepKey


class _Payload(BaseModel):
    x: int


@pytest.mark.asyncio
async def test_outbox_flush_factory_flushes_staged_rows() -> None:
    spec = OutboxSpec(name="events", codec=PydanticModelCodec(_Payload))
    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        await ctx.outbox.command(spec).stage("t", _Payload(x=1))
        hook = outbox_flush_tx_on_success_factory(spec)(ctx)
        await hook(0, 0)

        state = ctx.deps.provide(MockStateDepKey)
        assert len(state.outbox_rows["events"]) == 1


@pytest.mark.asyncio
async def test_outbox_flush_sequential_cycles_in_one_scope_both_persist() -> None:
    """Two stage->flush cycles (per-request tasks) within one runtime scope both persist."""

    spec = OutboxSpec(name="events", codec=PydanticModelCodec(_Payload))
    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        hook = outbox_flush_tx_on_success_factory(spec)(ctx)

        async def _cycle(x: int) -> None:
            await ctx.outbox.command(spec).stage("t", _Payload(x=x))
            await hook(0, 0)

        # Each cycle runs in its own task (fresh contextvars), like requests
        # served within a long-lived lifespan scope.
        await asyncio.create_task(_cycle(1))
        await asyncio.create_task(_cycle(2))

        state = ctx.deps.provide(MockStateDepKey)
        assert len(state.outbox_rows["events"]) == 2
