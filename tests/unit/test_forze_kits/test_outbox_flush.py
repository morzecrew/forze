"""Unit tests for outbox flush operation-plan factory."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze_kits.outbox import outbox_flush_tx_on_success_factory
from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.serialization import PydanticRecordMappingCodec
from forze_mock import MockDepsModule, MockStateDepKey


class _Payload(BaseModel):
    x: int


@pytest.mark.asyncio
async def test_outbox_flush_factory_flushes_staged_rows() -> None:
    spec = OutboxSpec(name="events", codec=PydanticRecordMappingCodec(_Payload))
    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module))

    async with runtime.scope():
        ctx = runtime.get_context()
        await ctx.outbox.command(spec).stage("t", _Payload(x=1))
        hook = outbox_flush_tx_on_success_factory(spec)(ctx)
        await hook(0, 0)

        state = ctx.deps.provide(MockStateDepKey)
        assert len(state.outbox_rows["events"]) == 1
