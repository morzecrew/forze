"""Unit tests for outbox background relay lifecycle."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import outbox_relay_background_lifecycle_step
from forze_kits.integrations.outbox.lifecycle import _OutboxRelayBackgroundStartup
from forze_mock import MockDepsModule


class _Payload(BaseModel):
    x: int


@pytest.mark.asyncio
async def test_background_lifecycle_starts_and_stops_task() -> None:
    codec = PydanticModelCodec(_Payload)
    outbox_spec = OutboxSpec(name="events", codec=codec)
    queue_spec = QueueSpec(name="jobs", codec=codec)
    step = outbox_relay_background_lifecycle_step(
        outbox_spec=outbox_spec,
        queue_spec=queue_spec,
        interval=timedelta(hours=1),
        reclaim_stale_after=None,
    )

    relay_mock = AsyncMock()
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze()
    )

    with patch(
        "forze_kits.integrations.outbox.lifecycle.relay_outbox_to_queue",
        relay_mock,
    ):
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)
            startup = step.startup
            assert isinstance(startup, _OutboxRelayBackgroundStartup)
            assert startup.task is not None
            await asyncio.sleep(0.05)
            await step.shutdown(ctx)

    relay_mock.assert_called()
    assert startup.task.done()
