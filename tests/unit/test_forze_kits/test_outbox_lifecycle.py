"""Unit tests for outbox background relay lifecycle."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxRelayResult, OutboxSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException
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


# ----------------------- #
# Drain-until-empty tick behavior


def _startup(**overrides: Any) -> _OutboxRelayBackgroundStartup:
    codec = PydanticModelCodec(_Payload)
    kwargs: dict[str, Any] = {
        "outbox_spec": OutboxSpec(name="events", codec=codec),
        "transport": "queue",
        "queue_spec": QueueSpec(name="jobs", codec=codec),
        "stream_spec": None,
        "pubsub_spec": None,
        "interval": timedelta(hours=1),
        "reclaim_stale_after": timedelta(minutes=5),
        "limit": 10,
        "max_attempts": 5,
        "retry_base_delay": timedelta(seconds=1),
        "retry_max_backoff": timedelta(minutes=5),
        "max_batches_per_tick": 100,
    }
    kwargs.update(overrides)
    return _OutboxRelayBackgroundStartup(**kwargs)


def _result(claimed: int) -> OutboxRelayResult:
    return OutboxRelayResult(claimed=claimed, published=claimed)


async def _run_tick(
    startup: _OutboxRelayBackgroundStartup,
    relay_mock: AsyncMock,
) -> None:
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze()
    )

    with patch(
        "forze_kits.integrations.outbox.lifecycle.relay_outbox_to_queue",
        relay_mock,
    ):
        async with runtime.scope():
            await startup._relay_once(runtime.get_context())


@pytest.mark.asyncio
async def test_relay_once_drains_backlog_until_short_claim() -> None:
    startup = _startup(limit=10)
    relay_mock = AsyncMock(
        side_effect=[_result(10), _result(10), _result(10), _result(3)]
    )

    await _run_tick(startup, relay_mock)

    # Backlog of 3 full batches drains in one tick: 3 full claims + 1 short.
    assert relay_mock.await_count == 4


@pytest.mark.asyncio
async def test_relay_once_respects_max_batches_per_tick_cap() -> None:
    startup = _startup(limit=10, max_batches_per_tick=5)
    relay_mock = AsyncMock(return_value=_result(10))

    await _run_tick(startup, relay_mock)

    assert relay_mock.await_count == 5


@pytest.mark.asyncio
async def test_relay_once_empty_backlog_claims_exactly_once() -> None:
    startup = _startup(limit=10)
    relay_mock = AsyncMock(return_value=_result(0))

    await _run_tick(startup, relay_mock)

    assert relay_mock.await_count == 1


@pytest.mark.asyncio
async def test_relay_once_reclaims_only_with_first_batch() -> None:
    reclaim = timedelta(minutes=5)
    startup = _startup(limit=10, reclaim_stale_after=reclaim)
    relay_mock = AsyncMock(side_effect=[_result(10), _result(10), _result(0)])

    await _run_tick(startup, relay_mock)

    reclaim_args = [
        call.kwargs["reclaim_stale_after"] for call in relay_mock.call_args_list
    ]
    assert reclaim_args == [reclaim, None, None]


@pytest.mark.asyncio
async def test_relay_once_failing_batch_is_logged_and_tick_continues() -> None:
    startup = _startup(limit=10)
    relay_mock = AsyncMock(side_effect=[RuntimeError("boom"), _result(0)])
    logger_mock = MagicMock()

    with patch("forze_kits.integrations.outbox.lifecycle.logger", logger_mock):
        await _run_tick(startup, relay_mock)

    assert relay_mock.await_count == 2
    logger_mock.exception.assert_called_once()


def test_lifecycle_step_rejects_invalid_options() -> None:
    codec = PydanticModelCodec(_Payload)
    outbox_spec = OutboxSpec(name="events", codec=codec)
    queue_spec = QueueSpec(name="jobs", codec=codec)

    with pytest.raises(CoreException, match="max_attempts"):
        outbox_relay_background_lifecycle_step(
            outbox_spec=outbox_spec,
            queue_spec=queue_spec,
            max_attempts=0,
        )

    with pytest.raises(CoreException, match="retry_base_delay"):
        outbox_relay_background_lifecycle_step(
            outbox_spec=outbox_spec,
            queue_spec=queue_spec,
            retry_base_delay=timedelta(0),
        )

    with pytest.raises(CoreException, match="retry_max_backoff"):
        outbox_relay_background_lifecycle_step(
            outbox_spec=outbox_spec,
            queue_spec=queue_spec,
            retry_base_delay=timedelta(seconds=10),
            retry_max_backoff=timedelta(seconds=1),
        )

    with pytest.raises(CoreException, match="batches per tick"):
        outbox_relay_background_lifecycle_step(
            outbox_spec=outbox_spec,
            queue_spec=queue_spec,
            max_batches_per_tick=0,
        )
