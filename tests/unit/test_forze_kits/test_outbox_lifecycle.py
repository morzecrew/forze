"""Unit tests for outbox background relay lifecycle."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxRelayResult, OutboxSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import (
    OutboxRelay,
    outbox_relay_background_lifecycle_step,
)
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

    with patch.object(OutboxRelay, "to_queue", relay_mock):
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

    with patch.object(OutboxRelay, "to_queue", relay_mock):
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
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze()
    )

    # autospec passes ``self`` → each batch builds a fresh OutboxRelay carrying that
    # batch's reclaim policy (first batch reclaims, the rest do not).
    with patch.object(OutboxRelay, "to_queue", autospec=True) as relay_mock:
        relay_mock.side_effect = [_result(10), _result(10), _result(0)]
        async with runtime.scope():
            await startup._relay_once(runtime.get_context())

    reclaims = [call.args[0].reclaim_stale_after for call in relay_mock.call_args_list]
    assert reclaims == [reclaim, None, None]


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


# ----------------------- #
# Tenant-sharded drain (namespace-tier outbox)


_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


async def _drain_capturing_tenants(startup: _OutboxRelayBackgroundStartup) -> list[UUID | None]:
    """Run one drain tick with ``to_queue`` mocked to record the bound tenant per pass."""

    seen: list[UUID | None] = []

    async def _capture(self: Any, ctx: Any, queue_spec: Any, *, limit: Any = None) -> OutboxRelayResult:
        tenant = ctx.inv_ctx.get_tenant()
        seen.append(tenant.tenant_id if tenant is not None else None)
        return _result(0)

    frozen = list(startup.tenants()) if startup.tenants is not None else None
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
    with patch.object(OutboxRelay, "to_queue", autospec=True, side_effect=_capture):
        async with runtime.scope():
            await startup._drain_tick(runtime.get_context(), frozen)

    return seen


@pytest.mark.asyncio
async def test_drain_tick_relays_each_assigned_tenant_bound() -> None:
    seen = await _drain_capturing_tenants(_startup(tenants=lambda: [_T1, _T2]))

    assert seen == [_T1, _T2]  # one pass per assigned tenant, each bound, in shard order


@pytest.mark.asyncio
async def test_drain_tick_without_tenants_runs_one_global_pass() -> None:
    seen = await _drain_capturing_tenants(_startup(tenants=None))

    assert seen == [None]  # tenant-global outbox: a single unbound pass


@pytest.mark.asyncio
async def test_drain_tick_isolates_a_failing_tenant() -> None:
    startup = _startup(tenants=lambda: [_T1, _T2])
    seen: list[UUID] = []

    async def _once(self: Any, ctx: Any) -> None:
        tenant = ctx.inv_ctx.get_tenant().tenant_id
        seen.append(tenant)
        if tenant == _T1:
            raise RuntimeError("boom")

    logger_mock = MagicMock()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    with patch.object(_OutboxRelayBackgroundStartup, "_relay_once", autospec=True, side_effect=_once):
        with patch("forze_kits.integrations.outbox.lifecycle.logger", logger_mock):
            async with runtime.scope():
                await startup._drain_tick(runtime.get_context(), [_T1, _T2])

    assert seen == [_T1, _T2]  # T1 failed but T2 still drained this tick
    logger_mock.exception.assert_called_once()  # the failing tenant was logged
