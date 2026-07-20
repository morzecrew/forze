"""Tests for :mod:`forze_temporal.execution.lifecycle.pool` schedule bootstrap."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

pytest.importorskip("temporalio")

from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleBootstrap,
    DurableWorkflowScheduleTiming,
)
from forze.application.execution import ExecutionContext
from forze_temporal.execution.deps.configs import TemporalWorkflowConfig
from forze_temporal.execution.deps.keys import (
    TemporalClientDepKey,
    TemporalScheduleBootstrapDepKey,
)
from forze_temporal.execution.lifecycle.pool import (
    TemporalStartupHook,
    _bootstrap_schedules,
    routed_temporal_lifecycle_step,
)
from forze_temporal.kernel.client import TemporalClient


class _Args(BaseModel):
    tenant_id: str = "t1"


@pytest.mark.asyncio
async def test_bootstrap_schedules_noop_when_dep_missing() -> None:
    ctx = MagicMock(spec=ExecutionContext)
    ctx.deps.exists = MagicMock(return_value=False)

    await _bootstrap_schedules(ctx, workflow_configs={"wf": TemporalWorkflowConfig(queue="q")})

    ctx.deps.provide.assert_not_called()


@pytest.mark.asyncio
async def test_bootstrap_schedules_upserts_matching_workflow() -> None:
    timing = DurableWorkflowScheduleTiming(interval=timedelta(minutes=5))
    bootstrap = DurableWorkflowScheduleBootstrap(
        workflow_name="MyWorkflow",
        schedule_id="sched-1",
        default_args=_Args(),
        timing=timing,
    )
    client = MagicMock(spec=TemporalClient)

    ctx = MagicMock(spec=ExecutionContext)
    ctx.deps.exists = MagicMock(return_value=True)
    ctx.deps.provide = MagicMock(
        side_effect=lambda key: {
            TemporalScheduleBootstrapDepKey: [bootstrap],
            TemporalClientDepKey: client,
        }[key],
    )
    ctx.inv_ctx.get_tenant = MagicMock(return_value=None)

    with patch(
        "forze_temporal.execution.lifecycle.pool.TemporalWorkflowScheduleCommandAdapter",
    ) as adapter_cls:
        adapter = MagicMock()
        adapter.upsert = AsyncMock()
        adapter_cls.return_value = adapter

        await _bootstrap_schedules(
            ctx,
            workflow_configs={"MyWorkflow": TemporalWorkflowConfig(queue="task-q")},
        )

    adapter.upsert.assert_awaited_once()


@pytest.mark.asyncio
async def test_startup_hook_with_bootstrap_invokes_schedules() -> None:
    client = MagicMock(spec=TemporalClient)
    client.initialize = AsyncMock()

    hook = TemporalStartupHook(
        host="localhost:7233",
        bootstrap_schedules=True,
        workflow_configs={"wf": TemporalWorkflowConfig(queue="q")},
    )
    ctx = MagicMock(spec=ExecutionContext)
    ctx.deps.provide = MagicMock(return_value=client)
    ctx.deps.exists = MagicMock(return_value=False)

    with patch(
        "forze_temporal.execution.lifecycle.pool._bootstrap_schedules",
        new_callable=AsyncMock,
    ) as bootstrap:
        await hook(ctx)

    client.initialize.assert_awaited_once()
    bootstrap.assert_awaited_once()


def test_routed_temporal_lifecycle_without_bootstrap_uses_base_step() -> None:
    client = MagicMock()

    step = routed_temporal_lifecycle_step(
        client=client,
        bootstrap_schedules=False,
        name="routed",
    )

    assert step.id == "routed"
    assert step.shutdown is not None


@pytest.mark.asyncio
async def test_routed_temporal_lifecycle_with_bootstrap_runs_startup() -> None:
    client = MagicMock()
    client.startup = AsyncMock()

    step = routed_temporal_lifecycle_step(
        client=client,
        bootstrap_schedules=True,
        workflow_configs={"wf": TemporalWorkflowConfig(queue="q")},
    )
    ctx = MagicMock(spec=ExecutionContext)
    ctx.deps.exists = MagicMock(return_value=False)

    with patch(
        "forze_temporal.execution.lifecycle.pool._bootstrap_schedules",
        new_callable=AsyncMock,
    ) as bootstrap:
        await step.startup(ctx)

    client.startup.assert_awaited_once()
    bootstrap.assert_awaited_once()
