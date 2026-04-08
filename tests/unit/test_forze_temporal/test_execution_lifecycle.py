"""Unit tests for Temporal execution lifecycle and deps wiring."""

from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("temporalio")

from forze.application.contracts.workflow import WorkflowCommandDepKey, WorkflowQueryDepKey
from forze.application.execution import ExecutionContext

from forze_temporal.execution.deps import TemporalClientDepKey, TemporalDepsModule
from forze_temporal.execution.lifecycle import TemporalStartupHook, temporal_lifecycle_step
from forze_temporal.kernel.platform import TemporalClient, TemporalConfig


@pytest.mark.asyncio
async def test_temporal_startup_hook_initializes_client() -> None:
    client = MagicMock(spec=TemporalClient)
    client.initialize = AsyncMock()

    hook = TemporalStartupHook(host="localhost:7233", config=TemporalConfig(namespace="default"))
    ctx = MagicMock(spec=ExecutionContext)
    ctx.dep = MagicMock(return_value=client)

    await hook(ctx)

    ctx.dep.assert_called_once_with(TemporalClientDepKey)
    client.initialize.assert_awaited_once_with("localhost:7233", config=hook.config)


def test_temporal_lifecycle_step_exposes_startup_hook() -> None:
    step = temporal_lifecycle_step(host="127.0.0.1:7233", name="t1")
    assert step.name == "t1"
    assert step.startup is not None
    assert isinstance(step.startup, TemporalStartupHook)
    assert step.startup.host == "127.0.0.1:7233"


def test_temporal_deps_module_registers_client_and_empty_workflow_routes() -> None:
    client = TemporalClient()
    module = TemporalDepsModule(client=client)

    deps = module()

    assert deps.plain_deps[TemporalClientDepKey] is client
    assert WorkflowCommandDepKey not in deps.routed_deps
    assert WorkflowQueryDepKey not in deps.routed_deps


def test_temporal_deps_module_merges_workflow_configs() -> None:
    from forze_temporal.execution.deps.configs import TemporalWorkflowConfig

    client = TemporalClient()
    wf_cfg: TemporalWorkflowConfig = {"queue": "q1", "tenant_aware": True}
    module = TemporalDepsModule(
        client=client,
        workflows={"MyWorkflow": wf_cfg},
    )

    deps = module()

    assert WorkflowCommandDepKey in deps.routed_deps
    assert WorkflowQueryDepKey in deps.routed_deps
    assert "MyWorkflow" in deps.routed_deps[WorkflowCommandDepKey]
    assert "MyWorkflow" in deps.routed_deps[WorkflowQueryDepKey]
