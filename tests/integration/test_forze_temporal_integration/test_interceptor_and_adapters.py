"""Integration tests: ``ExecutionContextInterceptor`` + workflow adapters on time-skipping env."""

import pytest

pytest.importorskip("temporalio")

from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from forze.application.contracts.workflow import WorkflowHandle, WorkflowSpec
from forze.application.contracts.workflow.specs import WorkflowInvokeSpec
from forze.application.contracts.authn import AuthnIdentity
from forze.application.execution import CallContext, ExecutionContext
from forze.application.execution.deps import Deps
from forze.base.primitives import uuid7
from forze_temporal.adapters.workflow import (
    TemporalWorkflowCommandAdapter,
    TemporalWorkflowQueryAdapter,
)
from forze_temporal.interceptors.context import ExecutionContextInterceptor
from forze_temporal.kernel.platform.client import TemporalClient

from ._workflow_defs import (
    CTX_BOX,
    ItContextProbeWorkflow,
    ItSumWorkflow,
    SumIn,
    SumOut,
    it_read_correlation,
    it_sum_pair,
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execution_context_interceptor_propagates_correlation_to_activity() -> None:
    """Inbound activity interceptor binds :class:`ExecutionContext` from Temporal headers."""

    fixed = uuid7()
    deps = Deps.plain({})
    exec_ctx = ExecutionContext(deps=deps)
    CTX_BOX["exec"] = exec_ctx
    try:
        eci = ExecutionContextInterceptor(ctx_dep=lambda: exec_ctx)
        env = await WorkflowEnvironment.start_time_skipping(
            data_converter=pydantic_data_converter,
            interceptors=[eci],
        )
        try:
            task_queue = "it-forze-ctx-probe"
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[ItContextProbeWorkflow],
                activities=[it_read_correlation],
            ):
                with exec_ctx.bind_call(
                    call=CallContext(
                        execution_id=uuid7(),
                        correlation_id=fixed,
                        causation_id=None,
                    ),
                    identity=AuthnIdentity(principal_id=uuid7()),
                ):
                    handle = await env.client.start_workflow(
                        ItContextProbeWorkflow.run,
                        id=f"ctx-probe-{fixed}",
                        task_queue=task_queue,
                    )
                    out = await handle.result()

            assert out == str(fixed)
        finally:
            await env.shutdown()
    finally:
        CTX_BOX["exec"] = None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_temporal_workflow_adapters_end_to_end() -> None:
    """``TemporalWorkflowCommandAdapter`` / ``TemporalWorkflowQueryAdapter`` against a live client."""

    deps = Deps.plain({})
    exec_ctx = ExecutionContext(deps=deps)
    CTX_BOX["exec"] = exec_ctx
    try:
        eci = ExecutionContextInterceptor(ctx_dep=lambda: exec_ctx)
        env = await WorkflowEnvironment.start_time_skipping(
            data_converter=pydantic_data_converter,
            interceptors=[eci],
        )
        try:
            temporal = TemporalClient()
            object.__setattr__(temporal, "_TemporalClient__client", env.client)

            spec = WorkflowSpec(
                name="ItSumWorkflow",
                run=WorkflowInvokeSpec(args_type=SumIn, return_type=SumOut),
            )
            task_queue = "it-forze-adapter-sum"
            cmd = TemporalWorkflowCommandAdapter(
                client=temporal,
                queue=task_queue,
                spec=spec,
                tenant_aware=False,
            )
            qry = TemporalWorkflowQueryAdapter(
                client=temporal,
                queue=task_queue,
                spec=spec,
                tenant_aware=False,
            )

            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[ItSumWorkflow],
                activities=[it_sum_pair],
            ):
                with exec_ctx.bind_call(
                    call=CallContext(
                        execution_id=uuid7(),
                        correlation_id=uuid7(),
                        causation_id=None,
                    ),
                ):
                    handle: WorkflowHandle = await cmd.start(SumIn(a=40, b=2))
                    out = await qry.result(handle)

            validated = SumOut.model_validate(out)
            assert validated.total == 42
        finally:
            await env.shutdown()
    finally:
        CTX_BOX["exec"] = None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_interceptor_is_worker_and_client_for_sdk_merge() -> None:
    """``ExecutionContextInterceptor`` is both client and worker interceptor (forwarded once)."""

    from temporalio.client import Interceptor as ClientInterceptor
    from temporalio.worker import Interceptor as WorkerInterceptor

    deps = Deps.plain({})
    exec_ctx = ExecutionContext(deps=deps)
    eci = ExecutionContextInterceptor(ctx_dep=lambda: exec_ctx)

    assert isinstance(eci, ClientInterceptor)
    assert isinstance(eci, WorkerInterceptor)
