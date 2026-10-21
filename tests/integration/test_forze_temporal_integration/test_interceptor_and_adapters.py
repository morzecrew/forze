"""Integration tests: ``ExecutionContextInterceptor`` + workflow adapters on time-skipping env."""

import pytest

pytest.importorskip("temporalio")

from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.durable.workflow import (
    DurableWorkflowHandle,
    DurableWorkflowRunStatus,
    DurableWorkflowSignalSpec,
    DurableWorkflowSpec,
)
from forze.application.contracts.durable.workflow.specs import DurableWorkflowInvokeSpec
from forze.application.execution import Deps, InvocationMetadata
from forze.base.exceptions import CoreException
from forze.base.primitives import uuid7
from forze_temporal.adapters.workflow import (
    TemporalWorkflowCommandAdapter,
    TemporalWorkflowQueryAdapter,
)
from forze_temporal.interceptors.context import ExecutionContextInterceptor
from forze_temporal.kernel.client.client import TemporalClient
from forze_temporal.sandbox import sandboxed_workflow_runner
from tests.support.execution_context import context_from_deps

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
async def test_execution_context_interceptor_propagates_correlation_to_activity() -> (
    None
):
    """Inbound activity interceptor binds :class:`ExecutionContext` from Temporal headers."""

    fixed = uuid7()
    deps = Deps.plain({})
    exec_ctx = context_from_deps(deps)
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
                workflow_runner=sandboxed_workflow_runner(),
            ):
                with exec_ctx.inv_ctx.bind(
                    metadata=InvocationMetadata(
                        execution_id=uuid7(),
                        correlation_id=fixed,
                        causation_id=None,
                    ),
                    authn=AuthnIdentity(principal_id=uuid7()),
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
    exec_ctx = context_from_deps(deps)
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

            spec = DurableWorkflowSpec(
                name="ItSumWorkflow",
                run=DurableWorkflowInvokeSpec(args_type=SumIn, return_type=SumOut),
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
                workflow_runner=sandboxed_workflow_runner(),
            ):
                with exec_ctx.inv_ctx.bind(
                    metadata=InvocationMetadata(
                        execution_id=uuid7(), correlation_id=uuid7(), causation_id=None
                    ),
                ):
                    handle: DurableWorkflowHandle = await cmd.start(SumIn(a=40, b=2))
                    running = await qry.describe(handle)
                    assert running.status == DurableWorkflowRunStatus.RUNNING
                    assert running.is_terminal is False
                    out = await qry.result(handle)

            validated = SumOut.model_validate(out)
            assert validated.total == 42
            completed = await qry.describe(handle)
            assert completed.status == DurableWorkflowRunStatus.COMPLETED
            assert completed.is_terminal is True
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
    exec_ctx = context_from_deps(deps)
    eci = ExecutionContextInterceptor(ctx_dep=lambda: exec_ctx)

    assert isinstance(eci, ClientInterceptor)
    assert isinstance(eci, WorkerInterceptor)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_missing_or_taken_workflow_id_is_the_callers_error() -> None:
    """A missing workflow id, or a closed run signalled or terminated, is ``not_found``, and a
    taken id is ``conflict``, not raw RPC errors.

    On the time-skipping server, whose ``NOT_FOUND`` carries no failure detail.
    """

    env = await WorkflowEnvironment.start_time_skipping(data_converter=pydantic_data_converter)

    try:
        temporal = TemporalClient()
        object.__setattr__(temporal, "_TemporalClient__client", env.client)
        spec = DurableWorkflowSpec(
            name="ItSumWorkflow",
            run=DurableWorkflowInvokeSpec(args_type=SumIn, return_type=SumOut),
        )
        cmd = TemporalWorkflowCommandAdapter(
            client=temporal, queue="it-forze-missing", spec=spec, tenant_aware=False
        )
        qry = TemporalWorkflowQueryAdapter(
            client=temporal, queue="it-forze-missing", spec=spec, tenant_aware=False
        )
        missing = DurableWorkflowHandle(workflow_id=f"it-missing-{uuid7().hex[:8]}")

        for operation in (
            lambda: cmd.cancel(missing),
            lambda: cmd.terminate(missing),
            lambda: qry.describe(missing),
            lambda: qry.result(missing),
        ):
            with pytest.raises(CoreException) as not_found:
                await operation()

            assert not_found.value.code == "workflow_not_found"

        # Nothing polls the queue, so the first run is still open when the second start comes.
        taken = await cmd.start(SumIn(a=1, b=2), workflow_id=f"it-taken-{uuid7().hex[:8]}")

        with pytest.raises(CoreException) as conflict:
            await cmd.start(SumIn(a=1, b=2), workflow_id=taken.workflow_id)

        assert conflict.value.code == "workflow_already_exists"

        # A closed run gets the same answer as a missing id, though describe still finds it.
        await cmd.terminate(taken)
        ping = DurableWorkflowSignalSpec(name="ping", args_type=SumIn)

        for operation in (
            lambda: cmd.signal(taken, signal=ping, args=SumIn(a=1, b=2)),
            lambda: cmd.terminate(taken),
        ):
            with pytest.raises(CoreException) as closed:
                await operation()

            assert closed.value.code == "workflow_not_found"
            assert closed.value.summary.endswith("not found or already closed")

        assert (await qry.describe(taken)).is_terminal is True

    finally:
        await env.shutdown()
