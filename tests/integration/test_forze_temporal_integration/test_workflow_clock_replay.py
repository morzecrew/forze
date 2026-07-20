"""Integration test: the workflow clock binding makes utcnow()/uuid7() replay-safe.

Runs a real workflow on a time-skipping Temporal server with the
``ExecutionContextInterceptor``; the workflow asserts that ``utcnow()`` resolves to
``workflow.now()`` and ``uuid7()`` to ``workflow.uuid4()`` (a version-4 id) — i.e. the
ambient time-source bind survives the workflow sandbox/event loop, so time/id reads
reproduce across replays instead of reading the system clock / secrets.
"""

import pytest

pytest.importorskip("temporalio")

from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from forze.application.execution import Deps, InvocationMetadata
from forze.base.primitives import uuid7
from forze_temporal.interceptors.context import ExecutionContextInterceptor
from forze_temporal.sandbox import sandboxed_workflow_runner
from tests.support.execution_context import context_from_deps

from ._workflow_defs import (
    CTX_BOX,
    ItClockProbeNonPassthroughWorkflow,
    ItClockProbeWorkflow,
)


async def _run_clock_probe(workflow_cls: type, task_queue: str) -> str:
    exec_ctx = context_from_deps(Deps.plain({}))
    CTX_BOX["exec"] = exec_ctx

    try:
        eci = ExecutionContextInterceptor(ctx_dep=lambda: exec_ctx)
        env = await WorkflowEnvironment.start_time_skipping(
            data_converter=pydantic_data_converter,
            interceptors=[eci],
        )

        try:
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[workflow_cls],
                workflow_runner=sandboxed_workflow_runner(),
            ):
                with exec_ctx.inv_ctx.bind(
                    metadata=InvocationMetadata(
                        execution_id=uuid7(),
                        correlation_id=uuid7(),
                        causation_id=None,
                    ),
                ):
                    handle = await env.client.start_workflow(
                        workflow_cls.run,
                        id=f"{task_queue}-{uuid7()}",
                        task_queue=task_queue,
                    )
                    return await handle.result()

        finally:
            await env.shutdown()

    finally:
        CTX_BOX["exec"] = None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_workflow_clock_routes_to_deterministic_temporal_source() -> None:
    # "True" -> utcnow() == workflow.now(); "4" -> uuid7() routed to workflow.uuid4().
    out = await _run_clock_probe(ItClockProbeWorkflow, "it-forze-clock-probe")
    assert out == "True:4"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_workflow_clock_deterministic_for_plain_import() -> None:
    # A plain ``import forze`` inside the workflow (no imports_passed_through) must still get the
    # deterministic clock — regression for the time_source-module passthrough. Without it the
    # workflow re-imported a second _TIME_SOURCE ContextVar and silently read the wall clock.
    out = await _run_clock_probe(
        ItClockProbeNonPassthroughWorkflow, "it-forze-clock-probe-plain"
    )
    assert out == "True:4"
