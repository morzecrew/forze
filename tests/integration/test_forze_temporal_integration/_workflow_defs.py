"""Module-scope workflow and activity definitions for Temporal integration tests."""

from datetime import timedelta
from typing import Any

from pydantic import BaseModel
from temporalio import activity, workflow

# Set by integration tests so activities can read :class:`~forze.application.execution.ExecutionContext`.
CTX_BOX: dict[str, Any] = {"exec": None}


@activity.defn(name="it_add_numbers")
async def it_add_numbers(a: int, b: int) -> int:
    return a + b


@workflow.defn(name="ItAddWorkflow")
class ItAddWorkflow:
    @workflow.run
    async def run(self, a: int, b: int) -> int:
        return await workflow.execute_activity(
            it_add_numbers,
            args=[a, b],
            schedule_to_close_timeout=timedelta(seconds=5),
        )


@workflow.defn(name="ItPingWorkflow")
class ItPingWorkflow:
    @workflow.run
    async def run(self) -> str:
        return "pong"


@activity.defn(name="it_read_correlation")
async def it_read_correlation() -> str:
    ctx = CTX_BOX.get("exec")
    if ctx is None:
        return "none"

    cc = ctx.get_call_ctx()
    return str(cc.correlation_id) if cc else "none"


@workflow.defn(name="ItContextProbeWorkflow")
class ItContextProbeWorkflow:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            it_read_correlation,
            args=[],
            schedule_to_close_timeout=timedelta(seconds=5),
        )


class SumIn(BaseModel):
    """Pydantic input for :class:`ItSumWorkflow`."""

    a: int
    b: int


class SumOut(BaseModel):
    """Pydantic output for :class:`ItSumWorkflow`."""

    total: int


@activity.defn(name="it_sum_pair")
async def it_sum_pair(a: int, b: int) -> int:
    return a + b


@workflow.defn(name="ItSumWorkflow")
class ItSumWorkflow:
    @workflow.run
    async def run(self, inp: SumIn) -> SumOut:
        t = await workflow.execute_activity(
            it_sum_pair,
            args=[inp.a, inp.b],
            schedule_to_close_timeout=timedelta(seconds=5),
        )
        return SumOut(total=t)
