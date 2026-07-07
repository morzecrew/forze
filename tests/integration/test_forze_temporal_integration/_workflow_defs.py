"""Module-scope workflow and activity definitions for Temporal integration tests."""

from datetime import timedelta
from typing import Any

from pydantic import BaseModel
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

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

    meta = ctx.inv_ctx.get_metadata()
    return str(meta.correlation_id) if meta else "none"


@workflow.defn(name="ItContextProbeWorkflow")
class ItContextProbeWorkflow:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            it_read_correlation,
            args=[],
            schedule_to_close_timeout=timedelta(seconds=5),
        )


@workflow.defn(name="ItClockProbeWorkflow")
class ItClockProbeWorkflow:
    @workflow.run
    async def run(self) -> str:
        # Under the ExecutionContextInterceptor's bound workflow clock, utcnow() must
        # route to workflow.now() (deterministic) and uuid7() to workflow.uuid4()
        # (a version-4 id) — never the non-deterministic system clock / secrets.
        with workflow.unsafe.imports_passed_through():
            from forze.base.primitives import utcnow, uuid7

        same_now = utcnow() == workflow.now()
        version = uuid7().version

        return f"{same_now}:{version}"


@workflow.defn(name="ItClockProbeNonPassthroughWorkflow")
class ItClockProbeNonPassthroughWorkflow:
    @workflow.run
    async def run(self) -> str:
        # A *plain* import — NOT wrapped in workflow.unsafe.imports_passed_through(). This is the
        # case the passthrough of ``forze.base.primitives.time_source`` fixes: even a normal
        # ``import forze`` must resolve to the single host ``_TIME_SOURCE`` ContextVar the
        # interceptor bound, so utcnow()/uuid7() stay deterministic (were silently the wall clock).
        from forze.base.primitives import utcnow, uuid7

        same_now = utcnow() == workflow.now()
        version = uuid7().version

        return f"{same_now}:{version}"


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


# ----------------------- #
# Saga driver (TemporalSaga) — compensation / forward-incomplete over activities.

# Records activity execution order so a test can assert compensation behaviour.
SAGA_RECORDER: list[str] = []


@activity.defn(name="it_saga_reserve")
async def it_saga_reserve(fail_at: str) -> str:
    SAGA_RECORDER.append("reserve")
    if fail_at == "reserve":
        raise ApplicationError("reserve failed", non_retryable=True)
    return "reserved"


@activity.defn(name="it_saga_unreserve")
async def it_saga_unreserve() -> str:
    SAGA_RECORDER.append("unreserve")
    return "unreserved"


@activity.defn(name="it_saga_charge")
async def it_saga_charge(fail_at: str) -> str:
    SAGA_RECORDER.append("charge")
    if fail_at == "charge":
        raise ApplicationError("charge failed", non_retryable=True)
    return "charged"


@activity.defn(name="it_saga_ship")
async def it_saga_ship(fail_at: str) -> str:
    SAGA_RECORDER.append("ship")
    if fail_at == "ship":
        raise ApplicationError("ship failed", non_retryable=True)
    return "shipped"


class SagaOut(BaseModel):
    """Outcome of :class:`ItCheckoutSagaWorkflow` (status + saga error code, if any)."""

    status: str
    code: str | None = None


@workflow.defn(name="ItCheckoutSagaWorkflow")
class ItCheckoutSagaWorkflow:
    @workflow.run
    async def run(self, fail_at: str) -> SagaOut:
        with workflow.unsafe.imports_passed_through():
            from forze.application.contracts.saga import SagaStepKind

            from forze_temporal import TemporalSaga

        saga = TemporalSaga(name="checkout")
        opts: dict[str, Any] = {
            "schedule_to_close_timeout": timedelta(seconds=5),
            "retry_policy": RetryPolicy(maximum_attempts=1),
        }

        try:
            await saga.step(
                "reserve",
                lambda: workflow.execute_activity(
                    it_saga_reserve, args=[fail_at], **opts
                ),
                compensation=lambda: workflow.execute_activity(
                    it_saga_unreserve, args=[], **opts
                ),
            )
            await saga.step(
                "charge",
                lambda: workflow.execute_activity(
                    it_saga_charge, args=[fail_at], **opts
                ),
                kind=SagaStepKind.PIVOT,
            )
            await saga.step(
                "ship",
                lambda: workflow.execute_activity(it_saga_ship, args=[fail_at], **opts),
                kind=SagaStepKind.RETRYABLE,
            )

        except ApplicationError as error:
            # TemporalSaga now raises an ApplicationError (so an *uncaught* saga failure fails the
            # workflow instead of retrying the task forever); its ``type`` carries the saga code.
            return SagaOut(status="failed", code=error.type or "")

        return SagaOut(status="completed")
