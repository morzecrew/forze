"""Module-scope workflow and activity definitions for Temporal integration tests."""

import asyncio
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


async def _drive_checkout_saga(fail_at: str) -> None:
    """Run the three-step checkout saga; a failure escapes as the saga's ApplicationError."""

    with workflow.unsafe.imports_passed_through():
        from forze.application.contracts.saga import SagaStepKind
        from forze_temporal import TemporalSaga

    saga = TemporalSaga(name="checkout")
    opts: dict[str, Any] = {
        "schedule_to_close_timeout": timedelta(seconds=5),
        "retry_policy": RetryPolicy(maximum_attempts=1),
    }

    await saga.step(
        "reserve",
        lambda: workflow.execute_activity(it_saga_reserve, args=[fail_at], **opts),
        compensation=lambda: workflow.execute_activity(it_saga_unreserve, args=[], **opts),
    )
    await saga.step(
        "charge",
        lambda: workflow.execute_activity(it_saga_charge, args=[fail_at], **opts),
        kind=SagaStepKind.PIVOT,
    )
    await saga.step(
        "ship",
        lambda: workflow.execute_activity(it_saga_ship, args=[fail_at], **opts),
        kind=SagaStepKind.RETRYABLE,
    )


@workflow.defn(name="ItCheckoutSagaWorkflow")
class ItCheckoutSagaWorkflow:
    @workflow.run
    async def run(self, fail_at: str) -> SagaOut:
        try:
            await _drive_checkout_saga(fail_at)

        except ApplicationError as error:
            # TemporalSaga raises an ApplicationError (so an *uncaught* saga failure fails the
            # workflow instead of retrying the task forever); its ``type`` carries the saga code.
            return SagaOut(status="failed", code=error.type or "")

        return SagaOut(status="completed")


@workflow.defn(name="ItUncaughtSagaWorkflow")
class ItUncaughtSagaWorkflow:
    """The production shape: the saga failure is *not* caught, so it leaves ``@workflow.run``.

    Whether that terminates the run or wedges it in a workflow-task retry loop is the whole
    point of the ApplicationError conversion — and is only observable on a real server.
    """

    @workflow.run
    async def run(self, fail_at: str) -> SagaOut:
        await _drive_checkout_saga(fail_at)
        return SagaOut(status="completed")


@workflow.defn(name="ItRawCoreFailureWorkflow")
class ItRawCoreFailureWorkflow:
    """Control for :class:`ItUncaughtSagaWorkflow`: the un-converted failure shape.

    A Forze ``CoreException`` is not a temporalio ``FailureError``, so raising it out of
    ``@workflow.run`` fails the *workflow task*, which Temporal retries forever — the run
    never reaches a terminal state. This is the behaviour ``TemporalSaga`` converts away.
    """

    @workflow.run
    async def run(self) -> None:
        with workflow.unsafe.imports_passed_through():
            from forze.base.exceptions import exc

        raise exc.validation("bad charge", code="charge.invalid")


# ----------------------- #
# Escape hatch (``TemporalClient.native``) — payload sealing at rest.


class EchoIn(BaseModel):
    """Input for :class:`ItEchoWorkflow`; carries a marker the test greps for at rest."""

    marker: str


class EchoOut(BaseModel):
    """Output for :class:`ItEchoWorkflow`."""

    echoed: str


@workflow.defn(name="ItEchoWorkflow")
class ItEchoWorkflow:
    """Echoes its input, so both the argument and the result payload carry the marker."""

    @workflow.run
    async def run(self, inp: EchoIn) -> EchoOut:
        return EchoOut(echoed=inp.marker)


# ----------------------- #
# Worker lifecycle — activity drain at shutdown.

# Records whether an in-flight activity finished or was cut off; cleared per test.
DRAIN_RECORDER: list[str] = []


@activity.defn(name="it_slow_drain")
async def it_slow_drain(seconds: float) -> str:
    """Sleeps, recording whether it reached the end or was cancelled mid-flight."""

    DRAIN_RECORDER.append("started")

    try:
        await asyncio.sleep(seconds)

    except asyncio.CancelledError:
        DRAIN_RECORDER.append("cancelled")
        raise

    DRAIN_RECORDER.append("finished")

    return "done"


@workflow.defn(name="ItSlowDrainWorkflow")
class ItSlowDrainWorkflow:
    """Runs one slow activity, so a shutdown lands while that activity is in flight."""

    @workflow.run
    async def run(self, seconds: float) -> str:
        return await workflow.execute_activity(
            it_slow_drain,
            args=[seconds],
            start_to_close_timeout=timedelta(seconds=60),
        )


# ----------------------- #
# Activity auto-heartbeat.


@activity.defn(name="it_heartbeat_probe")
async def it_heartbeat_probe(seconds: float) -> str:
    """Sleeps past its own ``heartbeat_timeout``, reporting nothing while it does.

    Exactly the activity the auto-heartbeat exists for: alive, busy, and with no
    incremental state worth a manual ``activity.heartbeat(details)``.
    """

    await asyncio.sleep(seconds)

    return "survived"


@workflow.defn(name="ItHeartbeatWorkflow")
class ItHeartbeatWorkflow:
    """One slow activity with a short heartbeat timeout and no retries."""

    @workflow.run
    async def run(self, seconds: float) -> str:
        return await workflow.execute_activity(
            it_heartbeat_probe,
            args=[seconds],
            start_to_close_timeout=timedelta(seconds=60),
            heartbeat_timeout=timedelta(seconds=2),
            # One attempt, so a heartbeat timeout surfaces as a failed run instead of
            # being retried into an eventual success.
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
