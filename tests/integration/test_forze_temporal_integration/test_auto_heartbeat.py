"""Auto-heartbeat against a real server, and the honesty of its default.

An activity that sleeps past its own ``heartbeat_timeout`` is exactly the case the
opt-in exists for. Both halves are pinned: with it on the activity survives, and with it
**off** the same activity is timed out — because a default that quietly kept every
activity alive would be a liveness guarantee nobody asked for.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

pytest.importorskip("temporalio")
pytest.importorskip("testcontainers")

from temporalio.exceptions import ActivityError, TimeoutError as TemporalTimeoutError
from temporalio.exceptions import TimeoutType

from forze.application.execution import Deps
from forze.testing import context_from_deps
from forze_temporal import ExecutionContextInterceptor, temporal_worker_lifecycle_step
from forze_temporal.kernel.client import TemporalClient, TemporalConfig

from ._workflow_defs import ItHeartbeatWorkflow, it_heartbeat_probe

# ----------------------- #

_SLEEP_SECONDS = 6.0
"""Three times the workflow's two-second ``heartbeat_timeout``."""


async def _run_probe(target: str, *, auto_heartbeat: bool):
    """Run one slow activity under a worker whose interceptor has the flag set."""

    exec_ctx = context_from_deps(Deps.plain({}))
    client = TemporalClient()
    await client.initialize(
        target,
        config=TemporalConfig(
            namespace="default",
            interceptors=[
                ExecutionContextInterceptor(
                    ctx_dep=lambda: exec_ctx,
                    auto_heartbeat=auto_heartbeat,
                ),
            ],
        ),
    )
    task_queue = f"heartbeat-tq-{uuid4()}"
    step = temporal_worker_lifecycle_step(
        client=client,
        task_queue=task_queue,
        workflows=[ItHeartbeatWorkflow],
        activities=[it_heartbeat_probe],
    )

    try:
        await step.startup(exec_ctx)

        handle = await client.native.start_workflow(
            ItHeartbeatWorkflow.run,
            _SLEEP_SECONDS,
            id=f"heartbeat-{uuid4()}",
            task_queue=task_queue,
        )

        return await handle.result()

    finally:
        await step.shutdown(exec_ctx)
        await client.close()


# ----------------------- #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_auto_heartbeat_keeps_a_slow_activity_alive(temporal_dev_target) -> None:
    """With the pump running, an activity outlives a heartbeat timeout it never meets."""

    assert await _run_probe(temporal_dev_target.grpc_address, auto_heartbeat=True) == (
        "survived"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_without_it_the_same_activity_is_timed_out(temporal_dev_target) -> None:
    """The default's honesty, pinned: nothing beats on the activity's behalf.

    The server reschedules at ``heartbeat_timeout``; with one attempt allowed, that
    surfaces as a failed run whose cause is a **heartbeat** timeout specifically — not
    ``start_to_close``, which the activity would also have breached eventually.
    """

    with pytest.raises(Exception) as caught:  # noqa: B017 - narrowed below
        await _run_probe(temporal_dev_target.grpc_address, auto_heartbeat=False)

    error = caught.value
    assert isinstance(error.__cause__, ActivityError)

    timeout = error.__cause__.__cause__
    assert isinstance(timeout, TemporalTimeoutError)
    assert timeout.type is TimeoutType.HEARTBEAT
