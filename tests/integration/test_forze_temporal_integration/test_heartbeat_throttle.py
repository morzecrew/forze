"""A worker's heartbeat throttle bounds how fast a cancel reaches a running activity.

The server delivers an activity's cancellation in the reply to a heartbeat, and the SDK
sends heartbeats at most every 0.8 x the activity's ``heartbeat_timeout`` unless the worker
caps it. With the cap, a cancel lands within about one capped interval.
"""

from __future__ import annotations

import asyncio
import time
from uuid import uuid4

import pytest

pytest.importorskip("temporalio")
pytest.importorskip("testcontainers")

from datetime import timedelta

from temporalio.api.enums.v1 import PendingActivityState
from temporalio.client import WorkflowFailureError

from forze.application.execution import Deps
from forze.testing import context_from_deps
from forze_temporal import temporal_worker_lifecycle_step

from ._workflow_defs import ItCancelWorkflow, it_cancel_probe
from .conftest import connected_client

# ----------------------- #


async def _wait_until_heartbeating(handle) -> None:
    """Return once the activity runs and the server has seen its first heartbeat."""

    for _ in range(100):
        description = await handle.describe()

        for pending in description.raw_description.pending_activities:
            if (
                pending.state == PendingActivityState.PENDING_ACTIVITY_STATE_STARTED
                and pending.HasField("last_heartbeat_time")
            ):
                return

        await asyncio.sleep(0.1)

    raise AssertionError("the activity never started heartbeating")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_capped_throttle_delivers_a_cancel_within_the_cap(temporal_dev_target) -> None:
    exec_ctx = context_from_deps(Deps.plain({}))
    client = await connected_client(temporal_dev_target.grpc_address)
    task_queue = f"throttle-tq-{uuid4()}"
    step = temporal_worker_lifecycle_step(
        client=client,
        task_queue=task_queue,
        workflows=[ItCancelWorkflow],
        activities=[it_cancel_probe],
        max_heartbeat_throttle_interval=timedelta(milliseconds=500),
    )

    try:
        await step.startup(exec_ctx)

        handle = await client.native.start_workflow(
            ItCancelWorkflow.run,
            id=f"throttle-{uuid4()}",
            task_queue=task_queue,
        )
        await _wait_until_heartbeating(handle)
        # Past the first heartbeat, which the SDK sends unthrottled.
        await asyncio.sleep(1.0)

        started = time.monotonic()
        await handle.cancel()

        with pytest.raises(WorkflowFailureError):
            await handle.result()

        # Uncapped, the next heartbeat (and so the cancel) waits up to 24 s.
        assert time.monotonic() - started < 5.0

    finally:
        await step.shutdown(exec_ctx)
        await client.close()
