"""The worker step against a real Temporal server: drain, boot failure, inherited context.

Supervision mechanics are unit-tested against a stub worker. What only a server can settle:
whether an activity that was *actually running* survives shutdown, whether a worker built
from the framework client really inherits its interceptors, and what a connection to a dead
address does at boot.
"""

from __future__ import annotations

import asyncio
import socket
from datetime import timedelta
from uuid import uuid4

import pytest

pytest.importorskip("temporalio")
pytest.importorskip("testcontainers")

from forze.application.contracts.authn import AuthnIdentity
from forze.application.execution import Deps, InvocationMetadata
from forze.base.primitives import uuid7
from forze.testing import context_from_deps
from forze_temporal import ExecutionContextInterceptor, temporal_worker_lifecycle_step
from forze_temporal.kernel.client import TemporalClient, TemporalConfig

from ._workflow_defs import (
    CTX_BOX,
    DRAIN_RECORDER,
    ItContextProbeWorkflow,
    ItSlowDrainWorkflow,
    it_read_correlation,
    it_slow_drain,
)

# ----------------------- #


@pytest.fixture(autouse=True)
def _clear_drain_recorder():
    DRAIN_RECORDER.clear()

    yield

    DRAIN_RECORDER.clear()


async def _connected(target: str, **config_kwargs) -> TemporalClient:
    client = TemporalClient()
    await client.initialize(
        target,
        config=TemporalConfig(namespace="default", **config_kwargs),
    )

    return client


async def _await_recorded(marker: str, *, timeout: float = 30.0) -> None:
    """Poll ``DRAIN_RECORDER`` until *marker* shows up."""

    deadline = asyncio.get_running_loop().time() + timeout

    while asyncio.get_running_loop().time() < deadline:
        if marker in DRAIN_RECORDER:
            return

        await asyncio.sleep(0.05)

    pytest.fail(f"{marker!r} never recorded; saw {DRAIN_RECORDER}")


# ----------------------- #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_in_flight_activity_completes_within_the_drain_window(
    temporal_dev_target,
) -> None:
    """An activity running when shutdown starts gets to finish.

    This is the difference the step exists to make. The SDK's own default graceful window
    is zero, so a hand-rolled worker cancels whatever it was doing at every deploy and
    pays for it in retries.
    """

    client = await _connected(temporal_dev_target.grpc_address)
    task_queue = f"drain-tq-{uuid4()}"
    step = temporal_worker_lifecycle_step(
        client=client,
        task_queue=task_queue,
        workflows=[ItSlowDrainWorkflow],
        activities=[it_slow_drain],
        graceful_shutdown=timedelta(seconds=10),
    )
    ctx = context_from_deps(Deps.plain({}))

    try:
        await step.startup(ctx)

        await client.native.start_workflow(
            ItSlowDrainWorkflow.run,
            1.0,
            id=f"drain-{uuid4()}",
            task_queue=task_queue,
        )
        await _await_recorded("started")

        await step.shutdown(ctx)

        assert "finished" in DRAIN_RECORDER
        assert "cancelled" not in DRAIN_RECORDER

    finally:
        await client.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_tiny_drain_window_cuts_the_activity_off(temporal_dev_target) -> None:
    """The control for the test above: the window is what saves the activity, not luck."""

    client = await _connected(temporal_dev_target.grpc_address)
    task_queue = f"drain-tq-{uuid4()}"
    step = temporal_worker_lifecycle_step(
        client=client,
        task_queue=task_queue,
        workflows=[ItSlowDrainWorkflow],
        activities=[it_slow_drain],
        graceful_shutdown=timedelta(milliseconds=50),
    )
    ctx = context_from_deps(Deps.plain({}))

    try:
        await step.startup(ctx)

        await client.native.start_workflow(
            ItSlowDrainWorkflow.run,
            30.0,
            id=f"drain-{uuid4()}",
            task_queue=task_queue,
        )
        await _await_recorded("started")

        await step.shutdown(ctx)
        await _await_recorded("cancelled", timeout=5.0)

        assert "finished" not in DRAIN_RECORDER

    finally:
        await client.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_worker_inherits_the_clients_context_interceptor(
    temporal_dev_target,
) -> None:
    """No ``interceptors=`` on the step, and activities still see the caller's context.

    The SDK prepends any client interceptor that is also a worker interceptor, and
    ``ExecutionContextInterceptor`` is both. That is why the step's signature has no
    interceptor knob — one wired on the client covers the worker in the same process.
    """

    exec_ctx = context_from_deps(Deps.plain({}))
    client = await _connected(
        temporal_dev_target.grpc_address,
        interceptors=[ExecutionContextInterceptor(ctx_dep=lambda: exec_ctx)],
    )
    task_queue = f"ctx-tq-{uuid4()}"
    step = temporal_worker_lifecycle_step(
        client=client,
        task_queue=task_queue,
        workflows=[ItContextProbeWorkflow],
        activities=[it_read_correlation],
    )
    CTX_BOX["exec"] = exec_ctx
    correlation = uuid7()

    try:
        await step.startup(exec_ctx)

        with exec_ctx.inv_ctx.bind(
            metadata=InvocationMetadata(
                execution_id=uuid7(),
                correlation_id=correlation,
                causation_id=None,
            ),
            authn=AuthnIdentity(principal_id=uuid7()),
        ):
            handle = await client.native.start_workflow(
                ItContextProbeWorkflow.run,
                id=f"ctx-probe-{uuid4()}",
                task_queue=task_queue,
            )
            seen = await handle.result()

        assert seen == str(correlation)

    finally:
        CTX_BOX["exec"] = None
        await step.shutdown(exec_ctx)
        await client.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_boot_fails_loudly_against_an_unreachable_server() -> None:
    """A worker process that cannot reach its cluster must die at boot, not poll silently.

    The client step is where that happens: it connects eagerly, so an unreachable address
    raises before any worker step is asked for a connection.
    """

    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    dead_port = probe.getsockname()[1]
    probe.close()

    with pytest.raises(RuntimeError, match="connect"):
        await _connected(f"127.0.0.1:{dead_port}")
