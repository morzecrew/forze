"""Configured start options reach a real Temporal server, and a per-call set overrides them.

Mapping tests prove the kwargs leave the process. Only the server can say the server
*accepted* them: an option Temporal silently ignores, or reads as "unset" because it was
non-positive, looks identical from inside the SDK call.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest

pytest.importorskip("temporalio")
pytest.importorskip("testcontainers")

from temporalio.common import RetryPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.worker import Worker

from forze.application.contracts.durable.workflow import DurableWorkflowSpec
from forze.application.contracts.durable.workflow.specs import DurableWorkflowInvokeSpec
from forze_temporal import TemporalStartOptions, sandboxed_workflow_runner
from forze_temporal.adapters.workflow import TemporalWorkflowCommandAdapter
from forze_temporal.kernel.client import TemporalClient

from ._workflow_defs import EchoIn, EchoOut, ItEchoWorkflow
from .conftest import connected_client

# ----------------------- #

_SPEC = DurableWorkflowSpec[EchoIn, EchoOut](
    name="ItEchoWorkflow",
    run=DurableWorkflowInvokeSpec(args_type=EchoIn, return_type=EchoOut),
)


@pytest.fixture
async def echo_client(temporal_dev_target):
    """A framework client on the dev server, with no worker polling by default."""

    client = await connected_client(temporal_dev_target.grpc_address)

    try:
        yield client

    finally:
        await client.close()


def _adapter(
    client: TemporalClient,
    task_queue: str,
    *,
    start_options: TemporalStartOptions | None = None,
) -> TemporalWorkflowCommandAdapter[EchoIn, EchoOut]:
    return TemporalWorkflowCommandAdapter(
        client=client,
        queue=task_queue,
        spec=_SPEC,
        tenant_aware=False,
        start_options=start_options,
    )


# ----------------------- #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_configured_timeouts_reach_the_server(echo_client) -> None:
    """Timeouts declared on the workflow kind show up in the run's server-side config.

    No worker runs here on purpose: the assertion is about what the *start* recorded,
    not about the run completing.
    """

    options = TemporalStartOptions(
        execution_timeout=timedelta(seconds=120),
        run_timeout=timedelta(seconds=60),
        task_timeout=timedelta(seconds=17),
    )
    adapter = _adapter(echo_client, f"opts-tq-{uuid4()}", start_options=options)

    handle = await adapter.start(EchoIn(marker="timeouts"))
    described = await echo_client.native.get_workflow_handle(handle.workflow_id).describe()
    config = described.raw_description.execution_config

    assert config.workflow_execution_timeout.ToTimedelta() == timedelta(seconds=120)
    assert config.workflow_run_timeout.ToTimedelta() == timedelta(seconds=60)
    assert config.default_workflow_task_timeout.ToTimedelta() == timedelta(seconds=17)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_per_call_override_wins_field_by_field_on_the_server(echo_client) -> None:
    """The overridden field changes; the configured field the caller did not mention stands."""

    adapter = _adapter(
        echo_client,
        f"opts-tq-{uuid4()}",
        start_options=TemporalStartOptions(
            execution_timeout=timedelta(seconds=120),
            run_timeout=timedelta(seconds=60),
        ),
    )

    handle = await adapter.start(
        EchoIn(marker="override"),
        options=TemporalStartOptions(run_timeout=timedelta(seconds=30)),
    )
    described = await echo_client.native.get_workflow_handle(handle.workflow_id).describe()
    config = described.raw_description.execution_config

    assert config.workflow_run_timeout.ToTimedelta() == timedelta(seconds=30)
    assert config.workflow_execution_timeout.ToTimedelta() == timedelta(seconds=120)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_configured_retry_policy_and_memo_reach_the_server(echo_client) -> None:
    """Retry policy rides the start event; memo comes back on describe."""

    adapter = _adapter(
        echo_client,
        f"opts-tq-{uuid4()}",
        start_options=TemporalStartOptions(
            retry_policy=RetryPolicy(maximum_attempts=4),
            memo={"team": "billing"},
        ),
    )

    handle = await adapter.start(EchoIn(marker="retry"))
    native_handle = echo_client.native.get_workflow_handle(handle.workflow_id)

    described = await native_handle.describe()
    assert await described.memo() == {"team": "billing"}

    history = await native_handle.fetch_history()
    started = next(
        event.workflow_execution_started_event_attributes
        for event in history.events
        if event.HasField("workflow_execution_started_event_attributes")
    )

    assert started.retry_policy.maximum_attempts == 4


@pytest.mark.integration
@pytest.mark.asyncio
async def test_id_reuse_policy_is_enforced_by_the_server(echo_client) -> None:
    """``REJECT_DUPLICATE`` refuses a second start on a *closed* run's id.

    Nothing about this is visible on describe — the policy only shows up as a refusal,
    so the assertion has to run a workflow to completion and start it again.
    """

    task_queue = f"opts-reuse-tq-{uuid4()}"
    workflow_id = f"opts-reuse-{uuid4()}"
    options = TemporalStartOptions(id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE)
    adapter = _adapter(echo_client, task_queue, start_options=options)

    async with Worker(
        echo_client.native,
        task_queue=task_queue,
        workflows=[ItEchoWorkflow],
        workflow_runner=sandboxed_workflow_runner(),
    ):
        handle = await adapter.start(EchoIn(marker="reuse"), workflow_id=workflow_id)
        await echo_client.get_workflow_result(handle.workflow_id, result_type=EchoOut)

        native_handle = echo_client.native.get_workflow_handle(workflow_id)
        first_run = (await native_handle.describe()).run_id

        with pytest.raises(WorkflowAlreadyStartedError):
            await adapter.start(EchoIn(marker="reuse-again"), workflow_id=workflow_id)

        # The control: the same closed id starts fine when the policy allows it, so the
        # refusal above is the policy talking and not some unrelated rejection.
        await adapter.start(
            EchoIn(marker="reuse-allowed"),
            workflow_id=workflow_id,
            options=TemporalStartOptions(
                id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            ),
        )

        assert (await native_handle.describe()).run_id != first_run
