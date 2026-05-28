from datetime import datetime, timezone
import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import DurableFunctionEventSpec
from forze.application.execution import Deps, ExecutionContext
from forze.base.serialization import PydanticRecordMappingCodec
from forze_inngest.adapters import InngestEventCommandAdapter
from forze_inngest.execution.deps import InngestClientDepKey

from tests.unit.test_forze_inngest.helpers import RecordingInngestClient


class _Payload(BaseModel):
    value: str


@pytest.mark.asyncio
async def test_send_builds_inngest_event() -> None:
    client = RecordingInngestClient()
    spec = DurableFunctionEventSpec(
        name="app/test",
        codec=PydanticRecordMappingCodec(model_type=_Payload),
    )

    adapter = InngestEventCommandAdapter(
        client=client,
        spec=spec,
        include_execution_context=False,
    )

    event_id = await adapter.send(
        _Payload(value="x"),
        event_id="dedup",
        occurred_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )

    assert event_id == "id-1"
    assert len(client.sent) == 1
    event = client.sent[0]
    assert event.name == "app/test"
    assert event.data == {"value": "x"}
    assert event.id == "dedup"
    assert event.ts == int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


@pytest.mark.asyncio
async def test_send_merges_execution_context_from_resolved_adapter() -> None:
    from uuid import uuid4

    from forze.application.execution import InvocationMetadata
    from forze_inngest.execution.deps.configs import InngestEventConfig
    from forze_inngest.execution.deps.deps import ConfigurableInngestEventCommand

    client = RecordingInngestClient()
    deps = Deps.plain({InngestClientDepKey: client})
    ctx = ExecutionContext(deps=deps)

    factory = ConfigurableInngestEventCommand(config=InngestEventConfig())
    spec = DurableFunctionEventSpec(
        name="app/test",
        codec=PydanticRecordMappingCodec(model_type=_Payload),
    )

    metadata = InvocationMetadata(
        execution_id=uuid4(),
        correlation_id=uuid4(),
    )

    adapter = factory(ctx, spec)

    with ctx.inv_ctx.bind_metadata(metadata=metadata):
        await adapter.send(_Payload(value="y"))

    assert "_forze" in client.sent[0].data
    assert client.sent[0].data["value"] == "y"
