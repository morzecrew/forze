"""Unit tests for DurableFunctionEventCommandPort."""

from datetime import datetime, timezone

import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandDepKey,
    DurableFunctionEventCommandPort,
)


class _Payload(BaseModel):
    value: str


class _StubEventCommand(DurableFunctionEventCommandPort[_Payload]):
    async def send(
        self,
        payload: _Payload,
        *,
        event_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> str:
        assert payload.value
        return event_id or "evt-1"


class TestDurableFunctionEventPorts:
    def test_runtime_checkable(self) -> None:
        assert isinstance(_StubEventCommand(), DurableFunctionEventCommandPort)

    def test_dep_key_name(self) -> None:
        assert DurableFunctionEventCommandDepKey.name == "durable_function_event_command"


@pytest.mark.asyncio
async def test_send_returns_event_id() -> None:
    cmd = _StubEventCommand()
    event_id = await cmd.send(
        _Payload(value="x"),
        event_id="dedup-1",
        occurred_at=datetime.now(tz=timezone.utc),
    )
    assert event_id == "dedup-1"
