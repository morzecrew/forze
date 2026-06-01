"""In-memory durable function event command adapter."""

from __future__ import annotations

from datetime import datetime
from typing import final

import attrs
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandPort,
    DurableFunctionEventSpec,
)
from forze_mock.state import MockState

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockDurableFunctionEventAdapter[M: BaseModel](DurableFunctionEventCommandPort[M]):
    spec: DurableFunctionEventSpec[M]
    state: MockState

    def _events(self) -> list[dict[str, object]]:
        with self.state.lock:
            return self.state.durable_events.setdefault(str(self.spec.name), [])

    async def send(
        self,
        payload: M,
        *,
        event_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> str:
        _ = occurred_at
        eid = event_id or self.state.next_id("evt")
        with self.state.lock:
            self._events().append(
                {
                    "id": eid,
                    "payload": payload.model_dump(mode="json"),
                }
            )
        return eid
