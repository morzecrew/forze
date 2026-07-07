"""In-memory durable function event command adapter."""

from __future__ import annotations

from datetime import datetime
from typing import TypedDict, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandPort,
    DurableFunctionEventSpec,
)
from forze.base.primitives import JsonDict
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin

# ----------------------- #


class _DurableEvent(TypedDict):
    """One recorded durable-function event envelope."""

    id: str
    payload: JsonDict


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockDurableFunctionEventAdapter[M: BaseModel](
    MockTenancyMixin,
    DurableFunctionEventCommandPort[M],
):
    spec: DurableFunctionEventSpec[M]
    state: MockState

    def _events(self) -> list[_DurableEvent]:
        # Mirrors the real Inngest adapter, which stamps tenant_id into the envelope.
        ns = self._partitioned_namespace(str(self.spec.name))
        with self.state.lock:
            return self.state.durable_events.setdefault(ns, [])

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
