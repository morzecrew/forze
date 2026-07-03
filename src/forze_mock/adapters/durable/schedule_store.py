"""In-memory durable-schedule store (tests and deterministic simulation)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence, final

import attrs

from forze.application.contracts.durable.function import (
    DurableScheduleRecord,
    DurableScheduleStorePort,
)
from forze_mock.state import MockState

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class MockDurableScheduleStore(DurableScheduleStorePort):
    """Back recurring schedules with :attr:`MockState.durable_run_schedules`.

    Same semantics as the Postgres store (upsert, due-claim, compare-and-set advance).
    """

    state: MockState

    # ....................... #

    async def put(self, record: DurableScheduleRecord) -> None:
        with self.state.lock:
            self.state.durable_run_schedules[record.schedule_id] = {
                "schedule_id": record.schedule_id,
                "name": record.name,
                "cron": record.cron,
                "tz": record.tz,
                "input": record.input_json,
                "next_fire_at": record.next_fire_at,
                "enabled": record.enabled,
                "tenant_id": record.tenant_id,
            }

    # ....................... #

    async def claim_due(
        self,
        *,
        now: datetime,
        limit: int,
    ) -> Sequence[DurableScheduleRecord]:
        with self.state.lock:
            due = [
                data
                for data in self.state.durable_run_schedules.values()
                if data["enabled"] and data["next_fire_at"] <= now
            ]

        due.sort(key=lambda data: data["next_fire_at"])

        return [_to_record(data) for data in due[:limit]]

    # ....................... #

    async def advance(
        self,
        schedule_id: str,
        *,
        from_fire_at: datetime,
        to_fire_at: datetime,
    ) -> bool:
        with self.state.lock:
            data = self.state.durable_run_schedules.get(schedule_id)

            # Compare-and-set: only the scheduler that still sees the fired instant advances.
            if data is None or data["next_fire_at"] != from_fire_at:
                return False

            data["next_fire_at"] = to_fire_at

            return True

    # ....................... #

    async def load(self, schedule_id: str) -> DurableScheduleRecord | None:
        with self.state.lock:
            data = self.state.durable_run_schedules.get(schedule_id)

            return None if data is None else _to_record(data)


# ....................... #


def _to_record(data: dict[str, Any]) -> DurableScheduleRecord:
    return DurableScheduleRecord(
        schedule_id=data["schedule_id"],
        name=data["name"],
        cron=data["cron"],
        next_fire_at=data["next_fire_at"],
        tz=data["tz"],
        input_json=data["input"],
        enabled=data["enabled"],
        tenant_id=data["tenant_id"],
    )
