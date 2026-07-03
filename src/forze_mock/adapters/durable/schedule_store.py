"""In-memory durable-schedule store (tests and deterministic simulation)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence, final
from uuid import UUID

import attrs

from forze.application.contracts.durable.function import (
    DurableScheduleRecord,
    DurableScheduleStorePort,
)
from forze.application.contracts.tenancy import TenantProviderPort
from forze_mock.state import MockState

# ----------------------- #


def _scoped_key(schedule_id: str, tenant_id: UUID | None) -> str:
    """Tenant-scope the storage key so two tenants reusing one schedule id stay distinct
    (mirrors the Postgres store's tenant-scoped ``schedule_id`` primary key)."""
    return schedule_id if tenant_id is None else f"{tenant_id}:{schedule_id}"


@final
@attrs.define(slots=True, kw_only=True)
class MockDurableScheduleStore(DurableScheduleStorePort):
    """Back recurring schedules with :attr:`MockState.durable_run_schedules`.

    Same semantics as the Postgres store (upsert, due-claim, compare-and-set advance); a
    bound tenant scopes ``put``/``claim_due`` to that tenant.
    """

    state: MockState
    tenant_provider: TenantProviderPort | None = None

    # ....................... #

    def _bound_tenant(self) -> UUID | None:
        if self.tenant_provider is None:
            return None

        tenant = self.tenant_provider()

        return tenant.tenant_id if tenant is not None else None

    # ....................... #

    async def put(self, record: DurableScheduleRecord) -> None:
        tenant_id = (
            record.tenant_id
            if record.tenant_id is not None
            else self._bound_tenant()
        )

        with self.state.lock:
            self.state.durable_run_schedules[_scoped_key(record.schedule_id, tenant_id)] = {
                "schedule_id": record.schedule_id,
                "name": record.name,
                "cron": record.cron,
                "tz": record.tz,
                "input": record.input_json,
                "next_fire_at": record.next_fire_at,
                "enabled": record.enabled,
                "tenant_id": tenant_id,
            }

    # ....................... #

    async def claim_due(
        self,
        *,
        now: datetime,
        limit: int,
    ) -> Sequence[DurableScheduleRecord]:
        bound_tenant = self._bound_tenant()

        with self.state.lock:
            due = [
                data
                for data in self.state.durable_run_schedules.values()
                if data["enabled"]
                and data["next_fire_at"] <= now
                and (bound_tenant is None or data["tenant_id"] == bound_tenant)
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
            key = _scoped_key(schedule_id, self._bound_tenant())
            data = self.state.durable_run_schedules.get(key)

            # Compare-and-set: only the scheduler that still sees the fired instant advances.
            if data is None or data["next_fire_at"] != from_fire_at:
                return False

            data["next_fire_at"] = to_fire_at

            return True

    # ....................... #

    async def load(self, schedule_id: str) -> DurableScheduleRecord | None:
        with self.state.lock:
            key = _scoped_key(schedule_id, self._bound_tenant())
            data = self.state.durable_run_schedules.get(key)

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
