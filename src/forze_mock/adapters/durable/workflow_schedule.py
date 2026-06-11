"""In-memory durable workflow schedule adapters."""

from __future__ import annotations

from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleCommandPort,
    DurableWorkflowScheduleDescription,
    DurableWorkflowScheduleHandle,
    DurableWorkflowScheduleQueryPort,
    DurableWorkflowScheduleTiming,
    DurableWorkflowSpec,
)
from forze.base.exceptions import exc
from forze_mock.state import MockState

# ----------------------- #


def _workflow_schedules(state: MockState, spec_name: str) -> dict[str, Any]:
    with state.lock:
        return state.durable_schedules.setdefault(str(spec_name), {})


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockDurableWorkflowScheduleCommandAdapter[In: BaseModel](
    DurableWorkflowScheduleCommandPort[In],
):
    spec: DurableWorkflowSpec[In, BaseModel]
    state: MockState

    def _schedules(self) -> dict[str, Any]:
        return _workflow_schedules(self.state, self.spec.name)

    async def create(
        self,
        schedule_id: str,
        args: In,
        timing: DurableWorkflowScheduleTiming,
        *,
        workflow_id_base: str | None = None,
        trigger_immediately: bool = False,
        note: str | None = None,
    ) -> DurableWorkflowScheduleHandle:
        _ = workflow_id_base, trigger_immediately, note
        with self.state.lock:
            self._schedules()[schedule_id] = {
                "args": args.model_dump(mode="json"),
                "timing": timing,
            }
        return DurableWorkflowScheduleHandle(schedule_id=schedule_id)

    async def upsert(
        self,
        schedule_id: str,
        args: In,
        timing: DurableWorkflowScheduleTiming,
        *,
        workflow_id_base: str | None = None,
        trigger_immediately: bool = False,
        note: str | None = None,
    ) -> DurableWorkflowScheduleHandle:
        return await self.create(
            schedule_id,
            args,
            timing,
            workflow_id_base=workflow_id_base,
            trigger_immediately=trigger_immediately,
            note=note,
        )

    async def update(
        self,
        handle: DurableWorkflowScheduleHandle,
        *,
        timing: DurableWorkflowScheduleTiming | None = None,
        args: In | None = None,
        workflow_id_base: str | None = None,
        note: str | None = None,
    ) -> None:
        _ = workflow_id_base, note
        with self.state.lock:
            entry = self._schedules().get(handle.schedule_id)
            if entry is None:
                raise exc.not_found(f"Schedule {handle.schedule_id!r} not found")
            if timing is not None:
                entry["timing"] = timing
            if args is not None:
                entry["args"] = args.model_dump(mode="json")

    async def delete(self, handle: DurableWorkflowScheduleHandle) -> None:
        with self.state.lock:
            self._schedules().pop(handle.schedule_id, None)

    async def pause(
        self,
        handle: DurableWorkflowScheduleHandle,
        *,
        note: str | None = None,
    ) -> None:
        _ = note
        with self.state.lock:
            entry = self._schedules().get(handle.schedule_id)
            if entry is not None:
                entry["paused"] = True

    async def unpause(
        self,
        handle: DurableWorkflowScheduleHandle,
        *,
        note: str | None = None,
    ) -> None:
        _ = note
        with self.state.lock:
            entry = self._schedules().get(handle.schedule_id)
            if entry is not None:
                entry["paused"] = False

    async def trigger(self, handle: DurableWorkflowScheduleHandle) -> None:
        if handle.schedule_id not in self._schedules():
            raise exc.not_found(f"Schedule {handle.schedule_id!r} not found")


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockDurableWorkflowScheduleQueryAdapter[In: BaseModel](
    DurableWorkflowScheduleQueryPort[In],
):
    spec: DurableWorkflowSpec[In, BaseModel]
    state: MockState

    def _schedules(self) -> dict[str, Any]:
        return _workflow_schedules(self.state, self.spec.name)

    async def describe(
        self,
        handle: DurableWorkflowScheduleHandle,
    ) -> DurableWorkflowScheduleDescription:
        with self.state.lock:
            entry = self._schedules().get(handle.schedule_id)
            if entry is None:
                raise exc.not_found(f"Schedule {handle.schedule_id!r} not found")
            return DurableWorkflowScheduleDescription(
                schedule_id=handle.schedule_id,
                workflow_name=self.spec.name,
                timing=entry["timing"],
                paused=bool(entry.get("paused", False)),
            )

    async def list(
        self,
        *,
        limit: int | None = None,
        next_page_token: str | None = None,
    ) -> tuple[tuple[DurableWorkflowScheduleDescription, ...], str | None]:
        _ = next_page_token
        with self.state.lock:
            items = [
                DurableWorkflowScheduleDescription(
                    schedule_id=sid,
                    workflow_name=self.spec.name,
                    timing=entry["timing"],
                    paused=bool(entry.get("paused", False)),
                )
                for sid, entry in self._schedules().items()
            ]
        if limit is not None:
            items = items[: int(limit)]
        return (tuple(items), None)
