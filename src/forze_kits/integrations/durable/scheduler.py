"""Durable scheduler: fire recurring durable-function runs from cron schedules."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Sequence, final
from uuid import UUID

import attrs

from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionSpec,
    DurableScheduleRecord,
)
from forze.application.integrations.durable import next_cron_fire, validate_cron
from forze.base.primitives import current_time_source

from ._resolve import resolve_durable_run_store, resolve_durable_schedule_store
from .telemetry import DurableTelemetry

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext
    from forze.base.primitives import JsonDict

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class DurableScheduler:
    """Fire recurring durable-function runs from cron schedules (fire-once, skip-missed).

    ``put`` registers/updates a schedule; ``tick`` claims the schedules that are due,
    enqueues one run per schedule (keyed by ``{schedule_id}:{fire_epoch}`` so a double-tick
    fires once), then advances each to its next occurrence. Skip-missed: a scheduler that
    wakes late jumps straight to the next future occurrence rather than backfilling every
    missed one. The enqueued runs are executed by the recovery scanner / runner, so run the
    scheduler step alongside ``durable_recovery_background_lifecycle_step``.

    "Now" is read from the ambient :class:`~forze.base.primitives.TimeSource`, so schedules
    are deterministic under simulation (pass *now* to pin it in a test).
    """

    telemetry: DurableTelemetry | None = None
    """Optional OpenTelemetry metrics (counts each schedule fire)."""

    async def put(
        self,
        ctx: ExecutionContext,
        schedule_id: str,
        name: str,
        cron: str,
        *,
        input_json: JsonDict | None = None,
        tz: str | None = None,
        tenant_id: UUID | None = None,
        enabled: bool = True,
        now: datetime | None = None,
    ) -> DurableScheduleRecord:
        """Register or replace a schedule; returns the stored record with its first fire."""

        validate_cron(cron, tz=tz)
        base = now or current_time_source().now()

        record = DurableScheduleRecord(
            schedule_id=schedule_id,
            name=name,
            cron=cron,
            next_fire_at=next_cron_fire(cron, after=base, tz=tz),
            tz=tz,
            input_json=input_json,
            enabled=enabled,
            tenant_id=tenant_id,
        )

        await resolve_durable_schedule_store(ctx).put(record)

        return record

    # ....................... #

    async def tick(
        self,
        ctx: ExecutionContext,
        *,
        now: datetime | None = None,
        limit: int = 100,
    ) -> int:
        """Fire every schedule due at *now*; return how many were due."""

        schedules = resolve_durable_schedule_store(ctx)
        runs = resolve_durable_run_store(ctx)
        moment = now or current_time_source().now()

        due = await schedules.claim_due(now=moment, limit=limit)

        for schedule in due:
            # Enqueue first (idempotent on the fired instant), then advance (CAS): a crash
            # between the two re-fires the same instant on the next tick without a duplicate
            # run, and concurrent schedulers converge on one run + one advance.
            fire_epoch = int(schedule.next_fire_at.timestamp())
            await runs.enqueue(
                schedule.name,
                input_json=schedule.input_json,
                idempotency_key=f"{schedule.schedule_id}:{fire_epoch}",
                tenant_id=schedule.tenant_id,
            )

            await schedules.advance(
                schedule.schedule_id,
                from_fire_at=schedule.next_fire_at,
                to_fire_at=next_cron_fire(schedule.cron, after=moment, tz=schedule.tz),
            )

            if self.telemetry is not None:
                self.telemetry.record_fire(schedule.name)

        return len(due)

    # ....................... #

    async def ensure_schedule(
        self,
        ctx: ExecutionContext,
        schedule_id: str,
        name: str,
        cron: str,
        *,
        input_json: JsonDict | None = None,
        tz: str | None = None,
        tenant_id: UUID | None = None,
        enabled: bool = True,
        now: datetime | None = None,
    ) -> DurableScheduleRecord:
        """Idempotently register a schedule.

        Creates it if absent, re-registers it if its cron/timezone changed, and otherwise
        leaves the existing schedule untouched — so calling this on every startup does **not**
        reset ``next_fire_at`` (which would skip a due fire) or un-pause a disabled schedule.
        """

        existing = await resolve_durable_schedule_store(ctx).load(schedule_id)

        if existing is not None and existing.cron == cron and existing.tz == tz:
            return existing

        return await self.put(
            ctx,
            schedule_id,
            name,
            cron,
            input_json=input_json,
            tz=tz,
            tenant_id=tenant_id,
            enabled=enabled,
            now=now,
        )

    # ....................... #

    async def ensure_cron_schedules(
        self,
        ctx: ExecutionContext,
        specs: Sequence[DurableFunctionSpec[Any, Any]],
        *,
        now: datetime | None = None,
    ) -> int:
        """Register a schedule for every ``DurableFunctionCronTrigger`` on *specs*.

        Each cron trigger becomes a schedule keyed ``{spec.name}:cron:{index}`` that fires the
        function ``spec.name`` (with no input — cron carries no payload). Idempotent via
        :meth:`ensure_schedule`; event triggers are ignored. Returns the number ensured.
        """

        ensured = 0

        for spec in specs:
            for index, trigger in enumerate(spec.triggers):
                if not isinstance(trigger, DurableFunctionCronTrigger):
                    continue

                await self.ensure_schedule(
                    ctx,
                    f"{spec.name}:cron:{index}",
                    str(spec.name),
                    trigger.expression,
                    now=now,
                )
                ensured += 1

        return ensured
