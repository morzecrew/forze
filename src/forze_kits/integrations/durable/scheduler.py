"""Durable scheduler: fire recurring durable-function runs from cron schedules."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, final
from uuid import UUID

import attrs

from forze.application.contracts.durable.function import DurableScheduleRecord
from forze.application.integrations.durable import next_cron_fire, validate_cron
from forze.base.primitives import current_time_source

from ._resolve import resolve_durable_run_store, resolve_durable_schedule_store

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

        return len(due)
