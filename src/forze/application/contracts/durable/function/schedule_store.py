"""Durable-schedule store contract: recurring cron triggers for durable functions.

A schedule fires a run on a cadence. The store persists schedule instances and hands out
**due** ones to the scheduler, which enqueues a run and advances the next fire. Cron math
lives in the scheduler (``forze_kits``), so the store is backend-agnostic and engine-free —
it only stores ``next_fire_at`` and does the claim/advance.
"""

from __future__ import annotations

from datetime import datetime
from typing import Awaitable, Protocol, Sequence, final, runtime_checkable
from uuid import UUID

import attrs

from forze.base.primitives import JsonDict

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableScheduleRecord:
    """A persisted recurring schedule for a durable function."""

    schedule_id: str
    """Stable schedule identifier (the natural key; re-putting it updates in place)."""

    name: str
    """Registered durable-function name a fire enqueues a run for."""

    cron: str
    """Cron expression driving the cadence."""

    next_fire_at: datetime
    """Next instant the schedule is due (tz-aware UTC)."""

    tz: str | None = None
    """IANA timezone the cron is evaluated in (``None`` = UTC)."""

    input_json: JsonDict | None = None
    """Encoded input handed to each enqueued run."""

    enabled: bool = True
    """When ``False`` the schedule is never claimed (paused)."""

    tenant_id: UUID | None = None
    """Owning tenant (tagged tier); ``None`` for single-tenant deployments."""


# ....................... #


@runtime_checkable
class DurableScheduleStorePort(Protocol):
    """Persist recurring schedules and hand out due ones to the scheduler.

    Single-relation, tagged-tier tenancy. Firing is made exactly-once by the enqueued run's
    idempotency key (``{schedule_id}:{fire_epoch}``) plus a compare-and-set on
    :meth:`advance`, so concurrent schedulers converge without a lease.
    """

    def put(self, record: DurableScheduleRecord) -> Awaitable[None]:
        """Insert or replace a schedule (keyed by ``schedule_id``, scoped per tenant).

        The key is scoped to the tenant, so two tenants may register the same
        ``schedule_id`` without one overwriting the other.
        """
        ...  # pragma: no cover

    def claim_due(
        self,
        *,
        now: datetime,
        limit: int,
    ) -> Awaitable[Sequence[DurableScheduleRecord]]:
        """Return up to *limit* enabled schedules due at *now* (``next_fire_at <= now``).

        Uses ``FOR UPDATE SKIP LOCKED`` to reduce cross-scanner contention; correctness does
        not depend on it (the run idempotency key + :meth:`advance` CAS make firing
        exactly-once regardless).
        """
        ...  # pragma: no cover

    def advance(
        self,
        schedule_id: str,
        *,
        from_fire_at: datetime,
        to_fire_at: datetime,
    ) -> Awaitable[bool]:
        """Compare-and-set the next fire: move ``next_fire_at`` *from_fire_at* → *to_fire_at*.

        Returns ``True`` iff this call advanced it — so if two schedulers fired the same due
        instant, only one advances and the schedule fires once.
        """
        ...  # pragma: no cover

    def load(self, schedule_id: str) -> Awaitable[DurableScheduleRecord | None]:
        """Return the schedule record, or ``None`` if unknown."""
        ...  # pragma: no cover
