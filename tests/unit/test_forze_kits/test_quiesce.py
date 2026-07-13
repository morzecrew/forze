"""Unit tests for the quiesce sweep and the outbox admin (observability) port.

# covers: forze_kits.integrations.quiesce.quiesce
# covers: forze.application.contracts.outbox.OutboxAdminPort

Quiesce stops the runtime admitting work, then waits for each operational plane to come to
rest. The outbox plane is only observable at all because of the admin port: emptiness used to
be visible solely through ``claim_pending``, which *claims* — so asking the question changed
the answer.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxSpec, OutboxStatus
from forze.application.execution import Deps, DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.quiesce import QuiesceReport, quiesce
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters import MockState
from forze_mock.adapters.outbox import MockOutboxRow

# ----------------------- #


class _Payload(BaseModel):
    x: int


_T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

OUTBOX = OutboxSpec(name="events", codec=PydanticModelCodec(_Payload))


def _row(
    *,
    index: int = 0,
    status: OutboxStatus = OutboxStatus.PENDING,
    available_at: datetime | None = None,
    created_at: datetime | None = None,
) -> MockOutboxRow:
    return MockOutboxRow(
        id=uuid4(),
        outbox_route="events",
        event_id=uuid4(),
        event_type="job.requested",
        payload={"x": index},
        status=status,
        tenant_id=None,
        execution_id=None,
        correlation_id=None,
        causation_id=None,
        occurred_at=_T0,
        created_at=created_at or _T0 + timedelta(microseconds=index),
        attempts=0,
        available_at=available_at,
    )


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


async def _quiesce(rows: list[MockOutboxRow], **kwargs: Any) -> QuiesceReport:
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        ctx.deps.provide(MockStateDepKey).outbox_rows["events"] = rows

        return await quiesce(
            runtime,
            outboxes=[OUTBOX],
            timeout=timedelta(milliseconds=150),
            poll=timedelta(milliseconds=10),
            **kwargs,
        )


# ----------------------- #
# The admin port


@pytest.mark.asyncio
async def test_depth_counts_undrained_buckets_and_ignores_published() -> None:
    # `published` is deliberately absent from the depth: nothing prunes it, so counting it
    # would grow with every event the app has ever emitted — and a published row is gone.
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        ctx.deps.provide(MockStateDepKey).outbox_rows["events"] = [
            _row(index=0),
            _row(index=1),
            _row(index=2, status=OutboxStatus.PROCESSING),
            _row(index=3, status=OutboxStatus.FAILED),
            _row(index=4, status=OutboxStatus.PUBLISHED),
            _row(index=5, status=OutboxStatus.PUBLISHED),
        ]

        depth = await ctx.outbox.admin(OUTBOX).depth()

    assert (depth.pending, depth.processing, depth.failed) == (2, 1, 1)
    assert depth.undrained == 3  # failed is not on its way out
    assert not depth.is_empty


@pytest.mark.asyncio
async def test_pending_counts_rows_parked_for_a_future_retry() -> None:
    # Unlike claim_pending, which hides them. A row backing off is still undelivered work, so
    # a quiesce that ignored it would attest an empty outbox with events still queued.
    future = datetime.now(tz=UTC) + timedelta(hours=1)
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        ctx.deps.provide(MockStateDepKey).outbox_rows["events"] = [
            _row(available_at=future)
        ]

        admin = ctx.outbox.admin(OUTBOX)

        assert await admin.has_undrained() is True
        assert (await admin.depth()).pending == 1
        # ...while the claim path cannot see it at all:
        assert await ctx.outbox.query(OUTBOX).claim_pending() == []


@pytest.mark.asyncio
async def test_has_undrained_is_false_once_everything_is_published() -> None:
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        ctx.deps.provide(MockStateDepKey).outbox_rows["events"] = [
            _row(index=0, status=OutboxStatus.PUBLISHED),
            _row(index=1, status=OutboxStatus.FAILED),  # terminal, never drains on its own
        ]

        admin = ctx.outbox.admin(OUTBOX)

        assert await admin.has_undrained() is False
        assert (await admin.depth()).is_empty


@pytest.mark.asyncio
async def test_oldest_pending_age_reports_the_head_of_the_backlog() -> None:
    now = datetime.now(tz=UTC)
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        ctx.deps.provide(MockStateDepKey).outbox_rows["events"] = [
            _row(index=0, created_at=now - timedelta(seconds=30)),
            _row(index=1, created_at=now - timedelta(seconds=90)),  # the oldest
        ]

        age = await ctx.outbox.admin(OUTBOX).oldest_pending_age()

    assert age is not None
    assert timedelta(seconds=85) < age < timedelta(seconds=95)


@pytest.mark.asyncio
async def test_oldest_pending_age_is_none_on_an_empty_route() -> None:
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()

        assert await ctx.outbox.admin(OUTBOX).oldest_pending_age() is None


# ----------------------- #
# The quiesce sweep


@pytest.mark.asyncio
async def test_quiesce_attests_when_every_plane_is_at_rest() -> None:
    report = await _quiesce([_row(status=OutboxStatus.PUBLISHED)])

    assert report.attested
    assert report.unsettled == ()
    report.raise_if_unsettled()  # must not raise


@pytest.mark.asyncio
async def test_quiesce_refuses_to_attest_an_outbox_that_never_drains() -> None:
    # Nothing is relaying, so the backlog stays. Quiesce waits for a relay; it does not relay
    # itself — and it must say so rather than quietly attesting a runtime holding un-emitted
    # events, which an export would then write out as if they had never existed.
    report = await _quiesce([_row(index=0), _row(index=1)])

    assert not report.attested

    (plane,) = report.unsettled
    assert plane.name == "outbox:events"
    assert plane.state == "residual"
    assert "2 pending" in plane.detail
    assert "oldest pending" in plane.detail  # names the cause: nothing is draining it

    with pytest.raises(CoreException, match="did not quiesce"):
        report.raise_if_unsettled()


@pytest.mark.asyncio
async def test_quiesce_settles_once_the_backlog_clears() -> None:
    # The realistic shape: a relay is running, so the rows drain while quiesce watches.
    rows = [_row(index=0), _row(index=1)]
    runtime = _runtime()

    async def _relay_after(delay: float) -> None:
        await asyncio.sleep(delay)

        for row in rows:
            row.status = OutboxStatus.PUBLISHED

    async with runtime.scope():
        ctx = runtime.get_context()
        ctx.deps.provide(MockStateDepKey).outbox_rows["events"] = rows

        relay = asyncio.create_task(_relay_after(0.05))
        report = await quiesce(
            runtime,
            outboxes=[OUTBOX],
            timeout=timedelta(seconds=2),
            poll=timedelta(milliseconds=10),
        )
        await relay

    assert report.attested


@pytest.mark.asyncio
async def test_quiesce_closes_the_gate_so_no_new_work_is_admitted() -> None:
    # The phase that makes every other plane finite: until commands stop being admitted, a
    # handler can commit and stage another row behind the sweep's back. It is one-way.
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()

        assert not ctx.drain_gate.draining

        await quiesce(runtime, timeout=timedelta(milliseconds=50))

        assert ctx.drain_gate.draining

        with pytest.raises(CoreException, match="drain"):
            ctx.drain_gate.admit("some.op")


@pytest.mark.asyncio
async def test_quiesce_reports_unwired_planes_without_holding_them_against_it() -> None:
    # A runtime that wires no outbox at all. "Nothing to settle" and "settled" are different
    # facts and the report keeps them apart — but neither blocks attestation, because there
    # is genuinely no work hiding in a plane the application does not have.
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_deps(Deps.plain({MockStateDepKey: MockState()})).freeze()
    )

    async with runtime.scope():
        report = await quiesce(
            runtime, outboxes=[OUTBOX], timeout=timedelta(milliseconds=50)
        )

    states = {plane.name: plane.state for plane in report.planes}

    assert states["outbox:events"] == "not_wired"
    assert states["durable"] == "not_wired"
    assert report.attested


@pytest.mark.asyncio
async def test_quiesce_settles_the_durable_plane_when_no_runs_are_outstanding() -> None:
    # The mock does wire the durable-run admin, so this plane is genuinely observed — not
    # skipped. An unfinished run (pending *or* running) would hold it open.
    report = await _quiesce([])

    durable = next(plane for plane in report.planes if plane.name == "durable")

    assert durable.state == "settled"
