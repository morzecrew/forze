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

from forze.application.contracts.inventory import SpecRegistry
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
    # Carries its inventory: a runtime without one can no longer attest (its operational
    # surface cannot be enumerated), which is exactly what these tests need it to do.
    return ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze(),
        spec_registry=SpecRegistry().register(OUTBOX).freeze(),
    )


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
async def test_quiesce_attests_when_every_plane_is_at_rest_and_the_gate_is_shut() -> None:
    report = await _quiesce([_row(status=OutboxStatus.PUBLISHED)])

    assert report.settled
    assert report.admission_held
    assert report.attested
    assert report.unsettled == ()
    report.raise_if_unattested()  # must not raise


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

    with pytest.raises(CoreException, match="not quiesced"):
        report.raise_if_unattested()


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

        report = await quiesce(runtime, timeout=timedelta(milliseconds=50))

        assert ctx.drain_gate.draining
        assert report.admission_held

        with pytest.raises(CoreException, match="drain"):
            ctx.drain_gate.admit("some.op")


@pytest.mark.asyncio
async def test_observe_mode_leaves_the_scope_serving() -> None:
    # Looking must not brick the runtime: closing the gate is one-way, so a caller who only
    # wants to know the state of the planes must be able to ask without paying for a restart.
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()

        await quiesce(runtime, close_gate=False, timeout=timedelta(milliseconds=50))

        assert not ctx.drain_gate.draining
        ctx.drain_gate.admit("some.op")  # still serving
        ctx.drain_gate.release()


@pytest.mark.asyncio
async def test_observe_mode_can_settle_but_never_attests() -> None:
    # The distinction the report exists to make. Every plane was at rest — but nothing was
    # holding the door, so it could have been filled the moment the sweep looked away. That
    # reading is fine for a health check and unsafe to build an export on.
    report = await _quiesce([_row(status=OutboxStatus.PUBLISHED)], close_gate=False)

    assert report.settled
    assert not report.admission_held
    assert not report.attested

    with pytest.raises(CoreException, match="admission was never closed"):
        report.raise_if_unattested()


@pytest.mark.asyncio
async def test_quiesce_reports_absent_planes_without_holding_them_against_it() -> None:
    # A runtime that genuinely has no durable plane (no run store wired). "Nothing to
    # settle" and "settled" are different facts and the report keeps them apart — but a
    # truly absent plane does not block attestation.
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_deps(Deps.plain({MockStateDepKey: MockState()})).freeze(),
        spec_registry=SpecRegistry().freeze(),
    )

    async with runtime.scope():
        report = await quiesce(runtime, outboxes=(), timeout=timedelta(milliseconds=50))

    states = {plane.name: plane.state for plane in report.planes}

    assert states["durable"] == "not_wired"
    assert report.attested


@pytest.mark.asyncio
async def test_a_named_outbox_with_no_admin_read_is_unobserved_and_blocks() -> None:
    # The outbox EXISTS (the caller or the inventory named it) but nothing can read its
    # depth: unreadable is not empty, so the plane is unobserved and attestation refused.
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_deps(Deps.plain({MockStateDepKey: MockState()})).freeze(),
        spec_registry=SpecRegistry().freeze(),
    )

    async with runtime.scope():
        report = await quiesce(
            runtime, outboxes=[OUTBOX], timeout=timedelta(milliseconds=50)
        )

    plane = next(p for p in report.planes if p.name == "outbox:events")

    assert plane.state == "unobserved"
    assert not report.attested

    with pytest.raises(CoreException, match="unobserved"):
        report.raise_if_unattested()


@pytest.mark.asyncio
async def test_a_runtime_without_an_inventory_never_attests() -> None:
    # Zero probed routes must never add up to an attested report: without an inventory the
    # sweep cannot enumerate the outbox/queue/stream/lock surface at all.
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze()
    )

    async with runtime.scope():
        report = await quiesce(runtime, timeout=timedelta(milliseconds=50))

    plane = next(p for p in report.planes if p.name == "inventory")

    assert plane.state == "unobserved"
    assert not report.attested


@pytest.mark.asyncio
async def test_catalogued_queues_locks_and_uncovered_streams_are_unobserved() -> None:
    # Catalogued planes the sweep has no probe for must weigh against attestation, not
    # vanish: a queued message, a held lock, or an unwatched consumer group is pending work.
    from forze.application.contracts.dlock import DistributedLockSpec
    from forze.application.contracts.queue import QueueSpec
    from forze.application.contracts.stream import StreamSpec

    registry = (
        SpecRegistry()
        .register(QueueSpec(name="jobs", codec=PydanticModelCodec(_Payload)))
        .register(DistributedLockSpec(name="leases"))
        .register(StreamSpec(name="firehose", codec=PydanticModelCodec(_Payload)))
    )
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze(),
        spec_registry=registry.freeze(),
    )

    async with runtime.scope():
        report = await quiesce(runtime, outboxes=(), timeout=timedelta(milliseconds=50))

    states = {plane.name: plane.state for plane in report.planes}

    assert states["queue:jobs"] == "unobserved"
    assert states["dlock:leases"] == "unobserved"
    assert states["stream:firehose"] == "unobserved"
    assert not report.attested


@pytest.mark.asyncio
async def test_quiesce_discovers_its_outboxes_from_the_spec_inventory() -> None:
    # The wart the inventory removes. A sweep handed its routes by the caller can only watch
    # the ones the caller remembered — and the easiest routes to forget are the ones nobody
    # wrote (a kit's `<search>_sync` relay mints an outbox out of one line of declaration).
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze(),
        spec_registry=SpecRegistry().register(OUTBOX).freeze(),
    )

    async with runtime.scope():
        ctx = runtime.get_context()
        ctx.deps.provide(MockStateDepKey).outbox_rows["events"] = [_row(), _row(index=1)]

        # No `outboxes=` argument at all.
        report = await quiesce(
            runtime, timeout=timedelta(milliseconds=100), poll=timedelta(milliseconds=10)
        )

    (plane,) = report.unsettled

    assert plane.name == "outbox:events"  # found it on its own
    assert "2 pending" in plane.detail


@pytest.mark.asyncio
async def test_quiesce_settles_the_durable_plane_when_no_runs_are_outstanding() -> None:
    # The mock does wire the durable-run admin, so this plane is genuinely observed — not
    # skipped. An unfinished run (pending *or* running) would hold it open.
    report = await _quiesce([])

    durable = next(plane for plane in report.planes if plane.name == "durable")

    assert durable.state == "settled"


# ----------------------- #
# ack-stream plane (the realtime gateway's consumer-group model)


async def test_ack_stream_plane_settles_when_group_is_at_rest() -> None:
    from forze.application.contracts.stream import (
        AckStreamGroupAdminDepKey,
    )
    from forze_kits.integrations.realtime import realtime_stream_spec

    spec = realtime_stream_spec()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        admin = ctx.deps.resolve_configurable(ctx, AckStreamGroupAdminDepKey, spec, route=spec.name)
        await admin.ensure_group("gw", str(spec.name), start_id="0")

        report = await quiesce(
            runtime,
            outboxes=(),
            ack_streams=[(spec, "gw")],
            timeout=timedelta(milliseconds=150),
            poll=timedelta(milliseconds=10),
            close_gate=False,
        )

    plane = next(p for p in report.planes if p.name == f"ack-stream:{spec.name}/gw")
    assert plane.state == "settled"


async def test_ack_stream_plane_reports_undelivered_backlog_as_residual() -> None:
    from forze.application.contracts.realtime import Audience, RealtimeSignal
    from forze.application.contracts.stream import (
        AckStreamGroupAdminDepKey,
        StreamCommandDepKey,
    )
    from forze_kits.integrations.realtime import realtime_stream_spec

    spec = realtime_stream_spec()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        admin = ctx.deps.resolve_configurable(ctx, AckStreamGroupAdminDepKey, spec, route=spec.name)
        await admin.ensure_group("gw", str(spec.name), start_id="0")

        command = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, spec, route=spec.name)
        signal = RealtimeSignal.of(Audience.topic("t"), "e", {})
        await command.append(str(spec.name), signal)  # appended, never consumed

        report = await quiesce(
            runtime,
            outboxes=(),
            ack_streams=[(spec, "gw")],
            timeout=timedelta(milliseconds=120),
            poll=timedelta(milliseconds=20),
            close_gate=False,
        )

    plane = next(p for p in report.planes if p.name == f"ack-stream:{spec.name}/gw")
    assert plane.state == "residual"
    assert "1 undelivered" in (plane.detail or "")


async def test_ack_stream_plane_probes_each_tenant() -> None:
    from uuid import UUID

    from forze.application.contracts.realtime import Audience, RealtimeSignal
    from forze.application.contracts.stream import (
        AckStreamGroupAdminDepKey,
        StreamCommandDepKey,
    )
    from forze.application.contracts.tenancy import TenantIdentity
    from forze_kits.integrations.realtime import realtime_stream_spec
    from forze_mock import MockRouteConfig

    spec = realtime_stream_spec()
    idle, busy = UUID(int=1), UUID(int=2)
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(
            MockDepsModule(routes={str(spec.name): MockRouteConfig(tenant_aware=True)})
        ).freeze()
    )

    async with runtime.scope():
        ctx = runtime.get_context()

        for tenant in (idle, busy):
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                admin = ctx.deps.resolve_configurable(
                    ctx, AckStreamGroupAdminDepKey, spec, route=spec.name
                )
                await admin.ensure_group("gw", str(spec.name), start_id="0")

        # only the busy tenant's per-tenant stream key holds an undelivered signal
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=busy)):
            command = ctx.deps.resolve_configurable(
                ctx, StreamCommandDepKey, spec, route=spec.name
            )
            await command.append(
                str(spec.name), RealtimeSignal.of(Audience.topic("t"), "e", {})
            )

        report = await quiesce(
            runtime,
            outboxes=(),
            ack_streams=[(spec, "gw")],
            tenants=[idle, busy],
            timeout=timedelta(milliseconds=120),
            poll=timedelta(milliseconds=20),
            close_gate=False,
        )

    plane = next(p for p in report.planes if p.name == f"ack-stream:{spec.name}/gw")
    # the busy tenant keeps the plane residual, and the detail names it — the idle one passed
    assert plane.state == "residual"
    assert f"(tenant {busy})" in (plane.detail or "")
    assert f"(tenant {idle})" not in (plane.detail or "")


async def test_ack_stream_plane_not_wired_and_trimmed_unknown_details() -> None:
    from forze.application.contracts.deps import Deps
    from forze.application.contracts.realtime import Audience, RealtimeSignal
    from forze.application.contracts.stream import (
        AckStreamGroupAdminDepKey,
        StreamCommandDepKey,
    )
    from forze_kits.integrations.realtime import realtime_stream_spec
    from forze_mock import MockRouteConfig

    spec = realtime_stream_spec()

    # a runtime with no ack admin registration at all → the plane reports not_wired
    bare = ExecutionRuntime(deps=DepsRegistry.from_modules(lambda: Deps.plain({})).freeze())
    async with bare.scope():
        report = await quiesce(
            bare,
            outboxes=(),
            ack_streams=[(spec, "gw")],
            timeout=timedelta(milliseconds=50),
            close_gate=False,
        )
    plane = next(p for p in report.planes if p.name.startswith("ack-stream"))
    # named but unreadable: the admin is not wired, so the group cannot pass for settled
    assert plane.state == "unobserved"

    # a capped route that evicted undelivered entries → residual with the unknown marker
    capped = ExecutionRuntime(
        deps=DepsRegistry.from_modules(
            MockDepsModule(
                routes={str(spec.name): MockRouteConfig(stream_retention_max_entries=1)}
            )
        ).freeze()
    )
    async with capped.scope():
        ctx = capped.get_context()
        admin = ctx.deps.resolve_configurable(ctx, AckStreamGroupAdminDepKey, spec, route=spec.name)
        await admin.ensure_group("gw", str(spec.name), start_id="0")

        command = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, spec, route=spec.name)
        for _ in range(3):  # cap 1 → two undelivered entries evicted past the idle group
            await command.append(str(spec.name), RealtimeSignal.of(Audience.topic("t"), "e", {}))

        report = await quiesce(
            capped,
            outboxes=(),
            ack_streams=[(spec, "gw")],
            timeout=timedelta(milliseconds=80),
            poll=timedelta(milliseconds=20),
            close_gate=False,
        )

    plane = next(p for p in report.planes if p.name.startswith("ack-stream"))
    assert plane.state == "residual"
    assert "backlog unknown" in (plane.detail or "")  # never attested as empty
