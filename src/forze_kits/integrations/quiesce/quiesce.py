"""Bring a runtime to a standstill and report whether it got there."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from datetime import timedelta
from typing import Any, cast
from uuid import UUID

from forze.application.contracts.durable.function import DurableRunStatus
from forze.application.contracts.inventory import SpecPlane
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.stream import AckStreamGroupAdminDepKey, StreamSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionRuntime
from forze.application.execution.context import ExecutionContext
from forze.base.primitives import utcnow
from forze_kits.integrations._logger import logger
from forze_kits.integrations.durable import resolve_durable_run_admin

from .report import QuiescePlane, QuiesceReport

# ----------------------- #

_DURABLE_ADMIN_KEY = "durable_function_run_admin"
_DURABLE_RUN_STORE_KEY = "durable_function_run_store"
_COMMIT_STREAM_ADMIN_KEY = "commit_stream_group_admin"
_ACK_STREAM_ADMIN_KEY = "stream_group_admin"

_UNFINISHED_RUNS = (DurableRunStatus.PENDING, DurableRunStatus.RUNNING)
"""Durable runs that still owe work. Both block: a ``PENDING`` run is enqueued (or parked on
a retry backoff) and its state lives only in the run store, so a runtime that is quiesced to
be copied elsewhere must not still be holding one."""


# ....................... #


def _wired(ctx: ExecutionContext, key_name: str, route: str | None = None) -> bool:
    """Whether a dependency is registered, without resolving it.

    Resolving an unregistered key raises ``exc.configuration``; a plane the app simply does
    not use is not an error, so ask the static inventory instead of catching one.

    A frame with no route is a *plain* registration — one provider serving every route, which
    is what the mock backend does — so it answers a route-specific question too.
    """

    return any(
        frame.key_name == key_name and (route is None or frame.route in (None, route))
        for frame in ctx.deps.registered_frames()
    )


# ....................... #


async def _settle(
    busy: Callable[[], Awaitable[str | None]],
    *,
    deadline: float,
    poll: float,
) -> str | None:
    """Poll *busy* until it reports idle, or the budget runs out.

    *busy* returns a description of what is still moving, or ``None`` once nothing is. The
    sleep between polls is what keeps this safe under simulation, where a hot loop would
    freeze the virtual clock and no deadline could ever fire.
    """

    loop = asyncio.get_running_loop()

    while True:
        residual = await busy()

        if residual is None:
            return None

        if loop.time() >= deadline:
            return residual

        await asyncio.sleep(min(poll, max(0.0, deadline - loop.time())))


# ....................... #


def _tenant_scopes(
    ctx: ExecutionContext,
    tenants: Sequence[UUID] | None,
) -> list[tuple[UUID | None, Any]]:
    """One (tenant, binder) pair per partition to probe — a single unbound pass when global."""

    if tenants is None:
        return [(None, None)]

    return [(tenant, TenantIdentity(tenant_id=tenant)) for tenant in tenants]


# ....................... #


async def _outbox_plane(
    ctx: ExecutionContext,
    spec: OutboxSpec[Any],
    *,
    tenants: Sequence[UUID] | None,
    deadline: float,
    poll: float,
) -> QuiescePlane:
    name = f"outbox:{spec.name}"

    if not _wired(ctx, "outbox_admin", str(spec.name)):
        # The outbox EXISTS — its spec reached this sweep — but its admin read is not
        # wired, so its backlog cannot be seen. Unobserved, not settled: an unreadable
        # outbox may be holding every event the export is about to not carry.
        return QuiescePlane(
            name=name,
            state="unobserved",
            detail="outbox route is bound but no outbox_admin dependency is wired to read "
            "its depth — add the admin binding (e.g. the backend's outbox admin adapter)",
        )

    async def _busy() -> str | None:
        holding: list[str] = []

        for tenant, identity in _tenant_scopes(ctx, tenants):
            if identity is None:
                depth = await ctx.outbox.admin(spec).depth()

            else:
                with ctx.inv_ctx.bind_identity(tenant=identity):
                    depth = await ctx.outbox.admin(spec).depth()

            if depth.is_empty:
                continue

            where = "" if tenant is None else f" (tenant {tenant})"
            holding.append(f"{depth.pending} pending, {depth.processing} processing{where}")

        return "; ".join(holding) if holding else None

    residual = await _settle(_busy, deadline=deadline, poll=poll)

    if residual is None:
        return QuiescePlane(name=name, state="settled")

    # A backlog that will not fall is usually a relay that is not running: quiesce waits for
    # the relay, it does not relay itself. Name that, so the failure explains itself.
    age = await ctx.outbox.admin(spec).oldest_pending_age() if tenants is None else None
    stuck = "" if age is None else f"; oldest pending {age.total_seconds():.0f}s"

    return QuiescePlane(name=name, state="residual", detail=f"{residual}{stuck}")


# ....................... #


async def _durable_plane(
    ctx: ExecutionContext,
    *,
    deadline: float,
    poll: float,
) -> QuiescePlane:
    if not _wired(ctx, _DURABLE_ADMIN_KEY):
        if _wired(ctx, _DURABLE_RUN_STORE_KEY):
            # The durable plane exists (a run store is wired) but its admin read is not
            # opted in — the runs cannot be seen, which is not the same as none existing.
            return QuiescePlane(
                name="durable",
                state="unobserved",
                detail="a durable run store is wired but its admin read is not — quiesce "
                "cannot see the runs; wire the durable run admin to attest this plane",
            )

        return QuiescePlane(name="durable", state="not_wired")

    admin = resolve_durable_run_admin(ctx)

    async def _busy() -> str | None:
        holding: list[str] = []

        for status in _UNFINISHED_RUNS:
            page = await admin.list_runs(status=status, limit=1)

            if page.records:
                holding.append(str(status.value))

        return f"runs still {' and '.join(holding)}" if holding else None

    residual = await _settle(_busy, deadline=deadline, poll=poll)

    if residual is None:
        return QuiescePlane(name="durable", state="settled")

    return QuiescePlane(name="durable", state="residual", detail=residual)


# ....................... #


async def _stream_plane(
    ctx: ExecutionContext,
    spec: StreamSpec[Any],
    group: str,
    *,
    deadline: float,
    poll: float,
) -> QuiescePlane:
    name = f"stream:{spec.name}/{group}"

    if not _wired(ctx, _COMMIT_STREAM_ADMIN_KEY, str(spec.name)):
        # The caller named this group explicitly, so the plane exists — an unwired admin
        # means it cannot be read, which must not pass for settled.
        return QuiescePlane(
            name=name,
            state="unobserved",
            detail="stream group was named but no commit-stream admin is wired to read its "
            "lag — wire the admin binding to attest it",
        )

    admin = ctx.stream.commit_admin(spec)

    async def _busy() -> str | None:
        behind = [one for one in await admin.lag(group) if one.lag > 0]

        if not behind:
            return None

        total = sum(one.lag for one in behind)

        return f"{total} message(s) behind across {len(behind)} partition(s)"

    residual = await _settle(_busy, deadline=deadline, poll=poll)

    if residual is None:
        return QuiescePlane(name=name, state="settled")

    return QuiescePlane(name=name, state="residual", detail=residual)


# ....................... #


async def _ack_stream_plane(
    ctx: ExecutionContext,
    spec: StreamSpec[Any],
    group: str,
    *,
    tenants: Sequence[UUID] | None,
    deadline: float,
    poll: float,
) -> QuiescePlane:
    """Settle one **ack-family** consumer group (the realtime gateway's model).

    An ack group has no committed offset to lag behind — its rest state is a known-zero
    backlog *and* an empty pending-entries list, read from
    :meth:`~forze.application.contracts.stream.AckStreamGroupAdminPort.depth`. An unknown
    backlog (Redis after a trim) is reported residual, never treated as empty.

    *tenants* mirrors the outbox plane: on a ``tenant_aware`` (namespace-tier) stream
    route each assigned tenant's per-tenant stream key holds its own group — every one is
    probed under its bound identity, and any one still moving keeps the plane busy.
    """

    name = f"ack-stream:{spec.name}/{group}"

    if not _wired(ctx, _ACK_STREAM_ADMIN_KEY, str(spec.name)):
        # Same posture as the commit-stream plane: named but unreadable is unobserved.
        return QuiescePlane(
            name=name,
            state="unobserved",
            detail="ack-stream group was named but no ack-stream admin is wired to read its "
            "depth — wire the admin binding to attest it",
        )

    async def _depth() -> Any:
        # resolved under the current (possibly tenant-bound) identity, so a tenant-aware
        # route reads that tenant's stream key — the same discipline as the ensure step
        admin = ctx.deps.resolve_configurable(ctx, AckStreamGroupAdminDepKey, spec, route=spec.name)

        return await admin.depth(group, str(spec.name))

    async def _busy() -> str | None:
        holding: list[str] = []

        for tenant, identity in _tenant_scopes(ctx, tenants):
            if identity is None:
                depth = await _depth()

            else:
                with ctx.inv_ctx.bind_identity(tenant=identity):
                    depth = await _depth()

            if depth.at_rest:
                continue

            where = "" if tenant is None else f" (tenant {tenant})"

            if depth.backlog is None:
                holding.append(f"backlog unknown (trimmed?), {depth.pending} pending{where}")
                continue

            oldest = depth.oldest_pending_idle
            holding.append(
                f"{depth.backlog} undelivered, {depth.pending} pending"
                + (f" (oldest idle {oldest.total_seconds():.1f}s)" if oldest is not None else "")
                + where
            )

        return "; ".join(holding) if holding else None

    residual = await _settle(_busy, deadline=deadline, poll=poll)

    if residual is None:
        return QuiescePlane(name=name, state="settled")

    return QuiescePlane(name=name, state="residual", detail=residual)


# ....................... #


async def _stop_in_process_loops(ctx: ExecutionContext, *, deadline: float) -> int:
    """Stop the loops this process owns, then ask each one that can to flush.

    Stopping a relay ends its ticks — and its ``stop`` only drains on its own when
    ``drain_on_shutdown`` is set, which defaults **off**. Without the explicit flush that
    follows, quiesce would halt the very relay it then waits on: the outbox plane it polls
    could never move, the whole budget would burn, and the plane would report residual on a
    backlog the (still-running) relay would have delivered. So after ``stop_all``, every
    stopped loop that exposes ``flush`` is asked to publish what is claimable now — the
    consumers have already finished and committed the batch in hand during the stop, and
    every plane below is finite from that moment.

    An **external** relay — a cron job, a worker in another process, the shape production
    usually takes — is not here to be stopped or flushed. Nothing in-process can reach it,
    and quiesce goes back to doing the only thing it can: waiting and reporting.
    """

    loops = ctx.drainables.loops

    if not loops:
        return 0

    logger.info(
        "Quiesce stopping %d in-process background loop(s)",
        len(loops),
        loops=[one.loop_name for one in loops],
    )

    stopped = await ctx.drainables.stop_all(
        grace=max(0.0, deadline - asyncio.get_running_loop().time())
    )

    for loop in loops:
        flush = getattr(loop, "flush", None)

        if flush is None:
            continue

        try:
            await flush(deadline=deadline)

        except asyncio.CancelledError:
            raise

        except Exception:
            # a failed flush must not hide the sweep: the plane reports residual instead
            logger.exception("Quiesce flush failed for loop", loop=loop.loop_name)

    return stopped


# ....................... #


async def _guarded(name: str, plane: Awaitable[QuiescePlane]) -> QuiescePlane:
    """Run one plane's sweep, turning a failed probe into an honest ``error`` rather than
    an exception that would hide every plane behind it."""

    try:
        return await plane

    except asyncio.CancelledError:
        raise

    except Exception as error:
        logger.exception("Quiesce probe failed for plane", plane=name)

        return QuiescePlane(name=name, state="error", detail=str(error))


# ....................... #


def _unprobeable_planes(
    runtime: ExecutionRuntime,
    *,
    outboxes: Sequence[OutboxSpec[Any]],
    streams: Sequence[tuple[StreamSpec[Any], str]],
    ack_streams: Sequence[tuple[StreamSpec[Any], str]],
) -> list[QuiescePlane]:
    """The catalogued planes this sweep has no way to observe — reported, never assumed empty.

    An attested report must mean "every plane that can hold in-flight work was **seen** at
    rest". Four catalogued kinds can hold work this sweep cannot see, and each one used to be
    silently omitted — which read as settled:

    - an **outbox** the caller excluded — an explicit ``outboxes=`` narrows the sweep, but a
      catalogued route left out of it may still hold pending events; narrowing must narrow
      the *probing*, never the attestation.
    - a **queue** — a queued message is undelivered work; no depth probe exists yet.
    - a **distributed lock** — a held lock marks work in flight somewhere; no holder
      enumeration exists yet.
    - a **stream** the caller named no consumer group for — a group's lag is the pending
      work, and no inventory can know the group names (they are the consumers' identity),
      so an uncovered stream cannot be attested.

    Three other catalogued kinds are deliberately **exempt**, because by contract they hold no
    recoverable in-flight work: a pub/sub channel retains nothing (live-only, at-most-once), and
    the inbox and idempotency planes are dedup *bookkeeping* — records of work already done,
    not work still owed.

    A runtime with **no inventory at all** cannot enumerate any of this, so it contributes one
    ``unobserved`` plane for the inventory itself: zero probed routes must never add up to an
    attested report.
    """

    if runtime.spec_registry is None:
        return [
            QuiescePlane(
                name="inventory",
                state="unobserved",
                detail="runtime carries no spec inventory, so the outbox/queue/stream/lock "
                "surface cannot be enumerated — zero probed planes is not a quiesced system; "
                "build the runtime with build_runtime(specs=…)",
            )
        ]

    covered = {str(spec.name) for spec, _group in (*streams, *ack_streams)}
    probed_outboxes = {str(spec.name) for spec in outboxes}
    planes: list[QuiescePlane] = []

    planes.extend(
        QuiescePlane(
            name=f"outbox:{entry.name}",
            state="unobserved",
            detail="catalogued outbox route was excluded from this sweep (outboxes=…) — an "
            "unprobed outbox may hold pending events; include it, or pass outboxes=None to "
            "sweep the whole inventory",
        )
        for entry in runtime.spec_registry.of_plane(SpecPlane.OUTBOX)
        if entry.name not in probed_outboxes
    )

    planes.extend(
        QuiescePlane(
            name=f"queue:{entry.name}",
            state="unobserved",
            detail="no queue-depth probe exists for this plane yet — a queued message is "
            "pending work this sweep cannot see",
        )
        for entry in runtime.spec_registry.of_plane(SpecPlane.QUEUE)
    )

    planes.extend(
        QuiescePlane(
            name=f"dlock:{entry.name}",
            state="unobserved",
            detail="no lock-holder probe exists for this plane yet — a held distributed "
            "lock marks in-flight work this sweep cannot see",
        )
        for entry in runtime.spec_registry.of_plane(SpecPlane.DLOCK)
    )

    planes.extend(
        QuiescePlane(
            name=f"stream:{entry.name}",
            state="unobserved",
            detail="no consumer group was named for this stream — group lag is the "
            "pending work, and group names are the consumers' identity, so pass "
            "streams=/ack_streams= covering it to attest",
        )
        for entry in runtime.spec_registry.of_plane(SpecPlane.STREAM)
        if entry.name not in covered
    )

    return planes


# ....................... #


def _inventoried_outboxes(runtime: ExecutionRuntime) -> tuple[OutboxSpec[Any], ...]:
    """Every outbox route the runtime's spec inventory knows about.

    The reason the inventory exists: a sweep that has to be *handed* its outboxes can only
    watch the ones the caller remembered, and the routes most likely to be forgotten are the
    ones no author wrote — a kit's ``<search>_sync`` relay, for instance.
    """

    if runtime.spec_registry is None:
        return ()

    return tuple(
        cast("OutboxSpec[Any]", entry.spec)
        for entry in runtime.spec_registry.of_plane(SpecPlane.OUTBOX)
    )


# ....................... #


async def quiesce(
    runtime: ExecutionRuntime,
    *,
    timeout: timedelta = timedelta(seconds=30),
    outboxes: Sequence[OutboxSpec[Any]] | None = None,
    streams: Sequence[tuple[StreamSpec[Any], str]] = (),
    ack_streams: Sequence[tuple[StreamSpec[Any], str]] = (),
    tenants: Sequence[UUID] | None = None,
    close_gate: bool = True,
    poll: timedelta = timedelta(milliseconds=200),
) -> QuiesceReport:
    """Stop admitting work, then wait for the operational planes to come to rest.

    In order: the runtime stops accepting new top-level invocations and waits for the ones in
    flight; then each named outbox route, the durable-run plane, and each named stream group
    is polled until it is empty or the budget runs out. The report says, plane by plane, what
    settled and what did not — and, in :attr:`QuiesceReport.attested`, whether the result is
    something a caller may actually build on.

    **Closing the gate is one-way.** The drain gate does not reopen: with *close_gate* (the
    default), every new invocation on this scope is refused with ``THROTTLED``/``draining``
    for as long as the scope lives. That is deliberate — it is the *shutdown* gate, and this
    is the step before a shutdown, an export, or a migration.

    Pass ``close_gate=False`` to only **look**: the sweep reads each plane and reports, and
    the scope keeps serving. Nothing is then holding the door shut, so the report can be
    :attr:`~QuiesceReport.settled` but never :attr:`~QuiesceReport.attested` — a plane that
    was empty when it was read can be filled the moment the sweep looks away. Use it for a
    health check; do not build an export on it.

    **It stops this process's loops, then asks them to flush.** With *close_gate*, quiesce
    stops the background loops the runtime owns (``ctx.drainables``) before it starts
    watching — the consumers finish and commit what they have in hand — and then explicitly
    asks each stopped loop that can flush (the outbox relay) to publish what is claimable
    now, regardless of its ``drain_on_shutdown`` setting: stopping ended the ticks, so
    without the flush the sweep would wait on a backlog nothing drains. A relay feeding a
    **pubsub** destination is the exception (at-most-once past the broker — publishing
    right before a shutdown or an export loses what lands after subscribers leave); its
    plane reports ``residual`` honestly. An outbox fed by an **external** worker — a cron
    job, another process, the shape production usually takes — cannot be reached from here
    at all: closing the gate makes its backlog finite, but if nothing drains it the plane
    is reported ``residual``, with the age of the oldest pending row to say so.

    Stopping the loops is one-way, like the gate. ``close_gate=False`` leaves them running.

    *outboxes* defaults to **every outbox route in the runtime's spec inventory** — pass an
    explicit sequence to narrow it, or ``()`` to skip the plane. This is what the inventory is
    for: a sweep handed its routes by the caller can only watch the ones the caller remembered,
    and the easiest routes to forget are the ones nobody wrote (a kit's ``<search>_sync`` relay
    mints an outbox, a queue and an inbox out of one line of declaration).

    *streams* stays explicit, and always will: a consumer **group** is not a property of a
    stream spec — it is the identity of whoever is reading — so no inventory can supply it.
    *ack_streams* is the same contract for **ack-family** groups (the realtime gateway's
    model): rest means known-zero backlog **and** an empty pending-entries list, read from
    the ack admin port's ``depth()``; a backlog the backend cannot compute (Redis after a
    trim) reports ``residual``, never empty.

    *tenants* mirrors the relay's shard: on a tenant-partitioned outbox, each partition is
    probed under its own bound tenant — and each *ack_streams* group likewise, so a
    tenant-aware realtime stream's per-tenant groups are all attested, not just a global key.

    Planes the runtime genuinely does not have are reported ``not_wired`` and do not count
    against attestation. Planes that **exist but cannot be read** are ``unobserved`` and
    **block attestation**: a catalogued outbox whose admin read is not wired, a catalogued
    queue or distributed lock (no probe exists for either kind yet), a catalogued stream no
    consumer group was named for, and — the degenerate case — a runtime with no spec
    inventory at all, which can enumerate none of this and therefore never attests. Three
    catalogued kinds are exempt by contract: pub/sub retains nothing (live-only), and the
    inbox and idempotency planes are dedup bookkeeping, records of work already done rather
    than work still owed.

    Two things are outside what this can speak for, in either mode: a Temporal-backed
    workflow (its state lives in the Temporal cluster), and a **sibling replica** — quiesce
    holds one process still, and a fleet that is still serving writes elsewhere will happily
    invalidate whatever this one attested. Stop the fleet first.
    """

    ctx = runtime.get_context()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout.total_seconds()
    poll_seconds = poll.total_seconds()

    logger.info("Quiescing runtime", timeout=timeout.total_seconds(), close_gate=close_gate)

    planes: list[QuiescePlane] = [
        await _operations_plane(ctx, deadline=deadline, close_gate=close_gate)
    ]

    if close_gate:
        # Only when we are already committing to a one-way quiesce. Stopping the loops is
        # destructive — they do not restart — so a pure observation must leave them alone.
        await _stop_in_process_loops(ctx, deadline=deadline)

    if outboxes is None:
        outboxes = _inventoried_outboxes(runtime)

    for spec in outboxes:
        planes.append(
            await _guarded(
                f"outbox:{spec.name}",
                _outbox_plane(ctx, spec, tenants=tenants, deadline=deadline, poll=poll_seconds),
            )
        )

    planes.append(
        await _guarded("durable", _durable_plane(ctx, deadline=deadline, poll=poll_seconds))
    )

    for stream_spec, group in streams:
        planes.append(
            await _guarded(
                f"stream:{stream_spec.name}/{group}",
                _stream_plane(ctx, stream_spec, group, deadline=deadline, poll=poll_seconds),
            )
        )

    for stream_spec, group in ack_streams:
        planes.append(
            await _guarded(
                f"ack-stream:{stream_spec.name}/{group}",
                _ack_stream_plane(
                    ctx,
                    stream_spec,
                    group,
                    tenants=tenants,
                    deadline=deadline,
                    poll=poll_seconds,
                ),
            )
        )

    # What the sweep could not see must weigh against attestation, not vanish from the
    # report: catalogued outboxes the caller excluded, queues/locks with no probe, streams
    # no group was named for, or a runtime with no inventory at all. Unobserved is not empty.
    planes.extend(
        _unprobeable_planes(runtime, outboxes=outboxes, streams=streams, ack_streams=ack_streams)
    )

    # Read the gate rather than trusting *close_gate*: a scope already going down was holding
    # the door before this sweep started, and that counts. The stamp and the probed tenant
    # set are the report's cross-checkable facts — a scoped consumer (the export gate)
    # verifies its tenant set against ``tenants`` instead of trusting a bare boolean.
    report = QuiesceReport(
        planes=tuple(planes),
        admission_held=ctx.drain_gate.draining,
        taken_at=utcnow(),
        tenants=tuple(tenants) if tenants is not None else None,
    )

    if report.attested:
        logger.info("Runtime quiesced", planes=len(report.planes))

    elif report.settled:
        logger.info(
            "Runtime is at rest but was not held there; not attested",
            planes=len(report.planes),
        )

    else:
        logger.warning(
            "Runtime did not fully quiesce",
            unsettled=[plane.name for plane in report.unsettled],
        )

    return report


# ....................... #


async def _operations_plane(
    ctx: ExecutionContext,
    *,
    deadline: float,
    close_gate: bool,
) -> QuiescePlane:
    """Stop admitting work and wait for what is in flight — or, when only looking, just read it.

    Closing the gate is what makes every *other* plane finite: until new commands stop being
    admitted, a handler can commit behind the sweep's back and stage another outbox row, and
    the sweep is chasing a moving target.

    Without it, waiting would be that same chase, so this takes the instantaneous reading
    instead. The report records that admission was never held, which is what stops such a
    reading from attesting anything.
    """

    if not close_gate:
        in_flight = ctx.drain_gate.in_flight

        if in_flight == 0:
            return QuiescePlane(name="operations", state="settled")

        return QuiescePlane(
            name="operations",
            state="residual",
            detail=f"{in_flight} operation(s) in flight",
        )

    loop = asyncio.get_running_loop()

    if await ctx.drain_gate.drain(max(0.0, deadline - loop.time())):
        return QuiescePlane(name="operations", state="settled")

    return QuiescePlane(
        name="operations",
        state="residual",
        detail=f"{ctx.drain_gate.in_flight} operation(s) still in flight",
    )
