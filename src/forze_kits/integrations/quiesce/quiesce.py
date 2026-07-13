"""Bring a runtime to a standstill and report whether it got there."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from datetime import timedelta
from typing import Any
from uuid import UUID

from forze.application.contracts.durable.function import DurableRunStatus
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.stream import StreamSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionRuntime
from forze.application.execution.context import ExecutionContext
from forze_kits.integrations._logger import logger
from forze_kits.integrations.durable import resolve_durable_run_admin

from .report import QuiescePlane, QuiesceReport

# ----------------------- #

_DURABLE_ADMIN_KEY = "durable_function_run_admin"
_COMMIT_STREAM_ADMIN_KEY = "commit_stream_group_admin"

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
        return QuiescePlane(name=name, state="not_wired")

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
        # Either no durable plane, or its admin read is not opted in — in which case quiesce
        # cannot see the runs at all, and says so rather than assuming there are none.
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
        return QuiescePlane(name=name, state="not_wired")

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


async def quiesce(
    runtime: ExecutionRuntime,
    *,
    timeout: timedelta = timedelta(seconds=30),
    outboxes: Sequence[OutboxSpec[Any]] = (),
    streams: Sequence[tuple[StreamSpec[Any], str]] = (),
    tenants: Sequence[UUID] | None = None,
    poll: timedelta = timedelta(milliseconds=200),
) -> QuiesceReport:
    """Stop admitting work, then wait for the operational planes to come to rest.

    In order: the runtime stops accepting new top-level invocations and waits for the ones
    in flight; then each named outbox route, the durable-run plane, and each named stream
    group is polled until it is empty or the budget runs out. The report says, plane by
    plane, what settled and what did not — see :meth:`QuiesceReport.attested`.

    **One-way.** The drain gate does not reopen: once this returns, every new invocation on
    this scope is refused with ``THROTTLED``/``draining`` for as long as the scope lives.
    This is the step before a shutdown, an export, or a migration — not a pause button.

    **It waits for the relay; it does not relay.** An outbox only empties because something
    is relaying it (the background lifecycle step, or an external worker). Quiesce holds the
    gate shut so no *new* rows can be staged, which makes the backlog finite — but if nothing
    is draining it, the plane will simply be reported as ``residual``, with the age of the
    oldest pending row to say so. Give the budget room for at least one relay tick.

    *outboxes* and *streams* are named explicitly because nothing enumerates an application's
    specs yet; a runtime-wide inventory would let quiesce discover them itself, and is the
    next piece of this arc. *tenants* mirrors the relay's shard: on a tenant-partitioned
    outbox, each partition is probed under its own bound tenant.

    Planes the runtime does not wire are reported ``not_wired`` and do not count against
    attestation. Work the framework cannot see at all — a Temporal-backed workflow lives in
    the Temporal cluster — is outside what quiesce can speak for: do not migrate mid-workflow.
    """

    ctx = runtime.get_context()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout.total_seconds()
    poll_seconds = poll.total_seconds()

    logger.info("Quiescing runtime", timeout=timeout.total_seconds())

    planes: list[QuiescePlane] = [await _operations_plane(ctx, deadline=deadline)]

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

    report = QuiesceReport(planes=tuple(planes))

    if report.attested:
        logger.info("Runtime quiesced", planes=len(report.planes))

    else:
        logger.warning(
            "Runtime did not fully quiesce",
            unsettled=[plane.name for plane in report.unsettled],
        )

    return report


# ....................... #


async def _operations_plane(ctx: ExecutionContext, *, deadline: float) -> QuiescePlane:
    """Close the gate and wait for in-flight operations — the phase that makes the rest finite.

    Until new commands stop being admitted, every other plane is chasing a moving target: a
    handler that commits while the outbox is being watched stages another row.
    """

    loop = asyncio.get_running_loop()
    settled = await ctx.drain_gate.drain(max(0.0, deadline - loop.time()))

    if settled:
        return QuiescePlane(name="operations", state="settled")

    return QuiescePlane(
        name="operations",
        state="residual",
        detail=f"{ctx.drain_gate.in_flight} operation(s) still in flight",
    )
