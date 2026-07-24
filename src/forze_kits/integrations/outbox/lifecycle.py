"""Lifecycle helpers for background outbox relay."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import Any, Final, Literal, final
from uuid import UUID

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.outbox import (
    OutboxDestinationKind,
    OutboxRelayResult,
    OutboxSpec,
)
from forze.application.contracts.pubsub import PubSubSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.contracts.stream import StreamSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution.background import BackgroundLoopControl
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, current_entropy_source
from forze_kits.integrations._logger import logger

from ._relay_core import validate_retry_options
from .relay import OutboxRelay

# ----------------------- #

_STOP_WAIT_SECONDS: Final[float] = 2.0
"""Slice of the shutdown budget the poll loop gets to finish its in-flight batch."""

_BATCH_ADMISSION_MARGIN: Final[float] = 1.5
"""Headroom over the last batch's duration required before a drain starts another."""


_PassStop = Literal["drained", "retry", "cap", "budget", "error", "stopped"]
"""Why a relay pass ended. Only ``drained`` means "nothing left to relay right now"."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _RelayPass:
    """What one relay pass over a route observed."""

    stop: _PassStop
    """Why the pass ended."""

    batches: int = 0
    """Batches relayed."""

    claimed: int = 0
    """Rows claimed across the pass."""

    published: int = 0
    """Rows published across the pass."""

    retried: int = 0
    """Rows rescheduled for a future retry across the pass."""

    failed: int = 0
    """Rows terminally failed across the pass."""

    # ....................... #

    @property
    def drained(self) -> bool:
        """Whether the route had nothing left to relay when the pass ended."""

        return self.stop == "drained"


# ....................... #


def _admits_batch(*, remaining: float, last_batch: float | None) -> bool:
    """Whether the budget left can be expected to cover another batch.

    A batch cut between ``claim_pending`` and its mark flush strands rows ``processing``
    — invisible to *every* replica until ``reclaim_stale_after`` elapses, which is worse
    than never having claimed them at all. So a drain only starts a batch when what is
    left of its budget comfortably exceeds how long the last one took, which keeps the
    hard timeout a backstop rather than the normal way a drain ends.
    """

    if remaining <= 0.0:
        return False

    if last_batch is None:
        return True

    return remaining > last_batch * _BATCH_ADMISSION_MARGIN


# ....................... #


def _log_drain_outcome(passes: Sequence[_RelayPass]) -> None:
    """Report what the shutdown drain managed to publish, and what it left behind."""

    if not passes:
        # Not one pass ran: an empty tenant shard, a budget already spent before the first
        # tenant, or every tenant raising. Each of those already logged its own warning, and
        # "published 0 row(s)" would land after it reading like a clean drain — the last word
        # on a teardown that left the backlog where it was.
        return

    published = sum(one.published for one in passes)
    incomplete = sorted({one.stop for one in passes if not one.drained})

    if not incomplete:
        logger.info("Outbox relay published %d row(s) on shutdown", published)
        return

    logger.warning(
        "Outbox relay shutdown drain stopped early (%s): published %d, rescheduled %d, "
        "failed %d — remaining rows stay pending for the next process",
        ", ".join(incomplete),
        published,
        sum(one.retried for one in passes),
        sum(one.failed for one in passes),
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _OutboxRelayBackgroundStartup(LifecycleHook):
    """Start a background task that periodically relays outbox rows."""

    outbox_spec: OutboxSpec[Any]
    """The outbox spec."""

    transport: OutboxDestinationKind
    """The transport to use."""

    queue_spec: QueueSpec[Any] | None
    """The queue spec to use."""

    stream_spec: StreamSpec[Any] | None
    """The stream spec to use."""

    pubsub_spec: PubSubSpec[Any] | None
    """The pubsub spec to use."""

    interval: timedelta
    """The interval to use."""

    jitter: float = 0.2
    """The jitter to use."""

    reclaim_stale_after: timedelta | None
    """The reclaim stale after to use."""

    limit: int | None
    """The limit to use."""

    max_attempts: int
    """The max attempts to use."""

    retry_base_delay: timedelta
    """The retry base delay to use."""

    retry_max_backoff: timedelta
    """The retry max backoff to use."""

    max_batches_per_tick: int
    """The max batches per tick to use."""

    tenants: Callable[[], Sequence[UUID]] | None = None
    """When set, the outbox is tenant-aware (partitioned): each tick drains every assigned
    tenant's partition under a bound tenant (namespace tier). The shard is
    evaluated **once at startup** and frozen for the process (restart to repartition),
    matching the gateway source. ``None`` = tenant-global; one unbound pass drains it."""

    drain_on_shutdown: bool = False
    """Publish what is still claimable when the process shuts down (see the step factory)."""

    shutdown_drain_timeout: timedelta = timedelta(seconds=5)
    """Total budget for stopping the loop and draining; must stay under the runtime's
    ``shutdown_step_timeout``."""

    # ....................... #

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(
            lambda self: BackgroundLoopControl(
                name=f"outbox_relay:{self.outbox_spec.name}",
                stop_grace=timedelta(seconds=_STOP_WAIT_SECONDS),
            ),
            takes_self=True,
        ),
        init=False,
    )
    """Stop signal and bounded teardown, shared with every other background loop."""

    tenant_shard: list[UUID] | None = attrs.field(default=None, init=False)
    """The assigned shard, frozen at startup. ``None`` = tenant-global."""

    scope_ctx: ExecutionContext | None = attrs.field(default=None, init=False, repr=False)
    """The scope's context, kept so the drain can run from :meth:`stop`, which the runtime
    calls without one."""

    drained: bool = attrs.field(default=False, init=False)
    """Whether a drain (shutdown or quiesce-time flush) **completed** this startup.

    :meth:`stop` is asked twice by design — once by the runtime before teardown, once by this
    step's own shutdown hook — and the drain must not be the part that repeats. A second pass
    would **re-claim the rows the first one just rescheduled** (they return to the head of the
    claim order once their backoff elapses), burning a second delivery attempt on each and
    dead-lettering a backlog the next process would have delivered.

    Only a pass that ran to completion claims the guard: one cut by its budget re-arms it,
    so the next ask retries the untouched remainder instead of silently stranding it — safe
    because a near-immediate retry cannot claim the rows the cut pass rescheduled (their
    backoff has not elapsed), and a strict pass ends on the first reschedule it does hit."""

    # ....................... #

    @property
    def task(self) -> asyncio.Task[None] | None:
        """The running loop, if any."""

        return self.control.task

    # ....................... #

    @property
    def loop_name(self) -> str:
        """Satisfies ``DrainableLoop``."""

        return self.control.loop_name

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.interval.total_seconds() <= 0:
            raise exc.configuration("Interval must be positive")

        if not 0.0 <= self.jitter < 1.0:
            raise exc.configuration("Jitter must be in [0, 1)")

        if self.reclaim_stale_after is not None and self.reclaim_stale_after.total_seconds() <= 0:
            raise exc.configuration("Reclaim stale after must be positive")

        if self.max_batches_per_tick < 1:
            raise exc.configuration("Max batches per tick must be >= 1")

        if self.shutdown_drain_timeout.total_seconds() <= 0:
            raise exc.configuration("Shutdown drain timeout must be positive")

        validate_retry_options(
            max_attempts=self.max_attempts,
            retry_base_delay=self.retry_base_delay,
            retry_max_backoff=self.retry_max_backoff,
        )

    # ....................... #

    @property
    def _stop_requested(self) -> bool:
        return self.control.stopping

    # ....................... #

    def request_stop(self) -> None:
        """Ask the loop to stop before its next batch, and wake it from its tick sleep."""

        self.control.request_stop()

    # ....................... #

    async def _relay_batch(
        self,
        ctx: ExecutionContext,
        *,
        reclaim_stale_after: timedelta | None,
    ) -> OutboxRelayResult:
        # Per-batch relay config: reclaim varies (only the first batch of a tick
        # reclaims stale rows), so build a fresh OutboxRelay per batch.
        relay = OutboxRelay(
            outbox_spec=self.outbox_spec,
            reclaim_stale_after=reclaim_stale_after,
            max_attempts=self.max_attempts,
            retry_base_delay=self.retry_base_delay,
            retry_max_backoff=self.retry_max_backoff,
        )

        if self.transport == "queue":
            if self.queue_spec is None:
                raise exc.precondition("queue_spec is required for queue background relay")

            return await relay.to_queue(ctx, self.queue_spec, limit=self.limit)

        return await relay.run(
            ctx,
            queue_spec=self.queue_spec,
            stream_spec=self.stream_spec,
            pubsub_spec=self.pubsub_spec,
            limit=self.limit,
        )

    # ....................... #

    async def _relay_once(
        self,
        ctx: ExecutionContext,
        *,
        strict: bool = False,
        reclaim: bool = True,
        deadline: float | None = None,
    ) -> _RelayPass:
        """Drain the backlog: relay batches until a claim comes back short.

        Stops when a batch claims fewer rows than the batch size (backlog drained) or
        after ``max_batches_per_tick`` batches (safety cap so a large backlog cannot
        starve the loop). Stale-processing *reclaim* runs only with the first batch.

        The steady tick is **tolerant**: a failing batch is logged and the tick carries
        on, because the next tick retries in ``interval``. The shutdown drain passes
        *strict*, which changes two things — it has no next tick to recover on:

        - a failing batch **ends the pass** instead of hammering a dead destination
          ``max_batches_per_tick`` times with no backoff;
        - a batch that **rescheduled** any row ends the pass, capping the drain at *one*
          delivery attempt per row (see the loop body for why that bound holds).

        *deadline* (a ``loop.time()`` value) is checked between batches, never mid-batch;
        see :func:`_admits_batch` for why a batch is never started on a thin budget.
        """

        loop = asyncio.get_running_loop()

        batches = claimed = published = retried = failed = 0
        last_batch: float | None = None
        stop: _PassStop = "cap"

        for batch_index in range(self.max_batches_per_tick):
            if not strict and self._stop_requested:
                # Shutdown asked us to stop: don't open another batch. The drain (or the
                # next process) picks the backlog up from here.
                stop = "stopped"
                break

            if deadline is not None and not _admits_batch(
                remaining=deadline - loop.time(), last_batch=last_batch
            ):
                stop = "budget"
                break

            started = loop.time()

            try:
                result = await self._relay_batch(
                    ctx,
                    reclaim_stale_after=(
                        self.reclaim_stale_after if reclaim and batch_index == 0 else None
                    ),
                )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception("Outbox background relay batch failed")

                if strict:
                    stop = "error"
                    break

                continue

            last_batch = loop.time() - started

            batches += 1
            claimed += result.claimed
            published += result.published
            retried += result.retried
            failed += result.failed

            if strict and result.retried:
                # Rescheduling is the *only* way a row comes back: it returns to
                # ``pending`` with a future ``available_at``, and ``claim_pending`` orders
                # by ``created_at`` — so a row this pass rescheduled is the oldest pending
                # row and would be re-claimed *first* once its backoff (as little as half
                # ``retry_base_delay``) elapses. A drain that kept going would walk the
                # same rows up to ``max_attempts`` and dead-letter a backlog the next
                # process would have delivered fine. Ending the pass here makes that
                # re-claim impossible: **one delivery attempt per row**, exactly one steady
                # tick's blast radius. ``failed`` is deliberately not a gate — a poison row
                # is terminal and says nothing about the destination's health.
                stop = "retry"
                break

            if result.claimed == 0 or (self.limit is not None and result.claimed < self.limit):
                stop = "drained"
                break

        return _RelayPass(
            stop=stop,
            batches=batches,
            claimed=claimed,
            published=published,
            retried=retried,
            failed=failed,
        )

    # ....................... #

    async def _drain_tick(
        self,
        ctx: ExecutionContext,
        tenants: Sequence[UUID] | None,
    ) -> None:
        """Drain the backlog once: globally, or per assigned tenant when sharded.

        *tenants* is the shard **frozen at startup** (``None`` for a tenant-global outbox).
        Sharded, each tenant's pass runs **bound** (so the relay reads/marks that tenant's
        partition and forwards under it) and is isolated — one tenant's failure must not
        skip the rest of the shard this tick. Tenants are drained **sequentially** (one DB
        connection at a time); shard across instances to parallelize.
        """

        if tenants is None:
            await self._relay_once(ctx)
            return

        for tenant in tenants:
            if self._stop_requested:
                # Leave the rest of the shard to the shutdown drain rather than opening a
                # claim per remaining tenant on a process that is going away.
                return

            try:
                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                    await self._relay_once(ctx)

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception("Outbox background relay failed for tenant", tenant=str(tenant))

    # ....................... #

    async def drain_for_shutdown(self, ctx: ExecutionContext, *, deadline: float) -> None:
        """Publish what is claimable right now, then leave the rest to the next process.

        Runs from the shutdown hook once the poll loop has stopped, so nothing else in
        this process is claiming the route. Each pass is strict (a failing batch, or one
        that reschedules a row, ends it), capped by ``max_batches_per_tick``, and bounded
        by *deadline*. Stale-processing reclaim is skipped: rows abandoned by some *other*
        dead process are not this teardown's business.

        Rows parked for a future retry, and rows this drain could not reach, stay
        ``pending`` — the next process relays them on its first tick.
        """

        loop = asyncio.get_running_loop()
        passes: list[_RelayPass] = []

        for tenant in self.tenant_shard if self.tenant_shard is not None else (None,):
            if loop.time() >= deadline:
                logger.warning(
                    "Outbox relay shutdown drain ran out of budget before every assigned "
                    "tenant was drained; the rest stay pending for the next process"
                )
                break

            try:
                if tenant is None:
                    passes.append(
                        await self._relay_once(ctx, strict=True, reclaim=False, deadline=deadline)
                    )
                    continue

                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                    passes.append(
                        await self._relay_once(ctx, strict=True, reclaim=False, deadline=deadline)
                    )

            except asyncio.CancelledError:
                raise

            except Exception:
                # One tenant's failure must not skip the rest of the shard.
                logger.exception(
                    "Outbox relay shutdown drain failed for tenant", tenant=str(tenant)
                )

        _log_drain_outcome(passes)

    # ....................... #

    async def _sleep_or_stop(self) -> bool:
        """Wait out the jittered tick interval; ``True`` when a stop was requested."""

        # Jittered tick: N replicas polling at the same fixed interval synchronize into a
        # thundering herd against the claim query; the multiplicative jitter desynchronizes
        # them. Desynchronization jitter, not security randomness.
        delay = self.interval.total_seconds() * (
            1.0 + current_entropy_source().as_random().uniform(-self.jitter, self.jitter)
        )

        return await self.control.sleep_or_stop(delay)

    # ....................... #

    async def _loop(self, ctx: ExecutionContext) -> None:
        while True:
            try:
                await self._drain_tick(ctx, self.tenant_shard)

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception("Outbox background relay failed")

            if await self._sleep_or_stop():
                return

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        """Stop the poll loop, then publish what is still claimable when asked to.

        The ``DrainableLoop`` entry point: the runtime calls this *before* lifecycle teardown,
        so the database client the drain needs is still open — which reverse-wave hook ordering
        could never guarantee. Idempotent, so the step's own shutdown hook can ask again.
        """

        clock = asyncio.get_running_loop()
        # The runtime's budget bounds us; our own is usually tighter.
        budget = min(deadline, clock.time() + self.shutdown_drain_timeout.total_seconds())
        stopped = await self.control.stop(deadline=budget)

        if not self.drain_on_shutdown or self.scope_ctx is None or self.drained:
            return stopped

        if not await self._bounded_drain(self.scope_ctx, deadline=budget, label="shutdown drain"):
            return False

        return stopped

    # ....................... #

    async def flush(self, *, deadline: float) -> None:
        """Publish what is claimable right now — the quiesce-time drain.

        Quiesce stops this loop before it watches the outbox plane, which ends the ticks:
        without an explicit flush, the backlog it then polls can never move (the stop-side
        drain only runs when ``drain_on_shutdown`` is set, and it defaults off), so the
        sweep burns its whole budget on a depth nothing is draining. This runs the same
        strict, reclaim-free drain the shutdown path uses, under the same once-per-startup
        guard — a later ``drain_on_shutdown`` teardown cannot re-claim (and burn a second
        delivery attempt on) the rows this pass just rescheduled. Only a **completed**
        pass claims the guard: a flush cut by its budget re-arms it, so the teardown
        drain retries the untouched remainder instead of silently stranding it.

        A ``pubsub`` destination is skipped for the same reason ``drain_on_shutdown``
        refuses it at wiring: pubsub is at-most-once past the broker, and quiesce precedes
        a shutdown or an export — publishing right as subscribers go away turns a delayed
        delivery into a silent loss. Its plane then reports residual honestly.
        """

        if self.transport == "pubsub":
            logger.info(
                "Outbox relay flush skipped for a pubsub destination; rows stay pending "
                "for the next process's relay"
            )
            return

        if self.scope_ctx is None or self.drained:
            return

        await self._bounded_drain(self.scope_ctx, deadline=deadline, label="flush")

    # ....................... #

    async def _bounded_drain(self, ctx: ExecutionContext, *, deadline: float, label: str) -> bool:
        """Claim the once-guard and run the strict drain inside the budget.

        Returns whether the pass completed. **Only a completed pass keeps the guard**
        — any other exit re-arms it: a pass cut by its budget (its cut batch's
        claimed rows sit ``processing`` — invisible to every replica — until
        ``reclaim_stale_after`` elapses), a cancellation (``stop_all`` cancels
        straggler stops when the grace elapses; a torn quiesce cancels its flush),
        or an unexpected raise. Holding the guard on any of those would make the
        next ask — the step's own shutdown hook, with a fresh budget — a silent
        no-op that strands the untouched remainder for another process. The retry
        is safe: it cannot re-claim what the cut pass rescheduled (those rows sit
        out their backoff), and a strict pass ends on the first reschedule it does
        hit.
        """

        self.drained = True
        clock = asyncio.get_running_loop()
        budget = min(deadline, clock.time() + self.shutdown_drain_timeout.total_seconds())

        try:
            async with asyncio.timeout_at(budget):
                await self.drain_for_shutdown(ctx, deadline=budget)

        except TimeoutError:
            logger.error(
                "Outbox relay %s exceeded its %.1fs budget and was cut mid-batch; "
                "claimed rows may sit in 'processing' until reclaim",
                label,
                self.shutdown_drain_timeout.total_seconds(),
            )
            self.drained = False
            return False

        except BaseException:
            # cancellation or an unexpected raise: not a completed pass — re-arm
            # exactly like the timeout path, then let the interruption propagate
            self.drained = False
            raise

        return True

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if self.control.running:
            # The runtime invokes startup once per scope; a direct double call
            # must not leak (and orphan) the previous relay task.
            logger.warning("Outbox relay already running; ignoring duplicate startup")
            return

        # Fresh per startup: an event reused across scopes still carries the previous
        # shutdown's stop, and the new loop would exit before its first tick.
        self.control.arm()
        self.scope_ctx = ctx
        self.drained = False

        # Freeze the assigned shard at **startup** (not inside the detached task), so a broken
        # tenants provider fails startup loudly instead of spawning a task that dies and
        # silently stops draining. Restart to repartition — one consistent onboarding model,
        # shared with the gateway source and the group-ensure step.
        self.tenant_shard = list(self.tenants()) if self.tenants is not None else None

        self.control.task = asyncio.create_task(self._loop(ctx), name=self.control.loop_name)

        # The runtime stops every registered loop before teardown begins.
        ctx.drainables.register(self)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _OutboxRelayBackgroundShutdown(LifecycleHook):
    """Stop the background outbox relay, draining what is claimable first when asked."""

    startup: _OutboxRelayBackgroundStartup
    """Startup hook."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        # Normally a no-op: the runtime stops every registered loop (``ctx.drainables``) before
        # teardown begins, which is both earlier and safer than here — the client the drain
        # needs is still open at that point, which reverse-wave hook ordering could never
        # guarantee. This stays the fallback for a hand-driven lifecycle, and ``stop`` is
        # idempotent, so asking twice costs nothing.
        clock = asyncio.get_running_loop()

        await self.startup.stop(
            deadline=clock.time() + self.startup.shutdown_drain_timeout.total_seconds()
        )


# ....................... #


def _validate_drain_wiring(*, transport: OutboxDestinationKind) -> None:
    """Fail closed on a drain that cannot be safe, at wiring time rather than at teardown."""

    if transport == "pubsub":
        raise exc.precondition(
            "drain_on_shutdown is not supported for a pubsub destination: pubsub is "
            "at-most-once past the broker, so publishing during teardown — exactly when "
            "subscribers are going away — turns a delayed delivery into a silent loss. "
            "Leave the rows pending; the next process relays them to live subscribers."
        )


# ....................... #


def outbox_relay_background_lifecycle_step(
    *,
    outbox_spec: OutboxSpec[Any],
    transport: OutboxDestinationKind = "queue",
    queue_spec: QueueSpec[Any] | None = None,
    stream_spec: StreamSpec[Any] | None = None,
    pubsub_spec: PubSubSpec[Any] | None = None,
    interval: timedelta = timedelta(seconds=30),
    jitter: float = 0.2,
    reclaim_stale_after: timedelta | None = timedelta(minutes=5),
    limit: int | None = None,
    max_attempts: int = 5,
    retry_base_delay: timedelta = timedelta(seconds=1),
    retry_max_backoff: timedelta = timedelta(minutes=5),
    max_batches_per_tick: int = 100,
    tenants: Callable[[], Sequence[UUID]] | None = None,
    drain_on_shutdown: bool = False,
    shutdown_drain_timeout: timedelta = timedelta(seconds=5),
    requires: tuple[StrKey, ...] = (),
    depends_on: tuple[StrKey, ...] = (),
    step_id: StrKey = "outbox_relay",
) -> LifecycleStep:
    """Build a lifecycle step that relays outbox rows on a background interval.

    *transport* selects which :class:`~forze_kits.integrations.outbox.OutboxRelay` method
    runs each tick (default ``queue``). Pass the matching spec for the transport. For
    ``queue``, *queue_spec* is required unless
    :attr:`~forze.application.contracts.outbox.OutboxSpec.destination` is unset and you
    relay via explicit *queue_spec* (``OutboxRelay.to_queue`` semantics).

    Each tick **drains the backlog**: batches of up to *limit* rows are relayed
    until a claim returns fewer rows than the batch size, capped at
    *max_batches_per_tick* batches so a large backlog cannot starve the loop;
    then the task sleeps *interval* with multiplicative *jitter* (default
    ±20%) so N replicas' relay loops do not synchronize into a thundering
    herd against the claim query. A failing batch is logged and does not
    abort the tick.

    *max_attempts*, *retry_base_delay*, and *retry_max_backoff* configure the
    per-row transient-failure retry policy — see
    :class:`~forze_kits.integrations.outbox.OutboxRelay`. Delivery is
    at-least-once and ordering is not preserved across failures/retries.

    When *tenants* is set the outbox is **tenant-aware** (partitioned): each tick drains
    every assigned tenant's partition under a bound tenant, so the relay can read a
    namespace-tier outbox (which fail-closes when read unbound) and forward each row to its
    tenant's destination. Tenants drain **sequentially** per tick; assign the **same shard**
    this instance's gateway consumes, and shard across instances to parallelize. Omit it for
    a tenant-global (tagged) outbox, which one unbound pass drains with per-row routing.

    Shutdown
    --------
    By default the relay task is simply cancelled: rows staged before shutdown stay
    ``pending`` until a later process claims them (up to *interval* later, or until the
    next deploy).

    Set *drain_on_shutdown* to publish what is still claimable before the process goes
    away. The drain stops the poll loop, then relays until the route is empty — burning
    **exactly one delivery attempt per row**, the same blast radius as one steady tick: it
    ends the moment a batch reschedules a row, so it can never re-claim (and re-attempt)
    what it just parked. It never waits on a future retry, never chases a failing
    destination, and never opens a batch it does not expect to finish. Anything it cannot
    reach within *shutdown_drain_timeout* stays ``pending`` for the next process, which is
    exactly where it would have been without the drain.

    The drain needs the database, and it gets it: the runtime stops every background loop
    (``ctx.drainables``) **before** lifecycle teardown begins, so the client is still open when
    the relay drains. That is why *requires* / *depends_on* are ordinary passthroughs here and
    not a precondition — declare them if the step needs ordering for its own reasons.

    Two things still constrain it:

    - Keep *shutdown_drain_timeout* below the runtime's ``shutdown_step_timeout``
      (default 10s), which bounds the whole loop-stopping pass. A batch cut mid-flight strands
      rows ``processing`` until *reclaim_stale_after* elapses — consider lowering that when
      draining.
    - A ``pubsub`` destination is rejected: it is at-most-once past the broker, so
      publishing while subscribers are going away turns a delayed delivery into a lost one.

    Opt-in for long-running processes. Production deployments often prefer
    external cron or workflow schedulers instead of in-process polling.
    """

    if transport == "queue" and queue_spec is None:
        raise exc.precondition("queue_spec is required when transport is queue")

    if transport == "stream" and stream_spec is None:
        raise exc.precondition("stream_spec is required when transport is stream")

    if transport == "pubsub" and pubsub_spec is None:
        raise exc.precondition("pubsub_spec is required when transport is pubsub")

    if drain_on_shutdown:
        _validate_drain_wiring(transport=transport)

    startup = _OutboxRelayBackgroundStartup(
        outbox_spec=outbox_spec,
        transport=transport,
        queue_spec=queue_spec,
        stream_spec=stream_spec,
        pubsub_spec=pubsub_spec,
        interval=interval,
        jitter=jitter,
        reclaim_stale_after=reclaim_stale_after,
        limit=limit,
        max_attempts=max_attempts,
        retry_base_delay=retry_base_delay,
        retry_max_backoff=retry_max_backoff,
        max_batches_per_tick=max_batches_per_tick,
        tenants=tenants,
        drain_on_shutdown=drain_on_shutdown,
        shutdown_drain_timeout=shutdown_drain_timeout,
    )
    shutdown = _OutboxRelayBackgroundShutdown(startup=startup)

    return LifecycleStep(
        id=step_id,
        startup=startup,
        shutdown=shutdown,
        requires=requires,
        depends_on=depends_on,
    )
