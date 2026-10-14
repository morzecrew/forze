"""Lifetime leadership: one replica runs a body for as long as it holds a renewing lease.

Every replica campaigns for a lease on the distributed-lock ports. The one that holds it runs
the body in a task, renews the lease while the body runs, and tells the body through its
:class:`Term` when the term is ending — early enough that the body's grace period finishes
before the lease could lapse. The lease is released only once the body is done, so a successor
takes over within one campaign interval rather than a full TTL.

Each term acquires under its own owner string, the caller's ``owner`` plus a fresh nonce,
because a lock backend compares owners by string alone: a late release or renewal from an old
term then never touches a newer holder's lease, even when two replicas were given the same
``owner``.
"""

from __future__ import annotations

import asyncio
import math
import weakref
from collections.abc import Awaitable, Callable, Iterable
from contextlib import suppress
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final, Literal, final

import attrs

from forze.application.contracts.dlock import (
    AcquiredLock,
    DistributedLockCommandPort,
    DistributedLockQueryPort,
    DistributedLockSpec,
)
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.background import (
    BackgroundLoopControl,
    is_terminal_crash,
    run_supervised,
)
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, current_entropy_source, monotonic
from forze_kits.lifecycle._logger import logger

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Meter, Observation

# ----------------------- #

TermEnd = Literal["lost", "expiring", "stopped", "yielded", "returned", "raised", "unclaimed"]
"""Why a term ended.

- ``lost`` — a renewal answered that the lease is gone; the body is cancelled at once.
- ``expiring`` — the soft deadline passed without a successful renewal; the body is cancelled
  ``release_timeout`` before the hard deadline, so its unwind is done by then.
- ``stopped`` — the step is stopping; the body is cancelled after ``yield_grace``, or
  ``release_timeout`` before the hard deadline if that comes first.
- ``yielded`` — another replica asked for the lease; cancelled as for ``stopped``.
- ``returned`` — the body returned on its own.
- ``raised`` — the body raised; the supervisor backs off before the next campaign.
- ``unclaimed`` — the ``claim`` hook found the term's token already claimed.
"""

Handoff = Literal["none", "on_request"]
"""Whether a replica that prefers the unit may ask the holder to yield it."""

LEADERSHIP_HELD_GAUGE = "forze.leadership.held"
LEADERSHIP_TERMS_ENDED_COUNTER = "forze.leadership.terms_ended"

_DEFER_INTERVALS: Final[int] = 3
"""Campaign intervals the lease may stay free before replicas stop deferring to an asker."""

_ASK_ATTEMPTS: Final[int] = 3
"""Consecutive campaigns an asker may miss before it releases the want-lock."""

_STOP_SLACK: Final[timedelta] = timedelta(seconds=1)
"""Room in the loop's stop grace for the release call after the body's grace and its unwind."""


# ....................... #


def _positive(name: str, value: timedelta) -> None:
    if value.total_seconds() <= 0:
        raise exc.configuration(f"Leadership {name} must be positive")


def _non_negative(name: str, value: timedelta) -> None:
    if value.total_seconds() < 0:
        raise exc.configuration(f"Leadership {name} must not be negative")


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class LeadershipSpec:
    """What a leadership step campaigns for, and how its terms are timed.

    Validated when built: a term's renewal interval, grace, unwind and clock margin must fit
    inside the lease, so a body told its term is ending — and, if it overruns, cancelled — is
    done before the lease could lapse; and a replica must campaign more often than a lease
    lives.
    """

    lock: DistributedLockSpec
    """The lock the lease lives on. Its ``ttl`` is the lease's length, and
    ``requires_fencing_token`` makes a backend without fencing tokens fail closed."""

    key: str
    """The lease's key. Every replica campaigning for the same unit of work uses the same key."""

    renew_every: timedelta | None = None
    """How often the holder renews the lease. ``None`` renews every third of ``lock.ttl``."""

    campaign_every: timedelta = timedelta(seconds=2)
    """How often a replica that does not hold the lease tries to acquire it."""

    margin: timedelta = timedelta(seconds=1)
    """Allowance for the lock backend's clock and the network.

    A term's hard deadline is ``lock.ttl - margin`` after the latest successful renewal was
    sent. The backend keeps the lease's expiry on its own clock, so a step of that clock
    forward ends the lease early; size the margin for the deployment's clock discipline.
    """

    yield_grace: timedelta = timedelta(seconds=5)
    """How long a body gets to finish after its term ends, before it is cancelled. Never cut
    short while renewals succeed; when they fail, cut at ``release_timeout`` before the hard
    deadline."""

    release_timeout: timedelta = timedelta(seconds=1)
    """How long a cancelled body gets to finish. One that takes longer keeps the lease until it
    lapses, unreleased. Also the bound on each release call and campaign read, and no longer
    than ``yield_grace``."""

    verify: bool = False
    """Make :meth:`Term.held` also read the lease's owner from the backend."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.key:
            raise exc.configuration("Leadership key must not be empty")

        _positive("campaign_every", self.campaign_every)
        _positive("release_timeout", self.release_timeout)
        _non_negative("margin", self.margin)
        _non_negative("yield_grace", self.yield_grace)

        if self.renew_every is not None:
            _positive("renew_every", self.renew_every)

        if self.release_timeout > self.yield_grace:
            raise exc.configuration(
                "Leadership release_timeout must not exceed yield_grace: a body is cancelled "
                "release_timeout before the hard deadline, which must not come before it is told",
                details={
                    "release_timeout": str(self.release_timeout),
                    "yield_grace": str(self.yield_grace),
                },
            )

        if (
            self.renew_interval + self.yield_grace + self.release_timeout + self.margin
            >= self.lock.ttl
        ):
            raise exc.configuration(
                "Leadership renew_every + yield_grace + release_timeout + margin must be shorter "
                "than the lock's ttl, so a term's grace and its unwind end before the lease could "
                "lapse",
                details={
                    "renew_every": str(self.renew_interval),
                    "yield_grace": str(self.yield_grace),
                    "release_timeout": str(self.release_timeout),
                    "margin": str(self.margin),
                    "ttl": str(self.lock.ttl),
                },
            )

        if self.campaign_every >= self.lock.ttl:
            raise exc.configuration(
                "Leadership campaign_every must be shorter than the lock's ttl",
                details={"campaign_every": str(self.campaign_every), "ttl": str(self.lock.ttl)},
            )

    # ....................... #

    @property
    def renew_interval(self) -> timedelta:
        """The renewal interval in force: ``renew_every``, or a third of the lock's ttl."""

        return self.renew_every if self.renew_every is not None else self.lock.ttl / 3


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, eq=False)
class Term:
    """One replica's hold on the lease, from its acquire until its body is done.

    The body watches :attr:`ended` — or checks :meth:`live` between units of work — and
    finishes the unit in hand when it is set. It passes :attr:`token` with every write it makes
    as leader, so the store can refuse a write from a holder whose lease has passed on.
    """

    key: str
    """The lease's key."""

    owner: str
    """The owner string this term acquired under: the step's ``owner`` plus this term's nonce."""

    token: int | None
    """The lock's fencing token for this term, stable for its whole life. ``None`` when the
    backend does not issue tokens."""

    ended: asyncio.Event = attrs.field(factory=asyncio.Event, init=False)
    """Set once, by the step, when the term ends."""

    reason: TermEnd | None = attrs.field(default=None, init=False)
    """Why the term ended; ``None`` while it is live."""

    _owner_of: Callable[[str], Awaitable[str | None]] | None = attrs.field(
        default=None,
        alias="owner_of",
        repr=False,
    )
    """Reads the lease's current owner; set when the spec asks to ``verify``."""

    # ....................... #

    def live(self) -> bool:
        """Whether the term has not ended. No I/O."""

        return not self.ended.is_set()

    # ....................... #

    async def held(self) -> bool:
        """Whether the term is live and, when the spec sets ``verify``, the backend still names
        this term as the lease's owner.

        The read costs one round trip; use it before a unit that is expensive or cannot be
        undone. A failed read raises.
        """

        if not self.live():
            return False

        if self._owner_of is None:
            return True

        current = await self._owner_of(self.key)

        return self.live() and current == self.owner

    # ....................... #

    def _end(self, reason: TermEnd) -> None:
        if self.ended.is_set():
            return

        self.reason = reason
        self.ended.set()


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, eq=False)
class _LeadershipStats:
    """What one step reports to the metrics: whether it holds its lease, and how terms ended."""

    key: str
    holding: bool = False
    terms_ended: dict[str, int] = attrs.field(factory=dict)

    # ....................... #

    def ended(self, reason: TermEnd) -> None:
        self.terms_ended[reason] = self.terms_ended.get(reason, 0) + 1


_LIVE_STATS: Final[weakref.WeakSet[_LeadershipStats]] = weakref.WeakSet()
"""Every live step's stats, read by :func:`instrument_leadership` at collection time."""


def _new_stats(startup: _LeadershipStartup) -> _LeadershipStats:
    stats = _LeadershipStats(key=startup.spec.key)
    _LIVE_STATS.add(stats)

    return stats


# ....................... #


def instrument_leadership(*, meter: Meter | None = None) -> None:
    """Export every leadership step's state as OpenTelemetry metrics.

    - ``forze.leadership.held`` (observable gauge): ``1`` while this process holds the lease,
      labelled ``forze.leadership.key``.
    - ``forze.leadership.terms_ended`` (observable counter): terms ended, labelled
      ``forze.leadership.key`` and ``forze.leadership.reason``.

    Emits via the global OTel meter unless *meter* is supplied. Call once at assembly time,
    alongside the other ``instrument_*`` calls.
    """

    from opentelemetry import metrics
    from opentelemetry.metrics import Observation

    meter = meter or metrics.get_meter("forze")

    def _held(_options: CallbackOptions) -> Iterable[Observation]:
        totals: dict[str, int] = {}

        for stats in list(_LIVE_STATS):
            totals[stats.key] = totals.get(stats.key, 0) + int(stats.holding)

        for key, value in totals.items():
            yield Observation(value, {"forze.leadership.key": key})

    def _ended(_options: CallbackOptions) -> Iterable[Observation]:
        totals: dict[tuple[str, str], int] = {}

        for stats in list(_LIVE_STATS):
            # A copy: the step's loop may add a reason while a reader thread collects.
            for reason, count in dict(stats.terms_ended).items():
                totals[stats.key, reason] = totals.get((stats.key, reason), 0) + count

        for (key, reason), value in totals.items():
            yield Observation(
                value,
                {"forze.leadership.key": key, "forze.leadership.reason": reason},
            )

    meter.create_observable_gauge(
        LEADERSHIP_HELD_GAUGE,
        callbacks=[_held],
        unit="1",
        description="Whether this process holds the leadership lease (0 or 1).",
    )
    meter.create_observable_counter(
        LEADERSHIP_TERMS_ENDED_COUNTER,
        callbacks=[_ended],
        unit="1",
        description="Leadership terms ended, by reason.",
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, eq=False)
class _Campaign:
    """One startup's campaign: its ports and context, and what it remembers between ticks."""

    spec: LeadershipSpec
    owner: str
    lead: Callable[[ExecutionContext, Term], Awaitable[None]]
    may_campaign: Callable[[ExecutionContext], Awaitable[bool]] | None
    handoff: Handoff
    prefer: Callable[[ExecutionContext], Awaitable[bool]] | None
    claim: Callable[[ExecutionContext, Term], Awaitable[bool]] | None
    ctx: ExecutionContext
    cmd: DistributedLockCommandPort
    query: DistributedLockQueryPort | None
    control: BackgroundLoopControl
    stats: _LeadershipStats

    # ....................... #

    candidate: str | None = None
    """The owner string the next term acquires under; minted when needed, kept by the term
    that acquires with it. Attempts answered ``None`` never held the lease, so they reuse it."""

    want_owner: str | None = None
    """The string this replica holds the want-lock under, while it asks for the lease."""

    missed: int = 0
    """Consecutive campaigns this replica missed while holding the want-lock."""

    ask_after: float = -math.inf
    """When an asker that gave up may ask again: holders yield to any asker, so one that cannot
    acquire would otherwise take the unit from each of them at every renewal."""

    free_since: float | None = None
    """When this replica, deferring to an asker, first saw the lease free."""

    abandoned: asyncio.Task[TermEnd] | None = None
    """A body that did not finish after it was cancelled. No new term starts beside it."""

    stragglers: set[asyncio.Task[None]] = attrs.field(factory=set)
    """Acquires the step stopped waiting for, settling in the background."""

    # ....................... #

    @property
    def want_key(self) -> str:
        return f"{self.spec.key}:want"

    @property
    def stop(self) -> asyncio.Event:
        event = self.control.event

        if event is None:  # pragma: no cover - the startup arms it before the loop runs
            raise exc.internal("Leadership campaign started without a stop signal")

        return event

    # ....................... #

    async def run(self) -> None:
        """Campaign until stopped. A body's error reaches the supervisor after its release."""

        error: Exception | None = None

        while not self.stop.is_set():
            error = await self._tick()

            if error is not None:
                break

            if await self.control.sleep_or_stop(self.spec.campaign_every.total_seconds()):
                break

        await self._retire()

        if error is not None:
            raise error

    # ....................... #

    async def _tick(self) -> Exception | None:
        if self.abandoned is not None and not self.abandoned.done():
            return None

        try:
            if not await self._allowed():
                return None

            acquired = await self._acquire()

        except Exception as error:
            if is_terminal_crash(error):
                return error

            logger.warning(
                "Leadership campaign for %s failed; retrying at the next tick",
                self.spec.key,
                exc_info=error,
            )
            await self._missed()
            return None

        if acquired is None:
            return None

        return await self._hold(*acquired)

    # ....................... #

    async def _allowed(self) -> bool:
        if self.may_campaign is not None and not await self.may_campaign(self.ctx):
            await self._drop_want()
            return False

        if self.handoff == "none" or self.prefer is None:
            return True

        if await self.prefer(self.ctx) and monotonic() >= self.ask_after:
            await self._ask()

        else:
            await self._drop_want()

        return self.want_owner is not None or not await self._deferring()

    # ....................... #

    async def _ask(self) -> None:
        candidate = self._candidate()

        if self.want_owner is not None and self.want_owner != candidate:
            # The want-lock rides the string the next term acquires under; a retired string
            # gives it up and asks again under the new one.
            await self._release(self.want_key, self.want_owner)
            self.want_owner = None

        if self.want_owner is not None and await self.cmd.reset(self.want_key, self.want_owner):
            return

        acquired = await self.cmd.acquire(self.want_key, candidate)
        self.want_owner = candidate if acquired is not None else None

    # ....................... #

    async def _deferring(self) -> bool:
        if self.query is None:  # pragma: no cover - resolved whenever handoff is on
            return False

        if await self.query.get_owner(self.want_key) is None:
            self.free_since = None
            return False

        if await self.query.is_locked(self.spec.key):
            self.free_since = None
            return True

        now = monotonic()

        if self.free_since is None:
            self.free_since = now

        return now - self.free_since <= _DEFER_INTERVALS * self.spec.campaign_every.total_seconds()

    # ....................... #

    async def _missed(self) -> None:
        # Only a failed acquire counts. One answered ``None`` met a holder, which yields at its
        # next renewal or prefers the unit and never will.
        if self.want_owner is None:
            return

        self.missed += 1

        if self.missed >= _ASK_ATTEMPTS:
            await self._drop_want()
            self.ask_after = monotonic() + self.spec.lock.ttl.total_seconds()

    # ....................... #

    async def _drop_want(self) -> None:
        owner, self.want_owner, self.missed = self.want_owner, None, 0

        if owner is not None:
            await self._release(self.want_key, owner)

    # ....................... #

    def _candidate(self) -> str:
        if self.candidate is None:
            self.candidate = f"{self.owner}:{current_entropy_source().randbits(64):016x}"

        return self.candidate

    # ....................... #

    async def _acquire(self) -> tuple[AcquiredLock, float] | None:
        owner = self._candidate()
        sent = monotonic()
        request = asyncio.ensure_future(self.cmd.acquire(self.spec.key, owner))

        try:
            await self._until({request}, timeout=self._soft(sent) - monotonic())

        except BaseException:
            self._settle_late(request, owner)
            raise

        if not request.done():
            # Cancelling the request would not stop it landing, so it runs on, and a lease it
            # lands is released under its own string. That string is retired with it.
            self._settle_late(request, owner)
            self.candidate = None

            if self.stop.is_set():
                return None

            raise TimeoutError(f"Leadership acquire of {self.spec.key} was not answered in time")

        acquired = request.result()

        if acquired is None:
            return None

        if self.stop.is_set():
            await self._release(self.spec.key, acquired.owner)
            return None

        return acquired, sent

    # ....................... #

    def _settle_late(self, request: asyncio.Future[AcquiredLock | None], owner: str) -> None:
        async def settle() -> None:
            with suppress(Exception):
                if await request is not None:
                    logger.info(
                        "Leadership acquire for %s landed after the step stopped waiting; "
                        "releasing it",
                        self.spec.key,
                    )
                    await self._release(self.spec.key, owner)

        task = asyncio.ensure_future(settle())
        self.stragglers.add(task)
        task.add_done_callback(self.stragglers.discard)

    # ....................... #

    async def _release(self, key: str, owner: str) -> bool:
        try:
            return await asyncio.wait_for(
                self.cmd.release(key, owner),
                timeout=self.spec.release_timeout.total_seconds(),
            )

        except Exception as error:
            logger.warning(
                "Leadership release of %s failed; it lapses at its ttl",
                key,
                error=str(error),
            )
            return False

    # ....................... #

    def _hard(self, sent: float) -> float:
        return sent + (self.spec.lock.ttl - self.spec.margin).total_seconds()

    def _soft(self, sent: float) -> float:
        return self._hard(sent) - self.spec.yield_grace.total_seconds()

    # ....................... #

    async def _until(
        self,
        waits: Iterable[asyncio.Future[Any]],
        *,
        timeout: float,
        watch_stop: bool = True,
    ) -> None:
        """Wait for the first of *waits* (and a stop request), or *timeout*; cancels nothing."""

        pending: set[asyncio.Future[Any]] = set(waits)
        stop = asyncio.ensure_future(self.stop.wait()) if watch_stop else None

        if stop is not None:
            pending.add(stop)

        try:
            await asyncio.wait(
                pending,
                timeout=max(0.0, timeout),
                return_when=asyncio.FIRST_COMPLETED,
            )

        finally:
            if stop is not None:
                stop.cancel()

    # ....................... #

    async def _hold(self, acquired: AcquiredLock, sent: float) -> Exception | None:
        """Run one term: the body in a task, renewed until it is done, then released."""

        self.candidate = None

        if self.want_owner is not None:
            await self._drop_want()

        term = Term(
            key=self.spec.key,
            owner=acquired.owner,
            token=acquired.token,
            owner_of=(
                self.query.get_owner if self.spec.verify and self.query is not None else None
            ),
        )
        self.stats.holding = True
        logger.info(
            "Leadership term for %s started",
            self.spec.key,
            owner=term.owner,
            fence=term.token,  # a key named *token* is scrubbed from logs
        )

        body = asyncio.ensure_future(self._body(term))

        try:
            lost = await self._supervise(term, body, sent)

            if not body.done():
                body.cancel()
                await self._until(
                    {body},
                    timeout=self.spec.release_timeout.total_seconds(),
                    watch_stop=False,
                )

        except BaseException:
            # Cancelled from outside, past the stop grace: the body cannot be confirmed done,
            # so the lease is left to lapse rather than released under it.
            body.cancel()
            self.stats.holding = False
            raise

        raised: BaseException | None = None
        outcome: TermEnd = "returned"

        if body.done() and not body.cancelled():
            raised = body.exception()
            outcome = "raised" if raised is not None else body.result()

        term._end(outcome)

        error = raised if term.reason == "raised" and isinstance(raised, Exception) else None

        if raised is not None and error is None:
            logger.error(
                "Leadership body for %s raised after its term ended",
                self.spec.key,
                exc_info=raised,
            )

        if not body.done():
            self.abandoned = body
            logger.error(
                "Leadership body for %s did not finish within %.1fs of being cancelled; "
                "leaving the lease to lapse at its ttl",
                self.spec.key,
                self.spec.release_timeout.total_seconds(),
            )

        elif not lost:
            await self._release(self.spec.key, term.owner)

        self.stats.holding = False
        self.stats.ended(term.reason or "returned")
        logger.info(
            "Leadership term for %s ended",
            self.spec.key,
            owner=term.owner,
            fence=term.token,  # a key named *token* is scrubbed from logs
            reason=term.reason,
        )

        return error

    # ....................... #

    async def _body(self, term: Term) -> TermEnd:
        if self.claim is not None and not await self.claim(self.ctx, term):
            return "unclaimed"

        await self.lead(self.ctx, term)

        return "returned"

    # ....................... #

    async def _supervise(  # skipcq: PY-R1000
        self,
        term: Term,
        body: asyncio.Future[TermEnd],
        sent: float,
    ) -> bool:
        """Renew until the body is done or must be cancelled now.

        :returns: ``True`` when a renewal answered that the lease is gone.
        """

        renew_every = self.spec.renew_interval.total_seconds()
        grace = self.spec.yield_grace.total_seconds()
        # The latest a body may be cancelled: one that finishes within release_timeout of its
        # cancellation is then done by the hard deadline.
        unwind = self.spec.release_timeout.total_seconds()

        hard, soft = self._hard(sent), self._soft(sent)
        next_renewal = sent + renew_every
        renewal: asyncio.Task[tuple[bool | None, bool]] | None = None
        renewal_sent = sent
        grace_end: float | None = None

        try:
            while not body.done():
                now = monotonic()

                if renewal is not None and renewal.done():
                    renewed, wanted = renewal.result()
                    renewal = None

                    if renewed is False:
                        term._end("lost")
                        logger.warning(
                            "Leadership lease for %s is gone; cancelling the body",
                            self.spec.key,
                            owner=term.owner,
                        )
                        return True

                    if renewed:
                        hard, soft = self._hard(renewal_sent), self._soft(renewal_sent)

                    if wanted and term.live():
                        term._end("yielded")
                        grace_end = min(now + grace, hard - unwind)

                if term.live():
                    if self.stop.is_set():
                        term._end("stopped")
                        grace_end = min(now + grace, hard - unwind)

                    elif now >= soft:
                        term._end("expiring")
                        grace_end = hard - unwind

                if grace_end is not None and now >= grace_end:
                    return False

                if renewal is None and now >= next_renewal:
                    bound = (soft if term.live() else hard) - now

                    if bound > 0:
                        renewal_sent = now
                        renewal = asyncio.ensure_future(self._renew(term, bound))

                    next_renewal = now + renew_every

                wakes = [soft if term.live() else grace_end]

                if renewal is None:
                    wakes.append(next_renewal)

                waits: set[asyncio.Future[Any]] = {body}

                if renewal is not None:
                    waits.add(renewal)

                await self._until(
                    waits,
                    timeout=min(t for t in wakes if t is not None) - monotonic(),
                    watch_stop=term.live(),
                )

            return False

        finally:
            if renewal is not None:
                renewal.cancel()

    # ....................... #

    async def _renew(self, term: Term, bound: float) -> tuple[bool | None, bool]:
        """Reset the lease, bounded; ``None`` when the answer is unknown.

        With handoff on, a successful renewal also reads the want-lock, within what is left of
        the bound: the second item says whether another replica asks and this one does not
        prefer the unit.
        """

        deadline = monotonic() + bound

        try:
            renewed = await asyncio.wait_for(self.cmd.reset(self.spec.key, term.owner), bound)

        except Exception as error:
            logger.warning(
                "Leadership renewal of %s failed; its deadlines stay where they were",
                self.spec.key,
                error=repr(error),
            )
            return None, False

        if not renewed or self.handoff == "none" or not term.live():
            return renewed, False

        try:
            wanted = await asyncio.wait_for(
                self._wanted_by_another(term),
                max(0.0, deadline - monotonic()),
            )

        except Exception as error:
            logger.warning(
                "Leadership handoff check for %s failed; holding on",
                self.spec.key,
                error=repr(error),
            )
            wanted = False

        return True, wanted

    # ....................... #

    async def _wanted_by_another(self, term: Term) -> bool:
        if self.query is None or self.prefer is None:  # pragma: no cover - set with handoff
            return False

        asker = await self.query.get_owner(self.want_key)

        return asker is not None and asker != term.owner and not await self.prefer(self.ctx)

    # ....................... #

    async def _retire(self) -> None:
        await self._drop_want()

        if self.stragglers:
            await asyncio.wait(
                set(self.stragglers),
                timeout=self.spec.release_timeout.total_seconds(),
            )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, eq=False)
class _LeadershipStartup(LifecycleHook):
    """Start the campaign loop under supervision, and register it to be stopped."""

    spec: LeadershipSpec
    owner: str
    lead: Callable[[ExecutionContext, Term], Awaitable[None]]
    may_campaign: Callable[[ExecutionContext], Awaitable[bool]] | None
    handoff: Handoff
    prefer: Callable[[ExecutionContext], Awaitable[bool]] | None
    claim: Callable[[ExecutionContext, Term], Awaitable[bool]] | None

    # ....................... #

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(
            lambda self: BackgroundLoopControl(
                name=f"leadership:{self.spec.key}",
                stop_grace=self.spec.yield_grace + self.spec.release_timeout + _STOP_SLACK,
            ),
            takes_self=True,
        ),
        init=False,
    )
    """Stop signal and bounded teardown; the grace covers a term's grace and its release."""

    stats: _LeadershipStats = attrs.field(
        default=attrs.Factory(_new_stats, takes_self=True),
        init=False,
    )

    # ....................... #

    @property
    def loop_name(self) -> str:
        """Satisfies ``DrainableLoop``."""

        return self.control.loop_name

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        """End the term as ``stopped``, let the body finish, release, and return. Idempotent."""

        return await self.control.stop(deadline=deadline)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if self.control.running:
            logger.warning(
                "Leadership step for %s already running; ignoring duplicate startup",
                self.spec.key,
            )
            return

        # Resolved from the live context, so a spec that requires fencing fails startup on a
        # backend without tokens instead of failing inside the loop.
        cmd = ctx.dlock.command(self.spec.lock)
        query = (
            ctx.dlock.query(self.spec.lock)
            if self.spec.verify or self.handoff == "on_request"
            else None
        )

        stop = self.control.arm()
        campaign = _Campaign(
            spec=self.spec,
            owner=self.owner,
            lead=self.lead,
            may_campaign=self.may_campaign,
            handoff=self.handoff,
            prefer=self.prefer,
            claim=self.claim,
            ctx=ctx,
            cmd=cmd,
            query=query,
            control=self.control,
            stats=self.stats,
        )

        self.control.task = asyncio.create_task(
            run_supervised(campaign.run, stop=stop, name=self.control.loop_name),
            name=self.control.loop_name,
        )
        ctx.drainables.register(self)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _LeadershipShutdown(LifecycleHook):
    """Stop the campaign loop.

    Normally a no-op — the runtime stops every registered loop before teardown begins. This is
    the fallback for a hand-driven lifecycle; ``stop`` is idempotent.
    """

    startup: _LeadershipStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        grace = self.startup.control.stop_grace.total_seconds()
        await self.startup.stop(deadline=clock.time() + grace)


# ....................... #


def leadership_lifecycle_step(
    spec: LeadershipSpec,
    *,
    owner: str,
    lead: Callable[[ExecutionContext, Term], Awaitable[None]],
    may_campaign: Callable[[ExecutionContext], Awaitable[bool]] | None = None,
    handoff: Handoff = "none",
    prefer: Callable[[ExecutionContext], Awaitable[bool]] | None = None,
    fence_floor: Callable[[ExecutionContext], Awaitable[int]] | None = None,
    claim: Callable[[ExecutionContext, Term], Awaitable[bool]] | None = None,
    step_id: StrKey | None = None,
) -> LifecycleStep:
    """Build a step that runs *lead* on one replica at a time, for as long as it holds the lease.

    Every replica campaigns every ``spec.campaign_every``. The one that acquires the lease runs
    ``lead(ctx, term)`` in a task and renews the lease every ``spec.renew_interval``. Both of a
    term's deadlines are measured from the moment the latest successful renewal (or the
    acquire) was *sent*, so a slow answer shortens the term and never lengthens it:

    - the **hard** deadline, ``ttl - margin`` after that moment, by which the lease may be
      gone;
    - the **soft** deadline, ``yield_grace`` before it, by which the body is told.

    A body still running when its grace ends is cancelled, at the latest ``release_timeout``
    before the hard deadline, so one that finishes within ``release_timeout`` of its
    cancellation is done by the hard deadline. A renewal answered ``False`` ends the term as
    ``lost`` and cancels the body at once. A
    renewal that fails or times out leaves the deadlines where they were. The lease is released
    only once the body is done; a cancelled body that does not finish within
    ``release_timeout`` keeps it until it lapses at its ttl, so no successor starts before
    then, and this replica starts no new term while that body runs.

    A responsive leader whose body finishes within ``release_timeout`` of being cancelled runs
    only while it holds the lease, as long as the lock backend's clock does not jump forward by
    more than ``margin``. A paused
    process, a blocked loop or a backend that lost its data is covered only by fencing: pass
    ``term.token`` with every write and have the store refuse a lower token.

    The step's startup runs on every replica and writes nothing shared, so it is marked only
    ``requires_long_running``: accepted under ``FLEET`` without a singleton guard, refused under
    ``SERVERLESS``.

    :param spec: The lease and the timing of its terms.
    :param owner: This replica's identity; each term appends its own nonce to it.
    :param lead: The body, run once per term.
    :param may_campaign: Consulted before each campaign; ``False`` sits the campaign out and
        never takes the want-lock.
    :param handoff: ``"on_request"`` lets a replica that prefers the unit ask the holder to
        yield it, through a second lock on ``{key}:want``.
    :param prefer: Whether this replica prefers the unit; required with handoff. A holder that
        prefers it never yields.
    :param fence_floor: Not supported yet: no lock backend can advance its fencing counter.
    :param claim: Claims the term's fencing token in the store before *lead* runs; ``False``
        ends the term as ``unclaimed``. Requires ``spec.lock.requires_fencing_token``.
    :param step_id: The lifecycle step id; defaults to ``leadership:<key>``.
    :raises CoreException: ``configuration``, for handoff without *prefer*, any
        *fence_floor*, or *claim* on a lock that does not require fencing tokens.
    """

    if not owner:
        raise exc.configuration("Leadership owner must not be empty")

    if handoff not in ("none", "on_request"):
        raise exc.configuration(f"Unknown leadership handoff {handoff!r}")

    if handoff == "on_request" and prefer is None:
        raise exc.configuration("Leadership handoff on request needs `prefer`")

    if fence_floor is not None:
        raise exc.configuration(
            "Leadership fence_floor needs a lock backend that can advance its fencing "
            "counter, and none can yet; omit it",
            code="leadership.fence_floor_unsupported",
        )

    if claim is not None and not spec.lock.requires_fencing_token:
        raise exc.configuration(
            "Leadership claim needs a fencing token to claim: set requires_fencing_token on "
            "the lock spec",
            code="leadership.claim_without_fencing",
        )

    startup = _LeadershipStartup(
        spec=spec,
        owner=owner,
        lead=lead,
        may_campaign=may_campaign,
        handoff=handoff,
        prefer=prefer,
        claim=claim,
    )

    return LifecycleStep(
        id=step_id if step_id is not None else f"leadership:{spec.key}",
        startup=startup,
        shutdown=_LeadershipShutdown(startup=startup),
        requires_long_running=True,
    )
