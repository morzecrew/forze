"""Fixed-window login lockout over the counter contract.

:class:`LoginLockoutGuard` is pure port composition — a
:class:`~forze.application.contracts.counter.CounterPort` plus the ambient
:func:`~forze.base.primitives.utcnow` time source — so it lives in the
integrations layer next to the orchestrator that consumes it.

**Why a fixed window.** Failures accumulate in the bucket
``floor(unix_now / window_seconds)`` and a locked login unlocks at the window
boundary. A sliding window would need per-attempt timestamps the counter port
cannot store; a fixed window needs one integer per bucket.

**Keying.** Counter suffix =
``authn_lockout:{login_digest}:{window_bucket}`` — the
:func:`~forze.application.contracts.authn.login_digest` pseudonym, never the
raw login, so counter key spaces never materialize logins.

**Expiry.** Each bucket is written with ``ttl = window + LOCKOUT_TTL_SKEW``, so it
expires on its own once its window is over instead of staying behind as a dead
key, and the check that runs on every attempt reads with ``incr(0)``, which does
not create a counter. Only a failed attempt (or a success resetting one) writes a
key.

A lockout that cannot expire its keys keeps locking; it only keeps its old keys.
On a counter backend that does not report
:class:`~forze.application.contracts.counter.CounterCapabilities` with
``expiry=True``, buckets are written without a ttl. On one that reports expiry but
refuses this ttl (``counter_ttl_unsupported`` — a Postgres counter table without
its ``expires_at`` column), the write is retried without one and a warning is
logged once per refusing table. Login availability outranks key growth, which is
what every deployment had before counters could expire.

**Lock duration.** With :attr:`LockoutConfig.lock_for` set, the failure that reaches the
threshold also writes the unlock instant (whole Unix seconds) to the digest's lock key,
``authn_lockout:{login_digest}:lock``, with ``ttl = lock_for + LOCKOUT_TTL_SKEW``. The check
reads that key with ``incr(0)`` too, and a login stays locked until the later of the window's
end and that instant.

A lock started in a window ends at least ``lock_for`` after that window began, so a stored
instant earlier than that belongs to an older lock. A writer that finds this window's lock
already stored leaves it, so a lock is never extended. A check that finds a full window
without its lock writes the lock then. That covers a lock write that failed and a slower write
from the previous window that replaced a later instant; the repaired lock counts from the check.

The stored instant decides when the login unlocks, so a backend that cannot expire keys still
unlocks on time. Mongo, Firestore and the mock time the key on the writer's clock, as the
instant is. Redis and Postgres time it on their own clock, so a writer whose clock runs ahead of
a reader's by more than the skew can see its key lapse first. The lock then ends
``lock_for + LOCKOUT_TTL_SKEW`` after it started. That is never shorter than ``lock_for``, but
it is before the announced ``Retry-After``.
"""

import math
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Final, final

import attrs

from forze.application.contracts.counter import (
    COUNTER_MAX_TTL,
    COUNTER_TTL_UNSUPPORTED_CODE,
    CounterPort,
    counter_capabilities,
)
from forze.base.exceptions import CoreException, exc
from forze.base.logging import Logger
from forze.base.primitives import CacheLane, StrKey, utcnow

# ----------------------- #

LOCKED_LOGIN_MSG: Final = "Too many login attempts"
"""Uniform message raised for a locked login (no detail enumeration)."""

LOCKED_LOGIN_CODE: Final = "login_locked"
"""Machine-readable code on the ``throttled`` (HTTP 429) lockout error."""

LOCKOUT_COUNTER_ROUTE: Final[StrKey] = "authn_lockout"
"""Default counter route the lockout guard resolves its ``CounterPort`` from."""

LOCKOUT_TTL_SKEW: Final[timedelta] = timedelta(minutes=1)
"""Lifetime a bucket gets beyond its window, for the spread between application clocks.

A bucket's window is chosen by the clock of the node that writes or reads it, and its
lifetime starts when it is created. The store's own clock offset cancels out: Redis takes the
ttl as a duration, Postgres compares its ``now()`` with an instant its ``now()`` wrote, and
Mongo, Firestore and the mock compare application instants with application instants. What
does not cancel is the spread between nodes. A fast node can create a bucket at the very
start of its window while a slow node still counts that window as current up to its end, so
a bucket timed as a duration must outlive its window by that spread, or a locked login
unlocks early on the slow node. One minute is far above the spread of NTP-synced nodes, and a
node off by more already disagrees about which window is current at all."""

_logger = Logger("integrations.authn.lockout")

_TTL_REFUSALS_REPORTED: Final[CacheLane[tuple[str, str], bool]] = CacheLane(max_entries=256)
"""Refusals already warned about, keyed by where they apply (the refusal's summary and
details, which name the table and partition), so a second table that refuses is still
reported. Capped, because per-tenant tables are unbounded."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LockoutConfig:
    """Login lockout policy.

    ``threshold`` failed attempts within the current fixed ``window`` lock the
    login string until the window rolls over, or until ``lock_for`` after the attempt
    that reached the threshold if that is later.

    Without ``lock_for`` a locked login unlocks at the window boundary, which is the
    same instant for every login string, so ``Retry-After`` discloses nothing. With it,
    ``Retry-After`` also shows when the threshold was reached; that instant is the same
    for a real and an unknown login, so it still discloses no account.
    """

    threshold: int = attrs.field(default=5)
    """Failed attempts allowed per window; once reached, further attempts are rejected."""

    window: timedelta = attrs.field(default=timedelta(minutes=15))
    """Fixed counting window; a locked login unlocks when the window rolls over."""

    lock_for: timedelta | None = attrs.field(default=None)
    """How long a login stays locked after the attempt that reached ``threshold``.

    ``None`` (default) unlocks at the window boundary. A lock that would end before the
    window does changes nothing: the login unlocks at the later of the two.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.threshold < 1:
            raise exc.configuration("LockoutConfig.threshold must be >= 1")

        # Each key lives its duration + LOCKOUT_TTL_SKEW, which the counter refuses past its cap.
        longest = COUNTER_MAX_TTL - LOCKOUT_TTL_SKEW

        if not timedelta(0) < self.window <= longest:
            raise exc.configuration(
                f"LockoutConfig.window must be positive and at most {longest!r}"
            )

        if self.lock_for is not None and not timedelta(0) < self.lock_for <= longest:
            raise exc.configuration(
                f"LockoutConfig.lock_for must be positive and at most {longest!r}"
            )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LoginLockoutGuard:
    """Fixed-window failed-login counter keyed by login digest.

    Callers (the authn orchestrator) drive the three-step protocol **before**
    password verification:

    1. :meth:`locked_until` — reject the attempt with
       ``exc.throttled(LOCKED_LOGIN_MSG, code=LOCKED_LOGIN_CODE)``, carrying the
       unlock instant as ``details["retry_at"]``, when the current window already
       holds ``threshold`` failures or a ``lock_for`` lock is running (:meth:`is_locked`
       answers the same as a bool);
    2. :meth:`record_failure` after a failed verification;
    3. :meth:`record_success` after a successful one (resets the current bucket).

    The guard sees only :func:`~forze.application.contracts.authn.login_digest`
    values, so it counts **login strings**, not accounts — attempts against
    nonexistent logins lock exactly like attempts against real ones, preserving
    the no-enumeration posture.

    Time reads :func:`~forze.base.primitives.utcnow`, so a bound
    :class:`~forze.base.primitives.FrozenTimeSource` drives the window bucket in
    tests.

    .. note::
       ``CounterPort`` is command-style with no read method, so :meth:`locked_until`
       reads via ``incr(0)``. That a zero increment returns the current value without
       moving it — and without creating the counter — is a pinned contract property the
       shared counter conformance battery asserts on every backend, so a check on an
       unseen digest leaves no key behind.
    """

    counter: CounterPort
    """Counter backend (resolve via ``ctx.counter`` on a dedicated route)."""

    config: LockoutConfig = attrs.field(factory=LockoutConfig)
    """Lockout policy."""

    # ....................... #

    def _window_seconds(self) -> float:
        # Not truncated to whole seconds: a sub-second window would divide by zero.
        return self.config.window.total_seconds()

    # ....................... #

    def _lifetime(self, ttl: timedelta) -> dict[str, timedelta]:
        """The ``ttl`` keyword for a write, or none on a backend that cannot expire.

        Left out rather than passed as ``None`` there, so a counter port written before the
        keyword existed keeps working.
        """

        if not counter_capabilities(self.counter).expiry:
            return {}

        return {"ttl": ttl}

    # ....................... #

    def _bucket(self) -> int:
        """The current fixed-window bucket number."""

        return int(utcnow().timestamp() // self._window_seconds())

    # ....................... #

    def _suffix(self, login_digest: str, bucket: int | None = None) -> str:
        """Counter suffix for the digest's fixed-window bucket (the current one by default)."""

        return f"authn_lockout:{login_digest}:{self._bucket() if bucket is None else bucket}"

    # ....................... #

    @staticmethod
    def _lock_suffix(login_digest: str) -> str:
        """Counter suffix holding the digest's ``lock_for`` unlock instant, in Unix seconds."""

        return f"authn_lockout:{login_digest}:lock"

    # ....................... #

    async def locked_until(self, login_digest: str) -> datetime | None:
        """When a locked login unlocks, or :obj:`None` if it is not locked.

        That is the current window's end once it holds ``threshold`` failures, or the
        ``lock_for`` unlock instant while that is in the future, whichever is later.

        The window's end depends only on the clock and :attr:`LockoutConfig.window`, never on
        the login or the account, so disclosing it (as ``Retry-After``) tells a client nothing
        it could not work out itself. A ``lock_for`` instant also shows when the threshold was
        reached, which is no account information either.
        """

        bucket = self._bucket()
        current = await self.counter.incr(0, suffix=self._suffix(login_digest, bucket))
        full = current >= self.config.threshold
        until = (
            datetime.fromtimestamp((bucket + 1) * self._window_seconds(), tz=UTC) if full else None
        )

        if self.config.lock_for is None:
            return until

        # Read in every window: the lock can outlive the window whose bucket set it.
        locked = await self._lock(login_digest, bucket, self.config.lock_for, arm=full)

        if locked <= utcnow().timestamp():
            return until

        lock_end = datetime.fromtimestamp(locked, tz=UTC)

        return lock_end if until is None else max(until, lock_end)

    # ....................... #

    async def is_locked(self, login_digest: str) -> bool:
        """Whether the login is locked now (see :meth:`locked_until`)."""

        return await self.locked_until(login_digest) is not None

    # ....................... #

    async def _lock(
        self,
        login_digest: str,
        bucket: int,
        lock_for: timedelta,
        *,
        arm: bool,
    ) -> int:
        """The digest's lock instant in Unix seconds (0 if none), written first if *arm* is
        set and *bucket*'s window has no lock stored yet. A stored lock of this window, or of
        a later one, is left as it is.
        """

        suffix = self._lock_suffix(login_digest)
        locked = await self.counter.incr(0, suffix=suffix)
        started_this_window = locked >= bucket * self._window_seconds() + lock_for.total_seconds()

        if not arm or started_this_window:
            return locked

        # Rounded up, so the lock never ends before lock_for has passed.
        locked = math.ceil((utcnow() + lock_for).timestamp())
        await self._write_key(self.counter.reset, locked, suffix, lock_for + LOCKOUT_TTL_SKEW)

        return locked

    # ....................... #

    async def _write_key(
        self,
        write: Callable[..., Awaitable[int]],
        value: int,
        suffix: str,
        ttl: timedelta,
    ) -> int:
        """Write one key with its lifetime, or without one if the counter refuses it.

        Only the ``counter_ttl_unsupported`` refusal is absorbed, and only here: a caller
        that asks a counter for a ttl directly still gets the ``configuration`` error.

        :returns: The counter's value after the write.
        """

        lifetime = self._lifetime(ttl)

        try:
            return await write(value, suffix=suffix, **lifetime)

        except CoreException as error:
            if not lifetime or error.code != COUNTER_TTL_UNSUPPORTED_CODE:
                raise

            reported = (error.summary, repr(sorted((error.details or {}).items())))

            if _TTL_REFUSALS_REPORTED.lookup(reported) is None:
                _TTL_REFUSALS_REPORTED.store(reported, True)
                _logger.warning(
                    "Login lockout counter refused a ttl; writing window keys without one, "
                    "so old windows stay stored until the counter can expire them",
                    reason=str(error),
                )

            return await write(value, suffix=suffix)

    # ....................... #

    async def record_failure(self, login_digest: str) -> None:
        """Count one failed attempt in the current window.

        An attempt that reaches ``threshold`` also starts the ``lock_for`` lock, if one is
        configured and this window has none stored yet. A failure racing past the gate after
        the lock started leaves it as it is. A failed lock write raises here, the same for
        a real and an unknown login, and the next check writes the lock.
        """

        bucket = self._bucket()
        failures = await self._write_key(
            self.counter.incr,
            1,
            self._suffix(login_digest, bucket),
            self.config.window + LOCKOUT_TTL_SKEW,
        )
        lock_for = self.config.lock_for

        if lock_for is not None and failures >= self.config.threshold:
            await self._lock(login_digest, bucket, lock_for, arm=True)

    # ....................... #

    async def record_success(self, login_digest: str) -> None:
        """Reset the current window bucket after a successful login.

        A running ``lock_for`` lock survives it: the login stays locked until the lock ends,
        which is what a lock means. A success can only meet one by racing the attempt that
        started it, since the gate rejects every attempt while it runs.
        """

        await self._write_key(
            self.counter.reset,
            0,
            self._suffix(login_digest),
            self.config.window + LOCKOUT_TTL_SKEW,
        )
