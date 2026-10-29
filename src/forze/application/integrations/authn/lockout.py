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
"""

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Final, final

import attrs

from forze.application.contracts.counter import (
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
    login string until the window rolls over. There is no separate ``lock_for``
    duration: a locked login unlocks at the window boundary, which is the same
    instant for every login string, so ``Retry-After`` discloses nothing.
    """

    threshold: int = attrs.field(default=5)
    """Failed attempts allowed per window; once reached, further attempts are rejected."""

    window: timedelta = attrs.field(default=timedelta(minutes=15))
    """Fixed counting window; a locked login unlocks when the window rolls over."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.threshold < 1:
            raise exc.configuration("LockoutConfig.threshold must be >= 1")

        if self.window <= timedelta(0):
            raise exc.configuration("LockoutConfig.window must be positive")


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
       holds ``threshold`` failures (:meth:`is_locked` answers the same as a bool);
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

    def _lifetime(self) -> dict[str, timedelta]:
        """The ``ttl`` keyword for a bucket write, or none on a backend that cannot expire.

        Left out rather than passed as ``None`` there, so a counter port written before the
        keyword existed keeps working.
        """

        if not counter_capabilities(self.counter).expiry:
            return {}

        return {"ttl": self.config.window + LOCKOUT_TTL_SKEW}

    # ....................... #

    def _bucket(self) -> int:
        """The current fixed-window bucket number."""

        return int(utcnow().timestamp() // self._window_seconds())

    # ....................... #

    def _suffix(self, login_digest: str, bucket: int | None = None) -> str:
        """Counter suffix for the digest's fixed-window bucket (the current one by default)."""

        return f"authn_lockout:{login_digest}:{self._bucket() if bucket is None else bucket}"

    # ....................... #

    async def locked_until(self, login_digest: str) -> datetime | None:
        """When a locked login unlocks — the current window's end — or :obj:`None` if not locked.

        The instant depends only on the clock and :attr:`LockoutConfig.window`, never on the
        login or the account, so disclosing it (as ``Retry-After``) tells a client nothing it
        could not work out itself.
        """

        bucket = self._bucket()
        current = await self.counter.incr(0, suffix=self._suffix(login_digest, bucket))

        if current < self.config.threshold:
            return None

        return datetime.fromtimestamp((bucket + 1) * self._window_seconds(), tz=UTC)

    # ....................... #

    async def is_locked(self, login_digest: str) -> bool:
        """Whether the current window already holds ``threshold`` failures."""

        return await self.locked_until(login_digest) is not None

    # ....................... #

    async def _write_bucket(
        self,
        write: Callable[..., Awaitable[int]],
        value: int,
        suffix: str,
    ) -> None:
        """Write one bucket with its lifetime, or without one if the counter refuses it.

        Only the ``counter_ttl_unsupported`` refusal is absorbed, and only here: a caller
        that asks a counter for a ttl directly still gets the ``configuration`` error.
        """

        lifetime = self._lifetime()

        try:
            await write(value, suffix=suffix, **lifetime)

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

            await write(value, suffix=suffix)

    # ....................... #

    async def record_failure(self, login_digest: str) -> None:
        """Count one failed attempt in the current window."""

        await self._write_bucket(self.counter.incr, 1, self._suffix(login_digest))

    # ....................... #

    async def record_success(self, login_digest: str) -> None:
        """Reset the current window bucket after a successful login."""

        await self._write_bucket(self.counter.reset, 0, self._suffix(login_digest))
