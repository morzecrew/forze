"""Fixed-window login lockout over the counter contract.

:class:`LoginLockoutGuard` is pure port composition — a
:class:`~forze.application.contracts.counter.CounterPort` plus the ambient
:func:`~forze.base.primitives.utcnow` time source — so it lives in the
integrations layer next to the orchestrator that consumes it.

**Why a fixed window.** :class:`CounterPort` exposes
``incr``/``incr_batch``/``decr``/``reset`` and nothing else — no TTL/expiry
surface (verified against the port and both shipped adapters). Without
key expiry, a sliding window or an explicit ``lock_for`` duration would need
per-key timestamps the port cannot store, so v1 uses fixed windows: failures
accumulate in the bucket ``floor(unix_now / window_seconds)`` and a locked
login unlocks at the window boundary. A ``lock_for`` override and counter-port
TTL (which would also let the Redis adapter expire stale buckets instead of
leaving dead keys behind) are noted as future counter-port capabilities.

**Keying.** Counter suffix =
``authn_lockout:{login_digest}:{window_bucket}`` — the
:func:`~forze.application.contracts.authn.login_digest` pseudonym, never the
raw login, so counter key spaces never materialize logins. Old buckets become
dead keys (value-only, no credential material); see the TTL note above.
"""

from datetime import timedelta
from typing import Final, final

import attrs

from forze.application.contracts.counter import CounterPort
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, utcnow

# ----------------------- #

LOCKED_LOGIN_MSG: Final = "Too many login attempts"
"""Uniform message raised for a locked login (no detail enumeration)."""

LOCKED_LOGIN_CODE: Final = "login_locked"
"""Machine-readable code on the ``throttled`` (HTTP 429) lockout error."""

LOCKOUT_COUNTER_ROUTE: Final[StrKey] = "authn_lockout"
"""Default counter route the lockout guard resolves its ``CounterPort`` from."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LockoutConfig:
    """Login lockout policy.

    ``threshold`` failed attempts within the current fixed ``window`` lock the
    login string until the window rolls over. There is deliberately no
    ``lock_for`` knob in v1: :class:`~forze.application.contracts.counter.CounterPort`
    has no TTL surface, so unlock-at-window-end is the only duration the port
    can express (see the module docstring).
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

    1. :meth:`is_locked` — reject the attempt with
       ``exc.throttled(LOCKED_LOGIN_MSG, code=LOCKED_LOGIN_CODE)`` when the
       current window already holds ``threshold`` failures;
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
       ``CounterPort`` is command-style with no read method, so :meth:`is_locked`
       reads via ``incr(0)`` — both shipped adapters (Redis ``INCRBY key 0`` and
       the mock) return the current value unchanged for a zero increment.
    """

    counter: CounterPort
    """Counter backend (resolve via ``ctx.counter`` on a dedicated route)."""

    config: LockoutConfig = attrs.field(factory=LockoutConfig)
    """Lockout policy."""

    # ....................... #

    def _suffix(self, login_digest: str) -> str:
        """Counter suffix for the digest's current fixed-window bucket."""

        window_seconds = int(self.config.window.total_seconds())
        bucket = int(utcnow().timestamp()) // window_seconds

        return f"authn_lockout:{login_digest}:{bucket}"

    # ....................... #

    async def is_locked(self, login_digest: str) -> bool:
        """Whether the current window already holds ``threshold`` failures."""

        current = await self.counter.incr(0, suffix=self._suffix(login_digest))

        return current >= self.config.threshold

    # ....................... #

    async def record_failure(self, login_digest: str) -> None:
        """Count one failed attempt in the current window."""

        await self.counter.incr(1, suffix=self._suffix(login_digest))

    # ....................... #

    async def record_success(self, login_digest: str) -> None:
        """Reset the current window bucket after a successful login."""

        await self.counter.reset(0, suffix=self._suffix(login_digest))
