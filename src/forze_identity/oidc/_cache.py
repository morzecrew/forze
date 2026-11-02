"""The bounded, short-lived assertion cache shared by the remote verifiers."""

import hashlib
import threading
from collections import OrderedDict
from collections.abc import Callable
from datetime import timedelta
from typing import Final, final

import attrs

from forze.application.contracts.authn import VerifiedAssertion
from forze.base.exceptions import exc
from forze.base.primitives import monotonic, utcnow

# ----------------------- #

MAX_CACHE_TTL: Final[timedelta] = timedelta(minutes=5)
"""The longest a remote verdict may be reused.

These caches exist to absorb a burst of requests carrying one token, not to extend trust: a
revoked token or session keeps working here until its entry expires."""


@final
@attrs.define(slots=True, kw_only=True, eq=False)
class TokenCache:
    """Remember verified assertions per token for at most :attr:`ttl`, keyed by digest.

    The key is the SHA-256 of the token, never the token. An entry lives :attr:`ttl` from when
    the remote call began, clamped to the expiry the verifier checked and to the assertion's
    ``expires_at``, so a token dies here at the TTL or at its expiry, whichever is first —
    never later. The expiry is also checked against the wall clock on every hit, so a forward
    jump of the system clock cannot serve an expired credential. Only positive verdicts are
    stored.
    """

    ttl: timedelta
    """How long a verdict is reused; zero disables the cache."""

    max_entries: int
    """Entries kept; the least recently used goes first."""

    clock: Callable[[], float] = monotonic
    """Seconds, for expiry."""

    _entries: OrderedDict[bytes, tuple[float, float | None, VerifiedAssertion]] = attrs.field(
        factory=OrderedDict[bytes, tuple[float, float | None, VerifiedAssertion]],
        init=False,
    )
    """Per digest: the monotonic deadline, the wall-clock expiry (POSIX) and the assertion."""

    _lock: threading.Lock = attrs.field(factory=threading.Lock, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not isinstance(self.ttl, timedelta) or not timedelta(0) <= self.ttl <= MAX_CACHE_TTL:
            raise exc.configuration(
                f"Cache TTL must be a timedelta between 0 and {MAX_CACHE_TTL}; these caches "
                "absorb bursts and must not outlive a revocation for long",
            )

        if (
            not isinstance(self.max_entries, int)
            or isinstance(self.max_entries, bool)
            or self.max_entries < 1
        ):
            raise exc.configuration("Cache max entries must be an integer of at least 1")

    # ....................... #

    @staticmethod
    def _key(token: str) -> bytes:
        return hashlib.sha256(token.encode()).digest()

    # ....................... #

    def begin(self) -> float:
        """The time a remote call began: pass it to :meth:`put`."""

        return self.clock()

    # ....................... #

    def get(self, token: str) -> VerifiedAssertion | None:
        """The assertion remembered for *token*, unless absent or expired."""

        key = self._key(token)

        with self._lock:
            entry = self._entries.get(key)

            if entry is None:
                return None

            deadline, wall_expiry, assertion = entry

            # The wall-clock check catches a forward jump of the system clock, which the
            # monotonic deadline cannot see: the credential's own expiry is wall-clock time.
            expired = wall_expiry is not None and utcnow().timestamp() >= wall_expiry

            if expired or self.clock() >= deadline:
                del self._entries[key]
                return None

            self._entries.move_to_end(key)

            return assertion

    # ....................... #

    def put(
        self,
        token: str,
        assertion: VerifiedAssertion,
        *,
        began: float,
        expires_at: float | None,
    ) -> None:
        """Remember *assertion* for *token* from a call that began at *began*.

        :param expires_at: The POSIX time the verifier checked the credential's expiry against,
            or ``None`` when the issuer gave none. Passed separately because the claim mapper
            may not carry it into the assertion.
        """

        now, wall = self.clock(), utcnow().timestamp()
        deadline = began + self.ttl.total_seconds()
        expiries: list[float] = []

        if expires_at is not None:
            expiries.append(float(expires_at))

        if assertion.expires_at is not None:
            expiries.append(assertion.expires_at.timestamp())

        wall_expiry = min(expiries, default=None)

        if wall_expiry is not None:
            deadline = min(deadline, now + wall_expiry - wall)

        if deadline <= now:
            return

        key = self._key(token)

        with self._lock:
            self._entries[key] = (deadline, wall_expiry, assertion)
            self._entries.move_to_end(key)

            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)
