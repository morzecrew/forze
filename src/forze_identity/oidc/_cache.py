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
    never later. Only positive verdicts are stored.
    """

    ttl: timedelta
    """How long a verdict is reused; zero disables the cache."""

    max_entries: int
    """Entries kept; the least recently used goes first."""

    clock: Callable[[], float] = monotonic
    """Seconds, for expiry."""

    _entries: OrderedDict[bytes, tuple[float, VerifiedAssertion]] = attrs.field(
        factory=OrderedDict[bytes, tuple[float, VerifiedAssertion]],
        init=False,
    )
    _lock: threading.Lock = attrs.field(factory=threading.Lock, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not timedelta(0) <= self.ttl <= MAX_CACHE_TTL:
            raise exc.configuration(
                f"Cache TTL must be between 0 and {MAX_CACHE_TTL}; these caches absorb bursts "
                "and must not outlive a revocation for long",
            )

        if isinstance(self.max_entries, bool) or self.max_entries < 1:
            raise exc.configuration("Cache max entries must be at least 1")

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

            expires_at, assertion = entry

            if self.clock() >= expires_at:
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

        if assertion.expires_at is not None:
            deadline = min(deadline, now + assertion.expires_at.timestamp() - wall)

        if expires_at is not None:
            deadline = min(deadline, now + expires_at - wall)

        if deadline <= now:
            return

        key = self._key(token)

        with self._lock:
            self._entries[key] = (deadline, assertion)
            self._entries.move_to_end(key)

            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)
