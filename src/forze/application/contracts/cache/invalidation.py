"""Optional cache-port capability: push-based invalidation for in-process L1s.

A backend that can broadcast key invalidations (e.g. Redis ``CLIENT TRACKING``
client-side caching) implements :class:`SupportsInvalidationPush`. Consumers —
the document read-through coordinator's L1 — detect the capability with
``isinstance`` and subscribe; entries then drop on push instead of waiting out
the L1 TTL, which demotes the TTL from "the staleness budget" to a backstop.
"""

from collections.abc import Awaitable, Callable
from typing import Protocol, final, runtime_checkable

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CacheInvalidation:
    """One pushed invalidation event.

    ``key is None`` means **flush everything** for this subscription — emitted
    when the push stream (re)connects or degrades, because invalidations may
    have been missed while it was down.
    """

    key: str | None
    """The logical cache key (as passed to the port), or ``None`` for flush."""

    tenant: str | None = None
    """Tenant discriminator parsed from the backend key, when tenant-scoped."""


# ....................... #

InvalidationCallback = Callable[[CacheInvalidation], None]
"""Synchronous subscriber callback; must be cheap and never raise."""

Unsubscribe = Callable[[], Awaitable[None]]
"""Detach a subscription (idempotent)."""


# ....................... #


@runtime_checkable
class SupportsInvalidationPush(Protocol):
    """Cache ports able to push key invalidations to in-process subscribers."""

    async def subscribe_invalidations(
        self,
        callback: InvalidationCallback,
    ) -> Unsubscribe | None:
        """Subscribe *callback* to this port's invalidation stream.

        Returns ``None`` when push is not available (feature disabled, backend
        too old, unsupported namespace shape) — the subscriber falls back to
        its TTL-only semantics. Implementations emit a flush event
        (``CacheInvalidation(key=None)``) on every (re)connect and on
        degradation, so a subscriber never trusts state that predates a gap
        in the stream.
        """
        ...
