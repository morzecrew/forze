from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class L1Spec:
    """Opt-in in-process L1 ahead of the distributed document cache.

    Hot-document reads are served from process memory — decoded, no transport
    round-trip and no JSON decode — instead of hitting the cache backend.

    **This is a consistency contract change.** Writes invalidate the L1 only
    on the replica that performed them; other replicas serve their L1 entry
    until :attr:`ttl` expires. The TTL is therefore the **cross-replica
    staleness budget** — keep it small, and enable L1 only on read models
    that tolerate reads that stale. Same-replica read-your-writes is
    preserved (local writes refresh or invalidate the local L1).
    """

    ttl: timedelta
    """Maximum cross-replica staleness; entries expire after this. Must be
    strictly smaller than the owning :attr:`CacheSpec.ttl` so the backend
    cache still sees periodic reads (keeping early refresh functional)."""

    capacity: int = 1024
    """Maximum entries held in process memory (LRU-evicted beyond this)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl.total_seconds() <= 0:
            raise exc.configuration("L1 TTL must be positive")

        if self.capacity < 1:
            raise exc.configuration("L1 capacity must be >= 1")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CacheSpec(BaseSpec):
    """Cache specification."""

    ttl: timedelta = timedelta(seconds=300)
    """Default TTL for cache entries."""

    ttl_pointer: timedelta = timedelta(seconds=60)
    """TTL for the cache pointers (when using versioned cache)."""

    early_refresh_beta: float | None = None
    """Opt-in probabilistic early refresh (XFetch) for document read-through.

    When set (typical ``1.0``), a cache hit may volunteer to recompute *before*
    expiry with probability rising as expiry nears, scaled by the entry's
    observed recompute cost — so refreshes desynchronize across replicas and a
    hot key never expires for everyone at once (Vattani et al., "Optimal
    Probabilistic Cache Stampede Prevention"). Entries gain a small metadata
    envelope; ``None`` (default) keeps the payload format byte-identical.
    Higher values refresh earlier/more often."""

    l1: L1Spec | None = None
    """Opt-in in-process L1 for document read-through (see :class:`L1Spec`).
    ``None`` (default) keeps every read on the backend cache."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl.total_seconds() <= 0:
            raise exc.configuration("TTL must be positive")

        if self.ttl_pointer.total_seconds() <= 0:
            raise exc.configuration("TTL pointer must be positive")

        if self.early_refresh_beta is not None and self.early_refresh_beta <= 0:
            raise exc.configuration("Early refresh beta must be positive")

        if self.l1 is not None and self.l1.ttl >= self.ttl:
            raise exc.configuration(
                "L1 TTL must be strictly smaller than the cache TTL — the "
                "backend cache must keep seeing periodic reads",
            )
