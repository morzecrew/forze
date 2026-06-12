from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc

from ..base import BaseSpec

# ----------------------- #


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

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl.total_seconds() <= 0:
            raise exc.configuration("TTL must be positive")

        if self.ttl_pointer.total_seconds() <= 0:
            raise exc.configuration("TTL pointer must be positive")

        if self.early_refresh_beta is not None and self.early_refresh_beta <= 0:
            raise exc.configuration("Early refresh beta must be positive")
