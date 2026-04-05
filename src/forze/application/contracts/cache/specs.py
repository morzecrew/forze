from datetime import timedelta
from typing import final

import attrs

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
