from datetime import timedelta
from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CacheSpec:
    """Cache specification."""

    name: str
    """Namespace used for cache keys."""

    ttl: timedelta = timedelta(seconds=300)
    """Default TTL for cache entries."""
