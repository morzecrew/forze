import attrs
from datetime import timedelta
from typing import final

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CacheSpec:
    """Cache specification."""

    namespace: str
    """Namespace used for cache keys."""

    ttl: timedelta = timedelta(seconds=300)
    """Default TTL for cache entries."""
