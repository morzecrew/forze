"""Inbox specification."""

from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class InboxSpec(BaseSpec):
    """Specification for a consumer-side dedup store."""

    ttl: timedelta = timedelta(days=7)
    """Dedup window (advisory; cleanup of expired entries is the store's responsibility)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl.total_seconds() <= 0:
            raise exc.configuration("TTL must be positive")
