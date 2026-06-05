from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DistributedLockSpec(BaseSpec):
    """Specification for distributed locks."""

    ttl: timedelta = timedelta(seconds=30)
    """Time-to-live for the lock.

    A very short default TTL is easy to lose under normal request latency; callers
    that need short leases should set ``ttl`` explicitly and use
    :class:`~forze_kits.scopes.DistributedLockScope` with
    ``extend_interval`` for long-held sections.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl.total_seconds() <= 0:
            raise exc.configuration("TTL must be positive")
