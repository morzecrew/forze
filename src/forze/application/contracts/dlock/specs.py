from datetime import timedelta
from typing import final

import attrs

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DistributedLockSpec(BaseSpec):
    """Specification for distributed locks."""

    ttl: timedelta = timedelta(milliseconds=200)
    """Time-to-live for the lock."""

    # wait_timeout: timedelta | None = None
    # """Timeout to wait for the lock acquisition."""

    # extend_interval: timedelta = timedelta(milliseconds=100)
    # """Interval at which to extend the lock's time-to-live."""

    # retry_interval: timedelta = timedelta(milliseconds=100)
    # """Interval at which to retry the lock acquisition."""
