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

    requires_fencing_token: bool = False
    """Require the backend to issue monotonic fencing tokens.

    When ``True``, acquiring the command port fails closed (``exc.configuration``) against a
    backend that cannot issue them (not ``FencingAware``, or ``fencing_tokens=False``), so a
    consumer relying on fencing for write-safety is never silently wired onto best-effort
    exclusion. Default ``False`` leaves tokens best-effort (``AcquiredLock.token`` may be
    ``None``).
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl.total_seconds() <= 0:
            raise exc.configuration("TTL must be positive")
