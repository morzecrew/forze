from datetime import timedelta

import attrs

from forze.base.exceptions import exc

from ..base import BaseSpec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class IdempotencySpec(BaseSpec):
    """Specification for idempotency behavior."""

    ttl: timedelta = timedelta(seconds=30)
    """Time-to-live for the idempotency snapshot."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl.total_seconds() <= 0:
            raise exc.configuration("TTL must be positive")
