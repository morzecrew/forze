from datetime import timedelta

import attrs

from ..base import BaseSpec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class IdempotencySpec(BaseSpec):
    """Specification for idempotency behavior."""

    ttl: timedelta = timedelta(seconds=30)
    """Time-to-live for the idempotency snapshot."""
