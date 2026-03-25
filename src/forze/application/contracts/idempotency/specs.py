from datetime import timedelta

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class IdempotencySpec:
    """Specification for idempotency behavior."""

    name: str
    """Logical idempotency name."""

    ttl: timedelta = timedelta(seconds=30)
    """Time-to-live for the idempotency snapshot."""
