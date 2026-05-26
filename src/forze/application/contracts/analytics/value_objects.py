"""Value objects returned by analytics ports."""

import attrs

from forze.base.primitives import JsonDict

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AnalyticsAppendResult:
    """Result of an append-only analytics ingest batch."""

    accepted: int
    """Number of rows accepted by the adapter."""

    rejected: int = 0
    """Number of rows rejected when the engine reports partial failures."""

    errors: tuple[JsonDict, ...] = ()
    """Optional row-level errors (capped by the integration client)."""
