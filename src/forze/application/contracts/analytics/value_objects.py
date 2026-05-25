"""Value objects returned by analytics ports."""

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AnalyticsAppendResult:
    """Result of an append-only analytics ingest batch."""

    accepted: int
    """Number of rows accepted by the adapter."""
