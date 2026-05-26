"""Per-request options for analytics query runs."""

from datetime import timedelta
from typing import TypedDict

# ----------------------- #


class AnalyticsRunOptions(TypedDict, total=False):
    """Adapter-specific options for a single analytics query execution.

    Omitted keys use adapter defaults from integration configuration.
    """

    dry_run: bool
    """When ``True``, validate and estimate cost without returning rows (where supported)."""

    max_rows: int
    """Upper bound on rows returned for this execution."""

    timeout: timedelta
    """Maximum time allowed for the query execution."""
