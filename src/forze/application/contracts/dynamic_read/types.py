"""Per-call options for governed dynamic-read executions."""

from datetime import timedelta
from typing import TypedDict

# ----------------------- #


class DynamicReadOptions(TypedDict, total=False):
    """Per-execution overrides for one dynamic-read call.

    Both keys **clamp down only**: the route's configured statement timeout and the spec's
    :attr:`~forze.application.contracts.dynamic_read.DynamicReadSpec.row_cap` are ceilings a
    call can tighten and never raise. A caller that needs more changes the wiring, where a
    reviewer sees it.
    """

    timeout: timedelta
    """Statement timeout for this execution, clamped to the route's configured ceiling."""

    row_cap: int
    """Row ceiling for this execution, clamped to the spec's ``row_cap``."""
