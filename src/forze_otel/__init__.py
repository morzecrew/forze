"""OpenTelemetry traces + metrics export for Forze operations."""

from ._compat import require_otel

require_otel()

# ....................... #

from .instrumentation import (
    DURATION_HISTOGRAM,
    OPERATIONS_COUNTER,
    instrument_operations,
)

# ----------------------- #

__all__ = [
    "instrument_operations",
    "OPERATIONS_COUNTER",
    "DURATION_HISTOGRAM",
    "require_otel",
]
