"""Names and bucket ladders the telemetry bootstrap configures on behalf of the framework."""

from typing import Final

# ----------------------- #

FORZE_METER_NAME: Final[str] = "forze"
"""The instrumentation scope every ``instrument_*`` helper records under.

Views are scoped to it so a bootstrap-installed bucket ladder never reshapes an
application's own histogram that happens to share a name.
"""

MILLISECOND_HISTOGRAM_BUCKETS: Final[tuple[float, ...]] = (
    1.0,
    2.0,
    5.0,
    10.0,
    25.0,
    50.0,
    100.0,
    250.0,
    500.0,
    1_000.0,
    2_500.0,
    5_000.0,
    10_000.0,
    30_000.0,
    60_000.0,
)
"""Explicit bucket boundaries for the framework's duration histograms, in milliseconds.

The SDK's default boundaries top out at 10 000 and start at 0/5/10 — a ladder built for
*seconds*. Forze records **milliseconds**, so under the default every sub-5 ms operation
collapses into one bucket and every request over 10 s into the overflow: the p50 of a fast
handler and the p99 of a slow one become equally unreadable. This ladder is roughly
logarithmic from 1 ms to a minute, which is the range these two instruments actually span.
"""

MILLISECOND_HISTOGRAM_INSTRUMENTS: Final[tuple[str, ...]] = (
    "forze.operation.duration",
    "forze.durable.run.duration",
)
"""Instruments the ms ladder is installed on.

Spelled as literals rather than imported from their defining modules on purpose:
``forze.base`` sits at the bottom of the layering and may not import
``forze.application`` (let alone ``forze_kits``, where the durable histogram lives). The
metric-name parity test asserts every entry here is a real, currently-declared metric
constant, so the duplication cannot rot silently.
"""
