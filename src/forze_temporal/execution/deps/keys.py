from forze.application.contracts.base import DepKey

from ...kernel.platform import TemporalClientPort

# ----------------------- #

TemporalClientDepKey: DepKey[TemporalClientPort] = DepKey("temporal_client")
"""Key used to register a Temporal client (single cluster or routed) in the deps container."""
