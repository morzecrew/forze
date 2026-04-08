from forze.application.contracts.base import DepKey

from ...kernel.platform import TemporalClient

# ----------------------- #

TemporalClientDepKey: DepKey[TemporalClient] = DepKey("temporal_client")
"""Key used to register the :class:`TemporalClient` in the deps container."""
