"""Shared realtime coordination constants — imported by both publish and gateway sides.

The gateway consumer-group name is the one identifier the kit's group-ensure step
and the transport's signal source **must agree on**: the step creates the group the
source reads from. Both packages import this contract, so neither hardcodes the name.
"""

from typing import Final

# ----------------------- #

DEFAULT_REALTIME_GROUP: Final[str] = "realtime-gateway"
"""Consumer group the gateway reads under and the group-ensure step creates."""
