"""Counter contracts for monotonic sequence generation.

Provides :class:`CounterPort` and dependency keys/routers for building
counter instances from execution context and namespace.
"""

from .deps import CounterDepKey, CounterDepPort, CounterDepRouter
from .ports import CounterPort

# ----------------------- #

__all__ = [
    "CounterPort",
    "CounterDepKey",
    "CounterDepPort",
    "CounterDepRouter",
]
