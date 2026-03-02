"""Dependency injection contracts: keys, ports, and routers.

Provides :class:`DepKey` (typed dependency keys), :class:`DepsPort` (container
protocol), and :class:`DepRouter` (spec-based routing to dependency providers).
"""

from .ports import DepsPort
from .router import DepRouter
from .value_objects import DepKey

# ----------------------- #

__all__ = ["DepKey", "DepsPort", "DepRouter"]
