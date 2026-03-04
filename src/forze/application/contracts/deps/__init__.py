"""Dependency injection contracts: keys, ports, and routers.

Provides :class:`DepKey` (typed dependency keys), :class:`DepsPort` (container
protocol), and :class:`DepRouter` (spec-based routing to dependency providers).
"""

from .key import DepKey
from .ports import DepsPort
from .router import DepRouter

# ----------------------- #

__all__ = ["DepKey", "DepsPort", "DepRouter"]
