"""Cache dependency keys and routers."""

from ..base import BaseDepPort, DepKey
from .ports import CachePort
from .specs import CacheSpec

# ----------------------- #

CacheDepPort = BaseDepPort[CacheSpec, CachePort]
"""Cache dependency port."""

CacheDepKey = DepKey[CacheDepPort]("cache")
"""Key used to register the :class:`CachePort` builder implementation."""
