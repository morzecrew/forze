"""Storage dependency keys and routers."""

from ..base import BaseDepPort, DepKey
from .ports import StoragePort
from .specs import StorageSpec

# ----------------------- #

StorageDepPort = BaseDepPort[StorageSpec, StoragePort]
"""Storage dependency port."""

StorageDepKey = DepKey[StorageDepPort]("storage")
"""Key used to register the :class:`StoragePort` builder implementation."""
