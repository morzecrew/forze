"""Storage dependency keys and routers."""

from ..base import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import StoragePort
from .specs import StorageSpec

# ----------------------- #

StorageDepPort = ConfigurableDepPort[StorageSpec, StoragePort]
"""Storage dependency port."""

StorageDepKey = DepKey[StorageDepPort]("storage")
"""Key used to register the :class:`StoragePort` builder implementation."""

# ....................... #


class StorageDeps(ConvenientDeps):
    """Convenience wrapper for storage dependencies."""

    def __call__(self, spec: StorageSpec) -> StoragePort:
        """Resolve a storage port for the given spec."""

        return self._resolve_configurable(StorageDepKey, spec, route=spec.name)
