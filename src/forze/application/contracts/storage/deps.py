"""Storage dependency keys and resolvers."""

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import StorageCommandPort, StorageQueryPort
from .specs import StorageSpec

# ----------------------- #

StorageQueryDepPort = ConfigurableDepPort[StorageSpec, StorageQueryPort]
"""Storage query dependency port."""

StorageCommandDepPort = ConfigurableDepPort[StorageSpec, StorageCommandPort]
"""Storage command dependency port."""

# ....................... #

StorageQueryDepKey = DepKey[StorageQueryDepPort]("storage_query")
"""Key used to register the :class:`StorageQueryPort` builder implementation."""

StorageCommandDepKey = DepKey[StorageCommandDepPort]("storage_command")
"""Key used to register the :class:`StorageCommandPort` builder implementation."""

# ....................... #


class StorageDeps(ConvenientDeps):
    """Convenience wrapper for storage dependencies."""

    def query(self, spec: StorageSpec) -> StorageQueryPort:
        """Resolve a storage query port for the given spec."""

        return self._resolve_configurable(StorageQueryDepKey, spec, route=spec.name)

    # ....................... #

    def command(self, spec: StorageSpec) -> StorageCommandPort:
        """Resolve a storage command port for the given spec."""

        return self._resolve_configurable(StorageCommandDepKey, spec, route=spec.name)
