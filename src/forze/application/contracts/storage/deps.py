"""Storage dependency keys and resolvers."""

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import StorageCommandPort, StorageQueryPort, StorageUploadSessionPort
from .specs import StorageSpec

# ----------------------- #

StorageQueryDepPort = ConfigurableDepPort[StorageSpec, StorageQueryPort]
"""Storage query dependency port."""

StorageCommandDepPort = ConfigurableDepPort[StorageSpec, StorageCommandPort]
"""Storage command dependency port."""

StorageUploadSessionDepPort = ConfigurableDepPort[StorageSpec, StorageUploadSessionPort]
"""Storage upload-session (multipart) dependency port."""

# ....................... #

StorageQueryDepKey = DepKey[StorageQueryDepPort]("storage_query")
"""Key used to register the :class:`StorageQueryPort` builder implementation."""

StorageCommandDepKey = DepKey[StorageCommandDepPort]("storage_command")
"""Key used to register the :class:`StorageCommandPort` builder implementation."""

StorageUploadSessionDepKey = DepKey[StorageUploadSessionDepPort]("storage_uploads")
"""Key used to register the :class:`StorageUploadSessionPort` builder implementation."""

# ....................... #


class StorageDeps(ConvenientDeps):
    """Convenience wrapper for storage dependencies."""

    def query(self, spec: StorageSpec) -> StorageQueryPort:
        """Resolve a storage query port for the given spec."""

        return self._resolve_configurable(StorageQueryDepKey, spec, route=spec.name)

    # ....................... #

    def command(self, spec: StorageSpec) -> StorageCommandPort:
        """Resolve a storage command port for the given spec."""

        return self._resolve_command(StorageCommandDepKey, spec, route=spec.name)

    # ....................... #

    def uploads(self, spec: StorageSpec) -> StorageUploadSessionPort:
        """Resolve a storage upload-session (multipart) port for the given spec.

        Multipart sessions are all writes, so this goes through the same
        CQRS write-guard as :meth:`command`: a read-only (``QUERY``) operation
        cannot acquire it and therefore cannot begin uploads.
        """

        return self._resolve_command(StorageUploadSessionDepKey, spec, route=spec.name)
