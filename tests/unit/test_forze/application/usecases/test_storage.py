"""Unit tests for forze.application.usecases.storage.

The storage usecases module is currently a placeholder with no concrete
usecase implementations. These tests document the current state and
ensure the package can be imported.
"""

# ----------------------- #


class TestStorageUsecasesModule:
    """Tests for the storage usecases package."""

    def test_storage_package_imports(self) -> None:
        """Storage usecases package can be imported."""
        from forze.application.usecases import storage

        assert storage is not None

    def test_storage_operation_enum_available(self) -> None:
        """StorageOperation enum from usecases.storage is available for future use."""
        from forze.application.usecases.storage import StorageOperation

        assert StorageOperation.UPLOAD == "upload"
        assert StorageOperation.LIST == "list"
        assert StorageOperation.DOWNLOAD == "download"
        assert StorageOperation.DELETE == "delete"
