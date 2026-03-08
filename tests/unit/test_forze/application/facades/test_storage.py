"""Unit tests for StorageOperation (usecases.storage)."""


from forze.application.usecases.storage import StorageOperation

# ----------------------- #


class TestStorageOperation:
    """Tests for StorageOperation enum."""

    def test_upload_value(self) -> None:
        assert StorageOperation.UPLOAD == "upload"

    def test_list_value(self) -> None:
        assert StorageOperation.LIST == "list"

    def test_download_value(self) -> None:
        assert StorageOperation.DOWNLOAD == "download"

    def test_delete_value(self) -> None:
        assert StorageOperation.DELETE == "delete"

    def test_all_members_string_values(self) -> None:
        for op in StorageOperation:
            assert isinstance(op.value, str)
            assert len(op.value) > 0
