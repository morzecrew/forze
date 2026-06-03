"""Unit tests for stored-file domain models."""

from forze_kits.domain.stored_file import (
    StoredFileCreateCmd,
    StoredFileKitSpec,
    StoredFileRead,
    StoredFileStatus,
    StoredFileUpdateCmd,
)


class TestStoredFileModels:
    def test_kit_spec_document_and_storage(self) -> None:
        kit = StoredFileKitSpec(name="files")
        assert kit.document.name == "files"
        assert kit.resolved_storage.name == "files"

    def test_default_search_spec(self) -> None:
        spec = StoredFileKitSpec.default_search("files")
        assert spec.model_type is StoredFileRead
        assert "filename" in spec.fields

    def test_create_cmd_defaults_to_pending(self) -> None:
        cmd = StoredFileCreateCmd(filename="a.txt", size=3)
        assert cmd.status == StoredFileStatus.PENDING

    def test_update_cmd_accepts_status(self) -> None:
        cmd = StoredFileUpdateCmd(status=StoredFileStatus.DELETED)
        assert cmd.status == StoredFileStatus.DELETED
