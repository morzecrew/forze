"""Unit tests for freeze_stored_file_registry."""

from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze_kits.domain.stored_file import StoredFileKitSpec
from forze_kits.aggregates.stored_file import freeze_stored_file_registry


def test_freeze_stored_file_registry_returns_frozen_registry() -> None:
    kit = StoredFileKitSpec(name="files")
    frozen = freeze_stored_file_registry(kit, tx_route="mock")
    assert isinstance(frozen, FrozenOperationRegistry)
