from forze.base.exceptions import CoreException
import pytest


from forze.base.primitives import JsonDict
from forze_kits.domain.soft_deletion import SoftDeletionMixin
from forze_kits.domain.soft_deletion.constants import SOFT_DELETE_FIELD

class SoftDoc(SoftDeletionMixin): ...

def test_soft_deletion_mixin_defaults_to_not_deleted() -> None:
    doc = SoftDoc()
    assert doc.is_deleted is False

def test_soft_deletion_validator_blocks_non_soft_delete_updates_for_deleted_doc() -> (
    None
):
    before = SoftDoc(is_deleted=True)
    after = SoftDoc(is_deleted=True)
    diff: JsonDict = {"other": 1}

    with pytest.raises(CoreException):
        SoftDoc._validate_soft_deletion(before, after, diff)  # type: ignore[misc]

def test_soft_deletion_validator_allows_soft_delete_only_update() -> None:
    before = SoftDoc(is_deleted=True)
    after = SoftDoc(is_deleted=True)
    diff: JsonDict = {SOFT_DELETE_FIELD: True}

    SoftDoc._validate_soft_deletion(before, after, diff)  # type: ignore[misc]

def test_soft_deletion_validator_allows_is_deleted_with_last_update_at() -> None:
    before = SoftDoc(is_deleted=True)
    after = SoftDoc(is_deleted=False)
    diff: JsonDict = {SOFT_DELETE_FIELD: False, "last_update_at": "2025-01-01T00:00:00Z"}

    SoftDoc._validate_soft_deletion(before, after, diff)  # type: ignore[misc]
