import pytest

from forze.base.errors import ValidationError
from forze.base.primitives import JsonDict
from forze.domain.constants import SOFT_DELETE_FIELD
from forze.domain.mixins import SoftDeletionMixin


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

    with pytest.raises(ValidationError):
        # Access the validator method directly to simulate update validator call
        SoftDoc._validate_soft_deletion(before, after, diff)  # type: ignore[misc]


def test_soft_deletion_validator_allows_soft_delete_only_update() -> None:
    before = SoftDoc(is_deleted=True)
    after = SoftDoc(is_deleted=True)
    diff: JsonDict = {SOFT_DELETE_FIELD: True}

    # Should not raise
    SoftDoc._validate_soft_deletion(before, after, diff)  # type: ignore[misc]


def test_soft_deletion_validator_allows_is_deleted_with_last_update_at() -> None:
    """Matches :meth:`~forze.domain.models.Document.update` diff shape."""

    before = SoftDoc(is_deleted=True)
    after = SoftDoc(is_deleted=False)
    diff: JsonDict = {SOFT_DELETE_FIELD: False, "last_update_at": "2025-01-01T00:00:00Z"}

    SoftDoc._validate_soft_deletion(before, after, diff)  # type: ignore[misc]
