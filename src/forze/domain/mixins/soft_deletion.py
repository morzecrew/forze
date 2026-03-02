"""Soft-deletion mixin for documents that support logical deletion.

Adds ``is_deleted`` and an :func:`~forze.domain.validation.update_validator`
that blocks updates to soft-deleted documents except for the deletion flag itself.
"""

from typing import Self

from forze.base.errors import ValidationError
from forze.base.primitives import JsonDict

from ..constants import SOFT_DELETE_FIELD
from ..models import CoreModel
from ..validation import update_validator

# ----------------------- #


class SoftDeletionMixin(CoreModel):
    """Mixin adding soft-deletion semantics via ``is_deleted``.

    Once a document is soft-deleted, only the ``is_deleted`` field may be
    updated; any other update raises :exc:`~forze.base.errors.ValidationError`.
    """

    is_deleted: bool = False
    """Flag indicating if the document is soft deleted."""

    # ....................... #

    @update_validator
    def _validate_soft_deletion(before: Self, after: Self, diff: JsonDict) -> None:
        """Reject updates to soft-deleted documents unless only ``is_deleted`` changes."""

        keys = set(diff.keys())
        soft_deletion = keys == {SOFT_DELETE_FIELD}

        if before.is_deleted and not soft_deletion:
            raise ValidationError("Cannot update a soft-deleted document.")
