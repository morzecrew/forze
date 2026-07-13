from typing import Self

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.domain.models import CoreModel
from forze.domain.validation import update_validator

from .constants import ALLOWED_SOFT_DELETE_DIFF_KEYS, SOFT_DELETE_FIELD

# ----------------------- #


class SoftDeletionMixin(CoreModel):
    """Mixin adding soft-deletion semantics via is deleted.

    Once a document is soft-deleted, only the is deleted field may be
    updated; any other update raises :exc:`~forze.base.exceptions.CoreException`.
    """

    is_deleted: bool = False
    """Flag indicating if the document is soft deleted."""

    # ....................... #

    @update_validator
    def _validate_soft_deletion(before: Self, _: Self, diff: JsonDict) -> None:
        """Reject updates to soft-deleted documents unless only is deleted changes."""

        keys = set(diff.keys())
        soft_deletion = SOFT_DELETE_FIELD in keys and keys <= ALLOWED_SOFT_DELETE_DIFF_KEYS

        if before.is_deleted and not soft_deletion:
            raise exc.domain("Cannot update a soft-deleted document.")
