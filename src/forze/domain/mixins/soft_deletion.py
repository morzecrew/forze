from typing import Self

from forze.base.errors import ValidationError
from forze.base.primitives import JsonDict

from ..constants import SOFT_DELETE_FIELD
from ..models import BaseDTO, CoreModel
from ..validation import update_validator

# ----------------------- #


class SoftDeletionMixin(CoreModel):
    """Mixin for soft deletion."""

    is_deleted: bool = False
    """Flag indicating if the document is soft deleted."""

    # ....................... #

    @update_validator
    def _validate_soft_deletion(before: Self, after: Self, diff: JsonDict) -> None:
        """Validate soft deletion."""

        keys = set(diff.keys())
        soft_deletion = keys == {SOFT_DELETE_FIELD}

        if before.is_deleted and not soft_deletion:
            raise ValidationError("Cannot update a soft-deleted document.")


# ....................... #


class SoftDeletionCreateCmdMixin(SoftDeletionMixin, BaseDTO):
    """Mixin for soft deletion create command."""


# ....................... #


class SoftDeletionUpdateCmdMixin(SoftDeletionCreateCmdMixin):
    """Mixin for soft deletion update command."""
