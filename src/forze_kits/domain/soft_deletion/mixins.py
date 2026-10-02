from typing import Any, ClassVar, Self

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.domain.models import CoreModel
from forze.domain.validation import update_validator

from .constants import ALLOWED_SOFT_DELETE_DIFF_KEYS, SOFT_DELETE_FIELD

# ----------------------- #


class SoftDeletionMixin(CoreModel):
    """Mixin adding soft-deletion semantics via is deleted.

    Once a document is soft-deleted, only the is deleted field — with the
    :attr:`soft_delete_companions` the model declares — may be updated; any other
    update raises :exc:`~forze.base.exceptions.CoreException`.
    """

    soft_delete_companions: ClassVar[frozenset[str]] = frozenset()
    """Fields a deleted row's update may change alongside ``is_deleted`` — e.g. a marker naming the
    cascade that deleted the row, cleared by the same write that restores it. A companion changes
    only together with the flag, never on its own. Each must be a field of the model; empty by
    default."""

    is_deleted: bool = False
    """Flag indicating if the document is soft deleted."""

    # ....................... #

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)

        declared: Any = cls.soft_delete_companions

        if isinstance(declared, str):
            raise exc.configuration(
                f"{cls.__qualname__}.soft_delete_companions must be a set of field names, "
                f"not the string {declared!r}.",
            )

        cls.soft_delete_companions = frozenset(declared)

        if unknown := cls.soft_delete_companions - set(cls.model_fields):
            raise exc.configuration(
                f"{cls.__qualname__} declares soft-delete companion(s) {sorted(unknown)} "
                "that are not fields of the model.",
            )

    # ....................... #

    @update_validator
    def _validate_soft_deletion(before: Self, _: Self, diff: JsonDict) -> None:
        """Reject updates to soft-deleted documents unless only is deleted and its companions change."""

        keys = set(diff.keys())
        allowed = ALLOWED_SOFT_DELETE_DIFF_KEYS | before.soft_delete_companions
        soft_deletion = SOFT_DELETE_FIELD in keys and keys <= allowed

        if before.is_deleted and not soft_deletion:
            raise exc.domain("Cannot update a soft-deleted document.")
