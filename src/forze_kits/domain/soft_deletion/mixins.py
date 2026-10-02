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
    """Fields that change only in the write that deletes or restores the row — e.g. a marker naming
    the cascade that deleted it, set by the delete and cleared by the restore. Any other update
    that changes one is refused, on a live row as on a deleted one. Each must be a field of the
    model, and the declaration a ``ClassVar``; empty by default."""

    is_deleted: bool = False
    """Flag indicating if the document is soft deleted."""

    # ....................... #

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)

        if "soft_delete_companions" in cls.model_fields:
            raise exc.configuration(
                f"{cls.__qualname__}.soft_delete_companions is declared as a field; annotate it "
                "ClassVar[frozenset[str]] or leave it unannotated, so the model decides what a "
                "delete or restore may change rather than each row's own data.",
            )

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
        """Reject updates to soft-deleted documents unless only is deleted and its companions
        change, and a companion change on any row unless the same write flips is deleted."""

        keys = set(diff.keys())
        companions = type(before).soft_delete_companions
        flips = SOFT_DELETE_FIELD in keys

        if not flips and (changed := keys & companions):
            raise exc.domain(
                f"{sorted(changed)} change only when the document is deleted or restored.",
            )

        if before.is_deleted and not (flips and keys <= ALLOWED_SOFT_DELETE_DIFF_KEYS | companions):
            raise exc.domain("Cannot update a soft-deleted document.")
