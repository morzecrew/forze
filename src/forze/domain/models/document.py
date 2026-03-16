"""Domain document models and commands.

The core :class:`Document` model implements versioning and update semantics
based on JSON-like diffs and pluggable update validators.
"""

from datetime import datetime
from typing import Any, ClassVar, Literal, Optional, Self, cast
from uuid import UUID

from pydantic import Field

from forze.base.errors import ValidationError
from forze.base.logging_v2 import getLogger
from forze.base.primitives import JsonDict, utcnow, uuid7
from forze.base.serialization import (
    apply_dict_patch,
    calculate_dict_difference,
    has_hybrid_patch_conflict,
    split_touches_from_merge_patch,
)

from ..validation import (
    UpdateValidator,
    UpdateValidatorMetadata,
    collect_update_validators,
)
from .base import BaseDTO, CoreModel

# ----------------------- #

logger = getLogger(__name__).bind(scope="domain")

# ....................... #


class Document(CoreModel):
    """Base document model with revision tracking and update validation."""

    _update_validators_: ClassVar[list[tuple[str, UpdateValidatorMetadata]]] = []
    """Update validators."""

    _update_validators_on_conflict: ClassVar[Literal["warn", "error", "overwrite"]] = (
        "warn"
    )
    """Update validators on conflict."""

    # ....................... #

    id: UUID = Field(default_factory=uuid7, frozen=True)
    """Unique identifier of the document."""

    rev: int = Field(default=1, frozen=True)
    """Revision number of the document."""

    created_at: datetime = Field(default_factory=utcnow, frozen=True)
    """Timestamp of the document creation."""

    last_update_at: datetime = Field(default_factory=utcnow)
    """Timestamp of the last document update."""

    # ....................... #

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)

        logger.trace(
            "Collecting update validators for document subclass %s",
            cls.__qualname__,
        )

        with logger.section():
            cls._update_validators_ = collect_update_validators(
                cls,
                on_conflict=cls._update_validators_on_conflict,
            )

            logger.trace(
                "Collected %d validator(s) for %s",
                len(cls._update_validators_),
                cls.__qualname__,
            )

    # ....................... #

    def _validate_update_data(self, data: JsonDict) -> JsonDict:
        """Validate incoming update data against model fields and frozen flags."""

        logger.trace("Validating update data for %s", type(self).__qualname__)

        valid: JsonDict = {}
        fields = type(self).model_fields

        for k, v in data.items():
            if k in fields:
                if fields[k].frozen:
                    raise ValidationError(
                        f"Field {k} is frozen and not allowed for update."
                    )

                valid[k] = v

            else:
                raise ValidationError(f"Field {k} is not found in the model.")

        return valid

    # ....................... #

    def _calculate_update_diff(self, data: JsonDict) -> JsonDict:
        """Return a minimal merge patch that represents ``data`` applied to self."""

        logger.trace(
            "Calculating update diff for %s",
            type(self).__qualname__,
        )

        with logger.section():
            patch = self._validate_update_data(data)
            before = self.model_dump(mode="json")
            after = apply_dict_patch(before, patch)
            diff = calculate_dict_difference(before, after)

        return diff

    # ....................... #

    def _apply_update(self, diff: JsonDict) -> Self:
        if not diff:
            logger.trace(
                "No diff for %s; returning original instance",
                type(self).__qualname__,
            )
            return self

        needs_deep = any(isinstance(v, (dict, list)) for v in diff.values())

        logger.trace(
            "Applying diff to %s (needs_deep=%s)",
            type(self).__qualname__,
            needs_deep,
        )

        return self.model_copy(update=diff, deep=needs_deep)

    # ....................... #

    def _run_update_validators(self, after: Self, diff: JsonDict) -> None:
        keys = diff.keys()
        cls = type(self)

        logger.trace("Running update validators for %s", cls.__qualname__)

        with logger.section():
            for name, meta in cls._update_validators_:
                if meta.fields is not None and keys.isdisjoint(meta.fields):
                    logger.trace("Skipping validator %s (fields=%s)", name, meta.fields)
                    continue

                logger.trace("Running validator %s (fields=%s)", name, meta.fields)

                method = cast(UpdateValidator[Self], getattr(cls, name))
                method(self, after, diff)

    # ....................... #

    def update(self, data: JsonDict) -> tuple[Self, JsonDict]:
        """Apply a validated update and return the new document and diff.

        The method:

        * validates the requested field changes,
        * computes a JSON merge-style diff,
        * bumps ``last_update_at``,
        * runs registered :func:`update_validator` hooks.
        """

        logger.debug(
            "Updating %s with keys=%s",
            type(self).__qualname__,
            tuple(data.keys()),
        )

        with logger.section():
            diff = self._calculate_update_diff(data)

            if diff:
                diff["last_update_at"] = utcnow()
                after = self._apply_update(diff)

            else:
                logger.trace("Update diff is empty; document remains unchanged")
                after = self

            self._run_update_validators(after, diff)

            return after, diff

    # ....................... #

    def touch(self) -> tuple[Self, JsonDict]:
        """Update only ``last_update_at`` and return a new instance and diff."""

        logger.debug("Touching %s", type(self).__qualname__)

        diff = {"last_update_at": utcnow()}
        model_copy = self.model_copy(update=diff)

        return model_copy, diff

    # ....................... #

    def validate_historical_consistency(self, old: Self, data: JsonDict) -> bool:
        """Check that applying ``data`` to ``old`` does not conflict with ``self``.

        This is used to prevent merging conflicting concurrent updates when
        reconstructing state from history.
        """

        logger.debug(
            "Validating historical consistency for %s with update keys=%s",
            type(self).__qualname__,
            tuple(data.keys()),
        )

        with logger.section():
            old_state = old.model_dump(mode="json")
            self_state = self.model_dump(mode="json")

            old_upd_state = apply_dict_patch(old_state, data)

            old_self_diff = calculate_dict_difference(old_state, self_state)
            old_upd_diff = calculate_dict_difference(old_state, old_upd_state)

            old_self_scalars, old_self_containers = split_touches_from_merge_patch(
                old_self_diff
            )
            old_upd_scalars, old_upd_containers = split_touches_from_merge_patch(
                old_upd_diff
            )

            has_conflict = has_hybrid_patch_conflict(
                old_self_scalars,
                old_self_containers,
                old_upd_scalars,
                old_upd_containers,
            )

            logger.trace("Historical consistency conflict=%s", has_conflict)

        return not has_conflict


# ....................... #


class CreateDocumentCmd(BaseDTO):
    """Create document command DTO."""

    id: Optional[UUID] = None
    """Unique identifier of the document. Added to ensure compatibility with external imports."""

    created_at: Optional[datetime] = None
    """Timestamp of the document creation. Added to ensure compatibility with external imports."""


# ....................... #


class ReadDocument(BaseDTO):
    """Read document model."""

    id: UUID
    """Unique identifier of the document."""

    rev: int
    """Revision number of the document."""

    created_at: datetime
    """Timestamp of the document creation."""

    last_update_at: datetime
    """Timestamp of the last document update."""


# ....................... #
# Document history


class DocumentHistory[D: Document](BaseDTO):
    """Document history entry representation."""

    source: str
    """Source of the document."""

    id: UUID
    """Unique identifier of the document."""

    rev: int
    """Revision number of the document."""

    created_at: datetime = Field(default_factory=utcnow)
    """Timestamp of the document history entry creation."""

    data: D
    """Document data."""
