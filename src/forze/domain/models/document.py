from datetime import datetime
from typing import Optional, Self
from uuid import UUID

from pydantic import Field, PositiveInt

from forze.base.errors import ValidationError
from forze.base.primitives import JsonDict, utcnow, uuid7
from forze.base.serialization import (
    apply_dict_patch,
    calculate_dict_difference,
    deep_dict_intersection,
)

from .base import BaseDTO, CoreModel

# ----------------------- #


class Document(CoreModel):
    """Base document model."""

    id: UUID = Field(default_factory=uuid7, frozen=True)
    """Unique identifier of the document."""

    rev: PositiveInt = Field(default=1, frozen=True)
    """Revision number of the document."""

    created_at: datetime = Field(default_factory=utcnow, frozen=True)
    """Timestamp of the document creation."""

    last_update_at: datetime = Field(default_factory=utcnow)
    """Timestamp of the last document update."""

    # ....................... #

    def _apply_update(self, diff: JsonDict) -> Self:
        if not diff:
            return self

        return self.model_copy(update=diff, deep=True)

    # ....................... #

    def _calculate_update_diff(self, data: JsonDict) -> JsonDict:
        patch = self._validate_update_data(data)
        before = self.model_dump(mode="json")

        after = apply_dict_patch(before, patch)
        diff = calculate_dict_difference(before, after)

        return diff

    # ....................... #

    def _validate_update_data(self, data: JsonDict) -> JsonDict:
        valid: JsonDict = {}
        fields = type(self).model_fields

        for k, v in data.items():
            if k in fields:
                if fields[k].frozen:
                    raise ValidationError(f"Поле {k} не разрешено для обновления.")

                valid[k] = v

            else:
                raise ValidationError(f"Поле {k} не разрешено для обновления.")

        return valid

    # ....................... #

    def update(self, data: JsonDict) -> tuple[Self, JsonDict]:
        diff = self._calculate_update_diff(data)

        if not diff:
            return self, {}

        diff["last_update_at"] = utcnow()
        after = self._apply_update(diff)

        return after, diff

    # ....................... #

    def touch(self) -> tuple[Self, JsonDict]:
        now = utcnow()
        self.last_update_at = now

        return self, {"last_update_at": now}

    # ....................... #

    def validate_historical_consistency(self, old: Self, data: JsonDict) -> bool:
        old_state = old.model_dump(mode="json")
        self_state = self.model_dump(mode="json")

        old_upd_state = apply_dict_patch(old_state, data)

        old_self_diff = calculate_dict_difference(old_state, self_state)
        old_upd_diff = calculate_dict_difference(old_state, old_upd_state)

        return deep_dict_intersection(old_self_diff, old_upd_diff) == set()


# ....................... #


class CreateDocumentCmd(BaseDTO):
    """Create document command DTO."""

    # id, created_at added to ensure compatibility with external imports

    id: Optional[UUID] = None
    """Unique identifier of the document."""

    created_at: Optional[datetime] = None
    """Timestamp of the document creation."""


# ....................... #


class ReadDocument(BaseDTO):
    """Read document model."""

    id: UUID
    """Unique identifier of the document."""

    rev: PositiveInt
    """Revision number of the document."""

    created_at: datetime
    """Timestamp of the document creation."""

    last_update_at: datetime
    """Timestamp of the last document update."""
