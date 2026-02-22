from typing import Optional

from pydantic import Field, PositiveInt

from forze.base.errors import ValidationError
from forze.base.primitives import JsonDict, LongString, String

from ..constants import SOFT_DELETE_FIELD
from .base import BaseDTO, CoreModel
from .document import CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class Entity(Document):
    """Base entity model."""

    is_deleted: bool = Field(default=False)
    """Flag indicating if the entity is soft deleted."""

    # ....................... #

    # Use validation hooks instead ? (from erp-v2 project)
    def _validate_update_data(self, data: JsonDict) -> JsonDict:
        valid = super()._validate_update_data(data)

        soft_deletion = SOFT_DELETE_FIELD in valid
        other_fields = len(valid.keys()) > 1

        if soft_deletion and other_fields:
            raise ValidationError(
                "Мягкое удаление не может быть комбинированно с другими полями."
            )

        elif not soft_deletion and self.is_deleted:
            raise ValidationError("Невозможно обновить удаленную сущность.")

        return valid


# ....................... #


class ReadEntity(ReadDocument):
    """Read entity model."""

    is_deleted: bool
    """Flag indicating if the entity is soft deleted."""


# ....................... #


class NameFields(CoreModel):
    """Base model with name fields."""

    name: String
    """Name of the entity."""

    display_name: Optional[String] = None
    """Display name of the entity."""

    short_name: Optional[String] = None
    """Short name of the entity."""

    description: Optional[LongString] = None
    """Description of the entity."""


# ....................... #


class NameFieldsAllOptional(CoreModel):
    """Base model with all name fields optional."""

    name: Optional[String] = None
    """Name of the entity."""

    display_name: Optional[String] = None
    """Display name of the entity."""

    short_name: Optional[String] = None
    """Short name of the entity."""

    description: Optional[LongString] = None
    """Description of the entity."""


# ....................... #


class NumberedEntity(Entity, NameFields):
    """Base numbered entity model."""

    number_id: PositiveInt
    """Unique number identifier of the entity."""


# ....................... #


class CreateNumberedEntityCmd(CreateDocumentCmd, NameFields):
    """Create numbered entity DTO."""

    # compatibility with external counter (increment)

    number_id: PositiveInt
    """Unique number identifier of the entity."""


# ....................... #


class UpdateNumberedEntityCmd(BaseDTO, NameFieldsAllOptional):
    """Update numbered entity DTO."""

    # compatibility with external counter (reset)

    number_id: Optional[PositiveInt] = None
    """Unique number identifier of the entity."""


# ....................... #


class ReadNumberedEntity(ReadEntity, NameFields):
    """Read numbered entity model."""

    number_id: PositiveInt
    """Unique number identifier of the numbered entity."""
