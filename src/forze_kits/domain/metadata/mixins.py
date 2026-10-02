from typing import Any

from pydantic import field_validator

from forze.domain.models import BaseDTO, CoreModel
from forze_kits.domain.base.types import LongString, String

# ----------------------- #


class _MetadataMixinOptionalFields(CoreModel):
    """Optional metadata fields shared by :class:`MetadataMixin` and :class:`MetadataUpdateCmdMixin`."""

    display_name: String | None = None
    """Display name of the document."""

    description: LongString | None = None
    """Description of the document."""

    # ....................... #

    @field_validator("display_name", "description", mode="before")
    @classmethod
    def _validate_metadata_fields(cls, v: Any) -> Any:
        """Read a blank field as unset.

        A value that is not a string is left to the field's own validation.
        """

        if not isinstance(v, str):
            return v

        v = v.strip()

        if not v:
            return None

        return v


# ....................... #


class MetadataMixin(_MetadataMixinOptionalFields):
    """Mixin adding a required primary name, optional display name and description fields.

    Inherit from this when a document must have a primary name. Use
    :class:`MetadataCreateCmdMixin` or :class:`MetadataUpdateCmdMixin` for command DTOs.
    """

    name: String
    """Name of the document."""


# ....................... #


class MetadataCreateCmdMixin(MetadataMixin, BaseDTO):
    """Create command mixin with required name, optional display name and description fields."""


# ....................... #


class MetadataUpdateCmdMixin(BaseDTO, _MetadataMixinOptionalFields):
    """Update command mixin with optional metadata-related fields.

    All fields are optional; only provided values are updated.
    """

    name: String | None = None
    """Name of the document."""
