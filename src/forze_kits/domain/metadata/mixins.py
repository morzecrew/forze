from typing import Any

from pydantic import ValidationInfo, field_validator

from forze.base.primitives import normalize_string
from forze.domain.models import BaseDTO, CoreModel
from forze_kits.domain.base.types import LongString, String, decode_text_input

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
    def _validate_metadata_fields(cls, v: Any, info: ValidationInfo) -> Any:
        """Read a blank field as unset, whether it arrives as text or as UTF-8 bytes.

        Blank is judged after normalization, so a value holding only invisible or control
        characters is unset too. A value that is not text is left to the field's own validation.
        """

        v = decode_text_input(v, info)

        if not isinstance(v, str):
            return v

        v = normalize_string(v).strip()

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
