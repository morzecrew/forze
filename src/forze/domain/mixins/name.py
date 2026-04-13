from pydantic import field_validator

from forze.base.primitives import LongString, String

from ..models import BaseDTO, CoreModel

# ----------------------- #


class _NameMixinOptionalFields(CoreModel):
    """Optional name fields shared by :class:`NameMixin` and :class:`NameUpdateCmdMixin`."""

    display_name: String | None = None
    """Display name of the document."""

    description: LongString | None = None
    """Description of the document."""

    # ....................... #

    @field_validator("display_name", "description", mode="before")
    @classmethod
    def _validate_name_fields(cls, v: str | None) -> str | None:
        if v is None:
            return v

        v = v.strip()

        if not v:
            return None

        return v


# ....................... #


class NameMixin(_NameMixinOptionalFields):
    """Mixin adding a required ``name`` and optional display/short names and description.

    Inherit from this when a document must have a primary name. Use
    :class:`NameCreateCmdMixin` or :class:`NameUpdateCmdMixin` for command DTOs.
    """

    name: String
    """Name of the document."""


# ....................... #


class NameCreateCmdMixin(NameMixin, BaseDTO):
    """Create command mixin with required ``name`` and optional name-related fields."""


# ....................... #


class NameUpdateCmdMixin(BaseDTO, _NameMixinOptionalFields):
    """Update command mixin with optional name-related fields.

    All fields are optional; only provided values are updated.
    """

    name: String | None = None
    """Name of the document."""
