from typing import Optional

from forze.base.primitives import LongString, String

from ..models import BaseDTO, CoreModel

# ----------------------- #


class _NameMixinOptionalFields(CoreModel):
    """Mixin for name optional fields."""

    display_name: Optional[String] = None
    """Display name of the document."""

    short_name: Optional[String] = None
    """Short name of the document."""

    description: Optional[LongString] = None
    """Description of the document."""


# ....................... #


class NameMixin(_NameMixinOptionalFields):
    """Mixin for name."""

    name: String
    """Name of the document."""


# ....................... #


class NameCreateCmdMixin(NameMixin, BaseDTO):
    """Mixin for name create command."""


# ....................... #


class NameUpdateCmdMixin(BaseDTO, _NameMixinOptionalFields):
    """Mixin for name update command."""

    name: Optional[String] = None
    """Name of the document."""
