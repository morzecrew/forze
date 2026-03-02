"""Name-related mixins for documents with display and short names.

Provides :class:`NameMixin` for models requiring a required ``name`` plus
optional ``display_name``, ``short_name``, and ``description``, and
corresponding command DTOs for create and update operations.
"""

from typing import Optional

from forze.base.primitives import LongString, String

from ..models import BaseDTO, CoreModel

# ----------------------- #


class _NameMixinOptionalFields(CoreModel):
    """Optional name fields shared by :class:`NameMixin` and :class:`NameUpdateCmdMixin`."""

    display_name: Optional[String] = None
    """Display name of the document."""

    short_name: Optional[String] = None
    """Short name of the document."""

    description: Optional[LongString] = None
    """Description of the document."""


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

    name: Optional[String] = None
    """Name of the document."""
