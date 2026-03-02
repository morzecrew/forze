"""Numeric identifier mixins for documents with human-readable IDs.

Provides :class:`NumberMixin` for models requiring a positive integer
``number_id``, and corresponding command DTOs for create and update operations.
"""

from typing import Optional

from pydantic import PositiveInt

from ..models import BaseDTO, CoreModel

# ----------------------- #


class NumberMixin(CoreModel):
    """Mixin adding a required positive integer ``number_id`` for human-readable identification.

    Use :class:`NumberCreateCmdMixin` or :class:`NumberUpdateCmdMixin` for
    command DTOs.
    """

    number_id: PositiveInt
    """Unique number identifier of the document."""


# ....................... #


class NumberCreateCmdMixin(BaseDTO):
    """Create command mixin with required ``number_id``."""

    number_id: PositiveInt
    """Unique number identifier of the document."""


# ....................... #


class NumberUpdateCmdMixin(BaseDTO):
    """Update command mixin with optional ``number_id``.

    When provided, updates the document's numeric identifier.
    """

    number_id: Optional[PositiveInt] = None
    """Unique number identifier of the document."""
