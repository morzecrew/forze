from pydantic import PositiveInt

from forze.domain.models import BaseDTO, CoreModel

# ----------------------- #


class NumberIdMixin(CoreModel):
    """Mixin adding a required positive integer number ID for human-readable identification.

    Use :class:`NumberCreateCmdMixin` or :class:`NumberUpdateCmdMixin` for
    command DTOs.
    """

    number_id: PositiveInt
    """Unique number identifier of the document."""


# ....................... #


class NumberIdCreateCmdMixin(BaseDTO):
    """Create command mixin with required number ID."""

    number_id: PositiveInt
    """Unique number identifier of the document."""


# ....................... #


class NumberIdUpdateCmdMixin(BaseDTO):
    """Update command mixin with optional number ID.

    When provided, updates the document's numeric identifier.
    """

    number_id: PositiveInt | None = None
    """Unique number identifier of the document."""
