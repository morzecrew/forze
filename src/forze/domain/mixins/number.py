from typing import Optional

from pydantic import PositiveInt

from ..models import BaseDTO, CoreModel

# ----------------------- #


class NumberMixin(CoreModel):
    """Mixin for numbering."""

    number_id: PositiveInt
    """Unique number identifier of the document."""


# ....................... #


class NumberCreateCmdMixin(BaseDTO):
    """Mixin for number create command."""

    number_id: PositiveInt
    """Unique number identifier of the document."""


# ....................... #


class NumberUpdateCmdMixin(BaseDTO):
    """Mixin for number update command."""

    number_id: Optional[PositiveInt] = None
    """Unique number identifier of the document."""
