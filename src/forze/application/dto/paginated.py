from pydantic import BaseModel

from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO

# ----------------------- #


class Paginated[T: BaseModel](BaseDTO):
    """Paginated response model."""

    hits: list[T]
    """Records of the paginated response."""

    page: int
    """Page number of the paginated response."""

    size: int
    """Size of the page of the paginated response."""

    count: int
    """Total number of records available."""


# ....................... #


class RawPaginated(BaseDTO):
    """Paginated response model."""

    hits: list[JsonDict]
    """Records of the paginated response."""

    page: int
    """Page number of the paginated response."""

    size: int
    """Size of the page of the paginated response."""

    count: int
    """Total number of records available."""
