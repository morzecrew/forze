from pydantic import BaseModel

from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO

# ----------------------- #


class Paginated[T: BaseModel](BaseDTO):
    """Paginated response model."""

    hits: list[T]
    """Hits of the response."""

    page: int
    """Page of the response."""

    size: int
    """Size of the response."""

    count: int
    """Count of the response."""


# ....................... #


class RawPaginated(BaseDTO):
    """Paginated response model."""

    hits: list[JsonDict]
    """Hits of the response."""

    page: int
    """Page of the response."""

    size: int
    """Size of the response."""

    count: int
    """Count of the response."""
