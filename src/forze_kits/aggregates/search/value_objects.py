import attrs
from pydantic import BaseModel

from forze.application.contracts.mapping import MapperFactory

from .dto import (
    CursorSearchRequestDTO,
    ProjectedCursorSearchRequestDTO,
    ProjectedSearchRequestDTO,
    SearchRequestDTO,
)

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchDTOs[M: BaseModel]:
    """DTO type mapping for a search aggregate."""

    read: type[M]
    """Read DTO type."""


# ....................... #

Sr = SearchRequestDTO
Psr = ProjectedSearchRequestDTO
Csr = CursorSearchRequestDTO
Pcsr = ProjectedCursorSearchRequestDTO


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchMappers:
    """Mappers for a search aggregate."""

    search: MapperFactory[Sr, Sr] | None = attrs.field(default=None)
    """Request DTO mapper."""

    projected_search: MapperFactory[Psr, Psr] | None = attrs.field(default=None)
    """Projected request DTO mapper."""

    cursor_search: MapperFactory[Csr, Csr] | None = attrs.field(default=None)
    """Cursor request DTO mapper."""

    projected_search_cursor: MapperFactory[Pcsr, Pcsr] | None = attrs.field(
        default=None
    )
    """Projected cursor request DTO mapper."""
