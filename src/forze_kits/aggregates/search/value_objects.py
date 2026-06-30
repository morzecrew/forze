import attrs
from pydantic import BaseModel

from forze.application.contracts.mapping import MapperFactory
from forze.application.contracts.search import SearchOptions

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
class SearchMappers[Opt: SearchOptions]:
    """Mappers for a search aggregate."""

    search: MapperFactory[Sr[Opt], Sr[Opt]] | None = attrs.field(default=None)
    """Request DTO mapper."""

    projected_search: MapperFactory[Psr[Opt], Psr[Opt]] | None = attrs.field(
        default=None
    )
    """Projected request DTO mapper."""

    cursor_search: MapperFactory[Csr[Opt], Csr[Opt]] | None = attrs.field(default=None)
    """Cursor request DTO mapper."""

    projected_search_cursor: MapperFactory[Pcsr[Opt], Pcsr[Opt]] | None = attrs.field(
        default=None
    )
    """Projected cursor request DTO mapper."""
