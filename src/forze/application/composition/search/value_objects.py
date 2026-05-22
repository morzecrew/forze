import attrs
from pydantic import BaseModel

from forze.application.contracts.mapping import MapperFactory
from forze.application.dto import (
    CursorSearchRequestDTO,
    RawCursorSearchRequestDTO,
    RawSearchRequestDTO,
    SearchRequestDTO,
)

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchDTOs[M: BaseModel]:
    """DTO type mapping for a search aggregate."""

    read: type[M]
    """Read DTO type."""


# ....................... #

SR = SearchRequestDTO
RSR = RawSearchRequestDTO
CSR = CursorSearchRequestDTO
RCSR = RawCursorSearchRequestDTO


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchMappers:
    """Mappers for a search aggregate."""

    search: MapperFactory[SR, SR] | None = attrs.field(default=None)
    """Read mapper."""

    raw_search: MapperFactory[RSR, RSR] | None = attrs.field(default=None)
    """Raw read mapper."""

    search_cursor: MapperFactory[CSR, CSR] | None = attrs.field(default=None)
    """Cursor read mapper."""

    raw_search_cursor: MapperFactory[RCSR, RCSR] | None = attrs.field(default=None)
    """Raw cursor read mapper."""
