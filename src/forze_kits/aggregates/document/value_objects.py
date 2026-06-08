from typing import Any, Generic, TypeVar

import attrs
from pydantic import BaseModel

from forze.application.contracts.mapping import MapperFactory
from forze.domain.models import BaseDTO

from .dto import (
    AggregatedListRequestDTO,
    CursorListRequestDTO,
    ListRequestDTO,
    ProjectedCursorListRequestDTO,
    ProjectedListRequestDTO,
)

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
C = TypeVar("C", bound=BaseDTO, default=BaseDTO)
U = TypeVar("U", bound=BaseDTO, default=BaseDTO)

C_cmd = TypeVar("C_cmd", bound=BaseDTO, default=Any)
U_cmd = TypeVar("U_cmd", bound=BaseDTO, default=Any)

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentDTOs(Generic[R, C, U]):
    """DTO type mapping for a document aggregate."""

    read: type[R]
    """Get command type."""

    create: type[C] | None = attrs.field(default=None)
    """Create command type; optional when create is not supported."""

    update: type[U] | None = attrs.field(default=None)
    """Update command type; optional when update is not supported."""


# ....................... #

Lr = ListRequestDTO
Plr = ProjectedListRequestDTO
Clr = CursorListRequestDTO
Pclr = ProjectedCursorListRequestDTO
Alr = AggregatedListRequestDTO


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentMappers(Generic[C, C_cmd, U, U_cmd]):
    """Mappers for document operations."""

    list: MapperFactory[Lr, Lr] | None = attrs.field(default=None)
    """Mapper for list operation."""

    projected_list: MapperFactory[Plr, Plr] | None = attrs.field(default=None)
    """Mapper for raw list operation."""

    cursor_list: MapperFactory[Clr, Clr] | None = attrs.field(default=None)
    """Mapper for list cursor operation."""

    projected_cursor_list: MapperFactory[Pclr, Pclr] | None = attrs.field(default=None)
    """Mapper for raw list cursor operation."""

    aggregated_list: MapperFactory[Alr, Alr] | None = attrs.field(default=None)
    """Mapper for aggregated list operation."""

    create: MapperFactory[C, C_cmd] | None = attrs.field(default=None)
    """Mapper for create operation."""

    update: MapperFactory[U, U_cmd] | None = attrs.field(default=None)
    """Mapper for update operation."""
