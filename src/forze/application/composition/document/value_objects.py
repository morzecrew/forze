from typing import Any, Generic, TypeVar

import attrs
from pydantic import BaseModel

from forze.application.contracts.mapping import Mapper
from forze.application.dto import (
    AggregatedListRequestDTO,
    CursorListRequestDTO,
    ListRequestDTO,
    RawCursorListRequestDTO,
    RawListRequestDTO,
)
from forze.domain.models import BaseDTO, CreateDocumentCmd

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
C = TypeVar("C", bound=BaseDTO, default=BaseDTO)
U = TypeVar("U", bound=BaseDTO, default=BaseDTO)

C_cmd = TypeVar("C_cmd", bound=CreateDocumentCmd, default=Any)
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

LR = ListRequestDTO
RLR = RawListRequestDTO
CLR = CursorListRequestDTO
RCLR = RawCursorListRequestDTO
ALR = AggregatedListRequestDTO


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentMappers(Generic[C, C_cmd, U, U_cmd]):
    """Mappers for document operations."""

    list: Mapper[LR, LR] | None = attrs.field(default=None)
    """Mapper for list operation."""

    raw_list: Mapper[RLR, RLR] | None = attrs.field(default=None)
    """Mapper for raw list operation."""

    list_cursor: Mapper[CLR, CLR] | None = attrs.field(default=None)
    """Mapper for list cursor operation."""

    raw_list_cursor: Mapper[RCLR, RCLR] | None = attrs.field(default=None)
    """Mapper for raw list cursor operation."""

    aggregated_list: Mapper[ALR, ALR] | None = attrs.field(default=None)
    """Mapper for aggregated list operation."""

    create: Mapper[C, C_cmd] | None = attrs.field(default=None)
    """Mapper for create operation."""

    update: Mapper[U, U_cmd] | None = attrs.field(default=None)
    """Mapper for update operation."""
