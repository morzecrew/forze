from typing import Generic, TypeVar

import attrs
from pydantic import BaseModel

from forze.application.dto import RawSearchRequestDTO, SearchRequestDTO
from forze.application.execution import UsecasesFacade, facade_op
from forze.application.usecases.search import RawSearch, TypedSearch

from .operations import SearchOperation

# ----------------------- #

M = TypeVar("M", bound=BaseModel)
tS = TypeVar("tS", bound=SearchRequestDTO, default=SearchRequestDTO)
rS = TypeVar("rS", bound=RawSearchRequestDTO, default=RawSearchRequestDTO)

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchDTOs(Generic[M, tS, rS]):
    """DTO type mapping for a search aggregate."""

    read: type[M]
    """Read DTO type."""

    typed: type[tS] | None = None
    """Typed search request DTO type. Provided only if typed search has custom DTO."""

    raw: type[rS] | None = None
    """Raw search request DTO type. Provided only if raw search has custom DTO."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchUsecasesFacade(UsecasesFacade, Generic[M, tS, rS]):
    """Typed facade for search usecases."""

    raw_search = facade_op(SearchOperation.RAW_SEARCH, uc=RawSearch[rS])
    """Raw search usecase."""

    search = facade_op(SearchOperation.TYPED_SEARCH, uc=TypedSearch[tS, M])
    """Typed search usecase."""
