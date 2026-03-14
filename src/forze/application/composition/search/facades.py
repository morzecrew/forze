from typing import Any, Generic, NotRequired, TypedDict, TypeVar

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.application.dto import (
    Paginated,
    RawPaginated,
    RawSearchRequestDTO,
    SearchRequestDTO,
)
from forze.application.execution import Usecase

from ..base import BaseUsecasesFacade, BaseUsecasesFacadeProvider
from .operations import SearchOperation

# ----------------------- #

M = TypeVar("M", bound=BaseModel)
tS = TypeVar("tS", bound=SearchRequestDTO, default=SearchRequestDTO)
rS = TypeVar("rS", bound=RawSearchRequestDTO, default=RawSearchRequestDTO)

# ....................... #


class SearchUsecasesFacade(BaseUsecasesFacade, Generic[M, tS, rS]):
    """Typed facade for search usecases."""

    def raw_search(self) -> Usecase[rS, RawPaginated]:
        """Return the raw search usecase."""

        return self.resolve(SearchOperation.RAW_SEARCH)

    # ....................... #

    def search(self) -> Usecase[tS, Paginated[M]]:
        """Return the typed search usecase."""

        return self.resolve(SearchOperation.TYPED_SEARCH)


# ....................... #


class SearchDTOSpec(TypedDict, Generic[M, tS, rS]):
    """DTO type mapping for a search aggregate."""

    read: type[M]
    """Read DTO type."""

    typed: NotRequired[type[tS]]
    """Typed search request DTO type. Provided only if typed search has custom DTO."""

    raw: NotRequired[type[rS]]
    """Raw search request DTO type. Provided only if raw search has custom DTO."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchUsecasesModule(Generic[M, tS, rS]):
    spec: SearchSpec[Any]
    dtos: SearchDTOSpec[M, tS, rS]
    provider: BaseUsecasesFacadeProvider[SearchUsecasesFacade[M, tS, rS]]
