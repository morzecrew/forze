from typing import Any, Generic, NotRequired, TypedDict, TypeVar, cast, final

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
tS = TypeVar("tS", bound=SearchRequestDTO)
rS = TypeVar("rS", bound=RawSearchRequestDTO)

# ....................... #


@final
class SearchUsecasesFacade(BaseUsecasesFacade, Generic[M, tS, rS]):
    """Typed facade for search usecases."""

    def raw(self) -> Usecase[rS, RawPaginated]:
        """Return the raw search usecase."""

        return self.resolve(SearchOperation.RAW_SEARCH)

    # ....................... #

    def typed(self) -> Usecase[tS, Paginated[M]]:
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


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchUsecasesFacadeProvider(
    BaseUsecasesFacadeProvider[SearchUsecasesFacade[M, tS, rS]], Generic[M, tS, rS]
):
    """Factory that produces a search usecases facade for a given context."""

    spec: SearchSpec[Any]
    """Search specification (used by registry factories)."""

    dtos: SearchDTOSpec[M, tS, rS]
    """DTO type mapping for facade typing."""

    # Non initable fields
    facade: type[SearchUsecasesFacade[M, tS, rS]] = attrs.field(
        default=cast(type[SearchUsecasesFacade[M, tS, rS]], SearchUsecasesFacade),
        init=False,
    )
    """Facade type to produce."""
