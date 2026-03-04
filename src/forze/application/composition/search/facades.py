from typing import Generic, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.dto import Paginated, RawPaginated
from forze.application.execution import (
    Usecase,
)
from forze.application.usecases.search import RawSearchArgs, TypedSearchArgs

from ..base import BaseUsecasesFacade, BaseUsecasesFacadeProvider
from .operations import SearchOperation

# ----------------------- #

M = TypeVar("M", bound=BaseModel)

# ....................... #


class SearchUsecasesFacade(BaseUsecasesFacade, Generic[M]):
    """Typed facade for search usecases."""

    def raw(self) -> Usecase[RawSearchArgs, RawPaginated]:
        """Return the raw search usecase."""

        return self.resolve(SearchOperation.RAW_SEARCH)

    # ....................... #

    def typed(self) -> Usecase[TypedSearchArgs, Paginated[M]]:
        """Return the typed search usecase."""

        return self.resolve(SearchOperation.TYPED_SEARCH)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchUsecasesFacadeProvider(
    BaseUsecasesFacadeProvider[SearchUsecasesFacade[M]], Generic[M]
):
    """Factory that produces a search usecases facade for a given context."""

    facade: type[SearchUsecasesFacade[M]] = attrs.field(
        default=SearchUsecasesFacade,
        init=False,
    )
    """Facade type to produce."""
