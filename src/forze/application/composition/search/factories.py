from typing import Any, Optional

from forze.application.contracts.search import SearchSpec
from forze.application.dto import RawSearchRequestDTO, SearchRequestDTO
from forze.application.execution import UsecasePlan, UsecaseRegistry
from forze.application.mapping import DTOMapper
from forze.application.usecases.search import RawSearch, TypedSearch

from .operations import SearchOperation

# ----------------------- #
#! TODO: extend properly


def build_search_plan() -> UsecasePlan:
    plan = UsecasePlan()

    return plan


# ....................... #


def build_search_registry(
    spec: SearchSpec[Any],
    *,
    typed_mapper: Optional[DTOMapper[SearchRequestDTO]] = None,
    raw_mapper: Optional[DTOMapper[RawSearchRequestDTO]] = None,
) -> UsecaseRegistry:
    reg = UsecaseRegistry(
        {
            SearchOperation.TYPED_SEARCH: lambda ctx: TypedSearch(
                ctx=ctx,
                search=ctx.search(spec),
                mapper=typed_mapper,
            ),
            SearchOperation.RAW_SEARCH: lambda ctx: RawSearch(
                ctx=ctx,
                search=ctx.search(spec),
                mapper=raw_mapper,
            ),
        }
    )

    return reg
