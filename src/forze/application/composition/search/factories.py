from typing import Any

from forze.application.contracts.search import SearchSpec
from forze.application.execution import UsecasePlan, UsecaseRegistry
from forze.application.usecases.search import RawSearch, TypedSearch

from .operations import SearchOperation

# ----------------------- #
#! TODO: extend properly


def build_search_plan() -> UsecasePlan:
    plan = UsecasePlan()

    return plan


# ....................... #


def build_search_registry(spec: SearchSpec[Any]) -> UsecaseRegistry:
    reg = UsecaseRegistry(
        {
            SearchOperation.TYPED_SEARCH: lambda ctx: TypedSearch(
                ctx=ctx,
                search=ctx.search(spec),
            ),
            SearchOperation.RAW_SEARCH: lambda ctx: RawSearch(
                ctx=ctx,
                search=ctx.search(spec),
            ),
        }
    )

    return reg
