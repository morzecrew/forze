from typing import Any, Optional

from forze.application.contracts.search import SearchSpec
from forze.application.dto import RawSearchRequestDTO, SearchRequestDTO
from forze.application.execution import UsecasePlan, UsecaseRegistry
from forze.application.mapping import DTOMapper, MappingStep
from forze.application.usecases.search import RawSearch, TypedSearch

from .facades import SearchDTOSpec
from .operations import SearchOperation

# ----------------------- #
#! TODO: extend properly


def build_search_plan() -> UsecasePlan:
    plan = UsecasePlan()

    return plan


# ....................... #


def build_search_typed_mapper(
    spec: SearchSpec[Any],
    dto_spec: SearchDTOSpec[Any, Any, Any],
    steps: tuple[MappingStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for typed search requests."""

    mapper = DTOMapper(
        in_=dto_spec.get("typed", SearchRequestDTO),
        out=SearchRequestDTO,
    )
    return mapper.with_steps(*steps)


# ....................... #


def build_search_raw_mapper(
    spec: SearchSpec[Any],
    dto_spec: SearchDTOSpec[Any, Any, Any],
    steps: tuple[MappingStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for raw search requests."""

    mapper = DTOMapper(
        in_=dto_spec.get("raw", RawSearchRequestDTO),
        out=RawSearchRequestDTO,
    )
    return mapper.with_steps(*steps)


# ....................... #


def build_search_registry(
    spec: SearchSpec[Any],
    dto_spec: SearchDTOSpec[Any, Any, Any],
    *,
    replace_typed_mapper: Optional[DTOMapper[Any, Any]] = None,
    replace_raw_mapper: Optional[DTOMapper[Any, Any]] = None,
) -> UsecaseRegistry:
    typed_mapper = replace_typed_mapper or build_search_typed_mapper(spec, dto_spec)
    raw_mapper = replace_raw_mapper or build_search_raw_mapper(spec, dto_spec)

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
