from typing import Any

from forze.application.contracts.search import SearchSpec
from forze.application.dto import RawSearchRequestDTO, SearchRequestDTO
from forze.application.execution import UsecaseRegistry
from forze.application.mapping import DTOMapper, MappingStep
from forze.application.usecases.search import RawSearch, TypedSearch

from .facades import SearchDTOs
from .operations import SearchOperation

# ----------------------- #


def build_search_typed_mapper(
    spec: SearchSpec[Any],
    dtos: SearchDTOs[Any, Any, Any],
    *,
    steps: tuple[MappingStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for typed search requests."""

    mapper = DTOMapper(
        in_=dtos.typed or SearchRequestDTO,
        out=SearchRequestDTO,
    )
    return mapper.with_steps(*steps)


# ....................... #


def build_search_raw_mapper(
    spec: SearchSpec[Any],
    dtos: SearchDTOs[Any, Any, Any],
    *,
    steps: tuple[MappingStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for raw search requests."""

    mapper = DTOMapper(
        in_=dtos.raw or RawSearchRequestDTO,
        out=RawSearchRequestDTO,
    )
    return mapper.with_steps(*steps)


# ....................... #


def build_search_registry(
    spec: SearchSpec[Any],
    dtos: SearchDTOs[Any, Any, Any],
    *,
    search_steps: tuple[MappingStep[Any], ...] = (),
    raw_search_steps: tuple[MappingStep[Any], ...] = (),
) -> UsecaseRegistry:
    typed_mapper = build_search_typed_mapper(spec, dtos, steps=search_steps)
    raw_mapper = build_search_raw_mapper(spec, dtos, steps=raw_search_steps)

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
