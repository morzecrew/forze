from typing import Any

from forze.application.contracts.search import (
    FederatedSearchSpec,
    HubSearchSpec,
    SearchSpec,
)
from forze.application.dto import RawSearchRequestDTO, SearchRequestDTO
from forze.application.execution import UsecaseRegistry
from forze.application.usecases.search import RawSearch, TypedSearch

from ..mapping import DTOMapper, DTOMapperStep
from .operations import SearchOperation

# ----------------------- #


def build_search_typed_mapper(
    *,
    steps: tuple[DTOMapperStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for typed search requests."""

    mapper = DTOMapper(
        in_=SearchRequestDTO,
        out=SearchRequestDTO,
    )
    return mapper.with_steps(*steps)


# ....................... #


def build_search_raw_mapper(
    *,
    steps: tuple[DTOMapperStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for raw search requests."""

    mapper = DTOMapper(
        in_=RawSearchRequestDTO,
        out=RawSearchRequestDTO,
    )
    return mapper.with_steps(*steps)


# ....................... #


def build_search_registry(
    spec: SearchSpec[Any],
    *,
    search_steps: tuple[DTOMapperStep[Any], ...] = (),
    raw_search_steps: tuple[DTOMapperStep[Any], ...] = (),
) -> UsecaseRegistry:
    typed_mapper = build_search_typed_mapper(steps=search_steps)
    raw_mapper = build_search_raw_mapper(steps=raw_search_steps)

    reg = UsecaseRegistry(
        {
            SearchOperation.TYPED_SEARCH: lambda ctx: TypedSearch(
                ctx=ctx,
                search=ctx.search_query(spec),
                mapper=typed_mapper,
            ),
            SearchOperation.RAW_SEARCH: lambda ctx: RawSearch(
                ctx=ctx,
                search=ctx.search_query(spec),
                mapper=raw_mapper,
            ),
        }
    )

    return reg


# ....................... #


def build_hub_search_registry(
    spec: HubSearchSpec[Any],
    *,
    search_steps: tuple[DTOMapperStep[Any], ...] = (),
    raw_search_steps: tuple[DTOMapperStep[Any], ...] = (),
) -> UsecaseRegistry:
    typed_mapper = build_search_typed_mapper(steps=search_steps)
    raw_mapper = build_search_raw_mapper(steps=raw_search_steps)

    reg = UsecaseRegistry(
        {
            SearchOperation.TYPED_SEARCH: lambda ctx: TypedSearch(
                ctx=ctx,
                search=ctx.hub_search_query(spec),
                mapper=typed_mapper,
            ),
            SearchOperation.RAW_SEARCH: lambda ctx: RawSearch(
                ctx=ctx,
                search=ctx.hub_search_query(spec),
                mapper=raw_mapper,
            ),
        }
    )
    return reg


# ....................... #


def build_federated_search_registry(
    spec: FederatedSearchSpec[Any],
    *,
    search_steps: tuple[DTOMapperStep[Any], ...] = (),
) -> UsecaseRegistry:
    typed_mapper = build_search_typed_mapper(steps=search_steps)

    reg = UsecaseRegistry(
        {
            SearchOperation.TYPED_SEARCH: lambda ctx: TypedSearch(
                ctx=ctx,
                search=ctx.federated_search_query(spec),
                mapper=typed_mapper,
            ),
        }
    )
    return reg
