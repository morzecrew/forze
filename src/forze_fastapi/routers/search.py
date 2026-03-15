from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from enum import Enum
from typing import Callable, Optional, TypedDict, TypeVar

from fastapi import APIRouter, Body, Depends
from pydantic import BaseModel

from forze.application.composition.search import SearchDTOs, SearchUsecasesFacade
from forze.application.contracts.search import SearchSpec
from forze.application.dto import (
    Paginated,
    RawPaginated,
    RawSearchRequestDTO,
    SearchRequestDTO,
)
from forze.application.execution import ExecutionContext, UsecaseRegistry

from ..routing.router import ForzeAPIRouter
from ._utils import facade_dependency, override_annotations

# ----------------------- #

M = TypeVar("M", bound=BaseModel)
R = TypeVar("R", bound=APIRouter | ForzeAPIRouter)
tS = TypeVar("tS", bound=SearchRequestDTO)
rS = TypeVar("rS", bound=RawSearchRequestDTO)

# ....................... #


class OverrideSearchEndpointPaths(TypedDict, total=False):
    """Override the default operation IDs and endpoint paths for search routes."""

    typed_search: str
    """Operation ID suffix and endpoint path for the typed search endpoint. Defaults to "search"""

    raw_search: str
    """Operation ID suffix and endpoint path for the raw search endpoint. Defaults to "raw-search"""


# ....................... #


def attach_search_routes(
    router: R,
    *,
    registry: UsecaseRegistry,
    spec: SearchSpec[M],
    dtos: SearchDTOs[M, tS, rS],
    ctx_dep: Callable[[], ExecutionContext],
    include_typed_search_endpoint: bool = True,
    include_raw_search_endpoint: bool = True,
    path_overrides: OverrideSearchEndpointPaths = {},
) -> R:
    """Attach typed and raw search endpoints to an existing router."""

    read_dto = dtos.read
    typed_dto = dtos.typed or SearchRequestDTO
    raw_dto = dtos.raw or RawSearchRequestDTO

    ucs_dep = facade_dependency(
        facade=SearchUsecasesFacade,
        reg=registry,
        ctx_dep=ctx_dep,
    )

    # ....................... #

    search_path = path_overrides.get("typed_search", "search")
    raw_search_path = path_overrides.get("raw_search", "raw-search")

    # ....................... #

    if include_typed_search_endpoint:

        @router.post(
            f"/{search_path}",
            response_model=Paginated[read_dto],  # type: ignore[valid-type]
            operation_id=f"{spec.namespace}.{search_path}",
        )
        @override_annotations({"dto": typed_dto})
        async def search(  # pyright: ignore[reportUnusedFunction]
            body: tS = Body(...),
            ucs: SearchUsecasesFacade[M, tS, rS] = Depends(ucs_dep),
        ) -> Paginated[M]:
            """Search documents using a typed search request body."""

            return await ucs.search(body)

    # ....................... #

    if include_raw_search_endpoint:

        @router.post(
            f"/{raw_search_path}",
            response_model=RawPaginated,
            operation_id=f"{spec.namespace}.{raw_search_path}",
        )
        @override_annotations({"dto": raw_dto})
        async def raw_search(  # pyright: ignore[reportUnusedFunction]
            body: rS = Body(...),
            ucs: SearchUsecasesFacade[M, tS, rS] = Depends(ucs_dep),
        ) -> RawPaginated:
            """Search documents using a raw (untyped) search body."""

            return await ucs.raw_search(body)

    # ....................... #

    return router


def build_search_router(
    prefix: str,
    tags: Optional[list[str | Enum]] = None,
    *,
    registry: UsecaseRegistry,
    spec: SearchSpec[M],
    dtos: SearchDTOs[M, tS, rS],
    ctx_dep: Callable[[], ExecutionContext],
    include_typed_search_endpoint: bool = True,
    include_raw_search_endpoint: bool = True,
    path_overrides: OverrideSearchEndpointPaths = {},
) -> ForzeAPIRouter:
    """Build a standalone :class:`ForzeAPIRouter` with search endpoints."""

    router = ForzeAPIRouter(
        prefix=prefix,
        tags=tags,
        context_dependency=ctx_dep,
    )

    attach_search_routes(
        router,
        registry=registry,
        spec=spec,
        dtos=dtos,
        ctx_dep=ctx_dep,
        include_typed_search_endpoint=include_typed_search_endpoint,
        include_raw_search_endpoint=include_raw_search_endpoint,
        path_overrides=path_overrides,
    )

    return router
