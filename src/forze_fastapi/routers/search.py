from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from enum import Enum
from typing import Callable, Optional, TypedDict, TypeVar

from fastapi import APIRouter, Body, Depends
from pydantic import BaseModel

from forze.application.composition.search import (
    SearchUsecasesFacade,
    SearchUsecasesModule,
)
from forze.application.dto import (
    Paginated,
    RawPaginated,
    RawSearchRequestDTO,
    SearchRequestDTO,
)
from forze.application.execution import ExecutionContext

from ..routing.router import ExecutionContextDependencyPort, ForzeAPIRouter
from ._utils import override_annotations

# ----------------------- #

M = TypeVar("M", bound=BaseModel)
R = TypeVar("R", bound=APIRouter | ForzeAPIRouter)
tS = TypeVar("tS", bound=SearchRequestDTO)
rS = TypeVar("rS", bound=RawSearchRequestDTO)

# ....................... #


def search_facade_dependency(
    module: SearchUsecasesModule[M, tS, rS],
    ctx: ExecutionContextDependencyPort,
) -> Callable[[ExecutionContext], SearchUsecasesFacade[M, tS, rS]]:
    """Create a FastAPI dependency that resolves a :class:`SearchUsecasesFacade`."""

    def facade(
        context: ExecutionContext = Depends(ctx),
    ) -> SearchUsecasesFacade[M, tS, rS]:
        return module.provider(context)

    return facade


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
    module: SearchUsecasesModule[M, tS, rS],
    context: ExecutionContextDependencyPort,
    path_overrides: OverrideSearchEndpointPaths = {},
) -> R:
    """Attach typed and raw search endpoints to an existing router."""

    read_dto = module.dtos["read"]
    typed_dto = module.dtos.get("typed", SearchRequestDTO)
    raw_dto = module.dtos.get("raw", RawSearchRequestDTO)

    ucs_dep = search_facade_dependency(module, context)

    # ....................... #

    search_path = path_overrides.get("typed_search", "search")
    raw_search_path = path_overrides.get("raw_search", "raw-search")

    # ....................... #

    @router.post(
        f"/{search_path}",
        response_model=Paginated[read_dto],  # type: ignore[valid-type]
        operation_id=f"{module.spec.namespace}.{search_path}",
    )
    @override_annotations({"dto": typed_dto})
    async def search(  # pyright: ignore[reportUnusedFunction]
        body: tS = Body(...),
        ucs: SearchUsecasesFacade[M, tS, rS] = Depends(ucs_dep),
    ) -> Paginated[M]:
        """Search documents using a typed search request body."""

        return await ucs.search()(body)

    # ....................... #

    @router.post(
        f"/{raw_search_path}",
        response_model=RawPaginated,
        operation_id=f"{module.spec.namespace}.{raw_search_path}",
    )
    @override_annotations({"dto": raw_dto})
    async def raw_search(  # pyright: ignore[reportUnusedFunction]
        body: rS = Body(...),
        ucs: SearchUsecasesFacade[M, tS, rS] = Depends(ucs_dep),
    ) -> RawPaginated:
        """Search documents using a raw (untyped) search body."""

        return await ucs.raw_search()(body)

    # ....................... #

    return router


def build_search_router(
    prefix: str,
    tags: Optional[list[str | Enum]] = None,
    *,
    module: SearchUsecasesModule[M, tS, rS],
    context: ExecutionContextDependencyPort,
    path_overrides: OverrideSearchEndpointPaths = {},
) -> ForzeAPIRouter:
    """Build a standalone :class:`ForzeAPIRouter` with search endpoints."""

    router = ForzeAPIRouter(
        prefix=prefix,
        tags=tags,
        context_dependency=context,
    )

    attach_search_routes(
        router,
        module=module,
        context=context,
        path_overrides=path_overrides,
    )

    return router
