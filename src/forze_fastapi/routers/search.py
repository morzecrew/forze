from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from enum import Enum
from typing import Callable, Optional, TypedDict, TypeVar

from fastapi import APIRouter, Body, Depends
from pydantic import BaseModel

from forze.application.composition.search import (
    SearchUsecasesFacade,
    SearchUsecasesFacadeProvider,
)
from forze.application.dto import (
    Paginated,
    RawPaginated,
    RawSearchRequestDTO,
    SearchRequestDTO,
)
from forze.application.execution import ExecutionContext

from ..routing.params import Pagination, pagination
from ..routing.router import ExecutionContextDependencyPort, ForzeAPIRouter

# ----------------------- #

M = TypeVar("M", bound=BaseModel)
R = TypeVar("R", bound=APIRouter | ForzeAPIRouter)

# ....................... #


def search_facade_dependency(
    provider: SearchUsecasesFacadeProvider[M],
    ctx: ExecutionContextDependencyPort,
) -> Callable[[ExecutionContext], SearchUsecasesFacade[M]]:
    """Create a FastAPI dependency that resolves a :class:`SearchUsecasesFacade`."""

    def facade(
        context: ExecutionContext = Depends(ctx),
    ) -> SearchUsecasesFacade[M]:
        return provider(context)

    return facade


# ....................... #


class OverrideSearchEndpointNames(TypedDict, total=False):
    """Override the default operation IDs and endpoint paths for search routes."""

    typed_search: str
    """Operation ID suffix and endpoint path for the typed search endpoint. Defaults to "search"""

    raw_search: str
    """Operation ID suffix and endpoint path for the raw search endpoint. Defaults to "raw-search"""


# ....................... #


def attach_search_routes(
    router: R,
    *,
    provider: SearchUsecasesFacadeProvider[M],
    context: ExecutionContextDependencyPort,
    name_overrides: OverrideSearchEndpointNames = {},
) -> R:
    """Attach typed and raw search endpoints to an existing router."""

    read_dto = provider.read_dto

    ucs_dep = search_facade_dependency(provider, context)

    # ....................... #

    search_path = name_overrides.get("typed_search", "search")
    raw_search_path = name_overrides.get("raw_search", "raw-search")

    # ....................... #

    @router.post(
        f"/{search_path}",
        response_model=Paginated[read_dto],  # type: ignore[valid-type]
        operation_id=f"{provider.spec.namespace}.{search_path}",
    )
    async def search(  # pyright: ignore[reportUnusedFunction]
        body: SearchRequestDTO = Body(...),
        pagi: Pagination = Depends(pagination),
        ucs: SearchUsecasesFacade[M] = Depends(ucs_dep),
    ) -> Paginated[M]:
        """Search documents using a typed search request body."""

        return await ucs.typed()(
            {
                "body": body,
                "page": pagi.page,
                "size": pagi.size,
            }
        )

    # ....................... #

    @router.post(
        f"/{raw_search_path}",
        response_model=RawPaginated,
        operation_id=f"{provider.spec.namespace}.{raw_search_path}",
    )
    async def raw_search(  # pyright: ignore[reportUnusedFunction]
        body: RawSearchRequestDTO = Body(...),
        pagi: Pagination = Depends(pagination),
        ucs: SearchUsecasesFacade[M] = Depends(ucs_dep),
    ) -> RawPaginated:
        """Search documents using a raw (untyped) search body."""

        return await ucs.raw()(
            {
                "body": body,
                "page": pagi.page,
                "size": pagi.size,
            }
        )

    # ....................... #

    return router


def build_search_router(
    prefix: str,
    tags: Optional[list[str | Enum]] = None,
    *,
    provider: SearchUsecasesFacadeProvider[M],
    context: ExecutionContextDependencyPort,
    name_overrides: OverrideSearchEndpointNames = {},
) -> ForzeAPIRouter:
    """Build a standalone :class:`ForzeAPIRouter` with search endpoints."""

    router = ForzeAPIRouter(
        prefix=prefix,
        tags=tags,
        context_dependency=context,
    )

    attach_search_routes(
        router,
        provider=provider,
        context=context,
        name_overrides=name_overrides,
    )

    return router
