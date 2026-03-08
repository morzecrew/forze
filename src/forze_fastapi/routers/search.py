from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from enum import Enum
from typing import Optional, TypeVar

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
):
    def facade(
        context: ExecutionContext = Depends(ctx),
    ) -> SearchUsecasesFacade[M]:
        return provider(context)

    return facade


# ....................... #


def attach_search_router(
    router: R,
    *,
    provider: SearchUsecasesFacadeProvider[M],
    context: ExecutionContextDependencyPort,
) -> R:
    read_dto = provider.read_dto

    ucs_dep = search_facade_dependency(provider, context)

    # ....................... #

    @router.post(
        "/search",
        response_model=Paginated[read_dto],
        operation_id=f"{provider.spec.namespace}.search",
    )
    async def search(  # pyright: ignore[reportUnusedFunction]
        body: SearchRequestDTO = Body(...),
        pagi: Pagination = Depends(pagination),
        ucs: SearchUsecasesFacade[M] = Depends(ucs_dep),
    ):
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
        "/raw-search",
        response_model=RawPaginated,
        operation_id=f"{provider.spec.namespace}.raw_search",
    )
    async def raw_search(  # pyright: ignore[reportUnusedFunction]
        body: RawSearchRequestDTO = Body(...),
        pagi: Pagination = Depends(pagination),
        ucs: SearchUsecasesFacade[M] = Depends(ucs_dep),
    ):
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
) -> ForzeAPIRouter:
    router = ForzeAPIRouter(
        prefix=prefix,
        tags=tags,
        context_dependency=context,
    )

    return attach_search_router(router, provider=provider, context=context)
