from forze.base.errors import CoreError
from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from enum import Enum
from typing import Optional, TypeVar, overload

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


@overload
def build_search_router(
    router: R,
    prefix: None = ...,
    tags: None = ...,
    *,
    provider: SearchUsecasesFacadeProvider[M],
    context: ExecutionContextDependencyPort,
) -> R: ...


@overload
def build_search_router(
    router: None = ...,
    prefix: str = ...,
    tags: Optional[list[str | Enum]] = ...,
    *,
    provider: SearchUsecasesFacadeProvider[M],
    context: ExecutionContextDependencyPort,
) -> ForzeAPIRouter: ...


def build_search_router(
    router: Optional[R] = None,
    prefix: Optional[str] = None,
    tags: Optional[list[str | Enum]] = None,
    *,
    provider: SearchUsecasesFacadeProvider[M],
    context: ExecutionContextDependencyPort,
):
    if router is None:
        if prefix is None:
            raise CoreError("Prefix is required when router is not provided")

        new_router = ForzeAPIRouter(
            prefix=prefix,
            tags=tags,
            context_dependency=context,
        )

    else:
        new_router = router

    read_dto = provider.read_dto

    ucs_dep = search_facade_dependency(provider, context)

    # ....................... #

    @new_router.post(
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

    @new_router.post(
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

    return new_router
