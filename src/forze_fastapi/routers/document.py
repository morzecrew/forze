from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from enum import Enum
from typing import Optional, TypeVar

from fastapi import Depends

from forze.application.composition import DocumentUsecasesFacadeProvider
from forze.application.dto.paginated import Paginated, RawPaginated
from forze.application.dto.search import RawSearchRequestDTO, SearchRequestDTO
from forze.application.facades import DocumentUsecasesFacade
from forze.application.kernel.dependencies import (
    ExecutionContext,
    IdempotencyDependencyPort,
)
from forze.domain.models import BaseDTO, ReadDocument

from ..routing.params import Pagination, RevQuery, UUIDQuery, pagination
from ..routing.router import ExecutionContextDependencyPort, ForzeAPIRouter

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


def document_facade_dependency(provider: DocumentUsecasesFacadeProvider[R, C, U]):
    """Build a FastAPI dependency that resolves :class:`DocumentUsecasesFacade`."""

    def facade(ctx: ExecutionContext) -> DocumentUsecasesFacade[R, C, U]:
        return provider(ctx)

    return facade


# ....................... #


def build_document_router(
    prefix: str,
    tags: Optional[list[str | Enum]] = None,
    *,
    provider: DocumentUsecasesFacadeProvider[R, C, U],
    context: ExecutionContextDependencyPort,
    idempotency: Optional[IdempotencyDependencyPort] = None,
):
    """Construct a router exposing CRUD and search endpoints for a document spec.

    The resulting router wires HTTP routes to the corresponding document
    usecases via :class:`DocumentUsecasesFacade`, including optional support for
    idempotent create operations and soft-delete/restore when the spec
    supports them.
    """

    router = ForzeAPIRouter(
        prefix=prefix,
        tags=tags,
        context_dependency=context,
        idempotency_dependency=idempotency,
    )

    read_dto = provider.dtos["read"]
    create_dto = provider.dtos.get("create")
    update_dto = provider.dtos.get("update")

    ucs_dep = document_facade_dependency(provider)

    # ....................... #

    @router.get(
        "/medatada",
        response_model=read_dto,
    )
    async def metadata(  # pyright: ignore[reportUnusedFunction]
        id: UUIDQuery,
        ucs: DocumentUsecasesFacade[R, C, U] = Depends(ucs_dep),
    ):
        """Return metadata for a single document by identifier."""

        return await ucs.get()(id)

    # ....................... #

    @router.post(
        "/search",
        response_model=Paginated[read_dto],
    )
    async def search(  # pyright: ignore[reportUnusedFunction]
        body: SearchRequestDTO,
        pagi: Pagination = Depends(pagination),
        ucs: DocumentUsecasesFacade[R, C, U] = Depends(ucs_dep),
    ):
        """Search documents using a typed search request body."""

        return await ucs.search()(
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
    )
    async def raw_search(  # pyright: ignore[reportUnusedFunction]
        body: RawSearchRequestDTO,
        pagi: Pagination = Depends(pagination),
        ucs: DocumentUsecasesFacade[R, C, U] = Depends(ucs_dep),
    ):
        """Search documents using a raw (untyped) search body."""

        return await ucs.raw_search()(
            {
                "body": body,
                "page": pagi.page,
                "size": pagi.size,
            }
        )

    # ....................... #

    if create_dto:

        @router.post(
            "/create",
            response_model=read_dto,
            idempotent=True,
            idempotency_config={
                "dto_param": "dto",
                #! TODO: add ttl configuration
            },
        )
        async def create(  # pyright: ignore[reportUnusedFunction]
            dto: C,
            ucs: DocumentUsecasesFacade[R, C, U] = Depends(ucs_dep),
        ):
            """Create a new document from the provided DTO."""

            return await ucs.create()(dto)

    # ....................... #

    if update_dto and provider.spec.supports_update():

        @router.patch(
            "/update",
            response_model=read_dto,
        )
        async def update(  # pyright: ignore[reportUnusedFunction]
            id: UUIDQuery,
            rev: RevQuery,
            dto: U,
            ucs: DocumentUsecasesFacade[R, C, U] = Depends(ucs_dep),
        ):
            """Apply a partial update to an existing document."""

            return await ucs.update()(
                {
                    "pk": id,
                    "dto": dto,
                    "rev": rev,
                }
            )

    # ....................... #

    if provider.spec.supports_soft_delete():

        @router.patch(
            "/delete",
            response_model=read_dto,
        )
        async def delete(  # pyright: ignore[reportUnusedFunction]
            id: UUIDQuery,
            rev: RevQuery,
            ucs: DocumentUsecasesFacade[R, C, U] = Depends(ucs_dep),
        ):
            """Soft-delete a document and return the new representation."""

            return await ucs.delete()(
                {
                    "pk": id,
                    "rev": rev,
                }
            )

        @router.patch(
            "/restore",
            response_model=read_dto,
        )
        async def restore(  # pyright: ignore[reportUnusedFunction]
            id: UUIDQuery,
            rev: RevQuery,
            ucs: DocumentUsecasesFacade[R, C, U] = Depends(ucs_dep),
        ):
            """Restore a previously soft-deleted document."""

            return await ucs.restore()(
                {
                    "pk": id,
                    "rev": rev,
                }
            )

    # ....................... #

    @router.delete(
        "/kill",
        response_model=None,
        status_code=204,
    )
    async def kill(  # pyright: ignore[reportUnusedFunction]
        id: UUIDQuery,
        ucs: DocumentUsecasesFacade[R, C, U] = Depends(ucs_dep),
    ):
        """Hard-delete a document without soft-delete semantics."""

        return await ucs.kill()(id)

    # ....................... #

    return router
