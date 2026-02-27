from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from enum import Enum
from typing import Optional, TypeVar

from fastapi import Body, Depends

from forze.application.composition import DocumentUsecasesFacadeProvider
from forze.application.contracts.idempotency import IdempotencyDepPort
from forze.application.dto.paginated import Paginated, RawPaginated
from forze.application.dto.search import RawSearchRequestDTO, SearchRequestDTO
from forze.application.execution import ExecutionContext
from forze.application.facades import DocumentUsecasesFacade
from forze.domain.models import BaseDTO, ReadDocument

from ..routing.params import Pagination, RevQuery, UUIDQuery, pagination
from ..routing.router import ExecutionContextDependencyPort, ForzeAPIRouter
from ._utils import override_annotations

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


def document_facade_dependency(
    provider: DocumentUsecasesFacadeProvider[R, C, U],
    ctx: ExecutionContextDependencyPort,
):
    """Build a FastAPI dependency that resolves :class:`DocumentUsecasesFacade`."""

    def facade(
        context: ExecutionContext = Depends(ctx),
    ) -> DocumentUsecasesFacade[R, C, U]:
        return provider(context)

    return facade


# ....................... #


def build_document_router(
    prefix: str,
    tags: Optional[list[str | Enum]] = None,
    *,
    provider: DocumentUsecasesFacadeProvider[R, C, U],
    context: ExecutionContextDependencyPort,
    idempotency: Optional[IdempotencyDepPort] = None,
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

    ucs_dep = document_facade_dependency(provider, context)

    # ....................... #

    @router.get(
        "/medatada",
        response_model=read_dto,
        operation_id=f"{provider.spec.namespace}.metadata",
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
        operation_id=f"{provider.spec.namespace}.search",
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
        operation_id=f"{provider.spec.namespace}.raw_search",
    )
    async def raw_search(  # pyright: ignore[reportUnusedFunction]
        body: RawSearchRequestDTO = Body(...),
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
            idempotency_config={"dto_param": "dto"},
            operation_id=f"{provider.spec.namespace}.create",
        )
        @override_annotations({"dto": create_dto})
        async def create(  # pyright: ignore[reportUnusedFunction]
            dto: C = Body(...),
            ucs: DocumentUsecasesFacade[R, C, U] = Depends(ucs_dep),
        ):
            """Create a new document from the provided DTO."""

            return await ucs.create()(dto)

    # ....................... #

    if update_dto and provider.spec.supports_update():

        @router.patch(
            "/update",
            response_model=read_dto,
            operation_id=f"{provider.spec.namespace}.update",
        )
        @override_annotations({"dto": update_dto})
        async def update(  # pyright: ignore[reportUnusedFunction]
            id: UUIDQuery,
            rev: RevQuery,
            dto: U = Body(...),
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
            operation_id=f"{provider.spec.namespace}.delete",
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
            operation_id=f"{provider.spec.namespace}.restore",
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
        operation_id=f"{provider.spec.namespace}.kill",
    )
    async def kill(  # pyright: ignore[reportUnusedFunction]
        id: UUIDQuery,
        ucs: DocumentUsecasesFacade[R, C, U] = Depends(ucs_dep),
    ):
        """Hard-delete a document without soft-delete semantics."""

        return await ucs.kill()(id)

    # ....................... #

    return router
