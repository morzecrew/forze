from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from enum import Enum
from typing import Callable, Optional, TypeVar

import orjson
from fastapi import Body, Depends

from forze.application.composition.document import (
    DocumentUsecasesFacade,
    DocumentUsecasesFacadeProvider,
)
from forze.application.execution import ExecutionContext
from forze.domain.models import BaseDTO, ReadDocument

from ..routing.params import RevQuery, UUIDQuery
from ..routing.router import ExecutionContextDependencyPort, ForzeAPIRouter
from ._utils import override_annotations

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


class DocumentETagProvider:
    """ETag provider that derives the tag from document ``id`` and ``rev``.

    Produces tags of the form ``{id}:{rev}`` for stable version identity
    without hashing the full response body.
    """

    def generate(self, response_body: bytes) -> str | None:
        """Extract ``id`` and ``rev`` from the JSON body to build the tag.

        :param response_body: Serialized JSON response payload.
        :returns: Tag string ``"{id}:{rev}"`` or ``None`` when fields are absent.
        """

        try:
            data = orjson.loads(response_body)
        except Exception:
            return None

        id_val = data.get("id")
        rev_val = data.get("rev")

        if id_val is None or rev_val is None:
            return None

        return f"{id_val}:{rev_val}"


# ....................... #


def document_facade_dependency(
    provider: DocumentUsecasesFacadeProvider[R, C, U],
    ctx: ExecutionContextDependencyPort,
) -> Callable[[ExecutionContext], DocumentUsecasesFacade[R, C, U]]:
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
) -> ForzeAPIRouter:
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
        etag=True,
        etag_config={"provider": DocumentETagProvider()},
    )
    async def metadata(  # pyright: ignore[reportUnusedFunction]
        id: UUIDQuery,
        ucs: DocumentUsecasesFacade[R, C, U] = Depends(ucs_dep),
    ) -> R:
        """Return metadata for a single document by identifier."""

        return await ucs.get()(id)

    # ....................... #

    if provider.spec.write is not None:

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
            ) -> R:
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
            ) -> R:
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
            ) -> R:
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
            ) -> R:
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
        ) -> None:
            """Hard-delete a document without soft-delete semantics."""

            return await ucs.kill()(id)

    # ....................... #

    return router
