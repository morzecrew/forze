from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from enum import Enum
from typing import Any, Callable, Optional, TypedDict, TypeVar

import orjson
from fastapi import Body, Depends

from forze.application.composition.document import DocumentDTOs, DocumentUsecasesFacade
from forze.application.contracts.document import DocumentSpec
from forze.application.dto import (
    ListRequestDTO,
    Paginated,
    RawListRequestDTO,
    RawPaginated,
)
from forze.application.execution import ExecutionContext, UsecaseRegistry
from forze.domain.models import BaseDTO, ReadDocument

from ..routing.params import RevQuery, UUIDQuery
from ..routing.router import ForzeAPIRouter
from ._utils import facade_dependency, override_annotations

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)
tL = TypeVar("tL", bound=ListRequestDTO)
rL = TypeVar("rL", bound=RawListRequestDTO)

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


class OverrideDocumentEndpointPaths(TypedDict, total=False):
    """Override the default operation IDs and endpoint paths for document routes."""

    get: str
    """Operation ID suffix and endpoint path for the get endpoint. Defaults to "get"""

    typed_list: str
    """Operation ID suffix and endpoint path for the list endpoint. Defaults to "list"""

    raw_list: str
    """Operation ID suffix and endpoint path for the raw list endpoint. Defaults to "raw-list"""

    create: str
    """Operation ID suffix and endpoint path for the create endpoint. Defaults to "create"""

    update: str
    """Operation ID suffix and endpoint path for the update endpoint. Defaults to "update"""

    delete: str
    """Operation ID suffix and endpoint path for the delete endpoint. Defaults to "delete"""

    restore: str
    """Operation ID suffix and endpoint path for the restore endpoint. Defaults to "restore"""

    kill: str
    """Operation ID suffix and endpoint path for the kill endpoint. Defaults to "kill"""


# ....................... #


def attach_document_routes(
    router: ForzeAPIRouter,
    *,
    registry: UsecaseRegistry,
    spec: DocumentSpec[R, Any, Any, Any],
    dtos: DocumentDTOs[R, C, U, tL, rL],
    ctx_dep: Callable[[], ExecutionContext],
    include_get_endpoint: bool = True,
    include_list_endpoints: bool = False,
    include_write_endpoints: bool = True,
    path_overrides: OverrideDocumentEndpointPaths = {},
) -> ForzeAPIRouter:
    """Attach document CRUD endpoints to an existing router."""

    read_dto = dtos.read
    create_dto = dtos.create
    update_dto = dtos.update
    list_dto = dtos.list or ListRequestDTO
    raw_list_dto = dtos.raw_list or RawListRequestDTO

    ucs_dep = facade_dependency(
        facade=DocumentUsecasesFacade,
        reg=registry,
        ctx_dep=ctx_dep,
    )

    # ....................... #

    get_path = path_overrides.get("get", "metadata")
    list_path = path_overrides.get("typed_list", "list")
    raw_list_path = path_overrides.get("raw_list", "raw-list")
    create_path = path_overrides.get("create", "create")
    update_path = path_overrides.get("update", "update")
    delete_path = path_overrides.get("delete", "delete")
    restore_path = path_overrides.get("restore", "restore")
    kill_path = path_overrides.get("kill", "kill")

    # ....................... #

    if include_get_endpoint:

        @router.get(
            f"/{get_path}",
            response_model=read_dto,
            operation_id=f"{spec.namespace}.{get_path}",
            etag=True,
            etag_config={"provider": DocumentETagProvider()},
        )
        async def metadata(  # pyright: ignore[reportUnusedFunction]
            id: UUIDQuery,
            ucs: DocumentUsecasesFacade[R, C, U, tL, rL] = Depends(ucs_dep),
        ) -> R:
            """Return metadata for a single document by identifier."""

            return await ucs.get(id)

    # ....................... #

    if include_list_endpoints:

        @router.post(
            f"/{list_path}",
            response_model=Paginated[read_dto],  # type: ignore[valid-type]
            operation_id=f"{spec.namespace}.{list_path}",
        )
        @override_annotations({"dto": list_dto})
        async def list(  # pyright: ignore[reportUnusedFunction]
            body: tL = Body(...),
            ucs: DocumentUsecasesFacade[R, C, U, tL, rL] = Depends(ucs_dep),
        ) -> Paginated[R]:
            """List documents by filters and sorts."""

            return await ucs.list(body)

        # ....................... #

        @router.post(
            f"/{raw_list_path}",
            response_model=RawPaginated,
            operation_id=f"{spec.namespace}.{raw_list_path}",
        )
        @override_annotations({"dto": raw_list_dto})
        async def raw_list(  # pyright: ignore[reportUnusedFunction]
            body: rL = Body(...),
            ucs: DocumentUsecasesFacade[R, C, U, tL, rL] = Depends(ucs_dep),
        ) -> RawPaginated:
            """List documents with raw results by filters and sorts."""

            return await ucs.raw_list(body)

    # ....................... #

    if spec.write is not None and include_write_endpoints:

        if create_dto:

            @router.post(
                f"/{create_path}",
                response_model=read_dto,
                idempotent=True,
                idempotency_config={"dto_param": "dto"},
                operation_id=f"{spec.namespace}.{create_path}",
            )
            @override_annotations({"dto": create_dto})
            async def create(  # pyright: ignore[reportUnusedFunction]
                dto: C = Body(...),
                ucs: DocumentUsecasesFacade[R, C, U, tL, rL] = Depends(ucs_dep),
            ) -> R:
                """Create a new document from the provided DTO."""

                return await ucs.create(dto)

        # ....................... #

        if update_dto and spec.supports_update():

            @router.patch(
                f"/{update_path}",
                response_model=read_dto,
                operation_id=f"{spec.namespace}.{update_path}",
            )
            @override_annotations({"dto": update_dto})
            async def update(  # pyright: ignore[reportUnusedFunction]
                id: UUIDQuery,
                rev: RevQuery,
                dto: U = Body(...),
                ucs: DocumentUsecasesFacade[R, C, U, tL, rL] = Depends(ucs_dep),
            ) -> R:
                """Apply a partial update to an existing document."""

                return await ucs.update(
                    {
                        "pk": id,
                        "dto": dto,
                        "rev": rev,
                    }
                )

        # ....................... #

        if spec.supports_soft_delete():

            @router.patch(
                f"/{delete_path}",
                response_model=read_dto,
                operation_id=f"{spec.namespace}.{delete_path}",
            )
            async def delete(  # pyright: ignore[reportUnusedFunction]
                id: UUIDQuery,
                rev: RevQuery,
                ucs: DocumentUsecasesFacade[R, C, U, tL, rL] = Depends(ucs_dep),
            ) -> R:
                """Soft-delete a document and return the new representation."""

                return await ucs.delete(
                    {
                        "pk": id,
                        "rev": rev,
                    }
                )

            @router.patch(
                f"/{restore_path}",
                response_model=read_dto,
                operation_id=f"{spec.namespace}.{restore_path}",
            )
            async def restore(  # pyright: ignore[reportUnusedFunction]
                id: UUIDQuery,
                rev: RevQuery,
                ucs: DocumentUsecasesFacade[R, C, U, tL, rL] = Depends(ucs_dep),
            ) -> R:
                """Restore a previously soft-deleted document."""

                return await ucs.restore(
                    {
                        "pk": id,
                        "rev": rev,
                    }
                )

        # ....................... #

        @router.delete(
            f"/{kill_path}",
            response_model=None,
            status_code=204,
            operation_id=f"{spec.namespace}.{kill_path}",
        )
        async def kill(  # pyright: ignore[reportUnusedFunction]
            id: UUIDQuery,
            ucs: DocumentUsecasesFacade[R, C, U, tL, rL] = Depends(ucs_dep),
        ) -> None:
            """Hard-delete a document without soft-delete semantics."""

            return await ucs.kill(id)

    return router


# ....................... #


def build_document_router(
    prefix: str,
    tags: Optional[list[str | Enum]] = None,
    *,
    registry: UsecaseRegistry,
    spec: DocumentSpec[R, Any, Any, Any],
    dtos: DocumentDTOs[R, C, U, tL, rL],
    ctx_dep: Callable[[], ExecutionContext],
    include_get_endpoint: bool = True,
    include_list_endpoints: bool = False,
    include_write_endpoints: bool = True,
    path_overrides: OverrideDocumentEndpointPaths = {},
) -> ForzeAPIRouter:
    """Construct a router exposing CRUD endpoints for a document spec.

    The resulting router wires HTTP routes to the corresponding document
    usecases via :class:`DocumentUsecasesFacade`, including optional support for
    idempotent create operations and soft-delete/restore when the spec
    supports them.
    """

    router = ForzeAPIRouter(
        prefix=prefix,
        tags=tags,
        context_dependency=ctx_dep,
    )

    attach_document_routes(
        router,
        registry=registry,
        dtos=dtos,
        spec=spec,
        ctx_dep=ctx_dep,
        include_get_endpoint=include_get_endpoint,
        include_list_endpoints=include_list_endpoints,
        include_write_endpoints=include_write_endpoints,
        path_overrides=path_overrides,
    )

    return router
