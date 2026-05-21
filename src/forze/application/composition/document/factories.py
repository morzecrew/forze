"""Factories for document plans, mappers, and registries."""

from typing import Any, TypeVar

from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.execution.registry import OperationRegistry
from forze.application.handlers.document import (
    AggregatedListDocuments,
    CreateDocument,
    DeleteDocument,
    GetDocument,
    KillDocument,
    RawCursorListDocuments,
    RawListDocuments,
    RestoreDocument,
    TypedCursorListDocuments,
    TypedListDocuments,
    UpdateDocument,
)
from forze.application.mapping import PydanticMapper
from forze.base.errors import CoreError
from forze.base.primitives import StrKeyNamespace
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from .operations import DocumentKernelOp
from .value_objects import DocumentDTOs, DocumentMappers

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
C = TypeVar("C", bound=BaseDTO, default=BaseDTO)
U = TypeVar("U", bound=BaseDTO, default=BaseDTO)

D = TypeVar("D", bound=Document, default=Any)
C_cmd = TypeVar("C_cmd", bound=CreateDocumentCmd, default=Any)
U_cmd = TypeVar("U_cmd", bound=BaseDTO, default=Any)

# ....................... #


def _build_default_create_mapper(
    spec: DocumentSpec[R, D, C_cmd, U_cmd],
    dtos: DocumentDTOs[R, C, U],
) -> PydanticMapper[C, C_cmd]:
    """Build default create mapper (pydantic)."""

    cdto = dtos.create
    c_cmd = spec.write["create_cmd"] if spec.write else None

    if cdto is None or c_cmd is None:
        raise CoreError("Create DTO or create command is not provided")

    return PydanticMapper(in_=cdto, out=c_cmd)


# ....................... #


def _build_default_update_mapper(
    spec: DocumentSpec[R, D, C_cmd, U_cmd],
    dtos: DocumentDTOs[R, C, U],
) -> PydanticMapper[U, U_cmd]:
    """Build default update mapper (pydantic)."""

    udto = dtos.update
    u_cmd = (
        spec.write["update_cmd"] if spec.write and "update_cmd" in spec.write else None
    )

    if udto is None or u_cmd is None:
        raise CoreError("Update DTO or update command is not provided")

    return PydanticMapper(in_=udto, out=u_cmd)


# ....................... #


def build_document_registry(
    spec: DocumentSpec[R, D, C_cmd, U_cmd],
    dtos: DocumentDTOs[R, C, U],
    mappers: DocumentMappers[C, C_cmd, U, U_cmd] = DocumentMappers(),
    *,
    ns: StrKeyNamespace | None = None,
) -> OperationRegistry:
    """Build document operation registry.

    :param spec: Document specification.
    :param dtos: Document DTO specification.
    :param mappers: Document mappers.
    :param ns: Optional namespace.
    :returns: Operation registry with all supported operations.
    """

    ns = ns or StrKeyNamespace(prefix=spec.name)

    reg = OperationRegistry(
        handlers={
            ns.key(DocumentKernelOp.GET): lambda ctx: GetDocument(
                doc=ctx.doc.query(spec),
            ),
            ns.key(DocumentKernelOp.LIST): lambda ctx: TypedListDocuments(
                doc=ctx.doc.query(spec),
                mapper=mappers.list,
            ),
            ns.key(DocumentKernelOp.RAW_LIST): lambda ctx: RawListDocuments(
                doc=ctx.doc.query(spec),
                mapper=mappers.raw_list,
            ),
            ns.key(DocumentKernelOp.LIST_CURSOR): lambda ctx: TypedCursorListDocuments(
                doc=ctx.doc.query(spec),
                mapper=mappers.list_cursor,
            ),
            ns.key(
                DocumentKernelOp.RAW_LIST_CURSOR
            ): lambda ctx: RawCursorListDocuments(
                doc=ctx.doc.query(spec),
                mapper=mappers.raw_list_cursor,
            ),
            ns.key(DocumentKernelOp.AGG_LIST): lambda ctx: AggregatedListDocuments(
                doc=ctx.doc.query(spec),
                mapper=mappers.aggregated_list,
            ),
        },
    )

    if spec.write is not None:
        reg = reg.set_handler(
            ns.key(DocumentKernelOp.KILL),
            lambda ctx: KillDocument(doc=ctx.doc.command(spec)),
        )

        if dtos.create is not None:
            create_mapper = mappers.create or _build_default_create_mapper(spec, dtos)

            reg = reg.set_handler(
                ns.key(DocumentKernelOp.CREATE),
                lambda ctx: CreateDocument[C, C_cmd, R](
                    doc=ctx.doc.command(spec),
                    mapper=create_mapper,
                ),
            )

        if spec.supports_update() and dtos.update is not None:
            update_mapper = mappers.update or _build_default_update_mapper(
                spec,
                dtos,
            )

            reg = reg.set_handler(
                ns.key(DocumentKernelOp.UPDATE),
                lambda ctx: UpdateDocument[U, U_cmd, R](
                    doc=ctx.doc.command(spec),
                    mapper=update_mapper,
                ),
            )

        #! Should not be here ...

        if spec.supports_soft_delete():
            reg = reg.set_handlers(
                {
                    ns.key(DocumentKernelOp.DELETE): lambda ctx: DeleteDocument(
                        doc=ctx.doc.command(spec),
                    ),
                    ns.key(DocumentKernelOp.RESTORE): lambda ctx: RestoreDocument(
                        doc=ctx.doc.command(spec),
                    ),
                },
            )

    return reg


# ....................... #


# def apply_default_tx_document_registry(
#     registry: UsecaseRegistry,
#     spec: DocumentSpec[Any, Any, Any, Any],
#     route: str | StrEnum,
#     *,
#     namespace: OperationNamespace | None = None,
# ) -> UsecaseRegistry:
#     """Apply the default transaction layout for document write operations.

#     :param registry: Registry to mutate.
#     :param spec: Document specification (used to derive the namespace when ``namespace`` is omitted).
#     :param route: Transaction route label.
#     :param namespace: Optional override; defaults to :func:`operation_namespace_for` ``(spec)``.
#     """

#     ops = namespace or operation_namespace_for(spec)

#     registry.tx(
#         [
#             ops.key(DocumentKernelOp.CREATE),
#             ops.key(DocumentKernelOp.UPDATE),
#             ops.key(DocumentKernelOp.DELETE),
#             ops.key(DocumentKernelOp.RESTORE),
#             ops.key(DocumentKernelOp.KILL),
#         ],
#         route=route,
#     )
#     return registry
