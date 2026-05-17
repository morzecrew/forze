"""Factories for document plans, mappers, and registries."""

from enum import StrEnum
from typing import Any

from forze.application.contracts.document import DocumentSpec
from forze.application.dto import (
    AggregatedListRequestDTO,
    CursorListRequestDTO,
    ListRequestDTO,
    RawCursorListRequestDTO,
    RawListRequestDTO,
)
from forze.application.execution import (
    OperationNamespace,
    UsecaseRegistry,
    operation_namespace_for,
)
from forze.application.usecases.document import (
    AggregatedListDocuments,
    CreateDocument,
    DeleteDocument,
    GetDocument,
    GetDocumentByNumberId,
    KillDocument,
    RawCursorListDocuments,
    RawListDocuments,
    RestoreDocument,
    TypedCursorListDocuments,
    TypedListDocuments,
    UpdateDocument,
)
from forze.base.errors import CoreError

from ..mapping import DTOMapper, DTOMapperStep
from .facades import DocumentDTOs
from .operations import DocumentKernelOp

# ----------------------- #


def build_document_create_mapper(
    spec: DocumentSpec[Any, Any, Any, Any],
    dtos: DocumentDTOs[Any, Any, Any],
    *,
    steps: tuple[DTOMapperStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for create commands.

    :param spec: Document specification.
    :param dto_spec: Document DTO specification.
    :param steps: Optional mapping steps to append to the mapper.
    :returns: DTO mapper for create commands.
    """

    if spec.write is None:
        raise CoreError("Document specification does not support write operations")

    create_dto = dtos.create

    if create_dto is None:
        raise CoreError("Document specification does not support create operations")

    mapper = DTOMapper(in_=create_dto, out=spec.write["create_cmd"])

    return mapper.with_steps(*steps)


# ....................... #


def build_document_update_mapper(
    spec: DocumentSpec[Any, Any, Any, Any],
    dtos: DocumentDTOs[Any, Any, Any],
    *,
    steps: tuple[DTOMapperStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for update commands.

    :param spec: Document specification.
    :param dto_spec: Document DTO specification.
    :param steps: Optional mapping steps to append to the mapper.
    :returns: DTO mapper for update commands.
    """

    if spec.write is None:
        raise CoreError("Document specification does not support write operations")

    update_dto = dtos.update

    if update_dto is None or "update_cmd" not in spec.write:
        raise CoreError("Document specification does not support update operations")

    mapper = DTOMapper(in_=update_dto, out=spec.write["update_cmd"])

    return mapper.with_steps(*steps)


# ....................... #


def build_document_list_mapper(
    *,
    steps: tuple[DTOMapperStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for list requests with optional steps."""

    mapper = DTOMapper(
        in_=ListRequestDTO,
        out=ListRequestDTO,
    )

    return mapper.with_steps(*steps)


# ....................... #


def build_document_aggregated_list_mapper(
    *,
    steps: tuple[DTOMapperStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for aggregated list requests."""

    mapper = DTOMapper(
        in_=AggregatedListRequestDTO,
        out=AggregatedListRequestDTO,
    )
    return mapper.with_steps(*steps)


# ....................... #


def build_document_raw_list_mapper(
    *,
    steps: tuple[DTOMapperStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for raw list requests.

    :param spec: Document specification.
    :param dto_spec: Document DTO specification.
    :param steps: Optional mapping steps to append to the mapper.
    :returns: DTO mapper for raw list requests.
    """

    mapper = DTOMapper(
        in_=RawListRequestDTO,
        out=RawListRequestDTO,
    )

    return mapper.with_steps(*steps)


# ....................... #


def build_document_list_cursor_mapper(
    *,
    steps: tuple[DTOMapperStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for cursor list requests."""

    mapper = DTOMapper(
        in_=CursorListRequestDTO,
        out=CursorListRequestDTO,
    )
    return mapper.with_steps(*steps)


# ....................... #


def build_document_raw_list_cursor_mapper(
    *,
    steps: tuple[DTOMapperStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for raw cursor list requests."""

    mapper = DTOMapper(
        in_=RawCursorListRequestDTO,
        out=RawCursorListRequestDTO,
    )
    return mapper.with_steps(*steps)


# ....................... #


def build_document_registry(
    spec: DocumentSpec[Any, Any, Any, Any],
    dtos: DocumentDTOs[Any, Any, Any],
    *,
    namespace: OperationNamespace | None = None,
    create_steps: tuple[DTOMapperStep[Any], ...] = (),
    update_steps: tuple[DTOMapperStep[Any], ...] = (),
    list_steps: tuple[DTOMapperStep[Any], ...] = (),
    raw_list_steps: tuple[DTOMapperStep[Any], ...] = (),
    aggregated_list_steps: tuple[DTOMapperStep[Any], ...] = (),
    list_cursor_steps: tuple[DTOMapperStep[Any], ...] = (),
    raw_list_cursor_steps: tuple[DTOMapperStep[Any], ...] = (),
) -> UsecaseRegistry:
    """Build a usecase registry for the given document spec.

    :param spec: Document specification.
    :param dto_spec: Document DTO specification.
    :param namespace: Operation namespace; defaults to :func:`operation_namespace_for` ``(spec)``.
    :param create_steps: Optional mapping steps to append to the create mapper.
    :param update_steps: Optional mapping steps to append to the update mapper.
    :param list_steps: Optional mapping steps to append to the list mapper.
    :param raw_list_steps: Optional mapping steps to append to the raw list mapper.
    :param aggregated_list_steps: Optional mapping steps to append to the aggregated list mapper.
    :param list_cursor_steps: Optional mapping steps for cursor list requests.
    :param raw_list_cursor_steps: Optional mapping steps for raw cursor list requests.
    :returns: Usecase registry with all supported operations.
    """

    ops = namespace or operation_namespace_for(spec)

    list_mapper = build_document_list_mapper(steps=list_steps)
    raw_list_mapper = build_document_raw_list_mapper(steps=raw_list_steps)
    list_cursor_mapper = build_document_list_cursor_mapper(steps=list_cursor_steps)
    raw_list_cursor_mapper = build_document_raw_list_cursor_mapper(
        steps=raw_list_cursor_steps,
    )
    aggregated_list_mapper = build_document_aggregated_list_mapper(
        steps=aggregated_list_steps
    )

    reg = UsecaseRegistry(
        {
            DocumentKernelOp.GET: lambda ctx: GetDocument(
                ctx=ctx,
                doc=ctx.doc_query(spec),
            ),
            DocumentKernelOp.LIST: lambda ctx: TypedListDocuments(
                ctx=ctx,
                doc=ctx.doc_query(spec),
                mapper=list_mapper,
            ),
            DocumentKernelOp.RAW_LIST: lambda ctx: RawListDocuments(
                ctx=ctx,
                doc=ctx.doc_query(spec),
                mapper=raw_list_mapper,
            ),
            DocumentKernelOp.LIST_CURSOR: lambda ctx: TypedCursorListDocuments(
                ctx=ctx,
                doc=ctx.doc_query(spec),
                mapper=list_cursor_mapper,
            ),
            DocumentKernelOp.RAW_LIST_CURSOR: lambda ctx: RawCursorListDocuments(
                ctx=ctx,
                doc=ctx.doc_query(spec),
                mapper=raw_list_cursor_mapper,
            ),
            DocumentKernelOp.AGG_LIST: lambda ctx: AggregatedListDocuments(
                ctx=ctx,
                doc=ctx.doc_query(spec),
                mapper=aggregated_list_mapper,
            ),
        },
        namespace=ops,
    )

    if spec.supports_number_id():
        reg.register(
            DocumentKernelOp.GET_BY_NUMBER_ID,
            lambda ctx: GetDocumentByNumberId(
                ctx=ctx,
                doc=ctx.doc_query(spec),
            ),
        )

    if spec.write is not None:
        if dtos.create is not None:
            create_mapper = build_document_create_mapper(
                spec,
                dtos,
                steps=create_steps,
            )
            reg.register(
                DocumentKernelOp.CREATE,
                lambda ctx: CreateDocument(
                    ctx=ctx,
                    doc=ctx.doc_command(spec),
                    mapper=create_mapper,
                ),
            )

        reg.register(
            DocumentKernelOp.KILL,
            lambda ctx: KillDocument(
                ctx=ctx,
                doc=ctx.doc_command(spec),
            ),
        )

        if spec.supports_update() and dtos.update is not None:
            update_mapper = build_document_update_mapper(
                spec,
                dtos,
                steps=update_steps,
            )

            reg.register(
                DocumentKernelOp.UPDATE,
                lambda ctx: UpdateDocument[Any, Any, Any](
                    ctx=ctx,
                    doc=ctx.doc_command(spec),
                    mapper=update_mapper,
                ),
            )

        if spec.supports_soft_delete():
            reg.register_many(
                {
                    DocumentKernelOp.DELETE: lambda ctx: DeleteDocument(
                        ctx=ctx,
                        doc=ctx.doc_command(spec),
                    ),
                    DocumentKernelOp.RESTORE: lambda ctx: RestoreDocument(
                        ctx=ctx,
                        doc=ctx.doc_command(spec),
                    ),
                },
            )

    return reg


# ....................... #


def apply_default_tx_document_registry(
    registry: UsecaseRegistry,
    spec: DocumentSpec[Any, Any, Any, Any],
    route: str | StrEnum,
    *,
    namespace: OperationNamespace | None = None,
) -> UsecaseRegistry:
    """Apply the default transaction layout for document write operations.

    :param registry: Registry to mutate.
    :param spec: Document specification (used to derive the namespace when ``namespace`` is omitted).
    :param route: Transaction route label.
    :param namespace: Optional override; defaults to :func:`operation_namespace_for` ``(spec)``.
    """

    ops = namespace or operation_namespace_for(spec)

    registry.tx(
        [
            ops.key(DocumentKernelOp.CREATE),
            ops.key(DocumentKernelOp.UPDATE),
            ops.key(DocumentKernelOp.DELETE),
            ops.key(DocumentKernelOp.RESTORE),
            ops.key(DocumentKernelOp.KILL),
        ],
        route=route,
    )
    return registry
