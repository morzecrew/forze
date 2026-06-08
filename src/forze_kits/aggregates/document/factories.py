"""Factories for document plans, mappers, and registries."""

from typing import Any, TypeVar

from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.execution.operations import OperationDescriptor, OperationRegistry
from .handlers import (
    AggregatedListDocuments,
    CreateDocument,
    CursorListDocuments,
    GetDocument,
    KillDocument,
    ListDocuments,
    ProjectedCursorListDocuments,
    ProjectedListDocuments,
    UpdateDocument,
)
from forze_kits.dto.paginated import (
    CursorPaginated,
    Paginated,
    ProjectedCursorPaginated,
    ProjectedPaginated,
)
from forze_kits.mapping import PydanticPipelineMapperFactory
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, StrKeyNamespace
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from .dto import (
    AggregatedListRequestDTO,
    CursorListRequestDTO,
    DocumentIdDTO,
    DocumentUpdateDTO,
    DocumentUpdateRes,
    ListRequestDTO,
    ProjectedCursorListRequestDTO,
    ProjectedListRequestDTO,
)
from .operations import DocumentKernelOp
from .value_objects import DocumentDTOs, DocumentMappers

# ----------------------- #

_READ_OPS: tuple[DocumentKernelOp, ...] = (
    DocumentKernelOp.GET,
    DocumentKernelOp.LIST,
    DocumentKernelOp.RAW_LIST,
    DocumentKernelOp.LIST_CURSOR,
    DocumentKernelOp.RAW_LIST_CURSOR,
    DocumentKernelOp.AGG_LIST,
)
"""Document operations that only acquire read (query) ports."""


def _parametrized(generic: Any, arg: Any) -> Any:
    """Parametrize a generic envelope (e.g. ``Paginated``) with a runtime read type.

    Kept off the static-type path: the read model is only known at build time, so the
    subscription must happen on values, not as a type annotation.
    """

    return generic[arg]

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
C = TypeVar("C", bound=BaseDTO, default=BaseDTO)
U = TypeVar("U", bound=BaseDTO, default=BaseDTO)

D = TypeVar("D", bound=Document, default=Any)
C_cmd = TypeVar("C_cmd", bound=CreateDocumentCmd, default=Any)
U_cmd = TypeVar("U_cmd", bound=BaseDTO, default=Any)

# ....................... #


def _default_create_mapper(
    spec: DocumentSpec[R, D, C_cmd, U_cmd],
    dtos: DocumentDTOs[R, C, U],
) -> PydanticPipelineMapperFactory[C, C_cmd]:
    """Build default create mapper factory (pydantic)."""

    cdto = dtos.create
    c_cmd = spec.write["create_cmd"] if spec.write else None

    if cdto is None or c_cmd is None:
        raise exc.configuration("Create DTO or create command is not provided")

    return PydanticPipelineMapperFactory(in_=cdto, out=c_cmd)


# ....................... #


def _default_update_mapper(
    spec: DocumentSpec[R, D, C_cmd, U_cmd],
    dtos: DocumentDTOs[R, C, U],
) -> PydanticPipelineMapperFactory[U, U_cmd]:
    """Build default update mapper factory (pydantic)."""

    udto = dtos.update
    u_cmd = (
        spec.write["update_cmd"] if spec.write and "update_cmd" in spec.write else None
    )

    if udto is None or u_cmd is None:
        raise exc.configuration("Update DTO or update command is not provided")

    return PydanticPipelineMapperFactory(in_=udto, out=u_cmd)


# ....................... #


def _build_document_descriptors(
    spec: DocumentSpec[R, D, C_cmd, U_cmd],
    dtos: DocumentDTOs[R, C, U],
) -> dict[StrKey, OperationDescriptor]:
    """Build catalog descriptors for the registered document operations.

    One descriptor per operation, carrying the request/response DTO types so a driving
    adapter can derive schemas. Write descriptors are emitted only when the matching DTO
    is configured, mirroring the handlers in :func:`build_document_registry`.
    """

    read = dtos.read

    descriptors: dict[StrKey, OperationDescriptor] = {
        DocumentKernelOp.GET: OperationDescriptor(
            input_type=DocumentIdDTO,
            output_type=read,
            description="Fetch a single document by primary key.",
        ),
        DocumentKernelOp.LIST: OperationDescriptor(
            input_type=ListRequestDTO,
            output_type=_parametrized(Paginated, read),
            description="List documents by filters and sorts (offset pagination).",
        ),
        DocumentKernelOp.RAW_LIST: OperationDescriptor(
            input_type=ProjectedListRequestDTO,
            output_type=ProjectedPaginated,
            description="List projected document fields (offset pagination).",
        ),
        DocumentKernelOp.LIST_CURSOR: OperationDescriptor(
            input_type=CursorListRequestDTO,
            output_type=_parametrized(CursorPaginated, read),
            description="List documents by filters and sorts (cursor pagination).",
        ),
        DocumentKernelOp.RAW_LIST_CURSOR: OperationDescriptor(
            input_type=ProjectedCursorListRequestDTO,
            output_type=ProjectedCursorPaginated,
            description="List projected document fields (cursor pagination).",
        ),
        DocumentKernelOp.AGG_LIST: OperationDescriptor(
            input_type=AggregatedListRequestDTO,
            output_type=ProjectedPaginated,
            description="List documents with aggregates by filters and sorts.",
        ),
    }

    if spec.write is not None:
        descriptors[DocumentKernelOp.KILL] = OperationDescriptor(
            input_type=DocumentIdDTO,
            output_type=None,
            description="Permanently delete a document by primary key (hard delete).",
        )

        if dtos.create is not None:
            descriptors[DocumentKernelOp.CREATE] = OperationDescriptor(
                input_type=dtos.create,
                output_type=read,
                description="Create a new document.",
            )

        if spec.supports_update() and dtos.update is not None:
            descriptors[DocumentKernelOp.UPDATE] = OperationDescriptor(
                input_type=_parametrized(DocumentUpdateDTO, dtos.update),
                output_type=_parametrized(DocumentUpdateRes, read),
                description="Update an existing document and return the result with diff.",
            )

    return descriptors


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

    ns = ns or spec.default_namespace

    reg = OperationRegistry(
        handlers={
            ns.key(DocumentKernelOp.GET): lambda ctx: GetDocument(
                doc=ctx.doc.query(spec),
            ),
            ns.key(DocumentKernelOp.LIST): lambda ctx: ListDocuments(
                doc=ctx.doc.query(spec),
                mapper=mappers.list(ctx) if mappers.list else None,
            ),
            ns.key(DocumentKernelOp.RAW_LIST): lambda ctx: ProjectedListDocuments(
                doc=ctx.doc.query(spec),
                mapper=mappers.projected_list(ctx) if mappers.projected_list else None,
            ),
            ns.key(DocumentKernelOp.LIST_CURSOR): lambda ctx: CursorListDocuments(
                doc=ctx.doc.query(spec),
                mapper=mappers.cursor_list(ctx) if mappers.cursor_list else None,
            ),
            ns.key(
                DocumentKernelOp.RAW_LIST_CURSOR
            ): lambda ctx: ProjectedCursorListDocuments(
                doc=ctx.doc.query(spec),
                mapper=(
                    mappers.projected_cursor_list(ctx)
                    if mappers.projected_cursor_list
                    else None
                ),
            ),
            ns.key(DocumentKernelOp.AGG_LIST): lambda ctx: AggregatedListDocuments(
                doc=ctx.doc.query(spec),
                mapper=(
                    mappers.aggregated_list(ctx) if mappers.aggregated_list else None
                ),
            ),
        },
    )

    if spec.write is not None:
        reg = reg.set_handler(
            ns.key(DocumentKernelOp.KILL),
            lambda ctx: KillDocument(doc=ctx.doc.command(spec)),
        )

        if dtos.create is not None:
            reg = reg.set_handler(
                ns.key(DocumentKernelOp.CREATE),
                lambda ctx: CreateDocument[C, C_cmd, R](
                    doc=ctx.doc.command(spec),
                    mapper=(
                        mappers.create(ctx)
                        if mappers.create
                        else _default_create_mapper(spec, dtos)(ctx)
                    ),
                ),
            )

        if spec.supports_update() and dtos.update is not None:
            reg = reg.set_handler(
                ns.key(DocumentKernelOp.UPDATE),
                lambda ctx: UpdateDocument[U, U_cmd, R](
                    doc=ctx.doc.command(spec),
                    mapper=(
                        mappers.update(ctx)
                        if mappers.update
                        else _default_update_mapper(spec, dtos)(ctx)
                    ),
                ),
            )

    # Read operations only acquire query ports — mark them so they run read-only and
    # surface as read-only in the operation catalog.
    reg = reg.bind(*_READ_OPS, namespace=ns).as_query().finish()

    # Attach catalog metadata (request/response schemas + descriptions).
    reg = reg.set_descriptors(_build_document_descriptors(spec, dtos), namespace=ns)

    return reg
