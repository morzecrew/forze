"""Factories for document plans, mappers, and registries."""

from typing import Any, Final, Literal, TypeVar, cast

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.querying import QueryFieldGuard, build_query_discovery
from forze.application.execution.operations import (
    OperationDescriptor,
    OperationRegistry,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, StrKeyNamespace
from forze.domain.models import BaseDTO, Document
from forze_kits.dto.paginated import (
    CursorPaginated,
    Paginated,
    ProjectedCursorPaginated,
    ProjectedPaginated,
)
from forze_kits.mapping import PydanticPipelineMapperFactory

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
    UpdateDocumentRecord,
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

UpdateReturns = Literal["result", "record"]
"""What a generated update returns: ``"result"`` wraps the record with its diff
(:class:`DocumentUpdateRes`); ``"record"`` returns the updated read model itself."""

_UPDATE_RETURNS: Final[frozenset[str]] = frozenset({"result", "record"})


def _query_guard(spec: DocumentSpec[Any, Any, Any, Any]) -> QueryFieldGuard | None:
    """Build a boundary field guard when the spec restricts a filter/sort axis.

    ``None`` when no ``query_policy`` is set, or when both axes are unrestricted — so a
    spec without explicit allow-sets pays no guard cost and keeps today's behavior.
    """

    policy = spec.query_policy

    if policy is None or (
        policy.filterable is None and policy.sortable is None and policy.aggregatable is None
    ):
        return None

    return QueryFieldGuard(
        policy=policy, spec_name=str(spec.name), filter_limits=spec.filter_limits
    )


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
C_cmd = TypeVar("C_cmd", bound=BaseDTO, default=Any)
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
    u_cmd = spec.write["update_cmd"] if spec.write and "update_cmd" in spec.write else None

    if udto is None or u_cmd is None:
        raise exc.configuration("Update DTO or update command is not provided")

    return PydanticPipelineMapperFactory(in_=udto, out=u_cmd)


# ....................... #


def _build_document_descriptors(
    spec: DocumentSpec[R, D, C_cmd, U_cmd],
    dtos: DocumentDTOs[R, C, U],
    update_returns: UpdateReturns,
) -> dict[StrKey, OperationDescriptor]:
    """Build catalog descriptors for the registered document operations.

    One descriptor per operation, carrying the request/response DTO types so a driving
    adapter can derive schemas. Write descriptors are emitted only when the matching DTO
    is configured, mirroring the handlers in :func:`build_document_registry`. A spec
    marked ``sensitive`` propagates the flag onto every descriptor so projection
    surfaces (generated routes, MCP) can refuse it at build time.
    """

    read = dtos.read

    # The read model's filter/sort/aggregate surface (per-field operators), attached to
    # every filter-accepting list op so generated HTTP/MCP surfaces can advertise it.
    discovery = build_query_discovery(
        spec.read,
        filterable=spec.filterable_fields(),
        sortable=spec.sortable_fields(),
        aggregatable=spec.aggregatable_fields(),
    )

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
            query_discovery=discovery,
        ),
        DocumentKernelOp.RAW_LIST: OperationDescriptor(
            input_type=ProjectedListRequestDTO,
            output_type=ProjectedPaginated,
            description="List projected document fields (offset pagination).",
            query_discovery=discovery,
        ),
        DocumentKernelOp.LIST_CURSOR: OperationDescriptor(
            input_type=CursorListRequestDTO,
            output_type=_parametrized(CursorPaginated, read),
            description="List documents by filters and sorts (cursor pagination).",
            query_discovery=discovery,
        ),
        DocumentKernelOp.RAW_LIST_CURSOR: OperationDescriptor(
            input_type=ProjectedCursorListRequestDTO,
            output_type=ProjectedCursorPaginated,
            description="List projected document fields (cursor pagination).",
            query_discovery=discovery,
        ),
        DocumentKernelOp.AGG_LIST: OperationDescriptor(
            input_type=AggregatedListRequestDTO,
            output_type=ProjectedPaginated,
            description="List documents with aggregates by filters and sorts.",
            query_discovery=discovery,
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
            descriptors[DocumentKernelOp.UPDATE] = (
                OperationDescriptor(
                    input_type=_parametrized(DocumentUpdateDTO, dtos.update),
                    output_type=read,
                    description="Update an existing document and return it.",
                )
                if update_returns == "record"
                else OperationDescriptor(
                    input_type=_parametrized(DocumentUpdateDTO, dtos.update),
                    output_type=_parametrized(DocumentUpdateRes, read),
                    description="Update an existing document and return the result with diff.",
                )
            )

    if spec.sensitive:
        descriptors = {
            op: attrs.evolve(descriptor, sensitive=True) for op, descriptor in descriptors.items()
        }

    return descriptors


# ....................... #


def build_document_registry(
    spec: DocumentSpec[R, D, C_cmd, U_cmd],
    dtos: DocumentDTOs[R, C, U] | None = None,
    mappers: DocumentMappers[C, C_cmd, U, U_cmd] = DocumentMappers(),
    *,
    ns: StrKeyNamespace | None = None,
    update_returns: UpdateReturns = "result",
) -> OperationRegistry:
    """Build document operation registry.

    :param spec: Document specification.
    :param dtos: Document DTO specification. Derived from the spec when omitted
        (:meth:`DocumentDTOs.from_spec`) — the common case where the inbound create/update
        DTOs are the spec's own commands; pass an explicit mapping to override or to disable
        an op.
    :param mappers: Document mappers.
    :param ns: Optional namespace.
    :param update_returns: ``"result"`` (default) returns the record with its diff;
        ``"record"`` returns the updated read model itself, and skips computing the diff.
    :returns: Operation registry with all supported operations.
    """

    if not isinstance(update_returns, str) or update_returns not in _UPDATE_RETURNS:
        raise exc.configuration(
            f"update_returns must be one of {sorted(_UPDATE_RETURNS)}, not {update_returns!r}."
        )

    # When omitted, the inbound DTOs are the spec's commands, so the derived ``C``/``U`` are
    # the spec's ``C_cmd``/``U_cmd`` — the cast records that identity for the type checker.
    dtos = (
        dtos
        if dtos is not None
        else cast(
            "DocumentDTOs[R, C, U]",
            DocumentDTOs.from_spec(  # pyright: ignore[reportUnknownMemberType]
                spec  # pyright: ignore[reportArgumentType]
            ),
        )
    )

    ns = ns or spec.default_namespace

    guard = _query_guard(spec)

    reg = OperationRegistry(
        handlers={
            ns.key(DocumentKernelOp.GET): lambda ctx: GetDocument(
                doc=ctx.doc.query(spec),
            ),
            ns.key(DocumentKernelOp.LIST): lambda ctx: ListDocuments(
                doc=ctx.doc.query(spec),
                mapper=mappers.list(ctx) if mappers.list else None,
                query_guard=guard,
            ),
            ns.key(DocumentKernelOp.RAW_LIST): lambda ctx: ProjectedListDocuments(
                doc=ctx.doc.query(spec),
                mapper=mappers.projected_list(ctx) if mappers.projected_list else None,
                query_guard=guard,
            ),
            ns.key(DocumentKernelOp.LIST_CURSOR): lambda ctx: CursorListDocuments(
                doc=ctx.doc.query(spec),
                mapper=mappers.cursor_list(ctx) if mappers.cursor_list else None,
                query_guard=guard,
            ),
            ns.key(DocumentKernelOp.RAW_LIST_CURSOR): lambda ctx: ProjectedCursorListDocuments(
                doc=ctx.doc.query(spec),
                mapper=(
                    mappers.projected_cursor_list(ctx) if mappers.projected_cursor_list else None
                ),
                query_guard=guard,
            ),
            ns.key(DocumentKernelOp.AGG_LIST): lambda ctx: AggregatedListDocuments(
                doc=ctx.doc.query(spec),
                mapper=(mappers.aggregated_list(ctx) if mappers.aggregated_list else None),
                query_guard=guard,
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

            def _update_mapper(ctx: Any) -> Any:
                if mappers.update:
                    return mappers.update(ctx)

                return _default_update_mapper(spec, dtos)(ctx)

            reg = reg.set_handler(
                ns.key(DocumentKernelOp.UPDATE),
                (
                    (
                        lambda ctx: UpdateDocumentRecord[U, U_cmd, R](
                            doc=ctx.doc.command(spec), mapper=_update_mapper(ctx)
                        )
                    )
                    if update_returns == "record"
                    else (
                        lambda ctx: UpdateDocument[U, U_cmd, R](
                            doc=ctx.doc.command(spec), mapper=_update_mapper(ctx)
                        )
                    )
                ),
            )

    # Read operations only acquire query ports — mark them so they run read-only and
    # surface as read-only in the operation catalog.
    reg = reg.bind(*_READ_OPS, namespace=ns).as_query().finish()

    # Attach catalog metadata (request/response schemas + descriptions).
    reg = reg.set_descriptors(_build_document_descriptors(spec, dtos, update_returns), namespace=ns)

    return reg
