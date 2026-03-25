"""Factories for document plans, mappers, and registries."""

from typing import Any

from forze.application.contracts.document import DocumentSpec
from forze.application.dto import ListRequestDTO, RawListRequestDTO
from forze.application.execution import UsecaseRegistry
from forze.application.mapping import DTOMapper, MappingStep
from forze.application.usecases.document import (
    CreateDocument,
    DeleteDocument,
    GetDocument,
    KillDocument,
    RawListDocuments,
    RestoreDocument,
    TypedListDocuments,
    UpdateDocument,
)
from forze.base.errors import CoreError

from .facades import DocumentDTOs
from .operations import DocumentOperation

# ----------------------- #


def build_document_create_mapper(
    spec: DocumentSpec[Any, Any, Any, Any],
    dtos: DocumentDTOs[Any, Any, Any],
    *,
    steps: tuple[MappingStep[Any], ...] = (),
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
    steps: tuple[MappingStep[Any], ...] = (),
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

    if update_dto is None:
        raise CoreError("Document specification does not support update operations")

    mapper = DTOMapper(in_=update_dto, out=spec.write["update_cmd"])

    return mapper.with_steps(*steps)


# ....................... #


def build_document_list_mapper(
    *,
    steps: tuple[MappingStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for list requests with optional steps."""

    mapper = DTOMapper(
        in_=ListRequestDTO,
        out=ListRequestDTO,
    )

    return mapper.with_steps(*steps)


# ....................... #


def build_document_raw_list_mapper(
    *,
    steps: tuple[MappingStep[Any], ...] = (),
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


def build_document_registry(
    spec: DocumentSpec[Any, Any, Any, Any],
    dtos: DocumentDTOs[Any, Any, Any],
    *,
    create_steps: tuple[MappingStep[Any], ...] = (),
    update_steps: tuple[MappingStep[Any], ...] = (),
    list_steps: tuple[MappingStep[Any], ...] = (),
    raw_list_steps: tuple[MappingStep[Any], ...] = (),
) -> UsecaseRegistry:
    """Build a usecase registry for the given document spec.

    :param spec: Document specification.
    :param dto_spec: Document DTO specification.
    :param create_steps: Optional mapping steps to append to the create mapper.
    :param update_steps: Optional mapping steps to append to the update mapper.
    :param list_steps: Optional mapping steps to append to the list mapper.
    :param raw_list_steps: Optional mapping steps to append to the raw list mapper.
    :returns: Usecase registry with all supported operations.
    """

    list_mapper = build_document_list_mapper(steps=list_steps)
    raw_list_mapper = build_document_raw_list_mapper(steps=raw_list_steps)

    reg = UsecaseRegistry(
        {
            DocumentOperation.GET: lambda ctx: GetDocument(
                ctx=ctx,
                doc=ctx.doc_read(spec),
            ),
            DocumentOperation.LIST: lambda ctx: TypedListDocuments(
                ctx=ctx,
                doc=ctx.doc_read(spec),
                mapper=list_mapper,
            ),
            DocumentOperation.RAW_LIST: lambda ctx: RawListDocuments(
                ctx=ctx,
                doc=ctx.doc_read(spec),
                mapper=raw_list_mapper,
            ),
        }
    )

    if spec.write is not None:
        create_mapper = build_document_create_mapper(
            spec,
            dtos,
            steps=create_steps,
        )
        update_mapper = build_document_update_mapper(
            spec,
            dtos,
            steps=update_steps,
        )

        reg = reg.register_many(
            {
                DocumentOperation.CREATE: lambda ctx: CreateDocument(
                    ctx=ctx,
                    doc=ctx.doc_write(spec),
                    mapper=create_mapper,
                ),
                DocumentOperation.KILL: lambda ctx: KillDocument(
                    ctx=ctx,
                    doc=ctx.doc_write(spec),
                ),
            }
        )

        if spec.supports_update():
            reg.register(
                DocumentOperation.UPDATE,
                lambda ctx: UpdateDocument[Any, Any, Any](
                    ctx=ctx,
                    doc=ctx.doc_write(spec),
                    mapper=update_mapper,
                ),
                inplace=True,
            )

        if spec.supports_soft_delete():
            reg.register_many(
                {
                    DocumentOperation.DELETE: lambda ctx: DeleteDocument(
                        ctx=ctx,
                        doc=ctx.doc_write(spec),
                    ),
                    DocumentOperation.RESTORE: lambda ctx: RestoreDocument(
                        ctx=ctx,
                        doc=ctx.doc_write(spec),
                    ),
                },
                inplace=True,
            )

    return reg
