"""Factories for document plans, mappers, and registries."""

from typing import Any, Optional

from forze.application.contracts.document import DocumentSpec
from forze.application.dto import ListRequestDTO, RawListRequestDTO
from forze.application.execution import UsecasePlan, UsecaseRegistry
from forze.application.mapping import DTOMapper, MappingStep, NumberIdStep
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
from forze.base.logging import getLogger

from .facades import DocumentDTOSpec
from .operations import DocumentOperation

# ----------------------- #

logger = getLogger(__name__)


def build_document_plan(
    *,
    tx_on_write: bool = True,
) -> UsecasePlan:
    """Build a usecase plan with optional transaction wrapping for write ops.

    When ``tx_on_write`` is ``True``, enables transactions for create, update,
    delete, restore, and kill operations.

    :param tx_on_write: Whether to wrap write operations in transactions.
    :returns: Usecase plan.
    """
    logger.trace("build_document_plan: tx_on_write=%s", tx_on_write)
    plan = UsecasePlan()

    if tx_on_write:
        for op in [
            DocumentOperation.CREATE,
            DocumentOperation.UPDATE,
            DocumentOperation.DELETE,
            DocumentOperation.RESTORE,
            DocumentOperation.KILL,
        ]:
            plan = plan.tx(op)

    return plan


# ....................... #


def build_document_create_mapper(
    spec: DocumentSpec[Any, Any, Any, Any],
    dto_spec: DocumentDTOSpec[Any, Any, Any, Any, Any],
    *,
    numbered: bool = False,
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for create commands.

    When ``numbered`` is ``True``, adds :class:`NumberIdStep` to inject
    ``number_id`` from the counter for the spec's namespace.

    :param spec: Document specification.
    :param numbered: Whether to add number_id injection.
    :returns: DTO mapper for create commands.
    """
    logger.trace(
        "build_document_create_mapper: namespace=%s, numbered=%s",
        spec.namespace,
        numbered,
    )

    if spec.write is None:
        raise CoreError("Document specification does not support write operations")

    create_dto = dto_spec.get("create")

    if create_dto is None:
        raise CoreError("Document specification does not support create operations")

    mapper = DTOMapper(in_=create_dto, out=spec.write["models"]["create_cmd"])

    if numbered:
        mapper = mapper.with_steps(NumberIdStep(namespace=spec.namespace))

    return mapper


# ....................... #


def build_document_update_mapper(
    spec: DocumentSpec[Any, Any, Any, Any],
    dto_spec: DocumentDTOSpec[Any, Any, Any, Any, Any],
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for update commands.

    :param spec: Document specification.
    :returns: DTO mapper for update commands.
    """
    logger.trace("build_document_update_mapper: namespace=%s", spec.namespace)
    if spec.write is None:
        raise CoreError("Document specification does not support write operations")

    update_dto = dto_spec.get("update")

    if update_dto is None:
        raise CoreError("Document specification does not support update operations")

    return DTOMapper(in_=update_dto, out=spec.write["models"]["update_cmd"])


# ....................... #


def build_document_list_mapper(
    spec: DocumentSpec[Any, Any, Any, Any],
    dto_spec: DocumentDTOSpec[Any, Any, Any, Any, Any],
    steps: tuple[MappingStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for list requests with optional steps."""
    logger.trace(
        "build_document_list_mapper: namespace=%s, steps=%d",
        spec.namespace,
        len(steps),
    )

    mapper = DTOMapper(
        in_=dto_spec.get("list", ListRequestDTO),
        out=ListRequestDTO,
    )

    return mapper.with_steps(*steps)


# ....................... #


def build_document_raw_list_mapper(
    spec: DocumentSpec[Any, Any, Any, Any],
    dto_spec: DocumentDTOSpec[Any, Any, Any, Any, Any],
    steps: tuple[MappingStep[Any], ...] = (),
) -> DTOMapper[Any, Any]:
    """Build a DTO mapper for raw list requests.

    :param spec: Document specification.
    :returns: DTO mapper for raw list requests.
    """
    logger.trace(
        "build_document_raw_list_mapper: namespace=%s, steps=%d",
        spec.namespace,
        len(steps),
    )

    mapper = DTOMapper(
        in_=dto_spec.get("raw_list", RawListRequestDTO),
        out=RawListRequestDTO,
    )

    return mapper.with_steps(*steps)


# ....................... #


def build_document_registry(
    spec: DocumentSpec[Any, Any, Any, Any],
    dto_spec: DocumentDTOSpec[Any, Any, Any, Any, Any],
    *,
    replace_create_mapper: Optional[DTOMapper[Any, Any]] = None,
    replace_update_mapper: Optional[DTOMapper[Any, Any]] = None,
    replace_list_mapper: Optional[DTOMapper[Any, Any]] = None,
    replace_raw_list_mapper: Optional[DTOMapper[Any, Any]] = None,
) -> UsecaseRegistry:
    """Build a usecase registry for the given document spec.

    Registers get, search, raw_search, create, and kill. Registers update when
    the spec supports it; registers delete and restore when the spec supports
    soft delete. Uses ``replace_create_mapper`` when provided, otherwise
    builds a default create mapper.

    :param spec: Document specification.
    :param replace_create_mapper: Optional custom create mapper.
    :param replace_update_mapper: Optional custom update mapper.
    :returns: Usecase registry with all supported operations.
    """
    logger.trace(
        "build_document_registry: namespace=%s",
        spec.namespace,
    )

    list_mapper = replace_list_mapper or build_document_list_mapper(spec, dto_spec)
    raw_list_mapper = replace_raw_list_mapper or build_document_raw_list_mapper(
        spec, dto_spec
    )

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
        create_mapper = replace_create_mapper or build_document_create_mapper(
            spec, dto_spec
        )
        update_mapper = replace_update_mapper or build_document_update_mapper(
            spec, dto_spec
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
