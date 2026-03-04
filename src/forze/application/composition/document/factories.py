"""Factories for document plans, mappers, and registries.

Provides :func:`build_document_plan`, :func:`build_document_create_mapper`, and
:func:`build_document_registry`. Used to assemble document usecases from a
:class:`DocumentSpec`.
"""

from typing import Any, Optional

from forze.application.contracts.document import DocumentSpec
from forze.application.execution import UsecasePlan, UsecaseRegistry
from forze.application.mapping import DTOMapper, NumberIdStep
from forze.application.usecases.document import (
    CreateDocument,
    DeleteDocument,
    GetDocument,
    KillDocument,
    RestoreDocument,
    UpdateDocument,
)

from .operations import DocumentOperation

# ----------------------- #


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
    *,
    numbered: bool = False,
) -> DTOMapper[Any]:
    """Build a DTO mapper for create commands.

    When ``numbered`` is ``True``, adds :class:`NumberIdStep` to inject
    ``number_id`` from the counter for the spec's namespace.

    :param spec: Document specification.
    :param numbered: Whether to add number_id injection.
    :returns: DTO mapper for create commands.
    """
    mapper = DTOMapper(out=spec.models["create_cmd"])

    if numbered:
        mapper = mapper.with_steps(NumberIdStep(namespace=spec.namespace))

    return mapper


# ....................... #


def build_document_registry(
    spec: DocumentSpec[Any, Any, Any, Any],
    *,
    replace_create_mapper: Optional[DTOMapper[Any]] = None,
) -> UsecaseRegistry:
    """Build a usecase registry for the given document spec.

    Registers get, search, raw_search, create, and kill. Registers update when
    the spec supports it; registers delete and restore when the spec supports
    soft delete. Uses ``replace_create_mapper`` when provided, otherwise
    builds a default create mapper.

    :param spec: Document specification.
    :param replace_create_mapper: Optional custom create mapper.
    :returns: Usecase registry with all supported operations.
    """
    create_mapper = replace_create_mapper or DTOMapper(out=spec.models["create_cmd"])
    update_mapper = DTOMapper(out=spec.models["update_cmd"])

    reg = UsecaseRegistry(
        {
            DocumentOperation.GET: lambda ctx: GetDocument(
                ctx=ctx,
                doc=ctx.doc(spec),
            ),
            DocumentOperation.CREATE: lambda ctx: CreateDocument(
                ctx=ctx,
                doc=ctx.doc(spec),
                mapper=create_mapper,
            ),
            DocumentOperation.KILL: lambda ctx: KillDocument(
                ctx=ctx,
                doc=ctx.doc(spec),
            ),
        }
    )

    if spec.supports_update():
        reg.register(
            DocumentOperation.UPDATE,
            lambda ctx: UpdateDocument[Any, Any, Any](
                ctx=ctx,
                doc=ctx.doc(spec),
                mapper=update_mapper,
            ),
            inplace=True,
        )

    if spec.supports_soft_delete():
        reg.register_many(
            {
                DocumentOperation.DELETE: lambda ctx: DeleteDocument(
                    ctx=ctx,
                    doc=ctx.doc(spec),
                ),
                DocumentOperation.RESTORE: lambda ctx: RestoreDocument(
                    ctx=ctx,
                    doc=ctx.doc(spec),
                ),
            },
            inplace=True,
        )

    return reg
