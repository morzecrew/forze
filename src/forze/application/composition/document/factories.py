from typing import Any, Optional

from forze.application.contracts.document import DocumentSpec
from forze.application.execution import UsecasePlan, UsecaseRegistry
from forze.application.mapping import DTOMapper, NumberIdStep
from forze.application.usecases.document import (
    CreateDocument,
    DeleteDocument,
    GetDocument,
    KillDocument,
    RawSearchDocument,
    RestoreDocument,
    SearchDocument,
    UpdateDocument,
)

from .operations import DocumentOperation

# ----------------------- #


def build_document_plan(
    *,
    tx_on_write: bool = True,
) -> UsecasePlan:
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
    """Build a usecase registry for the given document spec."""

    create_mapper = replace_create_mapper or DTOMapper(out=spec.models["create_cmd"])
    update_mapper = DTOMapper(out=spec.models["update_cmd"])

    reg = UsecaseRegistry(
        {
            DocumentOperation.GET: lambda ctx: GetDocument(
                ctx=ctx,
                doc=ctx.doc(spec),
            ),
            DocumentOperation.SEARCH: lambda ctx: SearchDocument(
                ctx=ctx,
                doc=ctx.doc(spec),
            ),
            DocumentOperation.RAW_SEARCH: lambda ctx: RawSearchDocument(
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
