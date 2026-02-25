from typing import Any, Callable, TypeVar

from forze.application.dto.mappers import DTOMapper
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
from forze.domain.models import BaseDTO, ReadDocument

from ..facades import DocumentOperation, DocumentUsecasesFacade
from ..kernel.dependencies import UsecaseContext
from ..kernel.plan import UsecasePlan
from ..kernel.registry import UsecaseRegistry
from ..kernel.specs import DocumentSpec

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


def build_document_registry(spec: DocumentSpec[Any, Any, Any, Any]) -> UsecaseRegistry:
    reg = UsecaseRegistry(
        {
            DocumentOperation.GET: lambda ctx: GetDocument(
                doc=ctx.doc(spec),
                runtime=ctx.runtime,
            ),
            DocumentOperation.SEARCH: lambda ctx: SearchDocument(
                doc=ctx.doc(spec),
                runtime=ctx.runtime,
            ),
            DocumentOperation.RAW_SEARCH: lambda ctx: RawSearchDocument(
                doc=ctx.doc(spec),
                runtime=ctx.runtime,
            ),
            DocumentOperation.CREATE: lambda ctx: CreateDocument(
                doc=ctx.doc(spec),
                runtime=ctx.runtime,
                mapper=DTOMapper(dto=spec.models["create_cmd"]),
            ),
            DocumentOperation.KILL: lambda ctx: KillDocument(
                doc=ctx.doc(spec),
                runtime=ctx.runtime,
            ),
        }
    )

    if spec.supports_update():
        reg.register(
            DocumentOperation.UPDATE,
            lambda ctx: UpdateDocument(
                doc=ctx.doc(spec),
                runtime=ctx.runtime,
                mapper=DTOMapper(dto=spec.models["update_cmd"]),
            ),
            inplace=True,
        )

    if spec.supports_soft_delete():
        reg.register_many(
            {
                DocumentOperation.DELETE: lambda ctx: DeleteDocument(
                    doc=ctx.doc(spec),
                    runtime=ctx.runtime,
                ),
                DocumentOperation.RESTORE: lambda ctx: RestoreDocument(
                    doc=ctx.doc(spec),
                    runtime=ctx.runtime,
                ),
            },
            inplace=True,
        )

    return reg


# ....................... #


def build_document_facade(
    spec: DocumentSpec[Any, Any, Any, Any],
    reg_builder: Callable[
        [DocumentSpec[Any, Any, Any, Any]],
        UsecaseRegistry,
    ] = build_document_registry,
    plan_builder: Callable[[], UsecasePlan] = lambda: UsecasePlan(),
) -> Callable[[UsecaseContext], DocumentUsecasesFacade[R, C, U]]:
    base_reg = reg_builder(spec)

    def factory(ctx: UsecaseContext) -> DocumentUsecasesFacade[R, C, U]:
        reg = base_reg.extend_plan(plan_builder())
        return DocumentUsecasesFacade[R, C, U](ctx=ctx, reg=reg)

    return factory
