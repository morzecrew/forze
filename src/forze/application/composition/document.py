from functools import cached_property
from typing import Any, Generic, NotRequired, Optional, TypedDict, TypeVar, final

import attrs

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
from ..kernel.dependencies import ExecutionContext
from ..kernel.plan import UsecasePlan
from ..kernel.registry import UsecaseRegistry
from ..kernel.specs import DocumentSpec

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


def build_document_registry(spec: DocumentSpec[Any, Any, Any, Any]) -> UsecaseRegistry:
    """Build a usecase registry for the given document spec."""

    reg = UsecaseRegistry(
        {
            DocumentOperation.GET: lambda ctx: GetDocument(doc=ctx.doc(spec)),
            DocumentOperation.SEARCH: lambda ctx: SearchDocument(doc=ctx.doc(spec)),
            DocumentOperation.RAW_SEARCH: lambda ctx: RawSearchDocument(
                doc=ctx.doc(spec)
            ),
            DocumentOperation.CREATE: lambda ctx: CreateDocument(
                doc=ctx.doc(spec),
                txmanager=ctx.txmanager(),
                mapper=DTOMapper(dto=spec.models["create_cmd"]),
            ),
            DocumentOperation.KILL: lambda ctx: KillDocument(
                doc=ctx.doc(spec),
                txmanager=ctx.txmanager(),
            ),
        }
    )

    if spec.supports_update():
        reg.register(
            DocumentOperation.UPDATE,
            lambda ctx: UpdateDocument(
                doc=ctx.doc(spec),
                txmanager=ctx.txmanager(),
                mapper=DTOMapper(dto=spec.models["update_cmd"]),
            ),
            inplace=True,
        )

    if spec.supports_soft_delete():
        reg.register_many(
            {
                DocumentOperation.DELETE: lambda ctx: DeleteDocument(
                    doc=ctx.doc(spec),
                    txmanager=ctx.txmanager(),
                ),
                DocumentOperation.RESTORE: lambda ctx: RestoreDocument(
                    doc=ctx.doc(spec),
                    txmanager=ctx.txmanager(),
                ),
            },
            inplace=True,
        )

    return reg


# ....................... #


class DTOSpec(TypedDict, Generic[R, C, U]):
    """DTO specification for a document aggregate."""

    read: type[R]
    """Read DTO."""

    create: NotRequired[type[C]]
    """Create DTO."""

    update: NotRequired[type[U]]
    """Update DTO."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentUsecasesFacadeProvider(Generic[R, C, U]):
    """Provider of document usecases facade for a document spec."""

    spec: DocumentSpec[Any, Any, Any, Any]
    """Document spec."""

    reg: Optional[UsecaseRegistry] = None
    """Usecase registry."""

    plan: UsecasePlan = attrs.field(factory=UsecasePlan)
    """Usecase plan."""

    dtos: DTOSpec[R, C, U]
    """DTO specification."""

    # ....................... #

    @cached_property
    def _reg(self) -> UsecaseRegistry:
        reg = self.reg or build_document_registry(self.spec)

        return reg.extend_plan(self.plan)

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> DocumentUsecasesFacade[R, C, U]:
        return DocumentUsecasesFacade(ctx=ctx, reg=self._reg)
