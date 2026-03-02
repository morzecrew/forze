from functools import cached_property
from typing import Any, Generic, NotRequired, Optional, TypedDict, TypeVar, final

import attrs

from forze.domain.models import BaseDTO, ReadDocument

from ..contracts.document import DocumentSpec
from ..dto.mappers import DTOMapper
from ..execution import ExecutionContext, UsecasePlan, UsecaseRegistry
from ..facades import DocumentOperation, DocumentUsecasesFacade
from ..usecases.document import (
    CreateDocument,
    DeleteDocument,
    GetDocument,
    KillDocument,
    RawSearchDocument,
    RestoreDocument,
    SearchDocument,
    UpdateDocument,
)

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


def build_document_registry(
    spec: DocumentSpec[Any, Any, Any, Any],
) -> UsecaseRegistry:
    """Build a usecase registry for the given document spec."""

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
                mapper=DTOMapper(dto=spec.models["create_cmd"]),
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
            lambda ctx: UpdateDocument(
                ctx=ctx,
                doc=ctx.doc(spec),
                mapper=DTOMapper(dto=spec.models["update_cmd"]),
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


# ....................... #


class DocumentDTOSpec(TypedDict, Generic[R, C, U]):
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

    dtos: DocumentDTOSpec[R, C, U]
    """DTO specification."""

    # ....................... #

    @cached_property
    def _reg(self) -> UsecaseRegistry:
        reg = self.reg or build_document_registry(self.spec)

        return reg.extend_plan(self.plan)

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> DocumentUsecasesFacade[R, C, U]:
        return DocumentUsecasesFacade(ctx=ctx, reg=self._reg)
