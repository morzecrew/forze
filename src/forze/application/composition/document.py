from enum import StrEnum
from functools import cached_property
from typing import Any, Generic, NotRequired, Optional, TypedDict, TypeVar, final
from uuid import UUID

import attrs

from forze.domain.models import BaseDTO, ReadDocument

from ..contracts.document import DocumentSpec
from ..dto.mappers import DTOMapper
from ..dto.paginated import Paginated, RawPaginated
from ..execution import ExecutionContext, Usecase, UsecasePlan, UsecaseRegistry
from ..usecases.document import (
    CreateDocument,
    DeleteDocument,
    GetDocument,
    KillDocument,
    RawSearchArgs,
    RawSearchDocument,
    RestoreDocument,
    SearchArgs,
    SearchDocument,
    SoftDeleteArgs,
    UpdateArgs,
    UpdateDocument,
)

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
class DocumentOperation(StrEnum):
    """Logical operation identifiers for document usecases."""

    GET = "get"
    SEARCH = "search"
    RAW_SEARCH = "raw_search"
    CREATE = "create"
    UPDATE = "update"
    KILL = "kill"
    DELETE = "delete"
    RESTORE = "restore"


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentUsecasesFacade(Generic[R, C, U]):
    ctx: ExecutionContext
    reg: UsecaseRegistry

    # ....................... #

    def get(self) -> Usecase[UUID, R]:
        return self.reg.resolve(DocumentOperation.GET, self.ctx)

    # ....................... #

    def search(self) -> Usecase[SearchArgs, Paginated[R]]:
        return self.reg.resolve(DocumentOperation.SEARCH, self.ctx)

    # ....................... #

    def raw_search(self) -> Usecase[RawSearchArgs, RawPaginated]:
        return self.reg.resolve(DocumentOperation.RAW_SEARCH, self.ctx)

    # ....................... #

    def create(self) -> Usecase[C, R]:
        return self.reg.resolve(DocumentOperation.CREATE, self.ctx)

    # ....................... #

    def update(self) -> Usecase[UpdateArgs[U], R]:
        return self.reg.resolve(DocumentOperation.UPDATE, self.ctx)

    # ....................... #

    def kill(self) -> Usecase[UUID, None]:
        return self.reg.resolve(DocumentOperation.KILL, self.ctx)

    # ....................... #

    def delete(self) -> Usecase[SoftDeleteArgs, R]:
        return self.reg.resolve(DocumentOperation.DELETE, self.ctx)

    # ....................... #

    def restore(self) -> Usecase[SoftDeleteArgs, R]:
        return self.reg.resolve(DocumentOperation.RESTORE, self.ctx)


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
