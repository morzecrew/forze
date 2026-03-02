from typing import Any, Generic, NotRequired, TypedDict, TypeVar, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentSpec
from forze.application.dto import Paginated, RawPaginated
from forze.application.execution import (
    ExecutionContext,
    Usecase,
    UsecasePlan,
    UsecaseRegistry,
)
from forze.application.usecases.document import (
    RawSearchArgs,
    SearchArgs,
    SoftDeleteArgs,
    UpdateArgs,
)
from forze.domain.models import BaseDTO, ReadDocument

from .operations import DocumentOperation

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

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

    reg: UsecaseRegistry
    """Usecase registry."""

    plan: UsecasePlan
    """Usecase plan."""

    dtos: DocumentDTOSpec[R, C, U]
    """DTO specification."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> DocumentUsecasesFacade[R, C, U]:
        reg = self.reg.extend_plan(self.plan)

        return DocumentUsecasesFacade(ctx=ctx, reg=reg)
