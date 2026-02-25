from enum import StrEnum
from typing import Generic, TypeVar, final
from uuid import UUID

import attrs

from forze.application.dto.paginated import Paginated, RawPaginated
from forze.application.usecases.document import (
    RawSearchArgs,
    SearchArgs,
    SoftDeleteArgs,
    UpdateArgs,
)
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, ReadDocument

from ..kernel.dependencies import UsecaseContext
from ..kernel.registry import UsecaseRegistry
from ..kernel.usecase import TxUsecase, Usecase

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
class DocumentOperation(StrEnum):
    """Logical operation identifiers for document usecases.

    The values correspond to operation keys used by :class:`DocumentUsecasesFacade`
    and the underlying :class:`forze.application.kernel.registry.UsecaseRegistry`
    when resolving concrete usecase instances.
    """

    GET = "get"
    SEARCH = "search"
    RAW_SEARCH = "raw_search"
    CREATE = "create"
    UPDATE = "update"
    KILL = "kill"
    DELETE = "delete"
    RESTORE = "restore"


# ....................... #


#!? Should we make it final? Or allow subclassing?
@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentUsecasesFacade(Generic[R, C, U]):
    ctx: UsecaseContext
    reg: UsecaseRegistry

    # ....................... #

    def get(self) -> Usecase[UUID, R]:
        return self.reg.resolve(
            DocumentOperation.GET,
            self.ctx,
            expected=Usecase[UUID, R],
        )

    # ....................... #

    def search(self) -> Usecase[SearchArgs, Paginated[R]]:
        return self.reg.resolve(
            DocumentOperation.SEARCH,
            self.ctx,
            expected=Usecase[SearchArgs, Paginated[R]],
        )

    # ....................... #

    def raw_search(self) -> Usecase[RawSearchArgs, RawPaginated]:
        return self.reg.resolve(
            DocumentOperation.RAW_SEARCH,
            self.ctx,
            expected=Usecase[RawSearchArgs, RawPaginated],
        )

    # ....................... #

    def create(self) -> TxUsecase[C, R]:
        return self.reg.resolve(
            DocumentOperation.CREATE,
            self.ctx,
            expected=TxUsecase[C, R],
        )

    # ....................... #

    def update(self) -> TxUsecase[UpdateArgs[U], R]:
        if not self.reg.exists(DocumentOperation.UPDATE):
            raise CoreError("Update operation is not supported for this document")

        return self.reg.resolve(
            DocumentOperation.UPDATE,
            self.ctx,
            expected=TxUsecase[UpdateArgs[U], R],
        )

    # ....................... #

    def kill(self) -> TxUsecase[UUID, None]:
        return self.reg.resolve(
            DocumentOperation.KILL,
            self.ctx,
            expected=TxUsecase[UUID, None],
        )

    # ....................... #

    def delete(self) -> TxUsecase[SoftDeleteArgs, R]:
        if not self.reg.exists(DocumentOperation.DELETE):
            raise CoreError("Delete operation is not supported for this document")

        return self.reg.resolve(
            DocumentOperation.DELETE,
            self.ctx,
            expected=TxUsecase[SoftDeleteArgs, R],
        )

    # ....................... #

    def restore(self) -> TxUsecase[SoftDeleteArgs, R]:
        if not self.reg.exists(DocumentOperation.RESTORE):
            raise CoreError("Restore operation is not supported for this document")

        return self.reg.resolve(
            DocumentOperation.RESTORE,
            self.ctx,
            expected=TxUsecase[SoftDeleteArgs, R],
        )
