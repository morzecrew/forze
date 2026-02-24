from typing import Generic, TypeVar
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


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentUsecasesFacade(Generic[R, C, U]):
    ctx: UsecaseContext
    reg: UsecaseRegistry

    # ....................... #

    def get(self) -> Usecase[UUID, R]:
        return self.reg.resolve(
            "get",
            self.ctx,
            expected=Usecase[UUID, R],
        )

    # ....................... #

    def search(self) -> Usecase[SearchArgs, Paginated[R]]:
        return self.reg.resolve(
            "search",
            self.ctx,
            expected=Usecase[SearchArgs, Paginated[R]],
        )

    # ....................... #

    def raw_search(self) -> Usecase[RawSearchArgs, RawPaginated]:
        return self.reg.resolve(
            "raw_search",
            self.ctx,
            expected=Usecase[RawSearchArgs, RawPaginated],
        )

    # ....................... #

    def create(self) -> TxUsecase[C, R]:
        return self.reg.resolve(
            "create",
            self.ctx,
            expected=TxUsecase[C, R],
        )

    # ....................... #

    def update(self) -> TxUsecase[UpdateArgs[U], R]:
        if not self.reg.exists("update"):
            raise CoreError("Update operation is not supported for this document")

        return self.reg.resolve(
            "update",
            self.ctx,
            expected=TxUsecase[UpdateArgs[U], R],
        )

    # ....................... #

    def kill(self) -> TxUsecase[UUID, None]:
        return self.reg.resolve(
            "kill",
            self.ctx,
            expected=TxUsecase[UUID, None],
        )

    # ....................... #

    def delete(self) -> TxUsecase[SoftDeleteArgs, R]:
        if not self.reg.exists("delete"):
            raise CoreError("Delete operation is not supported for this document")

        return self.reg.resolve(
            "delete",
            self.ctx,
            expected=TxUsecase[SoftDeleteArgs, R],
        )

    # ....................... #

    def restore(self) -> TxUsecase[SoftDeleteArgs, R]:
        if not self.reg.exists("restore"):
            raise CoreError("Restore operation is not supported for this document")

        return self.reg.resolve(
            "restore",
            self.ctx,
            expected=TxUsecase[SoftDeleteArgs, R],
        )
