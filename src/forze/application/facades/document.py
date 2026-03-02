from enum import StrEnum
from typing import Generic, TypeVar, final
from uuid import UUID

import attrs

from forze.domain.models import BaseDTO, ReadDocument

from ..dto.paginated import Paginated, RawPaginated
from ..execution import ExecutionContext, Usecase, UsecaseRegistry
from ..usecases.document import RawSearchArgs, SearchArgs, SoftDeleteArgs, UpdateArgs

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
#! TODO: move facades to composition for simplicity


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
