from typing import Any, Generic, NotRequired, TypedDict, TypeVar, cast, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentSpec
from forze.application.dto import (
    ListRequestDTO,
    Paginated,
    RawListRequestDTO,
    RawPaginated,
)
from forze.application.execution import Usecase
from forze.application.usecases.document import SoftDeleteArgs, UpdateArgs
from forze.domain.models import BaseDTO, ReadDocument

from ..base import BaseUsecasesFacade, BaseUsecasesFacadeProvider
from .operations import DocumentOperation

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
class DocumentUsecasesFacade(BaseUsecasesFacade, Generic[R, C, U]):
    """Typed facade for document usecases."""

    def get(self) -> Usecase[UUID, R]:
        """Return the get-document usecase."""

        return self.resolve(DocumentOperation.GET)

    # ....................... #

    def list(self) -> Usecase[ListRequestDTO, Paginated[R]]:
        """Return the list documents usecase."""

        return self.resolve(DocumentOperation.LIST)

    # ....................... #

    def raw_list(self) -> Usecase[RawListRequestDTO, RawPaginated]:
        """Return the raw list documents usecase."""

        return self.resolve(DocumentOperation.RAW_LIST)

    # ....................... #

    def create(self) -> Usecase[C, R]:
        """Return the create usecase."""

        return self.resolve(DocumentOperation.CREATE)

    # ....................... #

    def update(self) -> Usecase[UpdateArgs[U], R]:
        """Return the update usecase."""

        return self.resolve(DocumentOperation.UPDATE)

    # ....................... #

    def kill(self) -> Usecase[UUID, None]:
        """Return the hard-delete (kill) usecase."""

        return self.resolve(DocumentOperation.KILL)

    # ....................... #

    def delete(self) -> Usecase[SoftDeleteArgs, R]:
        """Return the soft-delete usecase."""

        return self.resolve(DocumentOperation.DELETE)

    # ....................... #

    def restore(self) -> Usecase[SoftDeleteArgs, R]:
        """Return the restore usecase."""

        return self.resolve(DocumentOperation.RESTORE)


# ....................... #


class DocumentDTOSpec(TypedDict, Generic[R, C, U]):
    """DTO type mapping for a document aggregate.

    Used by :class:`DocumentUsecasesFacade` and providers to type the facade
    methods. ``create`` and ``update`` are optional when the aggregate does
    not support those operations.
    """

    read: type[R]
    """Read model type (e.g. :class:`ReadDocument`)."""

    create: NotRequired[type[C]]
    """Create command type; optional when create is not supported."""

    update: NotRequired[type[U]]
    """Update command type; optional when update is not supported."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentUsecasesFacadeProvider(
    BaseUsecasesFacadeProvider[DocumentUsecasesFacade[R, C, U]],
    Generic[R, C, U],
):
    """Factory that produces a document usecases facade for a given context."""

    spec: DocumentSpec[Any, Any, Any, Any]
    """Document specification (used by registry factories)."""

    dtos: DocumentDTOSpec[R, C, U]
    """DTO type mapping for facade typing."""

    # Non initable fields
    facade: type[DocumentUsecasesFacade[R, C, U]] = attrs.field(
        default=cast(type[DocumentUsecasesFacade[R, C, U]], DocumentUsecasesFacade),
        init=False,
    )
    """Facade type to produce."""
