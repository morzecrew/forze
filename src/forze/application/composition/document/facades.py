"""Document usecases facades and DTO specifications.

Provides :class:`DocumentUsecasesFacade` (resolved usecases per operation),
:class:`DocumentDTOSpec` (typed DTO mapping), and
:class:`DocumentUsecasesFacadeProvider` (factory that merges plan and
registry to produce a facade).
"""

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
    """Resolved document usecases for a given execution context.

    Provides accessors for each document operation (get, search, create, etc.).
    Each method returns a composed usecase from the registry. Type parameters
    ``R`` (read), ``C`` (create), ``U`` (update) align with
    :class:`DocumentDTOSpec`.
    """

    ctx: ExecutionContext
    """Execution context for resolving usecases."""

    reg: UsecaseRegistry
    """Registry with plan merged; used to resolve usecases."""

    # ....................... #

    def get(self) -> Usecase[UUID, R]:
        """Return the get-document usecase."""
        return self.reg.resolve(DocumentOperation.GET, self.ctx)

    # ....................... #

    def search(self) -> Usecase[SearchArgs, Paginated[R]]:
        """Return the typed search usecase."""
        return self.reg.resolve(DocumentOperation.SEARCH, self.ctx)

    # ....................... #

    def raw_search(self) -> Usecase[RawSearchArgs, RawPaginated]:
        """Return the raw (field-projected) search usecase."""
        return self.reg.resolve(DocumentOperation.RAW_SEARCH, self.ctx)

    # ....................... #

    def create(self) -> Usecase[C, R]:
        """Return the create usecase."""
        return self.reg.resolve(DocumentOperation.CREATE, self.ctx)

    # ....................... #

    def update(self) -> Usecase[UpdateArgs[U], R]:
        """Return the update usecase."""
        return self.reg.resolve(DocumentOperation.UPDATE, self.ctx)

    # ....................... #

    def kill(self) -> Usecase[UUID, None]:
        """Return the hard-delete (kill) usecase."""
        return self.reg.resolve(DocumentOperation.KILL, self.ctx)

    # ....................... #

    def delete(self) -> Usecase[SoftDeleteArgs, R]:
        """Return the soft-delete usecase."""
        return self.reg.resolve(DocumentOperation.DELETE, self.ctx)

    # ....................... #

    def restore(self) -> Usecase[SoftDeleteArgs, R]:
        """Return the restore usecase."""
        return self.reg.resolve(DocumentOperation.RESTORE, self.ctx)


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
class DocumentUsecasesFacadeProvider(Generic[R, C, U]):
    """Factory that produces a document usecases facade for a given context.

    Merges :attr:`plan` into the registry and returns a facade bound to the
    context. Used when the same registry/plan pair is shared across requests
    but each request has its own context.
    """

    spec: DocumentSpec[Any, Any, Any, Any]
    """Document specification (used by registry factories)."""

    reg: UsecaseRegistry
    """Base usecase registry."""

    plan: UsecasePlan
    """Plan to merge into the registry when building the facade."""

    dtos: DocumentDTOSpec[R, C, U]
    """DTO type mapping for facade typing."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> DocumentUsecasesFacade[R, C, U]:
        """Build a facade for the given execution context.

        :param ctx: Execution context for resolving usecases.
        :returns: Facade with resolved usecases.
        """
        reg = self.reg.extend_plan(self.plan)

        return DocumentUsecasesFacade(ctx=ctx, reg=reg)
