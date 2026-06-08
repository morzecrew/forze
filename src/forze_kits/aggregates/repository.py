"""Aggregate repository: load a domain aggregate, persist decisions command-shaped.

The functional-decider companion to the reactive event dispatch wired into the document
command flow. A handler loads the aggregate, calls a pure decision method that returns a
merge-patch (raising on invalid), and persists it under the aggregate's revision (OCC).
Domain events the aggregate emits flow in-transaction via the command port — this
repository never dispatches them itself (that would double-dispatch).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
)
from forze.base.exceptions import exc
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AggregateRepository(Generic[R, D, C, U]):
    """Aggregate-centric facade over the document query/command ports."""

    query: DocumentQueryPort[R]
    command: DocumentCommandPort[R, D, C, U]
    domain_type: type[D]

    # ....................... #

    async def load(self, pk: UUID) -> D:
        """Load the domain aggregate by id so behavior methods can run.

        Reconstructed from the read model, so the read model must carry the domain's
        fields. Behavior methods (deciders) then validate state and return a patch.
        """

        read = await self.query.get(pk)
        return self.domain_type.model_validate(read.model_dump())

    # ....................... #

    async def add(self, create: C) -> R:
        """Persist a new aggregate from a create command."""

        return await self.command.create(create)

    # ....................... #

    async def apply(self, aggregate: D, patch: U) -> R:
        """Persist a decision as a merge-patch under the aggregate's revision (OCC).

        The command flow re-applies the patch, the aggregate's ``@event_emitter`` methods
        fire, and the resulting domain events are dispatched in-transaction.
        """

        return await self.command.update(aggregate.id, aggregate.rev, patch)


# ....................... #


def aggregate_repository(
    ctx: ExecutionContext,
    spec: DocumentSpec[R, D, C, U],
) -> AggregateRepository[R, D, C, U]:
    """Build an :class:`AggregateRepository` for *spec* from the execution context."""

    write = spec.write

    if write is None:
        raise exc.configuration(
            f"Aggregate repository for {spec.name!r} requires a write spec (domain type)."
        )

    return AggregateRepository(
        query=ctx.document.query(spec),
        command=ctx.document.command(spec),
        domain_type=write["domain"],
    )
