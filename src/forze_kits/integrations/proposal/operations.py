"""The proposal kit's operations, and what their handlers take."""

from collections.abc import Awaitable, Callable, Sequence
from enum import StrEnum
from typing import Any, Generic, TypeVar, final
from uuid import UUID

import attrs
from pydantic import BaseModel, Field

from forze.application.execution import ExecutionContext
from forze.domain.models import BaseDTO

from .record import ProposalItem, ProposalRecord

# ----------------------- #

C = TypeVar("C", bound=BaseModel)
E = TypeVar("E", bound=BaseModel)


class ProposalKernelOp(StrEnum):
    """The kit's operations, keyed under the kit's namespace."""

    REQUEST = "request"
    """Create a ``pending`` proposal for a scope and base, or return the one already pending
    there."""

    RUN = "run"
    """Run the proposer for a ``pending`` proposal: ``open`` with its items, or ``failed``."""

    DECIDE = "decide"
    """Record a reviewer's verdicts and run the accept effect, in one transaction."""


# ....................... #


class RequestProposalDTO(BaseDTO):
    scope: str
    """What the proposal is about; one pending proposal per scope and base."""

    base: str | None = None
    """The target version it is made against. Omitted, the kit's ``base_of`` supplies it."""


class ProposalIdDTO(BaseDTO):
    id: UUID


class VerdictDTO(BaseDTO, Generic[C]):
    key: str
    """The item it decides."""

    accept: bool
    change: C | None = None
    """Replaces what was proposed. Required to accept a question, or an item that proposes
    nothing."""


class DecideProposalDTO(BaseDTO, Generic[C]):
    id: UUID
    verdicts: list[VerdictDTO[C]] = Field(default_factory=list)
    """Items left out are rejected; none at all rejects the whole proposal."""

    note: str = ""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ProposalRequest:
    """What the proposer is asked to propose for."""

    id: UUID
    """The proposal being filled."""

    scope: str
    base: str | None


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ProposalDraft(Generic[C, E]):
    """What a proposer returns: the items, and what producing them cost."""

    items: Sequence[ProposalItem[C, E]]
    """Each with a key unique in the proposal, and a proposed change, a question, or both.
    Their verdicts are ignored: every item starts ``pending``."""

    model: str | None = None
    tokens: int = 0
    latency_ms: int = 0


type Proposer[C: BaseModel, E: BaseModel] = Callable[
    [ExecutionContext, ProposalRequest], Awaitable[ProposalDraft[C, E]]
]
"""Proposes the items for one request: a plain function, or one that calls a model. It runs
outside any request, so a model call may take as long as it needs. An exception fails the
proposal, with its message as the error."""

type Dispatch = Callable[[ExecutionContext, ProposalRecord[Any, Any]], Awaitable[None]]
"""Schedules ``run`` for a proposal ``request`` just created, inside the request's transaction:
a durable enqueue that joins it, or an event on the transactional outbox. A reused proposal is
never dispatched again, so the first dispatch must commit with it."""

type BaseOf = Callable[[ExecutionContext, str], Awaitable[str | None]]
"""The current version of a scope's target, which a proposal is made against and checked
against when it is decided."""

type AcceptEffect = Callable[
    [ExecutionContext, ProposalRecord[Any, Any], Sequence[ProposalItem[Any, Any]]],
    Awaitable[None],
]
"""What accepting authorises, run inside the decide transaction with the decided proposal
and its accepted items: it lands with the verdicts, or neither does, and it does not run when
no item is accepted. Write a long-running job's queue entry from it, through a transactional
outbox or a durable enqueue that joins the transaction, rather than running the job here. Write
the target conditionally on ``proposal.base``: the kit's stale check reads before the effect
writes, so two open proposals on one base can both pass it."""
