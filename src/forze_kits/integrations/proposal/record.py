"""The proposal collection — one row per proposal, its items and their verdicts (RFC 0039).

A proposal is machine-authored and inert until a human decides it. It is a batch of items,
each proposing one change with the evidence the proposer saw, and each decided on its own:
accepted, possibly with an amendment, or rejected. A single artifact is a proposal of one
item.

The item's change and evidence are the application's models, so :func:`proposal_spec` builds
the collection's models around them. The table is the application's, as the job collection's
is: it is catalogued ``KIT`` (RFC 0080 row 28), and the application migrates it.
"""

from datetime import datetime
from enum import StrEnum
from typing import Any, Generic, TypeVar, final
from uuid import UUID

import attrs
from pydantic import BaseModel, Field

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.domain.models import BaseDTO, Document, ReadDocument

# ----------------------- #

C = TypeVar("C", bound=BaseModel)
"""The application's model of what an item proposes."""

E = TypeVar("E", bound=BaseModel)
"""The application's model of the evidence an item carries."""


class ProposalStatus(StrEnum):
    """Where a proposal is in its lifecycle."""

    PENDING = "pending"
    """Requested; the proposer has not run yet, or is still running."""

    OPEN = "open"
    """Its items wait on a reviewer."""

    DECIDED = "decided"
    """Every item has a verdict. A proposal the proposer returned no items for is decided at
    once."""

    FAILED = "failed"
    """The proposer did not finish; :attr:`ProposalRecord.error` says why. Request again."""


class ItemVerdict(StrEnum):
    """A reviewer's verdict on one item."""

    PENDING = "pending"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class ProposalItem(BaseModel, Generic[C, E]):
    """One proposed change, the evidence for it, and its verdict."""

    key: str
    """The item's identifier within its proposal: what a verdict names."""

    proposed: C | None = None
    """What the proposer proposes. ``None`` for a question with no tentative answer."""

    question: str | None = None
    """What the reviewer is asked, when the proposer is not sure. A question is accepted only
    with the reviewer's answer."""

    evidence: E | None = None
    """The facts the proposer saw for this item, never a verdict."""

    reason: str = ""
    """Why the proposer proposes it, in words."""

    confidence: float | None = None
    """How sure the proposer is, from 0 to 1, when it says. It may rank a review queue; it
    never decides an item."""

    verdict: ItemVerdict = ItemVerdict.PENDING
    """The reviewer's verdict."""

    decided: C | None = None
    """What was accepted: the proposed change, or the reviewer's amendment or answer.
    ``None`` until the item is accepted."""


class _ProposalFields(BaseModel, Generic[C, E]):
    scope: str
    """What the proposal is about, chosen by the application: one pending proposal per scope."""

    base: str | None = None
    """The version of the target the proposal was made against. Deciding it after the target
    moved is refused as stale."""

    status: ProposalStatus = ProposalStatus.PENDING
    items: list[ProposalItem[C, E]] = Field(default_factory=list)
    error: str | None = None
    """Why the proposer failed, when the proposal is ``failed``."""

    model: str | None = None
    """The model the proposer called, when it called one."""

    tokens: int = 0
    """What the proposer's model call cost, in tokens."""

    latency_ms: int = 0
    """How long the proposer took."""

    decided_by: UUID | None = None
    """Who decided it: the nearest actor of a delegated call, as an audit row's actor."""

    decided_at: datetime | None = None
    note: str = ""
    """The reviewer's note on the decision."""


class ProposalDoc(_ProposalFields[C, E], Document, Generic[C, E]):
    """A proposal as the collection stores it."""


class ProposalRecord(_ProposalFields[C, E], ReadDocument, Generic[C, E]):
    """A proposal as it is read back."""


class CreateProposalCmd(_ProposalFields[C, E], BaseDTO, Generic[C, E]):
    """A new proposal."""


class UpdateProposalCmd(BaseDTO, Generic[C, E]):
    """What running and deciding a proposal changes."""

    status: ProposalStatus | None = None
    items: list[ProposalItem[C, E]] | None = None
    error: str | None = None
    model: str | None = None
    tokens: int | None = None
    latency_ms: int | None = None
    decided_by: UUID | None = None
    decided_at: datetime | None = None
    note: str | None = None


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ProposalSpec(Generic[C, E]):
    """A proposal collection over the application's change and evidence models.

    Build it with :func:`proposal_spec`; its :attr:`document` is what a document deps module
    binds under :attr:`name`.
    """

    name: str
    """The collection's route."""

    change: type[C]
    """What an item proposes."""

    evidence: type[E]
    """What an item's evidence is."""

    document: DocumentSpec[Any, Any, Any, Any] = attrs.field(
        init=False,
        default=attrs.Factory(lambda self: _document(self), takes_self=True),
    )
    """The collection's document spec."""

    # ....................... #

    @property
    def item(self) -> type[ProposalItem[C, E]]:
        """The item model over the application's models."""

        return ProposalItem[self.change, self.evidence]  # type: ignore[name-defined]

    @property
    def update_cmd(self) -> type[UpdateProposalCmd[C, E]]:
        return UpdateProposalCmd[self.change, self.evidence]  # type: ignore[name-defined]

    @property
    def create_cmd(self) -> type[CreateProposalCmd[C, E]]:
        return CreateProposalCmd[self.change, self.evidence]  # type: ignore[name-defined]


def _document(spec: ProposalSpec[Any, Any]) -> DocumentSpec[Any, Any, Any, Any]:
    # Pydantic caches each parametrization, so two specs over the same models hold the same
    # classes, and their document specs compare equal in a spec registry.
    change, evidence = spec.change, spec.evidence

    return DocumentSpec(
        name=spec.name,
        read=ProposalRecord[change, evidence],  # type: ignore[valid-type]
        write=DocumentWriteTypes(
            domain=ProposalDoc[change, evidence],  # type: ignore[valid-type]
            create_cmd=CreateProposalCmd[change, evidence],  # type: ignore[valid-type]
            update_cmd=UpdateProposalCmd[change, evidence],  # type: ignore[valid-type]
        ),
    )


def proposal_spec(name: str, *, change: type[C], evidence: type[E]) -> ProposalSpec[C, E]:
    """The proposal collection *name*, whose items propose a *change* and carry *evidence*.

    Wire :attr:`ProposalSpec.document` like any document spec, and merge
    :func:`~.inventory.proposal_spec_contributions` into the inventory.
    """

    return ProposalSpec(name=name, change=change, evidence=evidence)
