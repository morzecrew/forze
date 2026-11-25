"""Proposals — machine-authored changes that stay inert until a human decides them (RFC 0039).

A proposer, a plain function or one that calls a model, proposes a batch of changes with the
evidence it saw; a reviewer accepts some, possibly amended, and rejects the rest; what is
accepted is applied in the same transaction as the verdicts. Nothing accepts on its own.

- :func:`proposal_spec` — the collection, over the application's change and evidence models.
- :class:`ProposalKit` — the ``request``, ``run`` and ``decide`` operations over it.
- :func:`proposal_spec_contributions` — what to merge into the application's inventory.
"""

from .inventory import proposal_spec_contributions
from .kit import ProposalKit
from .operations import (
    AcceptEffect,
    BaseOf,
    DecideProposalDTO,
    ProposalDraft,
    ProposalIdDTO,
    ProposalKernelOp,
    ProposalRequest,
    Proposer,
    RequestProposalDTO,
    VerdictDTO,
)
from .record import (
    ItemVerdict,
    ProposalItem,
    ProposalRecord,
    ProposalSpec,
    ProposalStatus,
    proposal_spec,
)

# ----------------------- #

__all__ = [
    "AcceptEffect",
    "BaseOf",
    "DecideProposalDTO",
    "ItemVerdict",
    "ProposalDraft",
    "ProposalIdDTO",
    "ProposalItem",
    "ProposalKernelOp",
    "ProposalKit",
    "ProposalRecord",
    "ProposalRequest",
    "ProposalSpec",
    "ProposalStatus",
    "Proposer",
    "RequestProposalDTO",
    "VerdictDTO",
    "proposal_spec",
    "proposal_spec_contributions",
]
