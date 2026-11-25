"""The proposal kit: request, run and decide as operations over one collection (RFC 0039)."""

from collections.abc import Mapping
from typing import Any, Generic, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import OperationHandlerFactory
from forze.application.contracts.inventory import SpecRegistry
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.base.primitives import StrKey, StrKeyNamespace

from .handlers import DecideProposal, RequestProposal, RunProposal
from .inventory import proposal_spec_contributions
from .operations import (
    AcceptEffect,
    BaseOf,
    DecideProposalDTO,
    ProposalIdDTO,
    ProposalKernelOp,
    Proposer,
    RequestProposalDTO,
)
from .record import ProposalRecord, ProposalSpec

# ----------------------- #

C = TypeVar("C", bound=BaseModel)
E = TypeVar("E", bound=BaseModel)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ProposalKit(Generic[C, E]):
    """Machine-authored proposals that stay inert until a human decides them.

    Three operations over :attr:`spec`'s collection, under the namespace :attr:`spec`'s name
    names:

    - ``request`` creates a ``pending`` proposal for a scope, or returns the one pending there;
    - ``run`` calls :attr:`proposer` for it, from the application's worker or durable
      function, and stores its items as ``open``, or its error as ``failed``;
    - ``decide`` records a reviewer's verdicts and runs :attr:`on_accept`, in one transaction.

    There is no automatic acceptance: a confidence may rank a queue, never decide an item.
    :attr:`handlers` overrides a generated operation and :attr:`extra_ops` merges more, as
    :class:`~forze_kits.aggregates.AggregateKit`'s do.
    """

    spec: ProposalSpec[C, E]
    """The collection, from :func:`~.record.proposal_spec`."""

    proposer: Proposer
    """Proposes a request's items: a deterministic function, or one that calls a model."""

    base_of: BaseOf | None = None
    """The current version of a scope's target. A proposal is made against it and refused as
    stale when it moved. ``None`` keeps whatever base a request passes, and checks none."""

    on_accept: AcceptEffect | None = None
    """What accepting authorises, run inside the decide transaction. ``None`` records the
    verdicts alone, for an application that applies them from its own reader."""

    handlers: Mapping[StrKey, OperationHandlerFactory] = attrs.field(factory=dict)
    """Generated operations to replace, by kernel operation."""

    extra_ops: OperationRegistry | None = None
    """More operations to merge into the registry."""

    # ....................... #

    @property
    def namespace(self) -> StrKeyNamespace:
        """The namespace the operations are keyed under: the collection's name."""

        return StrKeyNamespace(prefix=self.spec.name)

    # ....................... #

    def registry(self, *, tx_route: StrKey = "default") -> FrozenOperationRegistry:
        """The kit's operations, ``decide`` in a transaction on *tx_route*, frozen."""

        ns = self.namespace
        spec = self.spec

        def _request(ctx: ExecutionContext) -> RequestProposal:
            return RequestProposal(ctx=ctx, spec=spec, base_of=self.base_of)

        def _run(ctx: ExecutionContext) -> RunProposal:
            return RunProposal(ctx=ctx, spec=spec, proposer=self.proposer)

        def _decide(ctx: ExecutionContext) -> DecideProposal:
            return DecideProposal(
                ctx=ctx, spec=spec, base_of=self.base_of, on_accept=self.on_accept
            )

        record: Any = ProposalRecord[spec.change, spec.evidence]  # type: ignore[name-defined]
        reg = OperationRegistry(
            handlers={
                ns.key(ProposalKernelOp.REQUEST): _request,
                ns.key(ProposalKernelOp.RUN): _run,
                ns.key(ProposalKernelOp.DECIDE): _decide,
            },
        ).set_descriptors(
            {
                ProposalKernelOp.REQUEST: OperationDescriptor(
                    input_type=RequestProposalDTO,
                    output_type=record,
                    description="Request a proposal for a scope; returns the one pending there.",
                ),
                ProposalKernelOp.RUN: OperationDescriptor(
                    input_type=ProposalIdDTO,
                    output_type=record,
                    description="Run the proposer for a pending proposal.",
                ),
                ProposalKernelOp.DECIDE: OperationDescriptor(
                    input_type=DecideProposalDTO[spec.change],  # type: ignore[name-defined]
                    output_type=record,
                    description=(
                        "Accept or reject a proposal's items; what is accepted is applied in "
                        "the same transaction."
                    ),
                ),
            },
            namespace=ns,
        )
        reg = (
            reg.bind(ns.key(ProposalKernelOp.DECIDE))
            .bind_tx()
            .set_route(tx_route)
            .finish(deep=True)
        )

        if self.handlers:
            reg = reg.set_handlers(dict(self.handlers), override=True, namespace=ns)

        if self.extra_ops is not None:
            reg = type(reg).merge(reg, self.extra_ops)

        return reg.freeze()

    # ....................... #

    def spec_contributions(self) -> SpecRegistry:
        """The collection, catalogued for the application's inventory (``KIT``)."""

        return proposal_spec_contributions(self.spec)
