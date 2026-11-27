"""The handlers behind the proposal kit's operations.

Acceptance needs a principal, and that is enforced here, in the decide handler: the
collection is an ordinary document, so a port could not stop a sibling document port from
flipping a status. What the kit can promise is that its own operations never
accept without one, and never automatically.
"""

from collections import Counter
from collections.abc import Sequence
from typing import Any, Final, final

import attrs

from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import StrKey, utcnow

from .operations import (
    AcceptEffect,
    BaseOf,
    DecideProposalDTO,
    Dispatch,
    ProposalIdDTO,
    ProposalRequest,
    Proposer,
    RequestProposalDTO,
)
from .record import ItemVerdict, ProposalItem, ProposalRecord, ProposalSpec, ProposalStatus

# ----------------------- #

_ERROR_CHARS: Final = 2000
"""The tail of a proposer's error a failed proposal keeps."""


def _query(ctx: ExecutionContext, spec: ProposalSpec[Any, Any]) -> DocumentQueryPort[Any]:
    return ctx.document.query(spec.document)


def _command(
    ctx: ExecutionContext, spec: ProposalSpec[Any, Any]
) -> DocumentCommandPort[Any, Any, Any, Any]:
    return ctx.document.command(spec.document)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RequestProposal:
    """Create a ``pending`` proposal for a scope and base, or return the one already pending
    there. A request against a newer base gets a new proposal."""

    ctx: ExecutionContext
    spec: ProposalSpec[Any, Any]
    base_of: BaseOf | None = None
    dispatch: Dispatch | None = None

    # ....................... #

    async def __call__(self, args: RequestProposalDTO) -> ProposalRecord[Any, Any]:
        base = args.base

        if base is None and self.base_of is not None:
            base = await self.base_of(self.ctx, args.scope)

        # Only a pending proposal on the same base is this request's: one on an older base is
        # already stale, and returning it would hand back a proposal decide refuses.
        same_base: Any = {"$null": True} if base is None else base
        pending = await _query(self.ctx, self.spec).find_many(
            filters={
                "$values": {
                    "status": ProposalStatus.PENDING,
                    "scope": args.scope,
                    "base": same_base,
                }
            },
            pagination={"limit": 1},
        )

        if pending.hits:
            return pending.hits[0]

        created = await _command(self.ctx, self.spec).create(
            self.spec.create_cmd(scope=args.scope, base=base, status=ProposalStatus.PENDING)
        )

        if self.dispatch is not None:
            await self.dispatch(self.ctx, created)

        return created


# ....................... #


def _checked(items: Sequence[ProposalItem[Any, Any]]) -> list[ProposalItem[Any, Any]]:
    """*items* as a proposal stores them: every verdict ``pending`` and no decided value.

    :raises CoreException: ``validation``, code ``proposal_items_invalid``, when two items
        share a key, or an item proposes nothing and asks nothing.
    """

    repeated = sorted(key for key, n in Counter(item.key for item in items).items() if n > 1)

    if repeated:
        raise exc.validation(
            f"The proposer returned the item key(s) {repeated} more than once; a verdict names "
            "one item by its key.",
            code="proposal_items_invalid",
        )

    if empty := sorted(i.key for i in items if i.proposed is None and i.question is None):
        raise exc.validation(
            f"The proposer returned item(s) {empty} that propose nothing and ask nothing.",
            code="proposal_items_invalid",
        )

    return [
        item.model_copy(update={"verdict": ItemVerdict.PENDING, "decided": None}) for item in items
    ]


_DRAFT_REFUSALS: Final = frozenset({ExceptionKind.VALIDATION, ExceptionKind.PRECONDITION})
"""What a store raises for a value it cannot keep, rather than for a race or an outage."""


def _failed(spec: ProposalSpec[Any, Any], error: Exception) -> Any:
    """The update that fails a proposal with *error*'s message."""

    return spec.update_cmd(
        status=ProposalStatus.FAILED,
        error=(str(error) or type(error).__name__)[-_ERROR_CHARS:],
    )


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RunProposal:
    """Run the proposer for a ``pending`` proposal and store what it returned.

    A proposal that is no longer ``pending`` is returned as it is, so a retried worker is a
    no-op. An exception from the proposer, or items it should not have returned, fail the
    proposal with the error; the caller requests a new one. The draft is written in a scope
    on :attr:`tx_route`, a savepoint inside a caller's transaction, so a draft the store
    refuses there still fails the proposal.
    """

    ctx: ExecutionContext
    spec: ProposalSpec[Any, Any]
    proposer: Proposer[Any, Any]
    tx_route: StrKey = "default"
    """The route the draft's write scope opens on."""

    # ....................... #

    async def __call__(self, args: ProposalIdDTO) -> ProposalRecord[Any, Any]:
        proposal = await _query(self.ctx, self.spec).get(args.id)

        if proposal.status is not ProposalStatus.PENDING:
            return proposal

        request = ProposalRequest(id=proposal.id, scope=proposal.scope, base=proposal.base)
        command = _command(self.ctx, self.spec)

        # Building the update is inside the try: it is what checks an item against the
        # collection's models, and a draft that fails it must fail, not stay pending.
        try:
            draft = await self.proposer(self.ctx, request)
            items = _checked(draft.items)
            update = self.spec.update_cmd(
                status=ProposalStatus.OPEN if items else ProposalStatus.DECIDED,
                items=items,
                model=draft.model,
                tokens=draft.tokens,
                latency_ms=draft.latency_ms,
            )
            # Through JSON as a store keeps it, derived fields dropped: a NaN passes the
            # models and reads back null, which only a required field then refuses. Fields
            # are compared, not models: private state is not stored.
            stored = update.model_dump_json(exclude_computed_fields=True)
            read = type(update).model_validate_json(stored)

            if read.model_dump(exclude_computed_fields=True) != update.model_dump(
                exclude_computed_fields=True
            ):
                raise ValueError(
                    "The draft does not read back as it was made: a NaN or an infinity "
                    "is stored as null."
                )

        except Exception as error:
            return await command.update(proposal.id, proposal.rev, _failed(self.spec, error))

        try:
            # A refusal aborts the transaction it lands in: in a caller's, as an inbox
            # consumer's, only this savepoint, so the failure below can still be written.
            async with self.ctx.tx_ctx.scope(self.tx_route):
                return await command.update(proposal.id, proposal.rev, update)

        except CoreException as error:
            # A store that refuses the draft's values, as Postgres a NUL byte in jsonb, fails
            # the proposal; a lost race or an unreachable store is the caller's to retry.
            if error.kind not in _DRAFT_REFUSALS or error.code == "revision_mismatch":
                raise

            return await command.update(proposal.id, proposal.rev, _failed(self.spec, error))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DecideProposal:
    """Record a reviewer's verdicts on an ``open`` proposal, and run the accept effect.

    The operation runs in one transaction, so the effect and the verdicts land together or
    neither does. The update is made at the revision read, so the second of two concurrent
    decisions fails the store's revision check, whose code the store and its history settings
    decide. Only a principal acting for itself decides: an agent acting for one is refused.

    :raises CoreException: ``authentication``, code ``proposal_principal_required``, with no
        authenticated principal; ``authorization``, code ``proposal_human_required``, for a
        delegated call; ``conflict``, codes ``proposal_pending``,
        ``proposal_decided`` and ``proposal_stale``; ``validation``, codes
        ``proposal_verdict_unknown``, ``proposal_verdict_repeated`` and
        ``proposal_answer_required``.
    """

    ctx: ExecutionContext
    spec: ProposalSpec[Any, Any]
    base_of: BaseOf | None = None
    on_accept: AcceptEffect | None = None

    # ....................... #

    async def __call__(self, args: DecideProposalDTO[Any]) -> ProposalRecord[Any, Any]:
        identity = self.ctx.inv_ctx.get_authn()

        if identity is None:
            raise exc.authentication(
                "Deciding a proposal needs an authenticated principal: nothing accepts one "
                "on its own.",
                code="proposal_principal_required",
            )

        # Inert until a human decides: an agent acting for one would accept on its own say.
        if identity.is_delegated:
            raise exc.authorization(
                "A proposal is decided by a principal acting for itself, not by an agent "
                "acting for one.",
                code="proposal_human_required",
            )

        proposal = await _query(self.ctx, self.spec).get(args.id)

        if proposal.status is ProposalStatus.PENDING:
            raise exc.conflict(
                "The proposer has not finished this proposal yet.", code="proposal_pending"
            )

        if proposal.status is not ProposalStatus.OPEN:
            raise exc.conflict("This proposal is already decided.", code="proposal_decided")

        if self.base_of is not None:
            current = await self.base_of(self.ctx, proposal.scope)

            if current != proposal.base:
                raise exc.conflict(
                    "What this proposal was made against has changed since: request a new one.",
                    code="proposal_stale",
                )

        items = self._decided(proposal, args)
        decided = await _command(self.ctx, self.spec).update(
            proposal.id,
            proposal.rev,
            self.spec.update_cmd(
                status=ProposalStatus.DECIDED,
                items=items,
                decided_by=identity.principal_id,
                decided_at=utcnow(),
                note=args.note,
            ),
        )
        accepted = [item for item in decided.items if item.verdict is ItemVerdict.ACCEPTED]

        if accepted and self.on_accept is not None:
            await self.on_accept(self.ctx, decided, accepted)

        return decided

    # ....................... #

    def _decided(
        self,
        proposal: ProposalRecord[Any, Any],
        args: DecideProposalDTO[Any],
    ) -> list[ProposalItem[Any, Any]]:
        keys = Counter(verdict.key for verdict in args.verdicts)

        if repeated := sorted(key for key, n in keys.items() if n > 1):
            raise exc.validation(
                f"The verdicts name item(s) {repeated} more than once; give each item one.",
                code="proposal_verdict_repeated",
            )

        verdicts = {verdict.key: verdict for verdict in args.verdicts}

        if unknown := sorted(set(verdicts) - {item.key for item in proposal.items}):
            raise exc.validation(
                f"The verdicts name item(s) {unknown} this proposal does not have.",
                code="proposal_verdict_unknown",
            )

        items: list[ProposalItem[Any, Any]] = []

        for item in proposal.items:
            verdict = verdicts.get(item.key)

            if verdict is None or not verdict.accept:
                items.append(item.model_copy(update={"verdict": ItemVerdict.REJECTED}))
                continue

            # A question is answered by the reviewer, never by the proposer's tentative guess.
            chosen = verdict.change

            if chosen is None and item.question is None:
                chosen = item.proposed

            if chosen is None:
                raise exc.validation(
                    f"Item {item.key!r} asks a question or proposes nothing: accept it with "
                    "the answer as its change.",
                    code="proposal_answer_required",
                )

            items.append(
                item.model_copy(
                    update={
                        "verdict": ItemVerdict.ACCEPTED,
                        "decided": self.spec.change.model_validate(chosen, from_attributes=True),
                    }
                )
            )

        return items
