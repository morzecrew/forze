"""The proposal kit: request, run and decide over one collection.

The property the kit exists for: a proposal that is not accepted changes nothing
downstream. The accept effect here writes a document, so every leg can count what it wrote,
and the legs that must not write assert the count stayed at zero.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any, Final
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.inference import InferenceSpec
from forze.application.contracts.inventory import SpecPlane, SpecSource
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze.testing import context_from_modules
from forze_kits.integrations.proposal import (
    DecideProposalDTO,
    ItemVerdict,
    ProposalDraft,
    ProposalIdDTO,
    ProposalItem,
    ProposalKit,
    ProposalRecord,
    ProposalRequest,
    ProposalStatus,
    RequestProposalDTO,
    VerdictDTO,
    proposal_spec,
)
from forze_mock import MockDepsModule, MockInferenceRegistry
from forze_mock.adapters.document import MockDocumentAdapter

pytestmark = pytest.mark.unit

# ----------------------- #


class Mapping(BaseModel):
    """What an item proposes: map a source path to a field."""

    field: str


class Seen(BaseModel):
    """What the proposer saw for a path."""

    filled: float


SPEC = proposal_spec("mappings", change=Mapping, evidence=Seen)
USER: Final = AuthnIdentity(principal_id=uuid4())


class _Applied(Document):
    field: str


class _AppliedCreate(BaseDTO):
    field: str


class _AppliedRead(ReadDocument):
    field: str


APPLIED = DocumentSpec(
    name="applied",
    read=_AppliedRead,
    write=DocumentWriteTypes(domain=_Applied, create_cmd=_AppliedCreate),
)
"""Downstream state: the accept effect writes one row per accepted item."""


def _item(key: str, field: str | None = "f", *, question: str | None = None) -> Any:
    return SPEC.item(
        key=key,
        proposed=Mapping(field=field) if field is not None else None,
        question=question,
        evidence=Seen(filled=0.9),
        confidence=0.8,
    )


def _proposer(*items: Any, model: str | None = "m-1") -> Any:
    calls: list[ProposalRequest] = []

    async def propose(ctx: ExecutionContext, request: ProposalRequest) -> ProposalDraft[Any, Any]:
        calls.append(request)

        return ProposalDraft(items=list(items), model=model, tokens=42, latency_ms=7)

    propose.calls = calls  # type: ignore[attr-defined]

    return propose


async def _apply(
    ctx: ExecutionContext,
    proposal: ProposalRecord[Any, Any],
    accepted: Sequence[ProposalItem[Any, Any]],
) -> None:
    for item in accepted:
        await ctx.document.command(APPLIED).create(_AppliedCreate(field=item.decided.field))


def _kit(proposer: Any, **kwargs: Any) -> ProposalKit[Mapping, Seen]:
    return ProposalKit(spec=SPEC, proposer=proposer, on_accept=_apply, **kwargs)


def _ctx() -> ExecutionContext:
    return context_from_modules(MockDepsModule())


async def _op(
    kit: ProposalKit[Any, Any],
    op: str,
    args: Any,
    ctx: ExecutionContext,
    identity: AuthnIdentity | None = USER,
) -> Any:
    reg = kit.registry(tx_route="mock")

    with ctx.inv_ctx.bind_identity(authn=identity):
        return await run_operation(reg, f"{SPEC.name}.{op}", args, ctx)


async def _applied(ctx: ExecutionContext) -> list[str]:
    return [row.field for row in (await ctx.document.query(APPLIED).find_many()).hits]


async def _open(kit: ProposalKit[Any, Any], ctx: ExecutionContext, scope: str = "s") -> Any:
    requested = await _op(kit, "request", RequestProposalDTO(scope=scope, base="r1"), ctx)

    return await _op(kit, "run", ProposalIdDTO(id=requested.id), ctx)


def _decide(proposal: Any, *verdicts: VerdictDTO[Any], note: str = "") -> DecideProposalDTO[Any]:
    return DecideProposalDTO[Mapping](id=proposal.id, verdicts=list(verdicts), note=note)


# ....................... #


class TestTheCollection:
    def test_two_specs_over_the_same_models_are_one_spec(self) -> None:
        again = proposal_spec("mappings", change=Mapping, evidence=Seen)

        assert again.document == SPEC.document

    def test_it_is_catalogued_as_the_kits(self) -> None:
        # Its items are the application's models, so its table is the application's.
        entries = _kit(_proposer()).spec_contributions().freeze().of_plane(SpecPlane.DOCUMENT)

        assert [(e.name, e.source) for e in entries] == [("mappings", SpecSource.KIT)]


class TestRequestAndRun:
    async def test_a_request_is_pending_until_the_proposer_runs(self) -> None:
        ctx = _ctx()
        proposer = _proposer(_item("a"), _item("b"))
        kit = _kit(proposer)

        requested = await _op(kit, "request", RequestProposalDTO(scope="s", base="r1"), ctx)
        ran = await _op(kit, "run", ProposalIdDTO(id=requested.id), ctx)

        assert requested.status is ProposalStatus.PENDING
        assert (ran.status, [i.key for i in ran.items]) == (ProposalStatus.OPEN, ["a", "b"])
        assert (ran.model, ran.tokens, ran.latency_ms, ran.base) == ("m-1", 42, 7, "r1")
        assert [(c.scope, c.base) for c in proposer.calls] == [("s", "r1")]

    async def test_a_second_request_for_a_pending_scope_returns_the_first(self) -> None:
        ctx = _ctx()
        kit = _kit(_proposer(_item("a")))

        first = await _op(kit, "request", RequestProposalDTO(scope="s"), ctx)
        again = await _op(kit, "request", RequestProposalDTO(scope="s"), ctx)
        other = await _op(kit, "request", RequestProposalDTO(scope="t"), ctx)

        assert again.id == first.id
        assert other.id != first.id

    async def test_a_request_against_a_newer_base_gets_a_new_proposal(self) -> None:
        # The pending one is already stale: returning it would hand back a proposal that
        # decide refuses.
        ctx = _ctx()
        kit = _kit(_proposer(_item("a")))

        old = await _op(kit, "request", RequestProposalDTO(scope="s", base="r1"), ctx)
        same = await _op(kit, "request", RequestProposalDTO(scope="s", base="r1"), ctx)
        newer = await _op(kit, "request", RequestProposalDTO(scope="s", base="r2"), ctx)
        unbased = await _op(kit, "request", RequestProposalDTO(scope="s"), ctx)
        unbased_again = await _op(kit, "request", RequestProposalDTO(scope="s"), ctx)

        assert same.id == old.id
        assert newer.id != old.id
        assert newer.base == "r2"
        assert unbased.id not in (old.id, newer.id)
        assert unbased_again.id == unbased.id

    async def test_concurrent_requests_leave_one_proposal_pending(self) -> None:
        # Both find nothing pending before either commits; only the collection's uniqueness
        # over pending proposals stops the second create. Dispatch holds each request open
        # until both have created theirs.
        ctx = _ctx()
        both_created = asyncio.Barrier(2)

        async def dispatch(ctx: ExecutionContext, proposal: Any) -> None:
            await both_created.wait()

        kit = _kit(_proposer(_item("a")), dispatch=dispatch)
        args = RequestProposalDTO(scope="s", base="r1")
        outcomes = await asyncio.gather(
            _op(kit, "request", args, ctx), _op(kit, "request", args, ctx), return_exceptions=True
        )

        won = [o for o in outcomes if not isinstance(o, BaseException)]
        lost = [o for o in outcomes if isinstance(o, CoreException)]

        assert len(won) == 1
        assert [e.kind for e in lost] == [ExceptionKind.CONFLICT]
        assert await ctx.document.query(SPEC.document).count() == 1
        assert (await _op(kit, "request", args, ctx)).id == won[0].id

    async def test_a_new_proposal_dispatches_its_run_in_the_requests_transaction(self) -> None:
        # A reused proposal is never re-dispatched, so the first dispatch has to commit with it.
        ctx = _ctx()
        dispatched: list[Any] = []

        async def dispatch(ctx: ExecutionContext, proposal: Any) -> None:
            dispatched.append(proposal.id)

        kit = _kit(_proposer(_item("a")), dispatch=dispatch)
        first = await _op(kit, "request", RequestProposalDTO(scope="s", base="r1"), ctx)
        await _op(kit, "request", RequestProposalDTO(scope="s", base="r1"), ctx)

        assert dispatched == [first.id]

    async def test_a_dispatch_that_fails_leaves_no_pending_proposal(self) -> None:
        ctx = _ctx()

        async def dispatch(ctx: ExecutionContext, proposal: Any) -> None:
            raise RuntimeError("the queue is down")

        kit = _kit(_proposer(_item("a")), dispatch=dispatch)

        with pytest.raises(RuntimeError):
            await _op(kit, "request", RequestProposalDTO(scope="s", base="r1"), ctx)

        assert await ctx.document.query(SPEC.document).count() == 0

    async def test_the_base_comes_from_base_of_when_a_request_omits_it(self) -> None:
        ctx = _ctx()

        async def base_of(ctx: ExecutionContext, scope: str) -> str:
            return f"{scope}@7"

        kit = _kit(_proposer(_item("a")), base_of=base_of)
        requested = await _op(kit, "request", RequestProposalDTO(scope="s"), ctx)

        assert requested.base == "s@7"

    async def test_no_items_decides_the_proposal_at_once(self) -> None:
        ran = await _open(_kit(_proposer()), _ctx())

        assert (ran.status, ran.items) == (ProposalStatus.DECIDED, [])

    async def test_a_failing_proposer_fails_the_proposal_with_its_error(self) -> None:
        async def broken(ctx: ExecutionContext, request: ProposalRequest) -> Any:
            raise RuntimeError("the model timed out")

        ran = await _open(_kit(broken), _ctx())

        assert (ran.status, ran.error) == (ProposalStatus.FAILED, "the model timed out")

    async def test_a_draft_the_store_refuses_fails_the_proposal(self) -> None:
        # JSON has no NaN: the stored draft cannot be read back as the evidence model. Left
        # pending, the proposal would block its scope and rerun the proposer on every retry.
        draft = SPEC.item(key="a", proposed=Mapping(field="f"), evidence=Seen(filled=float("nan")))

        ran = await _open(_kit(_proposer(draft)), _ctx())

        assert ran.status is ProposalStatus.FAILED
        assert ran.error

    async def test_a_draft_the_store_cannot_write_fails_the_proposal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # As Postgres refuses a NUL byte in a jsonb value: the draft's write is refused, and
        # the proposal must fail rather than stay pending.
        written = MockDocumentAdapter.update

        async def refusing(self: Any, pk: Any, rev: Any, dto: Any, **kwargs: Any) -> Any:
            if getattr(dto, "status", None) == ProposalStatus.OPEN:
                raise exc.precondition("Invalid value (data too long or invalid format).")

            return await written(self, pk, rev, dto, **kwargs)

        monkeypatch.setattr(MockDocumentAdapter, "update", refusing)

        ran = await _open(_kit(_proposer(_item("a"))), _ctx())

        assert ran.status is ProposalStatus.FAILED
        assert "Invalid value" in (ran.error or "")

    @pytest.mark.parametrize(
        ("items", "message"),
        [((("a", "f"), ("a", "g")), "more than once"), ((("a", None),), "propose nothing")],
    )
    async def test_items_a_verdict_could_not_name_fail_the_proposal(
        self, items: tuple[tuple[str, str | None], ...], message: str
    ) -> None:
        ran = await _open(_kit(_proposer(*(_item(k, f) for k, f in items))), _ctx())

        assert ran.status is ProposalStatus.FAILED
        assert message in (ran.error or "")

    async def test_an_item_of_the_wrong_models_fails_the_proposal(self) -> None:
        # The bare generic item takes any model; only the collection's own item type checks
        # it, so that check has to fail the proposal rather than leave it pending.
        wrong: ProposalItem[Any, Any] = ProposalItem(key="a", proposed=Seen(filled=1.0))
        ran = await _open(_kit(_proposer(wrong)), _ctx())

        assert ran.status is ProposalStatus.FAILED
        assert "items.0.proposed" in (ran.error or "")

    async def test_a_confidence_outside_zero_to_one_fails_the_proposal(self) -> None:
        async def propose(ctx: ExecutionContext, request: ProposalRequest) -> Any:
            return ProposalDraft(
                items=[SPEC.item(key="a", proposed=Mapping(field="f"), confidence=4)]
            )

        ran = await _open(_kit(propose), _ctx())

        assert ran.status is ProposalStatus.FAILED
        assert "confidence" in (ran.error or "")

    async def test_a_proposer_cannot_hand_in_a_verdict(self) -> None:
        accepted = _item("a").model_copy(
            update={"verdict": ItemVerdict.ACCEPTED, "decided": Mapping(field="x")}
        )
        ran = await _open(_kit(_proposer(accepted)), _ctx())

        assert [(i.verdict, i.decided) for i in ran.items] == [(ItemVerdict.PENDING, None)]

    async def test_running_a_proposal_that_already_ran_is_a_no_op(self) -> None:
        ctx = _ctx()
        proposer = _proposer(_item("a"))
        kit = _kit(proposer)
        ran = await _open(kit, ctx)

        again = await _op(kit, "run", ProposalIdDTO(id=ran.id), ctx)

        assert (again.rev, len(proposer.calls)) == (ran.rev, 1)


class TestDecide:
    async def test_accepted_items_are_applied_and_the_rest_rejected(self) -> None:
        ctx = _ctx()
        kit = _kit(_proposer(_item("a", "fa"), _item("b", "fb"), _item("c", "fc")))
        proposal = await _open(kit, ctx)

        decided = await _op(
            kit,
            "decide",
            _decide(
                proposal,
                VerdictDTO[Mapping](key="a", accept=True),
                VerdictDTO[Mapping](key="b", accept=True, change=Mapping(field="amended")),
                note="looks right",
            ),
            ctx,
        )

        assert decided.status is ProposalStatus.DECIDED
        assert [(i.key, i.verdict, i.decided) for i in decided.items] == [
            ("a", ItemVerdict.ACCEPTED, Mapping(field="fa")),
            ("b", ItemVerdict.ACCEPTED, Mapping(field="amended")),
            ("c", ItemVerdict.REJECTED, None),
        ]
        assert (decided.decided_by, decided.note) == (USER.performer_id, "looks right")
        assert sorted(await _applied(ctx)) == ["amended", "fa"]

    async def test_a_question_is_accepted_only_with_an_answer(self) -> None:
        ctx = _ctx()
        kit = _kit(_proposer(_item("q", "guess", question="Which field is this?")))
        proposal = await _open(kit, ctx)

        with pytest.raises(CoreException) as caught:
            await _op(
                kit, "decide", _decide(proposal, VerdictDTO[Mapping](key="q", accept=True)), ctx
            )

        assert caught.value.code == "proposal_answer_required"
        assert await _applied(ctx) == []

        answer = VerdictDTO[Mapping](key="q", accept=True, change=Mapping(field="answer"))
        decided = await _op(kit, "decide", _decide(proposal, answer), ctx)

        assert decided.items[0].decided == Mapping(field="answer")
        assert await _applied(ctx) == ["answer"]

    async def test_deciding_needs_a_principal(self) -> None:
        ctx = _ctx()
        kit = _kit(_proposer(_item("a")))
        proposal = await _open(kit, ctx)

        with pytest.raises(CoreException) as caught:
            await _op(
                kit,
                "decide",
                _decide(proposal, VerdictDTO[Mapping](key="a", accept=True)),
                ctx,
                None,
            )

        assert (caught.value.kind, caught.value.code) == (
            ExceptionKind.AUTHENTICATION,
            "proposal_principal_required",
        )
        assert await _applied(ctx) == []

    async def test_an_agent_acting_for_a_principal_cannot_decide(self) -> None:
        # Inert until a human decides: an agent's tool call carries the human as its subject,
        # and accepting through it would run the effect on the agent's say.
        ctx = _ctx()
        kit = _kit(_proposer(_item("a")))
        proposal = await _open(kit, ctx)
        delegated = AuthnIdentity(principal_id=uuid4(), actor=AuthnIdentity(principal_id=uuid4()))

        with pytest.raises(CoreException) as caught:
            await _op(
                kit,
                "decide",
                _decide(proposal, VerdictDTO[Mapping](key="a", accept=True)),
                ctx,
                delegated,
            )

        assert (caught.value.kind, caught.value.code) == (
            ExceptionKind.AUTHORIZATION,
            "proposal_human_required",
        )
        assert await _applied(ctx) == []

    async def test_a_pending_or_decided_proposal_is_refused(self) -> None:
        ctx = _ctx()
        kit = _kit(_proposer(_item("a")))
        pending = await _op(kit, "request", RequestProposalDTO(scope="s"), ctx)

        with pytest.raises(CoreException) as caught:
            await _op(kit, "decide", _decide(pending), ctx)

        assert caught.value.code == "proposal_pending"

        proposal = await _op(kit, "run", ProposalIdDTO(id=pending.id), ctx)
        await _op(kit, "decide", _decide(proposal, VerdictDTO[Mapping](key="a", accept=True)), ctx)

        with pytest.raises(CoreException) as caught:
            await _op(
                kit, "decide", _decide(proposal, VerdictDTO[Mapping](key="a", accept=True)), ctx
            )

        assert caught.value.code == "proposal_decided"
        assert await _applied(ctx) == ["f"]

    async def test_a_proposal_whose_base_moved_is_stale(self) -> None:
        ctx = _ctx()
        current = {"base": "r1"}

        async def base_of(ctx: ExecutionContext, scope: str) -> str:
            return current["base"]

        kit = _kit(_proposer(_item("a")), base_of=base_of)
        requested = await _op(kit, "request", RequestProposalDTO(scope="s"), ctx)
        proposal = await _op(kit, "run", ProposalIdDTO(id=requested.id), ctx)
        current["base"] = "r2"

        with pytest.raises(CoreException) as caught:
            await _op(
                kit, "decide", _decide(proposal, VerdictDTO[Mapping](key="a", accept=True)), ctx
            )

        assert caught.value.code == "proposal_stale"
        assert await _applied(ctx) == []

    async def test_a_verdict_on_an_item_the_proposal_lacks_is_refused(self) -> None:
        ctx = _ctx()
        kit = _kit(_proposer(_item("a")))
        proposal = await _open(kit, ctx)

        with pytest.raises(CoreException) as caught:
            await _op(
                kit, "decide", _decide(proposal, VerdictDTO[Mapping](key="z", accept=True)), ctx
            )

        assert caught.value.code == "proposal_verdict_unknown"

    async def test_two_verdicts_on_one_item_are_refused(self) -> None:
        ctx = _ctx()
        kit = _kit(_proposer(_item("a")))
        proposal = await _open(kit, ctx)
        twice = _decide(
            proposal,
            VerdictDTO[Mapping](key="a", accept=False),
            VerdictDTO[Mapping](key="a", accept=True),
        )

        with pytest.raises(CoreException) as caught:
            await _op(kit, "decide", twice, ctx)

        assert caught.value.code == "proposal_verdict_repeated"
        assert await _applied(ctx) == []

    async def test_an_effect_that_fails_rolls_the_verdicts_back(self) -> None:
        ctx = _ctx()

        async def failing(ctx: ExecutionContext, proposal: Any, accepted: Sequence[Any]) -> None:
            await _apply(ctx, proposal, accepted)
            raise RuntimeError("the apply failed")

        kit = ProposalKit(spec=SPEC, proposer=_proposer(_item("a")), on_accept=failing)
        proposal = await _open(kit, ctx)

        with pytest.raises(RuntimeError):
            await _op(
                kit, "decide", _decide(proposal, VerdictDTO[Mapping](key="a", accept=True)), ctx
            )

        stored = await ctx.document.query(SPEC.document).get(proposal.id)

        assert stored.status is ProposalStatus.OPEN
        assert await _applied(ctx) == []


class TestNothingUnacceptedTakesEffect:
    """An un-accepted proposal changes no downstream state."""

    async def test_requesting_running_and_rejecting_write_nothing_downstream(self) -> None:
        ctx = _ctx()
        kit = _kit(_proposer(_item("a"), _item("b")))
        proposal = await _open(kit, ctx)

        assert await _applied(ctx) == []

        decided = await _op(kit, "decide", _decide(proposal), ctx)

        assert {i.verdict for i in decided.items} == {ItemVerdict.REJECTED}
        assert await _applied(ctx) == []


class TestEscapeHatches:
    async def test_a_handler_override_replaces_a_generated_operation(self) -> None:
        ctx = _ctx()

        class _Refuse:
            def __init__(self, ctx: ExecutionContext) -> None:
                pass

            async def __call__(self, args: Any) -> Any:
                raise RuntimeError("decisions go through the review board")

        kit = _kit(_proposer(_item("a")), handlers={"decide": _Refuse})
        proposal = await _open(kit, ctx)

        with pytest.raises(RuntimeError, match="review board"):
            await _op(kit, "decide", _decide(proposal), ctx)

    async def test_extra_operations_merge_into_the_registry(self) -> None:
        extra = OperationRegistry(handlers={"mappings.count": lambda ctx: _Count(ctx=ctx)})
        reg = _kit(_proposer(), extra_ops=extra).registry(tx_route="mock")

        assert "mappings.count" in reg.catalog()


class _Count:
    def __init__(self, *, ctx: ExecutionContext) -> None:
        self.ctx = ctx

    async def __call__(self, args: Any) -> int:
        return await self.ctx.document.query(SPEC.document).count()


# ....................... #
# The deterministic proposer above first, a model-backed one second, through
# the same kit and no API of its own.


class _Profile(BaseModel):
    paths: list[str]


class _Guess(BaseModel):
    path: str
    field: str | None
    sure: float


class _Answer(BaseModel):
    guesses: list[_Guess]


MAPPER = InferenceSpec(name="mapper", input=_Profile, output=_Answer)


async def _model_proposer(
    ctx: ExecutionContext, request: ProposalRequest
) -> ProposalDraft[Any, Any]:
    answer = await ctx.inference.model(MAPPER).predict(_Profile(paths=["amount", "cust"]))
    items = [
        SPEC.item(
            key=guess.path,
            proposed=Mapping(field=guess.field) if guess.field is not None else None,
            question=None if guess.field is not None else f"What is {guess.path!r}?",
            evidence=Seen(filled=guess.sure),
            confidence=guess.sure,
        )
        for guess in answer.guesses
    ]

    return ProposalDraft(items=items, model=MAPPER.name)


class TestAModelBackedProposer:
    async def test_it_runs_through_the_same_kit(self) -> None:
        def answer(instances: Sequence[BaseModel]) -> Sequence[Any]:
            return [
                {
                    "guesses": [
                        {"path": "amount", "field": "amount", "sure": 0.95},
                        {"path": "cust", "field": None, "sure": 0.3},
                    ]
                }
                for _ in instances
            ]

        ctx = context_from_modules(
            MockDepsModule(inference=MockInferenceRegistry().on("mapper", answer))
        )
        kit = _kit(_model_proposer)
        proposal = await _open(kit, ctx)

        assert proposal.model == "mapper"
        assert [(i.key, i.question is not None) for i in proposal.items] == [
            ("amount", False),
            ("cust", True),
        ]

        decided = await _op(
            kit,
            "decide",
            _decide(
                proposal,
                VerdictDTO[Mapping](key="amount", accept=True),
                VerdictDTO[Mapping](key="cust", accept=True, change=Mapping(field="customer_id")),
            ),
            ctx,
        )

        assert {i.verdict for i in decided.items} == {ItemVerdict.ACCEPTED}
        assert sorted(await _applied(ctx)) == ["amount", "customer_id"]
