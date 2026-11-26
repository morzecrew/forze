"""Integration: the proposal kit on Postgres (RFC 0039).

Three things the mock cannot show. The items are the application's models nested in a list,
stored as ``jsonb`` and read back into those models; the accept effect lands in the same
Postgres transaction as the verdicts, so an effect that fails leaves the proposal open; and a
partial unique index keeps one proposal pending per scope and base.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import uuid4

import attrs
import pytest
import pytest_asyncio
from pydantic import BaseModel

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.transaction.deps import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_kits.integrations.proposal import (
    DecideProposalDTO,
    ItemVerdict,
    ProposalDraft,
    ProposalIdDTO,
    ProposalKit,
    ProposalRequest,
    ProposalStatus,
    RequestProposalDTO,
    VerdictDTO,
    proposal_spec,
)
from forze_postgres.execution.deps import ConfigurablePostgresDocument, postgres_txmanager
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps
from tests.support.owned_schema import create_document_table

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #


class Mapping(BaseModel):
    field: str
    type: str = "string"


class Seen(BaseModel):
    filled: float
    values: list[str] = []


SPEC = proposal_spec("pg_proposals", change=Mapping, evidence=Seen)


class _Applied(Document):
    field: str


class _AppliedCreate(BaseDTO):
    field: str


class _AppliedRead(ReadDocument):
    field: str


APPLIED = DocumentSpec(
    name="pg_applied",
    read=_AppliedRead,
    write=DocumentWriteTypes(domain=_Applied, create_cmd=_AppliedCreate),
)


def _document(table: str) -> ConfigurablePostgresDocument[Any, Any, Any, Any]:
    return ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table), write=("public", table), bookkeeping_strategy="application"
        )
    )


def _ctx(pg_client: PostgresClient) -> ExecutionContext:
    documents = {"pg_proposals": _document("pg_proposals"), "pg_applied": _document("pg_applied")}

    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        ).merge(
            Deps.routed(
                {
                    DocumentQueryDepKey: documents,
                    DocumentCommandDepKey: documents,
                    TransactionManagerDepKey: {"main": postgres_txmanager},
                }
            )
        )
    )


@pytest_asyncio.fixture(autouse=True)
async def _tables(pg_client: PostgresClient):
    await pg_client.execute("DROP TABLE IF EXISTS pg_proposals; DROP TABLE IF EXISTS pg_applied;")
    # The renderer prints only plain unique indexes, so the pending uniqueness is added the
    # way an application's own migration would add it.
    unguarded = attrs.evolve(SPEC.document, guarantees=())
    await create_document_table(pg_client, unguarded, "public", "pg_proposals")
    await pg_client.execute(
        "CREATE UNIQUE INDEX pg_proposals_one_pending ON pg_proposals (scope, base) "
        "NULLS NOT DISTINCT WHERE status = 'pending';"
    )
    await create_document_table(pg_client, APPLIED, "public", "pg_applied")
    yield


async def _propose(ctx: ExecutionContext, request: ProposalRequest) -> ProposalDraft[Any, Any]:
    return ProposalDraft(
        items=[
            SPEC.item(
                key="a",
                proposed=Mapping(field="amount", type="decimal"),
                evidence=Seen(filled=0.97, values=["1.50", "2"]),
                confidence=0.9,
            ),
            SPEC.item(key="b", question="Is `cust` the customer id?", evidence=Seen(filled=0.4)),
        ],
        model="m-1",
        tokens=120,
    )


async def _op(kit: ProposalKit[Any, Any], op: str, args: Any, ctx: ExecutionContext) -> Any:
    with ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=uuid4())):
        return await run_operation(kit.registry(tx_route="main"), f"pg_proposals.{op}", args, ctx)


async def _applied(pg_client: PostgresClient) -> list[str]:
    return sorted(row["field"] for row in await pg_client.fetch_all("SELECT field FROM pg_applied"))


def _verdicts(proposal: Any) -> DecideProposalDTO[Mapping]:
    return DecideProposalDTO[Mapping](
        id=proposal.id,
        verdicts=[
            VerdictDTO[Mapping](key="a", accept=True),
            VerdictDTO[Mapping](key="b", accept=True, change=Mapping(field="customer_id")),
        ],
    )


async def _apply(ctx: ExecutionContext, proposal: Any, accepted: Sequence[Any]) -> None:
    for item in accepted:
        await ctx.document.command(APPLIED).create(_AppliedCreate(field=item.decided.field))


# ....................... #


async def test_a_proposal_round_trips_its_items_and_applies_what_is_accepted(
    pg_client: PostgresClient,
) -> None:
    ctx = _ctx(pg_client)
    kit = ProposalKit(spec=SPEC, proposer=_propose, on_accept=_apply)

    requested = await _op(kit, "request", RequestProposalDTO(scope="orders", base="r1"), ctx)
    proposal = await _op(kit, "run", ProposalIdDTO(id=requested.id), ctx)

    assert proposal.status is ProposalStatus.OPEN
    assert proposal.items[0].evidence == Seen(filled=0.97, values=["1.50", "2"])
    assert proposal.items[1].question == "Is `cust` the customer id?"

    decided = await _op(kit, "decide", _verdicts(proposal), ctx)

    assert [(i.verdict, i.decided) for i in decided.items] == [
        (ItemVerdict.ACCEPTED, Mapping(field="amount", type="decimal")),
        (ItemVerdict.ACCEPTED, Mapping(field="customer_id")),
    ]
    assert await _applied(pg_client) == ["amount", "customer_id"]


async def test_an_effect_that_fails_leaves_the_proposal_open(pg_client: PostgresClient) -> None:
    ctx = _ctx(pg_client)

    async def failing(ctx: ExecutionContext, proposal: Any, accepted: Sequence[Any]) -> None:
        await _apply(ctx, proposal, accepted)
        raise RuntimeError("the apply failed")

    kit = ProposalKit(spec=SPEC, proposer=_propose, on_accept=failing)
    requested = await _op(kit, "request", RequestProposalDTO(scope="orders"), ctx)
    proposal = await _op(kit, "run", ProposalIdDTO(id=requested.id), ctx)

    with pytest.raises(RuntimeError):
        await _op(kit, "decide", _verdicts(proposal), ctx)

    rows = await pg_client.fetch_all("SELECT status FROM pg_proposals")

    assert [row["status"] for row in rows] == ["open"]
    assert await _applied(pg_client) == []


async def test_one_proposal_is_pending_per_scope_and_base(pg_client: PostgresClient) -> None:
    # A second pending create stands in for the request that lost the race: both found
    # nothing pending, so only the index refuses it.
    ctx = _ctx(pg_client)
    kit = ProposalKit(spec=SPEC, proposer=_propose)
    command = ctx.document.command(SPEC.document)

    for base in ("r1", None):
        first = await _op(kit, "request", RequestProposalDTO(scope="orders", base=base), ctx)

        with pytest.raises(CoreException) as refused:
            await command.create(SPEC.create_cmd(scope="orders", base=base))

        assert refused.value.kind is ExceptionKind.CONFLICT

        await _op(kit, "run", ProposalIdDTO(id=first.id), ctx)
        again = await _op(kit, "request", RequestProposalDTO(scope="orders", base=base), ctx)

        assert again.id != first.id
