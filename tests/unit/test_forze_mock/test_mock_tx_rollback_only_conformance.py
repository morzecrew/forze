"""Mock ``rollback_only()`` — the shared conformance battery.

Both managers that roll back: the journal manager (the default), whose nested scopes share the
root's transaction, and the strict manager, whose nested scopes are savepoints. The no-op
manager keeps writes through any rollback, so it has nothing to compare.
"""

from __future__ import annotations

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules
from tests.support.tx_rollback_only_conformance import (
    TX_ROLLBACK_ONLY_BATTERY,
    Check,
    RollbackOnlyHarness,
)

# ----------------------- #


class Thing(Document):
    name: str


class ThingCreate(CreateDocumentCmd):
    name: str


class ThingUpdate(BaseDTO):
    name: str | None = None


class ThingRead(ReadDocument):
    name: str


SPEC = DocumentSpec(
    name="things",
    read=ThingRead,
    write=DocumentWriteTypes(domain=Thing, create_cmd=ThingCreate, update_cmd=ThingUpdate),
)


@pytest.fixture(params=["journal", "strict"])
def harness(
    request: pytest.FixtureRequest, caplog: pytest.LogCaptureFixture
) -> RollbackOnlyHarness:
    state = MockState()
    strict = request.param == "strict"
    ctx = context_from_modules(
        MockDepsModule(state=state, transactions="strict" if strict else "journal")
    )

    async def write(key: str) -> None:
        await ctx.document.command(SPEC).create(ThingCreate(name=key))

    async def stored() -> set[str]:
        return {row["name"] for row in state.documents.get("things", {}).values()}

    return RollbackOnlyHarness(
        ctx=ctx,
        route="mock",
        write=write,
        stored=stored,
        savepoints=strict,
        caplog=caplog,
        backend=f"mock-{request.param}",
    )


@pytest.mark.conformance(plane="tx_rollback_only", engine="mock")
@pytest.mark.parametrize("check", TX_ROLLBACK_ONLY_BATTERY, ids=lambda check: check.__name__)
async def test_tx_rollback_only_battery(check: Check, harness: RollbackOnlyHarness) -> None:
    await check(harness)


@pytest.mark.parametrize("transactions", ["journal", "strict"])
async def test_another_route_on_the_same_state_joins_the_dry_run(transactions: str) -> None:
    """Every mock route shares one state, so a scope on another route is never separate."""

    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state, transactions=transactions))  # type: ignore[arg-type]

    async with ctx.tx_ctx.scope("orders") as tx:
        await ctx.document.command(SPEC).create(ThingCreate(name="outer"))

        async with ctx.tx_ctx.scope("billing"):
            await ctx.document.command(SPEC).create(ThingCreate(name="other-route"))

        # Decided at the end: the other route committed nothing of its own, so it is allowed.
        tx.rollback_only()

    assert state.documents.get("things", {}) == {}
