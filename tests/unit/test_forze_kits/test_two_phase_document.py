"""TwoPhaseDocumentHandler + builder: external work in prepare, write in apply.

End-to-end over the real execution engine + mock deps: the handler enriches its
input via an injected service in ``prepare`` and writes the enriched document in
``apply`` via its write port. The write commits and is readable afterwards. The
builder resolves the read/write ports from the context so the handler holds ports,
not the context. (That ``prepare`` runs outside the transaction is asserted at the
engine level in tests/unit/test_forze/application/execution/test_two_phase_runner.py.)
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import attrs
import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import (
    BaseDTO,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)
from forze_kits.aggregates.document import (
    TwoPhaseDocumentBuilder,
    TwoPhaseDocumentHandler,
)
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_deps

# ----------------------- #


class Widget(Document):
    price: int = 0


class WidgetCreate(CreateDocumentCmd):
    price: int


class WidgetUpdate(BaseDTO):
    price: int | None = None


class WidgetRead(ReadDocument):
    price: int


WIDGET_SPEC = DocumentSpec(
    name="widgets",
    read=WidgetRead,
    write=DocumentWriteTypes(
        domain=Widget,
        create_cmd=WidgetCreate,
        update_cmd=WidgetUpdate,
    ),
)


class QuoteRequest(BaseDTO):
    sku: str


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class QuoteAndCreate(
    TwoPhaseDocumentHandler[QuoteRequest, int, WidgetRead, WidgetCreate]
):
    """Quote the price via an external service in prepare, then create in apply."""

    enrich: Callable[[str], Awaitable[int]]

    async def prepare(self, args: QuoteRequest) -> int:
        return await self.enrich(args.sku)

    async def apply(self, args: QuoteRequest, payload: int) -> WidgetRead:
        return await self.writer.create(WidgetCreate(price=payload))


# ....................... #


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule()())


# ....................... #


class TestTwoPhaseDocumentHandler:
    @pytest.mark.asyncio
    async def test_prepare_enriches_apply_writes_and_commits(
        self, ctx: ExecutionContext
    ) -> None:
        async def enrich(_sku: str) -> int:
            return 42

        reg = (
            OperationRegistry(
                handlers={
                    "quote": TwoPhaseDocumentBuilder(
                        spec=WIDGET_SPEC,
                        build=lambda reader, writer: QuoteAndCreate(
                            reader=reader, writer=writer, enrich=enrich
                        ),
                    )
                }
            )
            .bind("quote")
            .two_phase()
            .bind_tx()
            .set_route("mock")
            .finish(deep=True)
            .freeze()
        )

        result = await reg.resolve("quote", ctx)(QuoteRequest(sku="abc"))

        assert result.price == 42

        # The write committed and is readable after the operation.
        fetched = await ctx.document.query(WIDGET_SPEC).get(result.id)
        assert fetched is not None
        assert fetched.price == 42
