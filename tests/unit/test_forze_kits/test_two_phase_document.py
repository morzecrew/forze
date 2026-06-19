"""TwoPhaseDocumentHandler: external work in prepare (outside tx), write in apply.

End-to-end over the real execution engine + mock deps: the handler enriches its
input via an injected service in ``prepare`` (outside the transaction) and writes
the enriched document in ``apply`` (inside the transaction). The write commits and
is readable afterwards; a handler that reaches for the write port in ``prepare``
is rejected by the read-only flag.
"""

from __future__ import annotations

from typing import Awaitable, Callable

import attrs
import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException
from forze.domain.models import (
    BaseDTO,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)
from forze_kits.aggregates.document import TwoPhaseDocumentHandler
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
    prepare_depths: list[int]

    async def prepare(self, args: QuoteRequest) -> int:
        # Records the tx depth (expected 0 — outside the transaction) and calls
        # the external service.
        self.prepare_depths.append(self.ctx.tx_ctx.depth())
        return await self.enrich(args.sku)

    async def apply(self, args: QuoteRequest, payload: int) -> WidgetRead:
        return await self.writer().create(WidgetCreate(price=payload))


@attrs.define(slots=True, kw_only=True, frozen=True)
class WriteInPrepare(
    TwoPhaseDocumentHandler[QuoteRequest, int, WidgetRead, WidgetCreate]
):
    """A misuse: reaching for the write port in prepare must be rejected."""

    async def prepare(self, args: QuoteRequest) -> int:
        self.writer()  # resolves a command port under the read-only flag -> raises
        return 0  # pragma: no cover

    async def apply(self, args: QuoteRequest, payload: int) -> WidgetRead:  # pragma: no cover
        return await self.writer().create(WidgetCreate(price=payload))


# ....................... #


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule()())


# ....................... #


class TestTwoPhaseDocumentHandler:
    @pytest.mark.asyncio
    async def test_prepare_enriches_outside_tx_apply_writes_and_commits(
        self, ctx: ExecutionContext
    ) -> None:
        depths: list[int] = []

        async def enrich(_sku: str) -> int:
            return 42

        reg = (
            OperationRegistry(
                handlers={
                    "quote": lambda c: QuoteAndCreate(
                        ctx=c,
                        spec=WIDGET_SPEC,
                        enrich=enrich,
                        prepare_depths=depths,
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
        assert depths == [0]  # prepare ran outside the transaction

        # The write committed and is readable after the operation.
        fetched = await ctx.document.query(WIDGET_SPEC).get(result.id)
        assert fetched is not None
        assert fetched.price == 42

    @pytest.mark.asyncio
    async def test_write_port_in_prepare_is_rejected(
        self, ctx: ExecutionContext
    ) -> None:
        reg = (
            OperationRegistry(
                handlers={"quote": lambda c: WriteInPrepare(ctx=c, spec=WIDGET_SPEC)}
            )
            .bind("quote")
            .two_phase()
            .bind_tx()
            .set_route("mock")
            .finish(deep=True)
            .freeze()
        )

        with pytest.raises(CoreException, match="read-only"):
            await reg.resolve("quote", ctx)(QuoteRequest(sku="x"))
