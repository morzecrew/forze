"""Tests for run_traced_operation harness."""

from __future__ import annotations

import attrs
import pytest

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.execution import Handler
from forze.application.execution import (
    TraceExpectation,
    assert_trace_contains,
    run_traced_operation,
)
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.execution.tracing import TracedOperationResult
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
# ----------------------- #


def _doc_spec() -> DocumentSpec:
    return DocumentSpec(
        name="projects",
        read=ReadDocument,
        write={
            "domain": Document,
            "create_cmd": CreateDocumentCmd,
            "update_cmd": CreateDocumentCmd,
        },
        cache=None,
    )


@attrs.define(slots=True)
class _TraceProbeHandler(Handler[None, str]):
    """Handler that exercises tx scope and document query."""

    ctx: object

    async def __call__(self, _args: None) -> str:
        async with self.ctx.tx_ctx.scope("mock"):  # type: ignore[attr-defined]
            port = self.ctx.document.query(_doc_spec())  # type: ignore[attr-defined]
            await port.count()
        return "ok"


def _trace_probe_factory(ctx: object) -> _TraceProbeHandler:
    return _TraceProbeHandler(ctx=ctx)


class TestRunTracedOperation:
    @pytest.mark.asyncio
    async def test_harness_collects_trace(self, traced_ctx) -> None:
        reg = (
            OperationRegistry(handlers={"probe": _trace_probe_factory})
            .bind("probe")
            .bind_tx()
            .set_route("mock")
            .finish(deep=True)
            .freeze()
        )

        outcome = await run_traced_operation(reg, "probe", None, traced_ctx)

        assert isinstance(outcome, TracedOperationResult)
        assert outcome.result == "ok"
        assert outcome.violations == ()
        assert outcome.trace is not None

        assert_trace_contains(
            outcome.trace,
            [
                TraceExpectation(domain="tx", op="enter", tx_depth=1),
                TraceExpectation(
                    domain="document",
                    op="count",
                    surface="document_query",
                    route="projects",
                    phase="query",
                    tx_depth=2,
                ),
                TraceExpectation(domain="tx", op="exit", tx_depth=1),
            ],
        )
