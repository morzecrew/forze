"""Unit tests for runtime tracing."""

from __future__ import annotations

import pytest

from forze.application.contracts.deps import DepKey
from forze.application.contracts.document import DocumentSpec
from forze.application.execution import (
    Deps,
    DepsRegistry,
    ExecutionContext,
    RuntimeTrace,
    TracingEvent,
    TracingViolation,
    validate_runtime_trace,
)
from forze.application.execution.tracing import bind_active_deps, record
from forze.application.execution.tracing.port_proxy import TracingPortProxy, wrap_port
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockDocumentAdapter, MockState
from tests.support.execution_context import context_from_deps

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


@pytest.fixture(autouse=True)
def _clear_runtime_tracing() -> None:
    bind_active_deps(None)
    yield
    bind_active_deps(None)


@pytest.fixture
def mock_state() -> MockState:
    return MockState()


@pytest.fixture
def ctx(mock_state: MockState) -> ExecutionContext:
    deps = MockDepsModule(state=mock_state)()
    return context_from_deps(deps)


@pytest.fixture
def traced_ctx(mock_state: MockState) -> ExecutionContext:
    frozen = (
        DepsRegistry.from_modules(
            lambda: MockDepsModule(state=mock_state)(),
        )
        .with_tracing(runtime=True)
        .freeze()
        .resolve()
    )
    return ExecutionContext(deps=frozen)


# ----------------------- #


class TestRuntimeTracingDisabled:
    def test_disabled_by_default(self, ctx: ExecutionContext) -> None:
        assert ctx.deps.trace_runtime is False
        assert ctx.deps.runtime_trace() is None

    def test_query_port_not_wrapped(self, ctx: ExecutionContext) -> None:
        port = ctx.document.query(_doc_spec())
        assert isinstance(port, MockDocumentAdapter)


class TestRuntimeTracingEnabled:
    def test_explicit_flag_creates_trace(self, traced_ctx: ExecutionContext) -> None:
        assert traced_ctx.deps.trace_runtime is True
        trace = traced_ctx.deps.runtime_trace()
        assert trace is not None
        assert trace.events == []

    def test_env_flag(
        self,
        mock_state: MockState,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("FORZE_RUNTIME_TRACE", "true")
        built = (
            DepsRegistry.from_modules(lambda: MockDepsModule(state=mock_state)())
            .freeze()
            .resolve()
        )
        assert built.trace_runtime is True

    def test_freeze_without_registry_tracing_disables_runtime_trace(self) -> None:
        _k = DepKey[str]("only")
        resolved = DepsRegistry.from_deps(
            Deps.plain({_k: 1}),
            Deps.plain({DepKey[str]("other"): 2}),
        ).freeze().resolve()
        assert resolved.trace_runtime is False

    def test_registry_with_tracing_enables_runtime_trace(self) -> None:
        _k = DepKey[str]("only")
        resolved = (
            DepsRegistry.from_deps(
                Deps.plain({_k: 1}),
                Deps.plain({DepKey[str]("other"): 2}),
            )
            .with_tracing(runtime=True)
            .freeze()
            .resolve()
        )
        assert resolved.trace_runtime is True

    def test_query_port_wrapped_when_enabled(self, traced_ctx: ExecutionContext) -> None:
        port = traced_ctx.document.query(_doc_spec())
        assert isinstance(port, TracingPortProxy)


class TestRuntimeTracingRecording:
    @pytest.mark.asyncio
    async def test_tx_and_port_sequence(self, traced_ctx: ExecutionContext) -> None:
        spec = _doc_spec()
        port = traced_ctx.document.query(spec)

        async with traced_ctx.tx_ctx.scope("mock"):
            await port.count()

        trace = traced_ctx.deps.runtime_trace()
        assert trace is not None

        assert any(
            e.domain == "tx" and e.op == "enter" and e.tx_depth == 1 for e in trace.events
        )
        assert any(
            e.domain == "document"
            and e.surface == "document_query"
            and e.op == "count"
            and e.phase == "query"
            and e.route == "projects"
            and e.tx_depth == 1
            for e in trace.events
        )
        assert any(
            e.domain == "tx" and e.op == "exit" and e.tx_depth == 1 for e in trace.events
        )

    @pytest.mark.asyncio
    async def test_proxy_passes_through_return_value(self, mock_state: MockState) -> None:
        deps = (
            DepsRegistry.from_modules(
                lambda: MockDepsModule(state=mock_state)(),
            )
            .with_tracing(runtime=True)
            .freeze()
            .resolve()
        )

        class _Inner:
            async def get(self) -> int:
                return 42

        wrapped = wrap_port(
            _Inner(),
            deps=deps,
            domain="document",
            surface="document_query",
            route="projects",
            phase="query",
        )
        assert await wrapped.get() == 42

    def test_record_noop_without_active_deps(self) -> None:
        record(domain="tx", op="enter")


class TestValidateRuntimeTrace:
    def test_validate_runtime_trace_delegates_to_validator(self) -> None:
        event = TracingEvent(
            seq=0,
            domain="document",
            op="get",
            surface="document_query",
            route="projects",
            phase="query",
        )

        def _validator(events: list[TracingEvent]) -> list[TracingViolation]:
            assert list(events) == [event]
            return [
                TracingViolation(
                    profile="test",
                    message="seen",
                    at_seq=0,
                )
            ]

        trace = RuntimeTrace(events=[event])
        violations = validate_runtime_trace(trace, validator=_validator)

        assert len(violations) == 1
        assert violations[0].profile == "test"
