"""Tests for forze.application.execution.context."""

from datetime import timedelta

import pytest

from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

from forze_mock import MockDepsModule, MockState

# ----------------------- #


@pytest.fixture
def mock_state() -> MockState:
    return MockState()


@pytest.fixture
def ctx(mock_state: MockState) -> ExecutionContext:
    module = MockDepsModule(state=mock_state)
    return ExecutionContext(deps=module())


def _doc_spec(
    *,
    cache: dict | None = None,
) -> DocumentSpec:
    return DocumentSpec(
        name="test",
        read=ReadDocument,
        write={
            "domain": Document,
            "create_cmd": CreateDocumentCmd,
            "update_cmd": CreateDocumentCmd,
        },
        cache=cache,
    )


def _search_spec() -> SearchSpec[ReadDocument]:
    return SearchSpec(
        name="test",
        model_type=ReadDocument,
        fields=["id"],
    )


# ----------------------- #


class TestExecutionContextDep:
    def test_resolves_dependency(self, ctx: ExecutionContext) -> None:
        from forze.application.contracts.counter import CounterDepKey

        factory = ctx.dep(CounterDepKey)
        assert callable(factory)

    def test_cycle_detection_raises(self, mock_state: MockState) -> None:
        from forze.application.contracts.deps import DepKey

        key: DepKey[int] = DepKey("cyclic")

        class CyclicDeps:
            def provide(self, k: DepKey) -> int:  # type: ignore[type-arg]
                return ctx.dep(key)

        ctx = ExecutionContext(deps=CyclicDeps())  # type: ignore[arg-type]
        with pytest.raises(RecursionError):
            ctx.dep(key)


class TestExecutionContextTransaction:
    async def test_basic_transaction(self, ctx: ExecutionContext) -> None:
        assert ctx.active_tx() is None
        async with ctx.transaction():
            assert ctx.active_tx() is not None
        assert ctx.active_tx() is None

    async def test_nested_transaction(self, ctx: ExecutionContext) -> None:
        async with ctx.transaction():
            h1 = ctx.active_tx()
            async with ctx.transaction():
                h2 = ctx.active_tx()
                assert h2 is not None
            assert ctx.active_tx() == h1

    async def test_transaction_cleanup_on_error(self, ctx: ExecutionContext) -> None:
        with pytest.raises(RuntimeError):
            async with ctx.transaction():
                raise RuntimeError("fail")
        assert ctx.active_tx() is None


class TestExecutionContextPorts:
    def test_doc_read(self, ctx: ExecutionContext) -> None:
        port = ctx.doc_read(_doc_spec())
        assert port is not None

    def test_doc_write(self, ctx: ExecutionContext) -> None:
        port = ctx.doc_write(_doc_spec())
        assert port is not None

    def test_doc_read_with_cache(self, ctx: ExecutionContext) -> None:
        spec = _doc_spec(
            cache={"enabled": True, "ttl": timedelta(seconds=60)},
        )
        port = ctx.doc_read(spec)
        assert port is not None

    def test_doc_write_with_cache(self, ctx: ExecutionContext) -> None:
        spec = _doc_spec(cache={"enabled": True})
        port = ctx.doc_write(spec)
        assert port is not None

    def test_doc_read_cache_disabled(self, ctx: ExecutionContext) -> None:
        spec = _doc_spec(cache={"enabled": False})
        port = ctx.doc_read(spec)
        assert port is not None

    def test_cache(self, ctx: ExecutionContext) -> None:
        spec = CacheSpec(name="test")
        port = ctx.cache(spec)
        assert port is not None

    def test_counter(self, ctx: ExecutionContext) -> None:
        port = ctx.counter("test")
        assert port is not None

    def test_txmanager(self, ctx: ExecutionContext) -> None:
        port = ctx.txmanager()
        assert port is not None

    def test_storage(self, ctx: ExecutionContext) -> None:
        port = ctx.storage("my-bucket")
        assert port is not None

    def test_search(self, ctx: ExecutionContext) -> None:
        port = ctx.search(_search_spec())
        assert port is not None
