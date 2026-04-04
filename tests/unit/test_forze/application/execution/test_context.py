"""Tests for forze.application.execution.context."""

from datetime import timedelta

import pytest

from forze.application.contracts.base import DepKey
from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.counter import CounterDepKey, CounterPort, CounterSpec
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import StorageDepKey, StorageSpec
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import MockCounterAdapter, MockStorageAdapter
from forze_mock.execution import MockStateDepKey

# ----------------------- #


@pytest.fixture
def mock_state() -> MockState:
    return MockState()


def _mock_counter_fac(ctx: ExecutionContext, spec: CounterSpec) -> CounterPort:
    return MockCounterAdapter(state=ctx.dep(MockStateDepKey), namespace=spec.name)


def _mock_storage_fac(ctx: ExecutionContext, spec: StorageSpec) -> MockStorageAdapter:
    return MockStorageAdapter(state=ctx.dep(MockStateDepKey), bucket=spec.name)


@pytest.fixture
def ctx(mock_state: MockState) -> ExecutionContext:
    base = MockDepsModule(state=mock_state)()
    plain = dict(base.plain_deps)
    plain[CounterDepKey] = _mock_counter_fac
    plain[StorageDepKey] = _mock_storage_fac
    return ExecutionContext(deps=Deps.plain(plain))


def _doc_spec(
    *,
    cache: CacheSpec | None = None,
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
        key: DepKey[int] = DepKey("cyclic")

        class CyclicDeps:
            def provide(
                self,
                k: DepKey[int],
                *,
                route: str | None = None,
                fallback_to_plain: bool = True,
            ) -> int:
                return ctx.dep(key)

        ctx = ExecutionContext(deps=CyclicDeps())  # type: ignore[arg-type]
        with pytest.raises(RecursionError):
            ctx.dep(key)


class TestExecutionContextTransaction:
    @pytest.mark.asyncio
    async def test_basic_transaction(self, ctx: ExecutionContext) -> None:
        async with ctx.transaction("mock"):
            pass

    @pytest.mark.asyncio
    async def test_nested_transaction(self, ctx: ExecutionContext) -> None:
        async with ctx.transaction("mock"):
            async with ctx.transaction("mock"):
                pass

    @pytest.mark.asyncio
    async def test_transaction_cleanup_on_error(self, ctx: ExecutionContext) -> None:
        with pytest.raises(RuntimeError):
            async with ctx.transaction("mock"):
                raise RuntimeError("fail")


class TestExecutionContextPorts:
    def test_doc_query(self, ctx: ExecutionContext) -> None:
        port = ctx.doc_query(_doc_spec())
        assert port is not None

    def test_doc_command(self, ctx: ExecutionContext) -> None:
        port = ctx.doc_command(_doc_spec())
        assert port is not None

    def test_doc_query_with_cache(self, ctx: ExecutionContext) -> None:
        spec = _doc_spec(
            cache=CacheSpec(name="doc-cache", ttl=timedelta(seconds=60)),
        )
        port = ctx.doc_query(spec)
        assert port is not None

    def test_doc_command_with_cache(self, ctx: ExecutionContext) -> None:
        spec = _doc_spec(cache=CacheSpec(name="doc-cache"))
        port = ctx.doc_command(spec)
        assert port is not None

    def test_doc_query_cache_disabled(self, ctx: ExecutionContext) -> None:
        spec = _doc_spec(cache=None)
        port = ctx.doc_query(spec)
        assert port is not None

    def test_cache(self, ctx: ExecutionContext) -> None:
        spec = CacheSpec(name="test")
        port = ctx.cache(spec)
        assert port is not None

    def test_counter(self, ctx: ExecutionContext) -> None:
        port = ctx.counter(CounterSpec(name="test"))
        assert port is not None

    def test_txmanager(self, ctx: ExecutionContext) -> None:
        port = ctx.txmanager("mock")
        assert port is not None

    def test_storage(self, ctx: ExecutionContext) -> None:
        port = ctx.storage(StorageSpec(name="my-bucket"))
        assert port is not None

    def test_search(self, ctx: ExecutionContext) -> None:
        port = ctx.search_query(_search_spec())
        assert port is not None
