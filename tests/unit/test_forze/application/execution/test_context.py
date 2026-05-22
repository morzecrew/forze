"""Tests for forze.application.execution.context."""

from datetime import timedelta
from enum import StrEnum

import pytest

from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.counter import CounterDepKey, CounterPort, CounterSpec
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import StorageDepKey, StorageSpec
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import CoreError
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import MockCounterAdapter, MockStorageAdapter
from forze_mock.execution import MockStateDepKey

# ----------------------- #


@pytest.fixture
def mock_state() -> MockState:
    return MockState()


def _mock_counter_fac(ctx: ExecutionContext, spec: CounterSpec) -> CounterPort:
    return MockCounterAdapter(
        state=ctx.deps.provide(MockStateDepKey), namespace=spec.name
    )


def _mock_storage_fac(ctx: ExecutionContext, spec: StorageSpec) -> MockStorageAdapter:
    return MockStorageAdapter(state=ctx.deps.provide(MockStateDepKey), bucket=spec.name)


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

        factory = ctx.deps.provide(CounterDepKey)
        assert callable(factory)


class TestExecutionContextTransaction:
    @pytest.mark.asyncio
    async def test_basic_transaction(self, ctx: ExecutionContext) -> None:
        async with ctx.tx.scope("mock"):
            pass

    @pytest.mark.asyncio
    async def test_nested_transaction(self, ctx: ExecutionContext) -> None:
        async with ctx.tx.scope("mock"):
            async with ctx.tx.scope("mock"):
                pass

    @pytest.mark.asyncio
    async def test_transaction_cleanup_on_error(self, ctx: ExecutionContext) -> None:
        with pytest.raises(RuntimeError):
            async with ctx.tx.scope("mock"):
                raise RuntimeError("fail")

    @pytest.mark.asyncio
    async def test_run_or_defer_outside_transaction_runs_immediately(
        self, ctx: ExecutionContext
    ) -> None:
        ran: list[int] = []

        async def _cb() -> None:
            ran.append(1)

        await ctx.tx.run_or_defer(_cb)
        assert ran == [1]

    @pytest.mark.asyncio
    async def test_run_or_defer_runs_fifo_on_success(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[str] = []

        async def _a() -> None:
            order.append("a")

        async def _b() -> None:
            order.append("b")

        async with ctx.tx.scope("mock"):
            await ctx.tx.run_or_defer(_a)
            await ctx.tx.run_or_defer(_b)

        assert order == ["a", "b"]

    @pytest.mark.asyncio
    async def test_run_or_defer_skipped_on_error(
        self, ctx: ExecutionContext
    ) -> None:
        ran: list[int] = []

        async def _cb() -> None:
            ran.append(1)

        with pytest.raises(RuntimeError, match="fail"):
            async with ctx.tx.scope("mock"):
                await ctx.tx.run_or_defer(_cb)
                raise RuntimeError("fail")

        assert ran == []

    @pytest.mark.asyncio
    async def test_run_or_defer_nested_fifo_after_outer_commit(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[str] = []

        async def _outer() -> None:
            order.append("outer")

        async def _inner() -> None:
            order.append("inner")

        async with ctx.tx.scope("mock"):
            await ctx.tx.run_or_defer(_outer)
            async with ctx.tx.scope("mock"):
                await ctx.tx.run_or_defer(_inner)

        assert order == ["outer", "inner"]


class TestExecutionContextPorts:
    def test_doc_query(self, ctx: ExecutionContext) -> None:
        port = ctx.document.query(_doc_spec())
        assert port is not None

    def test_doc_command(self, ctx: ExecutionContext) -> None:
        port = ctx.document.command(_doc_spec())
        assert port is not None

    def test_doc_query_with_cache(self, ctx: ExecutionContext) -> None:
        spec = _doc_spec(
            cache=CacheSpec(name="doc-cache", ttl=timedelta(seconds=60)),
        )
        port = ctx.document.query(spec)
        assert port is not None

    def test_doc_command_with_cache(self, ctx: ExecutionContext) -> None:
        spec = _doc_spec(cache=CacheSpec(name="doc-cache"))
        port = ctx.document.command(spec)
        assert port is not None

    def test_doc_query_cache_disabled(self, ctx: ExecutionContext) -> None:
        spec = _doc_spec(cache=None)
        port = ctx.document.query(spec)
        assert port is not None

    def test_cache(self, ctx: ExecutionContext) -> None:
        spec = CacheSpec(name="test")
        port = ctx.cache(spec)
        assert port is not None

    def test_counter(self, ctx: ExecutionContext) -> None:
        port = ctx.counter(CounterSpec(name="test"))
        assert port is not None

    def test_tx_resolver(self, ctx: ExecutionContext) -> None:
        port = ctx.tx.resolver("mock")
        assert port is not None

    def test_tx_resolver_accepts_str_enum_route(self, ctx: ExecutionContext) -> None:
        class TxRoute(StrEnum):
            MOCK = "mock"

        port = ctx.tx.resolver(TxRoute.MOCK)
        assert port is not None

    def test_storage(self, ctx: ExecutionContext) -> None:
        port = ctx.storage(StorageSpec(name="my-bucket"))
        assert port is not None

    def test_search(self, ctx: ExecutionContext) -> None:
        port = ctx.search.query(_search_spec())
        assert port is not None


class TestExecutionContextStrEnumNames:
    """Spec :attr:`~forze.application.contracts.base.BaseSpec.name` may be a :class:`StrEnum`."""

    def test_doc_query_command_cache_counter_storage_search_use_str_enum_name(
        self,
        ctx: ExecutionContext,
    ) -> None:
        class DocName(StrEnum):
            TEST = "test"

        spec = DocumentSpec(
            name=DocName.TEST,
            read=ReadDocument,
            write={
                "domain": Document,
                "create_cmd": CreateDocumentCmd,
                "update_cmd": CreateDocumentCmd,
            },
        )
        assert ctx.document.query(spec) is not None
        assert ctx.document.command(spec) is not None

        cache_spec = CacheSpec(name=DocName.TEST, ttl=timedelta(seconds=1))
        assert ctx.cache(cache_spec) is not None

        assert ctx.counter(CounterSpec(name=DocName.TEST)) is not None

        assert ctx.storage(StorageSpec(name=DocName.TEST)) is not None

        search_spec = SearchSpec(
            name=DocName.TEST,
            model_type=ReadDocument,
            fields=["id"],
        )
        assert ctx.search.query(search_spec) is not None


class TestExecutionContextStrEnumTransactionRoute:
    @pytest.mark.asyncio
    async def test_transaction_accepts_str_enum_route(
        self, ctx: ExecutionContext
    ) -> None:
        class TxRoute(StrEnum):
            MOCK = "mock"

        async with ctx.tx.scope(TxRoute.MOCK):
            pass
