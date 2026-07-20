"""Tests for forze.application.execution.context."""

import asyncio
from datetime import timedelta
from enum import StrEnum

import pytest

from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.counter import CounterDepKey, CounterPort, CounterSpec
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import (
    StorageCommandDepKey,
    StorageQueryDepKey,
    StorageSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.context.transaction import (
    AfterCommitError,
    AfterCommitErrorHandler,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import MockCounterAdapter, MockStorageAdapter
from forze_mock.execution import MockStateDepKey
from tests.support.execution_context import context_from_deps, frozen_deps_from_deps

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


def _build_ctx(
    mock_state: MockState,
    *,
    after_commit_error_handler: "AfterCommitErrorHandler | None" = None,
) -> ExecutionContext:
    base = MockDepsModule(state=mock_state)()
    plain = dict(base.plain_deps)
    plain[CounterDepKey] = _mock_counter_fac
    plain[StorageQueryDepKey] = _mock_storage_fac
    plain[StorageCommandDepKey] = _mock_storage_fac
    if after_commit_error_handler is None:
        return context_from_deps(Deps.plain(plain))
    return ExecutionContext(
        deps=frozen_deps_from_deps(Deps.plain(plain)),
        after_commit_error_handler=after_commit_error_handler,
    )


@pytest.fixture
def ctx(mock_state: MockState) -> ExecutionContext:
    return _build_ctx(mock_state)


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
        async with ctx.tx_ctx.scope("mock"):
            pass

    @pytest.mark.asyncio
    async def test_nested_transaction(self, ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"), ctx.tx_ctx.scope("mock"):
            pass

    @pytest.mark.asyncio
    async def test_transaction_cleanup_on_error(self, ctx: ExecutionContext) -> None:
        with pytest.raises(RuntimeError):
            async with ctx.tx_ctx.scope("mock"):
                raise RuntimeError("fail")

    @pytest.mark.asyncio
    async def test_run_or_defer_outside_transaction_runs_immediately(
        self, ctx: ExecutionContext
    ) -> None:
        ran: list[int] = []

        async def _cb() -> None:
            ran.append(1)

        await ctx.tx_ctx.run_or_defer(_cb)
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

        async with ctx.tx_ctx.scope("mock"):
            await ctx.tx_ctx.run_or_defer(_a)
            await ctx.tx_ctx.run_or_defer(_b)

        assert order == ["a", "b"]

    @pytest.mark.asyncio
    async def test_run_or_defer_skipped_on_error(self, ctx: ExecutionContext) -> None:
        ran: list[int] = []

        async def _cb() -> None:
            ran.append(1)

        with pytest.raises(RuntimeError, match="fail"):
            async with ctx.tx_ctx.scope("mock"):
                await ctx.tx_ctx.run_or_defer(_cb)
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

        async with ctx.tx_ctx.scope("mock"):
            await ctx.tx_ctx.run_or_defer(_outer)
            async with ctx.tx_ctx.scope("mock"):
                await ctx.tx_ctx.run_or_defer(_inner)

        assert order == ["outer", "inner"]


class TestAfterCommitCallbackFailures:
    @pytest.mark.asyncio
    async def test_all_callbacks_run_and_result_returned_no_raise(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[str] = []

        async def _a() -> None:
            order.append("a")
            raise RuntimeError("a failed")

        async def _b() -> None:
            order.append("b")

        async def _c() -> None:
            order.append("c")
            raise ValueError("c failed")

        # The transaction commits; a failing after-commit callback must NOT raise
        # (the committed result stands), so the block completes normally.
        committed = False
        async with ctx.tx_ctx.scope("mock"):
            await ctx.tx_ctx.run_or_defer(_a)
            await ctx.tx_ctx.run_or_defer(_b)
            await ctx.tx_ctx.run_or_defer(_c)
            committed = True

        # Every callback ran despite earlier failures, and the scope exited cleanly.
        assert order == ["a", "b", "c"]
        assert committed is True

    @pytest.mark.asyncio
    async def test_handler_notified_out_of_band_with_failures(
        self, mock_state: MockState
    ) -> None:
        captured: list[AfterCommitError] = []

        ctx = _build_ctx(mock_state, after_commit_error_handler=captured.append)

        async def _a() -> None:
            raise RuntimeError("a failed")

        async def _b() -> None:
            pass

        async def _c() -> None:
            raise ValueError("c failed")

        async with ctx.tx_ctx.scope("mock"):
            await ctx.tx_ctx.run_or_defer(_a)
            await ctx.tx_ctx.run_or_defer(_b)
            await ctx.tx_ctx.run_or_defer(_c)

        # The handler is notified exactly once, out-of-band, with only the failed
        # callbacks (by queue index), the committed operation having returned normally.
        assert len(captured) == 1
        report = captured[0]
        assert report.route == "mock"
        assert [f.index for f in report.failures] == [0, 2]
        assert [f.error for f in report.failures] == ["a failed", "c failed"]
        assert [f.callback.split(".")[-1] for f in report.failures] == ["_a", "_c"]

    @pytest.mark.asyncio
    async def test_raising_handler_is_suppressed(
        self, mock_state: MockState
    ) -> None:
        def _boom(_: AfterCommitError) -> None:
            raise RuntimeError("handler boom")

        ctx = _build_ctx(mock_state, after_commit_error_handler=_boom)

        async def _fail() -> None:
            raise ValueError("cb failed")

        # A raising handler must never turn a committed operation into a failure.
        completed = False
        async with ctx.tx_ctx.scope("mock"):
            await ctx.tx_ctx.run_or_defer(_fail)
            completed = True

        assert completed is True

    @pytest.mark.asyncio
    async def test_fatal_callback_reraises_after_all_ran(
        self, ctx: ExecutionContext
    ) -> None:
        ran: list[str] = []

        async def _domain_check() -> None:
            ran.append("check")
            raise ValueError("invariant violated")

        async def _effect() -> None:
            ran.append("effect")

        # A fatal (deliberate domain check) callback surfaces to the caller even though
        # the transaction committed, and every callback still ran first.
        with pytest.raises(ValueError, match="invariant violated"):
            async with ctx.tx_ctx.scope("mock"):
                await ctx.tx_ctx.run_or_defer(_domain_check, fatal=True)
                await ctx.tx_ctx.run_or_defer(_effect)

        assert ran == ["check", "effect"]


class TestAssertEnlisted:
    """A store whose writes don't commit in the ambient transaction fails closed."""

    @pytest.mark.asyncio
    async def test_unenlisted_resource_fails_closed(
        self, ctx: ExecutionContext
    ) -> None:
        class _NotEnlisted:
            def is_transactionally_enlisted(self) -> bool:
                return False

        async with ctx.tx_ctx.scope("mock"):
            with pytest.raises(CoreException) as ei:
                ctx.tx_ctx.assert_enlisted(_NotEnlisted(), what="Inbox route 'x'")

        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert ei.value.code == "core.tx.not_enlisted"

    @pytest.mark.asyncio
    async def test_enlisted_resource_passes(self, ctx: ExecutionContext) -> None:
        class _Enlisted:
            def is_transactionally_enlisted(self) -> bool:
                return True

        async with ctx.tx_ctx.scope("mock"):
            ctx.tx_ctx.assert_enlisted(_Enlisted(), what="x")  # no raise

    @pytest.mark.asyncio
    async def test_unreportable_resource_is_left_unchecked(
        self, ctx: ExecutionContext
    ) -> None:
        # A resource that cannot report enlistment is best-effort (not rejected).
        async with ctx.tx_ctx.scope("mock"):
            ctx.tx_ctx.assert_enlisted(object(), what="x")  # no raise

    @pytest.mark.asyncio
    async def test_requires_active_scope(self, ctx: ExecutionContext) -> None:
        class _Enlisted:
            def is_transactionally_enlisted(self) -> bool:
                return True

        with pytest.raises(CoreException):
            ctx.tx_ctx.assert_enlisted(_Enlisted(), what="x")  # no open scope


class TestTransactionCancellation:
    """Post-commit deferred callbacks are a critical section.

    Once the root transaction has committed, a task cancellation must not
    skip or tear the deferred drain (idempotency commits, after-commit
    dispatch); the drain runs to completion and the cancellation is
    re-raised afterwards. Cancellation during the body still rolls back.
    """

    @pytest.mark.asyncio
    async def test_cancel_during_drain_completes_all_callbacks(
        self, ctx: ExecutionContext
    ) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        ran: list[str] = []

        async def _slow() -> None:
            started.set()
            await release.wait()
            ran.append("slow")

        async def _second() -> None:
            ran.append("second")

        async def _op() -> None:
            async with ctx.tx_ctx.scope("mock"):
                await ctx.tx_ctx.run_or_defer(_slow)
                await ctx.tx_ctx.run_or_defer(_second)

        task = asyncio.create_task(_op())
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        release.set()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert ran == ["slow", "second"]

    @pytest.mark.asyncio
    async def test_cancel_during_body_skips_callbacks(
        self, ctx: ExecutionContext
    ) -> None:
        in_body = asyncio.Event()
        ran: list[str] = []

        async def _cb() -> None:
            ran.append("cb")

        async def _op() -> None:
            async with ctx.tx_ctx.scope("mock"):
                await ctx.tx_ctx.run_or_defer(_cb)
                in_body.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(_op())
        await in_body.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert ran == []

    @pytest.mark.asyncio
    async def test_cancellation_wins_over_callback_failure(
        self, ctx: ExecutionContext
    ) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        ran: list[str] = []

        async def _fail() -> None:
            started.set()
            await release.wait()
            raise RuntimeError("cb failed")

        async def _ok() -> None:
            ran.append("ok")

        async def _op() -> None:
            async with ctx.tx_ctx.scope("mock"):
                await ctx.tx_ctx.run_or_defer(_fail)
                await ctx.tx_ctx.run_or_defer(_ok)

        task = asyncio.create_task(_op())
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        release.set()

        # The cancellation propagates (not the aggregated after-commit
        # error), and the remaining callbacks still ran.
        with pytest.raises(asyncio.CancelledError):
            await task

        assert ran == ["ok"]


class TestNestedReadOnly:
    @pytest.mark.asyncio
    async def test_nested_conflicting_read_only_raises_precondition(
        self, ctx: ExecutionContext, mock_state: MockState
    ) -> None:
        with pytest.raises(CoreException) as ei:
            async with ctx.tx_ctx.scope("mock"):
                async with ctx.tx_ctx.scope("mock", read_only=True):
                    pass

        assert ei.value.kind is ExceptionKind.PRECONDITION
        assert ei.value.code == "tx_nested_read_only_conflict"

    @pytest.mark.asyncio
    async def test_nested_conflicting_read_write_inside_read_only_raises(
        self, ctx: ExecutionContext
    ) -> None:
        with pytest.raises(CoreException) as ei:
            async with ctx.tx_ctx.scope("mock", read_only=True):
                async with ctx.tx_ctx.scope("mock", read_only=False):
                    pass

        assert ei.value.kind is ExceptionKind.PRECONDITION

    @pytest.mark.asyncio
    async def test_nested_same_value_passes(self, ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock", read_only=True):
            async with ctx.tx_ctx.scope("mock", read_only=True):
                pass

        async with ctx.tx_ctx.scope("mock", read_only=False):
            async with ctx.tx_ctx.scope("mock", read_only=False):
                pass

    @pytest.mark.asyncio
    async def test_nested_unspecified_inherits_root(
        self, ctx: ExecutionContext
    ) -> None:
        async with ctx.tx_ctx.scope("mock", read_only=True), ctx.tx_ctx.scope("mock"):
            pass

        async with ctx.tx_ctx.scope("mock"), ctx.tx_ctx.scope("mock"):
            pass

    @pytest.mark.asyncio
    async def test_read_only_not_forwarded_to_nested_transaction(
        self, ctx: ExecutionContext, mock_state: MockState
    ) -> None:
        async with ctx.tx_ctx.scope("mock", read_only=True):
            async with ctx.tx_ctx.scope("mock", read_only=True):
                pass

        # Root opens read-only; the nested call gets no read_only option (the
        # mock adapter records its parameter default, False).
        assert mock_state.tx_read_only_calls == [True, False]


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
        port = ctx.tx_ctx.resolver("mock")
        assert port is not None

    def test_tx_resolver_accepts_str_enum_route(self, ctx: ExecutionContext) -> None:
        class TxRoute(StrEnum):
            MOCK = "mock"

        port = ctx.tx_ctx.resolver(TxRoute.MOCK)
        assert port is not None

    def test_storage(self, ctx: ExecutionContext) -> None:
        spec = StorageSpec(name="my-bucket")
        assert ctx.storage.query(spec) is not None
        assert ctx.storage.command(spec) is not None

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

        assert ctx.storage.query(StorageSpec(name=DocName.TEST)) is not None
        assert ctx.storage.command(StorageSpec(name=DocName.TEST)) is not None

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

        async with ctx.tx_ctx.scope(TxRoute.MOCK):
            pass
