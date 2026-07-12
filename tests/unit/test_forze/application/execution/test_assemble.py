"""Unit tests for :func:`forze.application.execution.assemble.build_runtime`."""

import attrs
import pytest

from forze.application.contracts.deps import DepKey
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.execution import Handler, LifecycleStep
from forze.application.execution import (
    Deps,
    ExecutionContext,
    ExecutionRuntime,
    build_runtime,
)
from forze.application.execution.context.transaction import AfterCommitError
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule

# ----------------------- #

_A = DepKey[int]("a")
_B = DepKey[int]("b")


def _counting_step(
    name: str,
    startups: dict[str, int],
    shutdowns: dict[str, int],
) -> LifecycleStep:
    async def up(_ctx) -> None:
        startups[name] = startups.get(name, 0) + 1

    async def down(_ctx) -> None:
        shutdowns[name] = shutdowns.get(name, 0) + 1

    return LifecycleStep(id=name, startup=up, shutdown=down)


# ----------------------- #


class TestBuildRuntimeDeps:
    def test_modules_only(self) -> None:
        rt = build_runtime(lambda: Deps.plain({_A: 1}), lambda: Deps.plain({_B: 2}))

        assert isinstance(rt, ExecutionRuntime)
        resolved = rt.deps.resolve()
        assert resolved.provide(_A) == 1
        assert resolved.provide(_B) == 2

    def test_deps_blobs_only(self) -> None:
        rt = build_runtime(deps=[Deps.plain({_A: 1}), Deps.plain({_B: 2})])

        resolved = rt.deps.resolve()
        assert resolved.provide(_A) == 1
        assert resolved.provide(_B) == 2

    def test_modules_and_deps_blobs_mixed(self) -> None:
        rt = build_runtime(
            lambda: Deps.plain({_A: 1}),
            deps=[Deps.plain({_B: 2})],
        )

        resolved = rt.deps.resolve()
        assert resolved.provide(_A) == 1
        assert resolved.provide(_B) == 2

    def test_validation_stays_at_freeze_time(self) -> None:
        # The assembler adds no validation of its own: a provider conflict
        # surfaces as the same freeze-time error as the hand-rolled dance.
        with pytest.raises(CoreException, match="Conflicting plain"):
            build_runtime(
                lambda: Deps.plain({_A: 1}),
                lambda: Deps.plain({_A: 2}),
            )


class TestBuildRuntimeLifecycle:
    @pytest.mark.asyncio
    async def test_lifecycle_modules(self) -> None:
        startups: dict[str, int] = {}
        shutdowns: dict[str, int] = {}

        def module() -> tuple[LifecycleStep, ...]:
            return (_counting_step("m", startups, shutdowns),)

        rt = build_runtime(lifecycle_modules=[module])

        async with rt.scope():
            assert startups == {"m": 1}

        assert shutdowns == {"m": 1}

    @pytest.mark.asyncio
    async def test_lifecycle_steps(self) -> None:
        startups: dict[str, int] = {}
        shutdowns: dict[str, int] = {}

        rt = build_runtime(lifecycle_steps=[_counting_step("s", startups, shutdowns)])

        async with rt.scope():
            assert startups == {"s": 1}

        assert shutdowns == {"s": 1}

    @pytest.mark.asyncio
    async def test_lifecycle_modules_and_steps_combined(self) -> None:
        startups: dict[str, int] = {}
        shutdowns: dict[str, int] = {}

        def module() -> tuple[LifecycleStep, ...]:
            return (_counting_step("m", startups, shutdowns),)

        rt = build_runtime(
            lifecycle_modules=[module],
            lifecycle_steps=[_counting_step("s", startups, shutdowns)],
        )

        async with rt.scope():
            assert startups == {"m": 1, "s": 1}

        assert shutdowns == {"m": 1, "s": 1}

    def test_concurrent_lifecycle_flag_propagates(self) -> None:
        step = LifecycleStep(id="s")

        rt = build_runtime(lifecycle_steps=[step], concurrent_lifecycle=True)
        assert rt.lifecycle.concurrent is True

        rt_default = build_runtime(lifecycle_steps=[step])
        assert rt_default.lifecycle.concurrent is False


class TestBuildRuntimeKnobs:
    def test_cache_knobs_propagate(self) -> None:
        rt = build_runtime(
            cache_resolved_operations=False,
            cache_resolved_ports=False,
        )

        assert rt.cache_resolved_operations is False
        assert rt.cache_resolved_ports is False

    def test_cache_knobs_default_on(self) -> None:
        rt = build_runtime()

        assert rt.cache_resolved_operations is True
        assert rt.cache_resolved_ports is True

    def test_cache_knobs_reach_the_context(self) -> None:
        rt = build_runtime(cache_resolved_operations=False)
        rt.create_context()
        ctx = rt.get_context()

        assert ctx.cache_operations is False
        assert ctx.cache_ports is True


class TestBuildRuntimeEmpty:
    @pytest.mark.asyncio
    async def test_empty_everything_is_a_working_runtime(self) -> None:
        rt = build_runtime()

        async with rt.scope():
            assert rt.get_context() is not None

        with pytest.raises(CoreException, match="not set"):
            rt.get_context()


# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _EchoHandler(Handler[str, str]):
    async def __call__(self, args: str) -> str:
        return f"result:{args}"


_DOC_SPEC = DocumentSpec(
    name="things",
    read=ReadDocument,
    write={
        "domain": Document,
        "create_cmd": CreateDocumentCmd,
        "update_cmd": CreateDocumentCmd,
    },
)


class TestBuildRuntimeEndToEnd:
    @pytest.mark.asyncio
    async def test_runs_operation_against_mock_deps(self) -> None:
        registry = OperationRegistry(
            handlers={"echo": lambda _ctx: _EchoHandler()},
        ).freeze()
        rt = build_runtime(MockDepsModule())

        async with rt.scope():
            resolved = registry.resolve("echo", rt.get_context())
            assert await resolved("foo") == "result:foo"

    @pytest.mark.asyncio
    async def test_document_roundtrip_against_mock_deps(self) -> None:
        rt = build_runtime(MockDepsModule())

        async with rt.scope():
            ctx = rt.get_context()
            created = await ctx.document.command(_DOC_SPEC).create(CreateDocumentCmd())
            fetched = await ctx.document.query(_DOC_SPEC).get(created.id)
            assert fetched.id == created.id


# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _CommitThenFailEffectHandler(Handler[str, str]):
    """Commits a transaction whose non-fatal after-commit callback raises."""

    ctx: ExecutionContext

    async def __call__(self, args: str) -> str:
        async def _failing_effect() -> None:
            raise RuntimeError("effect failed")

        async with self.ctx.tx_ctx.scope("mock"):
            await self.ctx.tx_ctx.run_or_defer(_failing_effect)

        return f"committed:{args}"


class TestBuildRuntimeAfterCommitErrorHandler:
    def test_default_is_none(self) -> None:
        rt = build_runtime()

        assert rt.after_commit_error_handler is None

        rt.create_context()
        assert rt.get_context().after_commit_error_handler is None

    def test_handler_reaches_the_context(self) -> None:
        captured: list[AfterCommitError] = []

        rt = build_runtime(after_commit_error_handler=captured.append)
        rt.create_context()

        assert rt.get_context().after_commit_error_handler is not None

    @pytest.mark.asyncio
    async def test_handler_receives_failed_after_commit_effects(self) -> None:
        captured: list[AfterCommitError] = []

        registry = OperationRegistry(
            handlers={"commit": lambda ctx: _CommitThenFailEffectHandler(ctx=ctx)},
        ).freeze()
        rt = build_runtime(
            MockDepsModule(),
            after_commit_error_handler=captured.append,
        )

        async with rt.scope():
            resolved = registry.resolve("commit", rt.get_context())

            # The failed effect never discards the committed result...
            assert await resolved("foo") == "committed:foo"

        # ...and the handler is notified out-of-band with the failure.
        assert len(captured) == 1
        report = captured[0]
        assert report.route == "mock"
        assert [f.error for f in report.failures] == ["effect failed"]
