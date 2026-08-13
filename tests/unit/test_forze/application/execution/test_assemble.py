"""Unit tests for :func:`forze.application.execution.assemble.build_runtime`."""

from datetime import timedelta

import attrs
import pytest

from forze.application.contracts.deps import DepKey
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.execution import Handler, LifecycleStep
from forze.application.contracts.inventory import SpecRegistry
from forze.application.contracts.querying import CursorTokenCipher, CursorTokenSigner
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
        rt = build_runtime([lambda: Deps.plain({_A: 1}), lambda: Deps.plain({_B: 2})])

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
                [lambda: Deps.plain({_A: 1}), lambda: Deps.plain({_A: 2})],
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

        rt = build_runtime(lifecycle_steps=[step], lifecycle_concurrent=True)
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


# ----------------------- #


class TestBuildRuntimeOneOrMany:
    """Every collection takes a lone item as readily as a sequence of them."""

    def test_a_lone_module_needs_no_brackets(self) -> None:
        rt = build_runtime(lambda: Deps.plain({_A: 1}))

        assert rt.deps.resolve().provide(_A) == 1

    def test_modules_can_be_named(self) -> None:
        rt = build_runtime(deps_modules=[lambda: Deps.plain({_A: 1})])

        assert rt.deps.resolve().provide(_A) == 1

    def test_a_lone_deps_blob_needs_no_brackets(self) -> None:
        rt = build_runtime(deps=Deps.plain({_A: 1}))

        assert rt.deps.resolve().provide(_A) == 1

    @pytest.mark.asyncio
    async def test_a_lone_lifecycle_step_needs_no_brackets(self) -> None:
        startups: dict[str, int] = {}
        shutdowns: dict[str, int] = {}

        rt = build_runtime(lifecycle_steps=_counting_step("s", startups, shutdowns))

        async with rt.scope():
            assert startups == {"s": 1}

        assert shutdowns == {"s": 1}

    @pytest.mark.asyncio
    async def test_a_lone_lifecycle_module_needs_no_brackets(self) -> None:
        startups: dict[str, int] = {}
        shutdowns: dict[str, int] = {}
        step = _counting_step("m", startups, shutdowns)

        rt = build_runtime(lifecycle_modules=lambda: (step,))

        async with rt.scope():
            assert startups == {"m": 1}

        assert shutdowns == {"m": 1}

    def test_a_generator_of_modules_is_read_once(self) -> None:
        """Sequences are not privileged over any other iterable — a generator works too."""

        rt = build_runtime(m for m in (lambda: Deps.plain({_A: 1}), lambda: Deps.plain({_B: 2})))

        resolved = rt.deps.resolve()
        assert resolved.provide(_A) == 1
        assert resolved.provide(_B) == 2


# ----------------------- #


class TestBuildRuntimeSpecs:
    @staticmethod
    def _registry(name: str) -> SpecRegistry:
        return SpecRegistry().register(
            DocumentSpec(name=name, read=ReadDocument, write={"domain": Document}),
        )

    def test_contributions_are_merged(self) -> None:
        rt = build_runtime(
            MockDepsModule(),
            specs=[self._registry("orders"), self._registry("invoices")],
            allow_unregistered=True,
        )

        assert rt.spec_registry is not None
        assert {entry.name for entry in rt.spec_registry.entries} == {"orders", "invoices"}

    def test_merging_leaves_the_caller_registries_alone(self) -> None:
        """The merge must not fold into the first argument.

        ``SpecRegistry.merge`` mutates its receiver, so an in-place fold would leave an
        author's own registry silently carrying every kit's specs — visible only later,
        wherever that registry is used again.
        """

        mine = self._registry("orders")
        kit = self._registry("invoices")

        build_runtime(MockDepsModule(), specs=[mine, kit], allow_unregistered=True)

        assert {entry.name for entry in mine.freeze().entries} == {"orders"}
        assert {entry.name for entry in kit.freeze().entries} == {"invoices"}

    def test_a_lone_registry_is_frozen_for_the_caller(self) -> None:
        rt = build_runtime(
            MockDepsModule(),
            specs=self._registry("orders"),
            allow_unregistered=True,
        )

        assert rt.spec_registry is not None
        assert {entry.name for entry in rt.spec_registry.entries} == {"orders"}

    def test_an_already_frozen_registry_is_taken_as_is(self) -> None:
        rt = build_runtime(
            MockDepsModule(),
            specs=self._registry("orders").freeze(),
            allow_unregistered=True,
        )

        assert rt.spec_registry is not None
        assert {entry.name for entry in rt.spec_registry.entries} == {"orders"}

    def test_no_contributions_at_all_reaches_the_empty_inventory_refusal(self) -> None:
        """An empty list is an empty inventory, not a skipped check.

        Reconciliation already refuses that, and says why an empty registry is worse than
        no registry — so this shape is left to travel there rather than being caught early
        with a thinner message. ``specs=None`` remains the way to skip.
        """

        with pytest.raises(CoreException, match="inventory is EMPTY"):
            build_runtime(MockDepsModule(), specs=[])

    def test_a_frozen_registry_cannot_be_merged_with_others(self) -> None:
        """Refused rather than silently dropped: a frozen registry has no merge."""

        with pytest.raises(CoreException, match="frozen registry cannot be merged"):
            build_runtime(
                MockDepsModule(),
                specs=[self._registry("orders"), self._registry("invoices").freeze()],
                allow_unregistered=True,
            )


# ----------------------- #


class TestBuildRuntimePassThroughs:
    """The knobs that previously forced a caller off the assembler entirely."""

    def test_shutdown_step_timeout_propagates(self) -> None:
        rt = build_runtime(shutdown_step_timeout=timedelta(seconds=3))

        assert rt.shutdown_step_timeout == timedelta(seconds=3)

    def test_omitting_it_keeps_the_runtimes_own_default(self) -> None:
        """Not defaulted at the assembler, so the runtime stays the single source."""

        assert build_runtime().shutdown_step_timeout == ExecutionRuntime().shutdown_step_timeout

    def test_cursor_token_signer_and_cipher_propagate(self) -> None:
        signer = CursorTokenSigner(secret=b"0" * 32)
        cipher = CursorTokenCipher(secret=b"1" * 32)

        rt = build_runtime(cursor_token_signer=signer, cursor_token_cipher=cipher)

        assert rt.cursor_token_signer is signer
        assert rt.cursor_token_cipher is cipher
