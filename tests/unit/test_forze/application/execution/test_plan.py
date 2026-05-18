"""Unit tests for execution stage modeling and registry-authored runtime chains."""

from enum import StrEnum

import pytest

from forze.application.execution import Deps, ExecutionContext, Usecase, UsecaseRegistry
from forze.application.execution.engine import Stage
from forze.application.execution.plan import (
    WILDCARD,
    DagNode,
    MiddlewareSpec,
    OperationPlan,
    PlanDag,
    StepExplainKind,
)
from forze.base.errors import CoreError


class StubUsecase(Usecase[str, str]):
    async def main(self, args: str) -> str:
        return f"ok:{args}"


class BoomUsecase(Usecase[str, str]):
    async def main(self, _args: str) -> str:
        raise ValueError("boom")


def _noop_factory(_ctx):
    return None


def _resolve_authored(
    authored: UsecaseRegistry,
    op: str,
    ctx: ExecutionContext,
    factory,
    *,
    capability_execution_trace: list[object] | None = None,
):
    runtime = UsecaseRegistry.merge(
        authored,
        UsecaseRegistry({op: factory}),
        on_conflict="overwrite",
    )
    runtime.finalize()
    return runtime.resolve(
        op,
        ctx,
        capability_execution_trace=capability_execution_trace,
    )


class TestMiddlewareSpec:
    def test_priority_bounds(self) -> None:
        spec = MiddlewareSpec(priority=0, factory=_noop_factory)
        assert spec.priority == 0

    def test_priority_too_low_raises(self) -> None:
        with pytest.raises((ValueError, TypeError)):
            MiddlewareSpec(priority=int(-1e6), factory=_noop_factory)

    def test_priority_too_high_raises(self) -> None:
        with pytest.raises((ValueError, TypeError)):
            MiddlewareSpec(priority=int(1e6), factory=_noop_factory)


class TestOperationPlan:
    def test_default_operation_plan_has_empty_stages(self) -> None:
        plan = OperationPlan()

        assert plan.specs(Stage.before) == ()
        assert plan.specs(Stage.after_success) == ()
        assert plan.specs(Stage.finally_) == ()
        assert plan.specs(Stage.on_failure) == ()
        assert plan.specs(Stage.tx_before) == ()
        assert plan.specs(Stage.after_commit) == ()
        assert plan.tx is None

    def test_add_appends_to_stage(self) -> None:
        spec = MiddlewareSpec(priority=1, factory=_noop_factory)
        plan = OperationPlan().add(Stage.before, spec)

        assert plan.specs(Stage.before) == (spec,)

    def test_validate_tx_stage_without_route_raises(self) -> None:
        spec = MiddlewareSpec(priority=1, factory=_noop_factory)
        plan = OperationPlan().add(Stage.tx_before, spec)

        with pytest.raises(CoreError, match="tx_\\* or after_commit"):
            plan.validate()

    def test_validate_after_commit_without_tx_raises(self) -> None:
        spec = MiddlewareSpec(priority=1, factory=_noop_factory)
        plan = OperationPlan().add(Stage.after_commit, spec)

        with pytest.raises(
            CoreError, match="after_commit middlewares but tx\\(\\) is not enabled"
        ):
            plan.validate()

    def test_build_sorts_by_priority_descending(self) -> None:
        high = MiddlewareSpec(priority=10, factory=lambda _ctx: 1)
        low = MiddlewareSpec(priority=5, factory=lambda _ctx: 2)
        plan = OperationPlan().add(Stage.before, low).add(Stage.before, high)

        assert [spec.priority for spec in plan.build(Stage.before)] == [10, 5]

    def test_build_priority_collision_raises(self) -> None:
        plan = (
            OperationPlan()
            .add(Stage.before, MiddlewareSpec(priority=5, factory=lambda _ctx: 1))
            .add(Stage.before, MiddlewareSpec(priority=5, factory=lambda _ctx: 2))
        )

        with pytest.raises(CoreError, match="Priority collision"):
            plan.build(Stage.before)

    def test_build_dedupes_same_factory_priority(self) -> None:
        spec = MiddlewareSpec(priority=1, factory=_noop_factory)
        plan = OperationPlan().add(Stage.before, spec).add(Stage.before, spec)

        assert plan.build(Stage.before) == (spec,)

    def test_merge_combines_stages(self) -> None:
        p1 = OperationPlan().add(
            Stage.before, MiddlewareSpec(priority=1, factory=lambda _ctx: 1)
        )
        p2 = OperationPlan().add(
            Stage.after_success, MiddlewareSpec(priority=2, factory=lambda _ctx: 2)
        )
        merged = OperationPlan.merge(p1, p2)

        assert len(merged.specs(Stage.before)) == 1
        assert len(merged.specs(Stage.after_success)) == 1

    def test_merge_preserves_tx_route(self) -> None:
        merged = OperationPlan.merge(
            OperationPlan().with_tx("mock"),
            OperationPlan(),
        )

        assert merged.tx is not None
        assert merged.tx.route == "mock"

    def test_merge_conflicting_tx_routes_raises(self) -> None:
        left = OperationPlan().with_tx("a")
        right = OperationPlan().with_tx("b")

        with pytest.raises(CoreError, match="Conflicting transaction routes"):
            OperationPlan.merge(left, right)

    def test_merge_base_and_specific_allows_specific_tx_override(self) -> None:
        base = OperationPlan().with_tx("base")
        specific = OperationPlan().with_tx("specific")

        merged = OperationPlan.merge_base_and_specific(base, specific)

        assert merged.tx is not None
        assert merged.tx.route == "specific"


class TestRegistryStages:
    def test_before_adds_guard(self) -> None:
        def guard_factory(_ctx):
            async def guard(_args):
                return None

            return guard

        reg = UsecaseRegistry().before("get", guard_factory, priority=1)
        assert len(reg._stages["get"].specs(Stage.before)) == 1
        assert reg._stages["get"].specs(Stage.before)[0].priority == 1

    def test_after_success_adds_hook(self) -> None:
        def hook_factory(_ctx):
            async def hook(_args, _result):
                return None

            return hook

        reg = UsecaseRegistry().after_success("get", hook_factory, priority=2)
        assert len(reg._stages["get"].specs(Stage.after_success)) == 1
        assert reg._stages["get"].specs(Stage.after_success)[0].priority == 2

    def test_tx_enables_transaction(self) -> None:
        reg = UsecaseRegistry().tx("create", route="mock")
        assert reg._stages["create"].tx is not None
        assert reg._stages["create"].tx.route == "mock"

    def test_tx_accepts_str_enum_route(self) -> None:
        class TxRoute(StrEnum):
            MOCK = "mock"

        reg = UsecaseRegistry().tx("create", route=TxRoute.MOCK)
        assert reg._stages["create"].tx is not None
        assert reg._stages["create"].tx.route == "mock"

    def test_wrap_adds_middleware(self) -> None:
        def mw_factory(_ctx):
            async def wrap(next_fn, args):
                return await next_fn(args)

            return wrap

        reg = UsecaseRegistry().wrap("get", mw_factory, priority=1)
        assert len(reg._stages["get"].specs(Stage.wrap)) == 1

    def test_tx_stage_methods_add_specs(self) -> None:
        def guard_factory(_ctx):
            async def guard(_args):
                return None

            return guard

        def hook_factory(_ctx):
            async def hook(_args, _result):
                return None

            return hook

        def mw_factory(_ctx):
            async def wrap(next_fn, args):
                return await next_fn(args)

            return wrap

        reg = (
            UsecaseRegistry()
            .tx("create", route="mock")
            .tx_before("create", guard_factory, priority=1)
            .tx_after_success("create", hook_factory, priority=1)
            .tx_wrap("create", mw_factory, priority=1)
            .after_commit("create", hook_factory, priority=1)
        )

        assert len(reg._stages["create"].specs(Stage.tx_before)) == 1
        assert len(reg._stages["create"].specs(Stage.tx_after_success)) == 1
        assert len(reg._stages["create"].specs(Stage.tx_wrap)) == 1
        assert len(reg._stages["create"].specs(Stage.after_commit)) == 1

    def test_before_dag_adds_guard_specs(self) -> None:
        def guard_factory(_ctx):
            async def guard(_args):
                return None

            return guard

        dag = PlanDag(
            nodes=(
                DagNode(id="authn", factory=guard_factory, priority=10),
                DagNode(id="authz", factory=guard_factory, priority=5),
            ),
            edges=(("authn", "authz"),),
        )

        reg = UsecaseRegistry().before_dag("get", dag)

        assert len(reg._stages["get"].specs(Stage.before)) == 2
        assert [spec.priority for spec in reg._stages["get"].specs(Stage.before)] == [
            10,
            5,
        ]

    def test_explain_uses_stage_labels(self) -> None:
        def guard_factory(_ctx):
            async def guard(_args):
                return None

            return guard

        report = (
            UsecaseRegistry()
            .before("op", guard_factory, priority=1, step_label="g")
            .tx("op", route="mock")
            .explain("op")
        )

        assert any(
            step.stage == "before" and step.label == "g" for step in report.steps
        )
        assert any(
            step.stage == "tx_boundary" and step.kind == StepExplainKind.tx
            for step in report.steps
        )

    def test_explain_wildcard_raises(self) -> None:
        with pytest.raises(CoreError, match="wildcard"):
            UsecaseRegistry().before(WILDCARD, _noop_factory).explain(WILDCARD)

    def test_after_commit_without_tx_raises(self) -> None:
        def hook_factory(_ctx):
            async def hook(_args, _result):
                return None

            return hook

        with pytest.raises(
            CoreError, match="after_commit middlewares but tx\\(\\) is not enabled"
        ):
            UsecaseRegistry().after_commit("op", hook_factory).explain("op")

    def test_conflicting_tx_routes_raise_when_merging_same_op(self) -> None:
        left = UsecaseRegistry().tx("op", route="a")
        right = UsecaseRegistry().tx("op", route="b")

        with pytest.raises(CoreError, match="Conflicting transaction routes"):
            UsecaseRegistry.merge(left, right)._merged_operation_stages("op")

    def test_wildcard_tx_route_overridden_by_op_specific_route(self) -> None:
        merged = UsecaseRegistry.merge(
            UsecaseRegistry().tx(WILDCARD, route="base"),
            UsecaseRegistry().tx("op", route="specific"),
        )

        assert merged._merged_operation_stages("op").tx is not None
        assert merged._merged_operation_stages("op").tx.route == "specific"

    def test_tx_with_list(self) -> None:
        reg = UsecaseRegistry().tx(["get", "list", "search"], route="mock")
        for op in ("get", "list", "search"):
            assert reg._stages[op].tx is not None
            assert reg._stages[op].tx.route == "mock"

    def test_stage_methods_with_list(self) -> None:
        def guard_factory(_ctx):
            async def guard(_args):
                return None

            return guard

        def hook_factory(_ctx):
            async def hook(_args, _result):
                return None

            return hook

        def wrap_factory(_ctx):
            async def wrap(next_fn, args):
                return await next_fn(args)

            return wrap

        reg = (
            UsecaseRegistry()
            .before(["get", "list"], guard_factory, priority=3)
            .after_success(["x", "y"], hook_factory, priority=4)
            .wrap(["p", "q"], wrap_factory, priority=2)
            .tx(["create", "update"], route="r")
            .tx_before(["create", "update"], guard_factory, priority=1)
            .tx_after_success(["create", "update"], hook_factory, priority=1)
            .tx_wrap(["create", "update"], wrap_factory, priority=1)
        )

        for op in ("get", "list"):
            assert len(reg._stages[op].specs(Stage.before)) == 1

        for op in ("x", "y"):
            assert len(reg._stages[op].specs(Stage.after_success)) == 1

        for op in ("p", "q"):
            assert len(reg._stages[op].specs(Stage.wrap)) == 1

        for op in ("create", "update"):
            assert len(reg._stages[op].specs(Stage.tx_before)) == 1
            assert len(reg._stages[op].specs(Stage.tx_after_success)) == 1
            assert len(reg._stages[op].specs(Stage.tx_wrap)) == 1

    def test_list_mixed_str_and_str_enum(self) -> None:
        class Route(StrEnum):
            MOCK = "mock"

        def guard_factory(_ctx):
            async def guard(_args):
                return None

            return guard

        reg = UsecaseRegistry().before([Route.MOCK, "other"], guard_factory, priority=0)

        assert "mock" in reg._stages
        assert "other" in reg._stages

    @pytest.mark.asyncio
    async def test_resolve_with_guard_runs_guard(self) -> None:
        seen: list[str] = []

        def guard_factory(_ctx):
            async def guard(_args):
                seen.append("guard")

            return guard

        authored = UsecaseRegistry().before("get", guard_factory, priority=1)
        uc = _resolve_authored(
            authored,
            "get",
            ExecutionContext(deps=Deps()),
            lambda ctx: StubUsecase(ctx=ctx),
        )

        await uc("x")

        assert seen == ["guard"]

    @pytest.mark.asyncio
    async def test_resolve_with_after_success_runs_hook_and_keeps_result(self) -> None:
        seen: list[str] = []

        def hook_factory(_ctx):
            async def hook(args: str, result: str) -> None:
                seen.append(f"{args}:{result}")
                return None

            return hook

        authored = UsecaseRegistry().after_success("get", hook_factory, priority=1)
        uc = _resolve_authored(
            authored,
            "get",
            ExecutionContext(deps=Deps()),
            lambda ctx: StubUsecase(ctx=ctx),
        )

        assert await uc("x") == "ok:x"
        assert seen == ["x:ok:x"]

    @pytest.mark.asyncio
    async def test_resolve_with_tx_and_after_commit(
        self, stub_ctx: ExecutionContext
    ) -> None:
        seen: list[str] = []

        def hook_factory(_ctx):
            async def hook(_args, result: str) -> None:
                seen.append(result)
                return None

            return hook

        authored = (
            UsecaseRegistry()
            .tx("create", route="mock")
            .after_commit(
                "create",
                hook_factory,
                priority=1,
            )
        )
        uc = _resolve_authored(
            authored, "create", stub_ctx, lambda ctx: StubUsecase(ctx=ctx)
        )

        assert await uc("x") == "ok:x"
        assert seen == ["ok:x"]

    @pytest.mark.asyncio
    async def test_resolve_with_tx_stages(self, stub_ctx: ExecutionContext) -> None:
        seen: list[str] = []

        def tx_guard(_ctx):
            async def guard(_args):
                seen.append("tx_before")

            return guard

        def tx_hook(_ctx):
            async def hook(_args, _result):
                seen.append("tx_after_success")
                return None

            return hook

        authored = (
            UsecaseRegistry()
            .tx("create", route="mock")
            .tx_before("create", tx_guard, priority=1)
            .tx_after_success("create", tx_hook, priority=1)
        )
        uc = _resolve_authored(
            authored, "create", stub_ctx, lambda ctx: StubUsecase(ctx=ctx)
        )

        assert await uc("x") == "ok:x"
        assert seen == ["tx_before", "tx_after_success"]


class TestRegistryStagesFinallyOnFailure:
    @pytest.mark.asyncio
    async def test_outer_on_failure_then_outer_finally_on_error(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        seen: list[str] = []

        def on_fail(_ctx):
            async def hook(_args: str, _exc: Exception):
                seen.append("on_failure")

            return hook

        def fin(_ctx):
            async def hook(_args: str, _outcome) -> None:
                seen.append("finally")

            return hook

        authored = (
            UsecaseRegistry()
            .tx("create", route="mock")
            .finally_("create", fin, priority=1)
            .on_failure("create", on_fail, priority=1)
        )
        uc = _resolve_authored(
            authored, "create", stub_ctx, lambda ctx: BoomUsecase(ctx=ctx)
        )

        with pytest.raises(ValueError, match="boom"):
            await uc("x")

        assert seen == ["on_failure", "finally"]

    @pytest.mark.asyncio
    async def test_outer_finally_on_success_only(
        self, stub_ctx: ExecutionContext
    ) -> None:
        seen: list[str] = []

        def fin(_ctx):
            async def hook(_args: str, outcome) -> None:
                seen.append(type(outcome).__name__)

            return hook

        authored = (
            UsecaseRegistry()
            .tx("create", route="mock")
            .finally_("create", fin, priority=0)
        )
        uc = _resolve_authored(
            authored, "create", stub_ctx, lambda ctx: StubUsecase(ctx=ctx)
        )

        await uc("x")

        assert seen == ["Success"]

    @pytest.mark.asyncio
    async def test_after_commit_skipped_when_main_raises(
        self, stub_ctx: ExecutionContext
    ) -> None:
        seen: list[str] = []

        def ac(_ctx):
            async def hook(_args: str, _result: str) -> None:
                seen.append("after_commit")

            return hook

        authored = (
            UsecaseRegistry()
            .tx("create", route="mock")
            .after_commit("create", ac, priority=1)
        )
        uc = _resolve_authored(
            authored, "create", stub_ctx, lambda ctx: BoomUsecase(ctx=ctx)
        )

        with pytest.raises(ValueError):
            await uc("x")

        assert seen == []

    @pytest.mark.asyncio
    async def test_tx_on_failure_before_outer_on_failure(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        seen: list[str] = []

        def inner(_ctx):
            async def hook(_args: str, _exc: Exception) -> None:
                seen.append("tx")

            return hook

        def outer(_ctx):
            async def hook(_args: str, _exc: Exception) -> None:
                seen.append("outer")

            return hook

        authored = (
            UsecaseRegistry()
            .tx("create", route="mock")
            .on_failure("create", outer, priority=1)
            .tx_on_failure("create", inner, priority=1)
        )
        uc = _resolve_authored(
            authored, "create", stub_ctx, lambda ctx: BoomUsecase(ctx=ctx)
        )

        with pytest.raises(ValueError):
            await uc("x")

        assert seen == ["tx", "outer"]

    @pytest.mark.asyncio
    async def test_outer_hooks_without_tx(self, stub_ctx: ExecutionContext) -> None:
        seen: list[str] = []

        def on_fail(_ctx):
            async def hook(_args: str, _exc: Exception) -> None:
                seen.append("fail")

            return hook

        def fin(_ctx):
            async def hook(_args: str, outcome) -> None:
                seen.append(type(outcome).__name__)

            return hook

        authored = (
            UsecaseRegistry()
            .finally_("solo", fin, priority=0)
            .on_failure(
                "solo",
                on_fail,
                priority=0,
            )
        )

        with pytest.raises(ValueError):
            await _resolve_authored(
                authored, "solo", stub_ctx, lambda ctx: BoomUsecase(ctx=ctx)
            )("z")

        assert seen == ["fail", "Failure"]

        seen.clear()
        await _resolve_authored(
            authored, "solo", stub_ctx, lambda ctx: StubUsecase(ctx=ctx)
        )("z")
        assert seen == ["Success"]
