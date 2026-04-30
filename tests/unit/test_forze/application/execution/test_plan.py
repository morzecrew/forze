"""Unit tests for forze.application.execution.plan."""

from enum import StrEnum

import pytest

from forze.application.execution import Deps, ExecutionContext, Usecase
from forze.application.execution.plan import (
    WILDCARD,
    MiddlewareSpec,
    OperationPlan,
    TransactionSpec,
    UsecasePlan,
)

# ----------------------- #


class StubUsecase(Usecase[str, str]):
    async def main(self, args: str) -> str:
        return f"ok:{args}"


class BoomUsecase(Usecase[str, str]):
    async def main(self, args: str) -> str:
        raise ValueError("boom")


class TestMiddlewareSpec:
    """Tests for MiddlewareSpec."""

    def test_priority_bounds(self) -> None:
        def factory(ctx):
            return None

        spec = MiddlewareSpec(priority=0, factory=factory)
        assert spec.priority == 0

    def test_priority_too_low_raises(self) -> None:
        def factory(ctx):
            return None

        with pytest.raises((ValueError, TypeError)):
            MiddlewareSpec(priority=int(-1e6), factory=factory)

    def test_priority_too_high_raises(self) -> None:
        def factory(ctx):
            return None

        with pytest.raises((ValueError, TypeError)):
            MiddlewareSpec(priority=int(1e6), factory=factory)


class TestDerivedDispatchEdges:
    """Dispatch edges derived from :class:`UsecaseDelegate` on plan builders."""

    def test_after_with_delegate_records_edge(self) -> None:
        from forze.application.execution import UsecaseDelegate, UsecaseRegistry

        reg = UsecaseRegistry()
        fac = UsecaseDelegate[str, str, str, str](
            target_op="child",
            map_in=lambda x, y: x,
        ).effect_factory(reg)
        plan = UsecasePlan().after("parent", fac)
        assert ("parent", "child") in plan.derived_dispatch_edges()


class TestOperationPlan:
    """Tests for OperationPlan."""

    def test_default_operation_plan_has_empty_buckets(self) -> None:
        op = OperationPlan()
        assert op.outer_before == ()
        assert op.outer_after == ()
        assert op.outer_finally == ()
        assert op.outer_on_failure == ()
        assert op.in_tx_finally == ()
        assert op.in_tx_on_failure == ()
        assert op.tx is None

    def test_add_appends_to_bucket(self) -> None:
        def factory(ctx):
            return None

        spec = MiddlewareSpec(priority=1, factory=factory)
        plan = OperationPlan().add("outer_before", spec)
        assert len(plan.outer_before) == 1
        assert plan.outer_before[0].priority == 1

    def test_add_invalid_bucket_raises(self) -> None:
        from forze.base.errors import CoreError

        def factory(ctx):
            return None

        spec = MiddlewareSpec(priority=1, factory=factory)
        with pytest.raises(CoreError, match="Invalid bucket"):
            OperationPlan().add("invalid_bucket", spec)

    def test_validate_in_tx_without_tx_raises(self) -> None:
        from forze.application.execution.middleware import GuardMiddleware
        from forze.base.errors import CoreError

        async def noop_guard(args):
            pass

        def factory(ctx):
            return GuardMiddleware(guard=noop_guard)

        spec = MiddlewareSpec(priority=1, factory=factory)
        plan = OperationPlan(in_tx_before=(spec,))
        with pytest.raises(CoreError, match="tx.*not enabled"):
            plan.validate()

    def test_validate_in_tx_finally_without_tx_raises(self) -> None:
        from forze.application.execution.middleware import FinallyMiddleware
        from forze.base.errors import CoreError

        async def hook(args, outcome):
            pass

        def factory(ctx):
            return FinallyMiddleware(hook=hook)

        spec = MiddlewareSpec(priority=1, factory=factory)
        plan = OperationPlan(in_tx_finally=(spec,))
        with pytest.raises(CoreError, match="tx.*not enabled"):
            plan.validate()

    def test_validate_in_tx_on_failure_without_tx_raises(self) -> None:
        from forze.application.execution.middleware import OnFailureMiddleware
        from forze.base.errors import CoreError

        async def hook(args, exc):
            pass

        def factory(ctx):
            return OnFailureMiddleware(hook=hook)

        spec = MiddlewareSpec(priority=1, factory=factory)
        plan = OperationPlan(in_tx_on_failure=(spec,))
        with pytest.raises(CoreError, match="tx.*not enabled"):
            plan.validate()

    def test_build_sorts_by_priority_descending(self) -> None:
        def f1(ctx):
            return None

        def f2(ctx):
            return None

        s1 = MiddlewareSpec(priority=10, factory=f1)
        s2 = MiddlewareSpec(priority=5, factory=f2)
        plan = OperationPlan(outer_before=(s1, s2))
        built = plan.build("outer_before")
        assert built[0].priority == 10
        assert built[1].priority == 5

    def test_merge_combines_plans(self) -> None:
        def f1(ctx):
            return None

        def f2(ctx):
            return None

        p1 = OperationPlan(outer_before=(MiddlewareSpec(priority=1, factory=f1),))
        p2 = OperationPlan(outer_before=(MiddlewareSpec(priority=2, factory=f2),))
        merged = OperationPlan.merge(p1, p2)
        assert len(merged.outer_before) == 2

    def test_build_priority_collision_raises(self) -> None:
        from forze.base.errors import CoreError

        def f1(ctx):
            return None

        def f2(ctx):
            return None

        s1 = MiddlewareSpec(priority=5, factory=f1)
        s2 = MiddlewareSpec(priority=5, factory=f2)
        plan = OperationPlan(outer_before=(s1, s2))
        with pytest.raises(CoreError, match="Priority collision"):
            plan.build("outer_before")

    def test_build_invalid_bucket_raises(self) -> None:
        from forze.base.errors import CoreError

        plan = OperationPlan()
        with pytest.raises(CoreError, match="Invalid bucket"):
            plan.build("invalid_bucket")

    def test_build_dedupes_same_factory_priority(self) -> None:
        def f1(ctx):
            return None

        spec = MiddlewareSpec(priority=1, factory=f1)
        plan = OperationPlan(outer_before=(spec, spec))
        built = plan.build("outer_before")
        assert len(built) == 1

    def test_merge_preserves_tx(self) -> None:
        p1 = OperationPlan(tx=TransactionSpec(route="mock"))
        p2 = OperationPlan()
        merged = OperationPlan.merge(p1, p2)
        assert merged.tx is not None
        assert merged.tx.route == "mock"

    def test_transaction_spec_accepts_str_enum_route(self) -> None:
        class TxRoute(StrEnum):
            MOCK = "mock"

        plan = OperationPlan(tx=TransactionSpec(route=TxRoute.MOCK))
        assert plan.tx is not None
        assert plan.tx.route == TxRoute.MOCK
        assert str(plan.tx.route) == "mock"

    def test_merge_from_instance_includes_self(self) -> None:
        """Instance merge (p1.merge(p2)) includes p1 in the result."""

        def f1(ctx):
            return None

        def f2(ctx):
            return None

        p1 = OperationPlan(
            outer_before=(MiddlewareSpec(priority=1, factory=f1),),
            tx=TransactionSpec(route="mock"),
        )
        p2 = OperationPlan(outer_after=(MiddlewareSpec(priority=2, factory=f2),))
        merged = p1.merge(p2)
        assert len(merged.outer_before) == 1
        assert len(merged.outer_after) == 1
        assert merged.tx is not None

    def test_merge_from_instance_single_arg(self) -> None:
        """Instance merge with one other plan: p1.merge(p2) equals OperationPlan.merge(p1, p2)."""

        def f1(ctx):
            return None

        def f2(ctx):
            return None

        p1 = OperationPlan(outer_before=(MiddlewareSpec(priority=1, factory=f1),))
        p2 = OperationPlan(outer_before=(MiddlewareSpec(priority=2, factory=f2),))
        via_class = OperationPlan.merge(p1, p2)
        via_instance = p1.merge(p2)
        assert len(via_class.outer_before) == len(via_instance.outer_before) == 2


class TestUsecasePlan:
    """Tests for UsecasePlan."""

    def test_before_adds_guard(self) -> None:
        def guard(ctx: ExecutionContext):
            async def _guard(args):
                pass

            return _guard

        plan = UsecasePlan().before("get", guard, priority=1)
        assert len(plan.ops["get"].outer_before) == 1
        assert plan.ops["get"].outer_before[0].priority == 1

    def test_after_adds_effect(self) -> None:
        def effect(ctx: ExecutionContext):
            async def _effect(args, res):
                return res

            return _effect

        plan = UsecasePlan().after("get", effect, priority=2)
        assert len(plan.ops["get"].outer_after) == 1
        assert plan.ops["get"].outer_after[0].priority == 2

    def test_merge_combines_plans(self) -> None:
        def guard_a(ctx: ExecutionContext):
            async def _guard(args):
                pass

            return _guard

        def guard_b(ctx: ExecutionContext):
            async def _guard(args):
                pass

            return _guard

        plan_a = UsecasePlan().before("get", guard_a, priority=1)
        plan_b = UsecasePlan().before("create", guard_b, priority=1)
        merged = UsecasePlan.merge(plan_a, plan_b)
        assert len(merged.ops["get"].outer_before) == 1
        assert len(merged.ops["create"].outer_before) == 1

    def test_merge_with_wildcard_base(self) -> None:
        def base_guard(ctx):
            async def guard(args):
                pass

            return guard

        def op_guard(ctx):
            async def guard(args):
                pass

            return guard

        base = UsecasePlan().before(WILDCARD, base_guard, priority=0)
        op_specific = UsecasePlan().before("get", op_guard, priority=1)
        merged = UsecasePlan.merge(base, op_specific)
        assert WILDCARD in merged.ops
        assert "get" in merged.ops
        assert len(merged.ops[WILDCARD].outer_before) == 1
        assert len(merged.ops["get"].outer_before) == 1

    def test_merge_from_instance_includes_self(self) -> None:
        """Instance merge (plan_a.merge(plan_b)) includes plan_a in the result."""

        def guard_a(ctx: ExecutionContext):
            async def _guard(args):
                pass

            return _guard

        def guard_b(ctx: ExecutionContext):
            async def _guard(args):
                pass

            return _guard

        plan_a = UsecasePlan().before("get", guard_a, priority=1)
        plan_b = UsecasePlan().before("create", guard_b, priority=1)
        merged = plan_a.merge(plan_b)
        assert "get" in merged.ops
        assert "create" in merged.ops
        assert len(merged.ops["get"].outer_before) == 1
        assert len(merged.ops["create"].outer_before) == 1

    def test_merge_from_instance_equals_class_merge(self) -> None:
        """plan_a.merge(plan_b) equals UsecasePlan.merge(plan_a, plan_b)."""

        def guard_a(ctx: ExecutionContext):
            async def _guard(args):
                pass

            return _guard

        def guard_b(ctx: ExecutionContext):
            async def _guard(args):
                pass

            return _guard

        plan_a = UsecasePlan().before("get", guard_a, priority=1)
        plan_b = UsecasePlan().before("create", guard_b, priority=1)
        via_class = UsecasePlan.merge(plan_a, plan_b)
        via_instance = plan_a.merge(plan_b)
        assert set(via_class.ops.keys()) == set(via_instance.ops.keys())
        assert len(via_instance.ops["get"].outer_before) == 1
        assert len(via_instance.ops["create"].outer_before) == 1

    def test_tx_enables_transaction(self) -> None:
        plan = UsecasePlan().tx("create", route="mock")
        assert plan.ops["create"].tx is not None
        assert plan.ops["create"].tx.route == "mock"

    def test_tx_accepts_str_enum_route(self) -> None:
        class TxRoute(StrEnum):
            MOCK = "mock"

        plan = UsecasePlan().tx("create", route=TxRoute.MOCK)
        assert plan.ops["create"].tx is not None
        assert plan.ops["create"].tx.route == TxRoute.MOCK

    def test_resolve_builds_composed_usecase(self) -> None:
        ctx = ExecutionContext(deps=Deps())
        plan = UsecasePlan()
        uc = plan.resolve("get", ctx, lambda ctx: StubUsecase(ctx=ctx))
        assert uc is not None
        assert isinstance(uc, StubUsecase)

    @pytest.mark.asyncio
    async def test_resolve_with_guard_runs_guard(self) -> None:
        seen: list[str] = []

        def guard_factory(ctx):
            async def guard(args):
                seen.append("guard")

            return guard

        ctx = ExecutionContext(deps=Deps())
        plan = UsecasePlan().before("get", guard_factory, priority=1)
        uc = plan.resolve("get", ctx, lambda ctx: StubUsecase(ctx=ctx))
        await uc("x")
        assert seen == ["guard"]

    @pytest.mark.asyncio
    async def test_resolve_with_after_runs_effect(self) -> None:
        seen: list[str] = []

        def effect_factory(ctx):
            async def effect(args, res):
                seen.append("after")
                return res

            return effect

        ctx = ExecutionContext(deps=Deps())
        plan = UsecasePlan().after("get", effect_factory, priority=1)
        uc = plan.resolve("get", ctx, lambda ctx: StubUsecase(ctx=ctx))
        result = await uc("x")
        assert result == "ok:x"
        assert seen == ["after"]

    def test_resolve_wildcard_raises(self) -> None:
        from forze.base.errors import CoreError

        ctx = ExecutionContext(deps=Deps())
        plan = UsecasePlan()
        with pytest.raises(CoreError, match="wildcard"):
            plan.resolve(WILDCARD, ctx, lambda ctx: StubUsecase(ctx=ctx))

    def test_wrap_adds_middleware(self) -> None:
        from forze.application.execution.middleware import GuardMiddleware

        def mw_factory(ctx):
            async def guard(args):
                pass

            return GuardMiddleware(guard=guard)

        plan = UsecasePlan().wrap("get", mw_factory, priority=1)
        assert len(plan.ops["get"].outer_wrap) == 1

    def test_in_tx_before_adds_guard(self) -> None:
        def guard_factory(ctx):
            async def guard(args):
                pass

            return guard

        plan = (
            UsecasePlan().tx("create", route="mock").in_tx_before("create", guard_factory, priority=1)
        )
        assert plan.ops["create"].tx is not None
        assert len(plan.ops["create"].in_tx_before) == 1

    def test_in_tx_after_adds_effect(self) -> None:
        def effect_factory(ctx):
            async def effect(args, res):
                return res

            return effect

        plan = (
            UsecasePlan().tx("create", route="mock").in_tx_after("create", effect_factory, priority=1)
        )
        assert len(plan.ops["create"].in_tx_after) == 1

    def test_in_tx_wrap_adds_middleware(self) -> None:
        from forze.application.execution.middleware import GuardMiddleware

        def mw_factory(ctx):
            async def guard(args):
                pass

            return GuardMiddleware(guard=guard)

        plan = UsecasePlan().tx("create", route="mock").in_tx_wrap("create", mw_factory, priority=1)
        assert len(plan.ops["create"].in_tx_wrap) == 1

    def test_after_commit_adds_effect(self) -> None:
        def effect_factory(ctx):
            async def effect(args, res):
                return res

            return effect

        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .after_commit("create", effect_factory, priority=1)
        )
        assert len(plan.ops["create"].after_commit) == 1

    @pytest.mark.asyncio
    async def test_resolve_with_tx_and_after_commit(self, stub_ctx) -> None:
        seen: list[str] = []

        def effect_factory(ctx):
            async def effect(args, res):
                seen.append("after_commit")
                return res

            return effect

        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .after_commit("create", effect_factory, priority=1)
        )
        uc = plan.resolve("create", stub_ctx, lambda ctx: StubUsecase(ctx=ctx))
        result = await uc("x")
        assert result == "ok:x"
        assert "after_commit" in seen

    @pytest.mark.asyncio
    async def test_resolve_with_in_tx_chain(self, stub_ctx) -> None:
        seen: list[str] = []

        def in_tx_guard(ctx):
            async def guard(args):
                seen.append("in_tx_before")

            return guard

        def in_tx_effect(ctx):
            async def effect(args, res):
                seen.append("in_tx_after")
                return res

            return effect

        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .in_tx_before("create", in_tx_guard, priority=1)
            .in_tx_after("create", in_tx_effect, priority=1)
        )
        uc = plan.resolve("create", stub_ctx, lambda ctx: StubUsecase(ctx=ctx))
        result = await uc("x")
        assert result == "ok:x"
        assert "in_tx_before" in seen
        assert "in_tx_after" in seen

    def test_resolve_after_commit_non_effect_middleware_raises(self, stub_ctx) -> None:
        from forze.application.execution.middleware import GuardMiddleware
        from forze.base.errors import CoreError

        async def guard(args):
            pass

        def bad_factory(ctx):
            return GuardMiddleware(guard=guard)

        op_plan = OperationPlan(tx=TransactionSpec(route="mock")).add(
            "after_commit",
            MiddlewareSpec(priority=1, factory=bad_factory),
        )
        plan = UsecasePlan(ops={"create": op_plan})
        with pytest.raises(CoreError, match="Expected EffectMiddleware"):
            plan.resolve("create", stub_ctx, lambda ctx: StubUsecase(ctx=ctx))


def _guards3() -> tuple:
    def g0(ctx: ExecutionContext):
        async def _guard(args):
            pass

        return _guard

    def g1(ctx: ExecutionContext):
        async def _guard(args):
            pass

        return _guard

    def g2(ctx: ExecutionContext):
        async def _guard(args):
            pass

        return _guard

    return (g0, g1, g2)


def _effects3() -> tuple:
    def e0(ctx: ExecutionContext):
        async def _effect(args, res):
            return res

        return _effect

    def e1(ctx: ExecutionContext):
        async def _effect(args, res):
            return res

        return _effect

    def e2(ctx: ExecutionContext):
        async def _effect(args, res):
            return res

        return _effect

    return (e0, e1, e2)


class TestUsecasePlanPipelines:
    """UsecasePlan *_pipeline methods: batch attach with stepped priorities."""

    def test_before_pipeline_adds_guards_with_stepped_priority(self) -> None:
        g0, g1, g2 = _guards3()
        plan = UsecasePlan().before_pipeline("get", [g0, g1, g2], first_priority=100)
        specs = plan.ops["get"].outer_before
        assert [s.priority for s in specs] == [100, 90, 80]

    def test_after_pipeline_adds_effects_with_stepped_priority(self) -> None:
        e0, e1, e2 = _effects3()
        plan = UsecasePlan().after_pipeline("get", [e0, e1, e2], first_priority=50)
        assert [s.priority for s in plan.ops["get"].outer_after] == [50, 40, 30]

    def test_wrap_pipeline_adds_middlewares_with_stepped_priority(self) -> None:
        from forze.application.execution.middleware import GuardMiddleware

        def m0(ctx):
            async def guard(args):
                pass

            return GuardMiddleware(guard=guard)

        def m1(ctx):
            async def guard(args):
                pass

            return GuardMiddleware(guard=guard)

        plan = UsecasePlan().wrap_pipeline("get", [m0, m1], first_priority=7)
        assert [s.priority for s in plan.ops["get"].outer_wrap] == [7, -3]

    def test_in_tx_before_pipeline(self) -> None:
        g0, g1, _ = _guards3()
        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .in_tx_before_pipeline("create", [g0, g1], first_priority=20)
        )
        assert [s.priority for s in plan.ops["create"].in_tx_before] == [20, 10]

    def test_in_tx_after_pipeline(self) -> None:
        e0, e1, e2 = _effects3()
        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .in_tx_after_pipeline("create", [e0, e1, e2], first_priority=0)
        )
        assert [s.priority for s in plan.ops["create"].in_tx_after] == [0, -10, -20]

    def test_in_tx_wrap_pipeline(self) -> None:
        from forze.application.execution.middleware import GuardMiddleware

        def m0(ctx):
            async def guard(args):
                pass

            return GuardMiddleware(guard=guard)

        def m1(ctx):
            async def guard(args):
                pass

            return GuardMiddleware(guard=guard)

        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .in_tx_wrap_pipeline("create", [m0, m1], first_priority=15)
        )
        assert [s.priority for s in plan.ops["create"].in_tx_wrap] == [15, 5]

    def test_after_commit_pipeline(self) -> None:
        e0, e1, _e2 = _effects3()
        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .after_commit_pipeline("create", [e0, e1], first_priority=2)
        )
        assert [s.priority for s in plan.ops["create"].after_commit] == [2, -8]

    def test_outer_finally_pipeline_priorities(self) -> None:
        def f0(ctx: ExecutionContext):
            async def _hook(args, outcome):
                pass

            return _hook

        def f1(ctx: ExecutionContext):
            async def _hook(args, outcome):
                pass

            return _hook

        plan = UsecasePlan().outer_finally_pipeline("get", [f0, f1], first_priority=11)
        assert [s.priority for s in plan.ops["get"].outer_finally] == [11, 1]

    def test_outer_on_failure_pipeline_priorities(self) -> None:
        def h0(ctx: ExecutionContext):
            async def _hook(args, exc):
                pass

            return _hook

        def h1(ctx: ExecutionContext):
            async def _hook(args, exc):
                pass

            return _hook

        plan = UsecasePlan().outer_on_failure_pipeline("get", [h0, h1], first_priority=4)
        assert [s.priority for s in plan.ops["get"].outer_on_failure] == [4, -6]

    def test_in_tx_finally_pipeline_priorities(self) -> None:
        def f0(ctx: ExecutionContext):
            async def _hook(args, outcome):
                pass

            return _hook

        def f1(ctx: ExecutionContext):
            async def _hook(args, outcome):
                pass

            return _hook

        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .in_tx_finally_pipeline("create", [f0, f1], first_priority=8)
        )
        assert [s.priority for s in plan.ops["create"].in_tx_finally] == [8, -2]

    def test_in_tx_on_failure_pipeline_priorities(self) -> None:
        def h0(ctx: ExecutionContext):
            async def _hook(args, exc):
                pass

            return _hook

        def h1(ctx: ExecutionContext):
            async def _hook(args, exc):
                pass

            return _hook

        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .in_tx_on_failure_pipeline("create", [h0, h1], first_priority=3)
        )
        assert [s.priority for s in plan.ops["create"].in_tx_on_failure] == [3, -7]

    def test_empty_pipeline_does_not_create_op(self) -> None:
        base = UsecasePlan()
        plan = base.before_pipeline("get", [])
        assert plan is base
        assert "get" not in plan.ops

    def test_pipeline_does_not_mutate_original(self) -> None:
        g0, g1, _ = _guards3()
        base = UsecasePlan()
        derived = base.before_pipeline("get", [g0, g1])
        assert "get" not in base.ops
        assert len(derived.ops["get"].outer_before) == 2

    @pytest.mark.asyncio
    async def test_resolve_before_pipeline_runs_guards_in_order(self) -> None:
        seen: list[int] = []

        def g(i: int):
            def factory(ctx: ExecutionContext):
                async def guard(args):
                    seen.append(i)

                return guard

            return factory

        ctx = ExecutionContext(deps=Deps())
        plan = UsecasePlan().before_pipeline("get", [g(1), g(2), g(3)], first_priority=30)
        uc = plan.resolve("get", ctx, lambda ctx: StubUsecase(ctx=ctx))
        await uc("x")
        assert seen == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_resolve_after_pipeline_runs_effects_in_list_order(self, stub_ctx) -> None:
        seen: list[int] = []

        def e(i: int):
            def factory(ctx: ExecutionContext):
                async def effect(args, res):
                    seen.append(i)
                    return res

                return effect

            return factory

        plan = (
            UsecasePlan()
            .tx("x", route="mock")
            .after_pipeline("x", [e(1), e(2), e(3)], first_priority=20)
        )
        uc = plan.resolve("x", stub_ctx, lambda ctx: StubUsecase(ctx=ctx))
        await uc("a")
        assert seen == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_resolve_in_tx_after_pipeline_runs_effects_in_list_order(
        self, stub_ctx
    ) -> None:
        seen: list[int] = []

        def e(i: int):
            def factory(ctx: ExecutionContext):
                async def effect(args, res):
                    seen.append(i)
                    return res

                return effect

            return factory

        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .in_tx_after_pipeline("create", [e(1), e(2), e(3)], first_priority=0)
        )
        uc = plan.resolve("create", stub_ctx, lambda ctx: StubUsecase(ctx=ctx))
        await uc("a")
        assert seen == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_resolve_in_tx_wrap_pipeline_first_in_list_is_innermost(self, stub_ctx) -> None:
        from forze.application.execution.middleware import GuardMiddleware

        seen: list[str] = []

        def w(label: str):
            def factory(ctx: ExecutionContext):
                async def guard(args):
                    seen.append(label)

                return GuardMiddleware(guard=guard)

            return factory

        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .in_tx_wrap_pipeline("create", [w("inner"), w("outer")], first_priority=20)
        )
        uc = plan.resolve("create", stub_ctx, lambda ctx: StubUsecase(ctx=ctx))
        await uc("a")
        assert seen == ["outer", "inner"]


class TestUsecasePlanListOp:
    """``op`` as ``list[OpKey]`` applies the same spec to every listed operation."""

    def test_tx_with_list(self) -> None:
        plan = UsecasePlan().tx(["get", "list", "search"], route="mock")
        for k in ("get", "list", "search"):
            assert plan.ops[k].tx is not None
            assert plan.ops[k].tx.route == "mock"

    def test_no_tx_with_list(self) -> None:
        plan = (
            UsecasePlan()
            .tx(["a", "b"], route="mock")
            .no_tx(["a", "b"])
        )
        for k in ("a", "b"):
            assert plan.ops[k].tx is None

    def test_before_with_list_same_middleware(self) -> None:
        def guard(ctx: ExecutionContext):
            async def _guard(args):
                pass

            return _guard

        plan = UsecasePlan().before(["get", "list"], guard, priority=3)
        for k in ("get", "list"):
            assert len(plan.ops[k].outer_before) == 1
            assert plan.ops[k].outer_before[0].priority == 3

    def test_after_with_list(self) -> None:
        e0, _e1, _e2 = _effects3()
        plan = UsecasePlan().after(["x", "y"], e0, priority=4)
        for k in ("x", "y"):
            assert len(plan.ops[k].outer_after) == 1
            assert plan.ops[k].outer_after[0].priority == 4

    def test_wrap_with_list(self) -> None:
        from forze.application.execution.middleware import GuardMiddleware

        def mw(ctx):
            async def guard(args):
                pass

            return GuardMiddleware(guard=guard)

        plan = UsecasePlan().wrap(["p", "q"], mw, priority=2)
        for k in ("p", "q"):
            assert len(plan.ops[k].outer_wrap) == 1

    def test_before_pipeline_with_list(self) -> None:
        g0, g1, _ = _guards3()
        plan = UsecasePlan().before_pipeline(
            ["get", "list"],
            [g0, g1],
            first_priority=50,
        )
        for k in ("get", "list"):
            assert [s.priority for s in plan.ops[k].outer_before] == [50, 40]

    def test_in_tx_variants_with_list(self) -> None:
        g0, _g1, _ = _guards3()
        e0, _e1, _ = _effects3()
        from forze.application.execution.middleware import GuardMiddleware

        def wrap_mw(ctx):
            async def guard(args):
                pass

            return GuardMiddleware(guard=guard)

        plan = (
            UsecasePlan()
            .tx(["create", "update"], route="r")
            .in_tx_before(["create", "update"], g0, priority=1)
            .in_tx_after(["create", "update"], e0, priority=1)
            .in_tx_wrap(["create", "update"], wrap_mw, priority=1)
        )
        for k in ("create", "update"):
            assert len(plan.ops[k].in_tx_before) == 1
            assert len(plan.ops[k].in_tx_after) == 1
            assert len(plan.ops[k].in_tx_wrap) == 1

    def test_in_tx_pipelines_with_list(self) -> None:
        g0, g1, _ = _guards3()
        e0, e1, _ = _effects3()
        from forze.application.execution.middleware import GuardMiddleware

        def mw0(ctx):
            async def guard(args):
                pass

            return GuardMiddleware(guard=guard)

        def mw1(ctx):
            async def guard(args):
                pass

            return GuardMiddleware(guard=guard)

        plan = (
            UsecasePlan()
            .tx(["create", "update"], route="m")
            .in_tx_before_pipeline(["create", "update"], [g0, g1], first_priority=10)
            .in_tx_after_pipeline(["create", "update"], [e0, e1], first_priority=3)
            .in_tx_wrap_pipeline(["create", "update"], [mw0, mw1], first_priority=0)
        )
        for k in ("create", "update"):
            assert [s.priority for s in plan.ops[k].in_tx_before] == [10, 0]
            assert [s.priority for s in plan.ops[k].in_tx_after] == [3, -7]
            assert [s.priority for s in plan.ops[k].in_tx_wrap] == [0, -10]

    def test_after_commit_with_list(self) -> None:
        e0, e1, _ = _effects3()
        plan = (
            UsecasePlan()
            .tx(["a", "b"], route="z")
            .after_commit(["a", "b"], e0, priority=5)
            .after_commit_pipeline(["a", "b"], [e0, e1], first_priority=1)
        )
        for k in ("a", "b"):
            assert len(plan.ops[k].after_commit) == 3
            assert [s.priority for s in plan.ops[k].after_commit] == [5, 1, -9]

    def test_outer_pipeline_with_list(self) -> None:
        g0, _g1, _ = _guards3()
        e0, _e1, _ = _effects3()
        from forze.application.execution.middleware import GuardMiddleware

        def w0(ctx):
            async def guard(args):
                pass

            return GuardMiddleware(guard=guard)

        plan = UsecasePlan().outer_pipeline(
            ["a", "b"],
            before=[g0],
            after=[e0],
            wrap=[w0],
            first_priority=6,
        )
        for k in ("a", "b"):
            assert [s.priority for s in plan.ops[k].outer_before] == [6]
            assert [s.priority for s in plan.ops[k].outer_after] == [6]
            assert [s.priority for s in plan.ops[k].outer_wrap] == [6]

    def test_in_tx_pipeline_with_list(self) -> None:
        """``in_tx_pipeline`` with ``op`` list wires in-tx buckets for each key."""

        g0, _g1, _ = _guards3()
        e0, _e1, _ = _effects3()
        from forze.application.execution.middleware import GuardMiddleware

        def w0(ctx):
            async def guard(args):
                pass

            return GuardMiddleware(guard=guard)

        plan = (
            UsecasePlan()
            .tx(["a", "b"], route="m")
            .in_tx_pipeline(
                ["a", "b"],
                before=[g0],
                after=[e0],
                wrap=[w0],
                first_priority=2,
            )
        )
        for k in ("a", "b"):
            assert plan.ops[k].tx is not None
            assert [s.priority for s in plan.ops[k].in_tx_before] == [2]
            assert [s.priority for s in plan.ops[k].in_tx_after] == [2]
            assert [s.priority for s in plan.ops[k].in_tx_wrap] == [2]
            assert plan.ops[k].outer_before == ()
            assert plan.ops[k].outer_after == ()
            assert plan.ops[k].outer_wrap == ()

    def test_in_tx_pipeline_list_multiple_ops_stepped_priorities(self) -> None:
        g0, g1, _ = _guards3()
        e0, e1, _ = _effects3()
        from forze.application.execution.middleware import GuardMiddleware

        def mw0(ctx):
            async def guard(args):
                pass

            return GuardMiddleware(guard=guard)

        def mw1(ctx):
            async def guard(args):
                pass

            return GuardMiddleware(guard=guard)

        keys = ("create", "update", "delete")
        plan = (
            UsecasePlan()
            .tx(list(keys), route="db")
            .in_tx_pipeline(
                list(keys),
                before=[g0, g1],
                after=[e0, e1],
                wrap=[mw0, mw1],
                first_priority=20,
            )
        )
        for k in keys:
            assert [s.priority for s in plan.ops[k].in_tx_before] == [20, 10]
            assert [s.priority for s in plan.ops[k].in_tx_after] == [20, 10]
            assert [s.priority for s in plan.ops[k].in_tx_wrap] == [20, 10]
            assert plan.ops[k].outer_before == ()
            assert plan.ops[k].outer_after == ()
            assert plan.ops[k].outer_wrap == ()

    def test_in_tx_pipeline_list_partial_sections(self) -> None:
        """Only pass ``before`` / ``after`` / ``wrap`` sections that are needed."""

        g0, _g1, _ = _guards3()
        e0, e1, _ = _effects3()

        plan = (
            UsecasePlan()
            .tx(["p", "q"], route="r")
            .in_tx_pipeline(["p", "q"], before=[g0], first_priority=5)
        )
        for k in ("p", "q"):
            assert [s.priority for s in plan.ops[k].in_tx_before] == [5]
            assert plan.ops[k].in_tx_after == ()
            assert plan.ops[k].in_tx_wrap == ()
            assert plan.ops[k].in_tx_finally == ()
            assert plan.ops[k].in_tx_on_failure == ()

        plan2 = (
            UsecasePlan()
            .tx(["u", "v"], route="r")
            .in_tx_pipeline(["u", "v"], after=[e0, e1], first_priority=0)
        )
        for k in ("u", "v"):
            assert [s.priority for s in plan2.ops[k].in_tx_after] == [0, -10]
            assert plan2.ops[k].in_tx_before == ()
            assert plan2.ops[k].in_tx_wrap == ()
            assert plan2.ops[k].in_tx_finally == ()
            assert plan2.ops[k].in_tx_on_failure == ()

    @pytest.mark.asyncio
    async def test_resolve_in_tx_pipeline_list_runs_in_tx_guards(self, stub_ctx) -> None:
        seen: list[str] = []

        def in_tx_guard(ctx):
            async def guard(args):
                seen.append("in_tx_before")

            return guard

        plan = (
            UsecasePlan()
            .tx(["op_a", "op_b"], route="mock")
            .in_tx_pipeline(
                ["op_a", "op_b"],
                before=[in_tx_guard],
                first_priority=1,
            )
        )
        for op in ("op_a", "op_b"):
            uc = plan.resolve(op, stub_ctx, lambda ctx: StubUsecase(ctx=ctx))
            result = await uc("x")
            assert result == "ok:x"
        assert seen == ["in_tx_before", "in_tx_before"]

    @pytest.mark.asyncio
    async def test_resolve_before_list_runs_guard_per_op(self) -> None:
        seen: list[str] = []

        def guard(ctx: ExecutionContext):
            async def _guard(args):
                seen.append("g")

            return _guard

        ctx = ExecutionContext(deps=Deps())
        plan = UsecasePlan().before(["get", "list"], guard, priority=1)
        await plan.resolve("get", ctx, lambda ctx: StubUsecase(ctx=ctx))("x")
        await plan.resolve("list", ctx, lambda ctx: StubUsecase(ctx=ctx))("y")
        assert seen == ["g", "g"]

    def test_list_mixed_str_and_str_enum(self) -> None:
        class Route(StrEnum):
            MOCK = "mock"

        def guard(ctx: ExecutionContext):
            async def _guard(args):
                pass

            return _guard

        plan = UsecasePlan().before([Route.MOCK, "other"], guard, priority=0)
        assert "mock" in plan.ops
        assert "other" in plan.ops
        assert len(plan.ops["mock"].outer_before) == 1
        assert len(plan.ops["other"].outer_before) == 1


class TestUsecasePlanFinallyOnFailure:
    """Integration tests for finally and on_failure plan buckets."""

    @pytest.mark.asyncio
    async def test_outer_on_failure_then_outer_finally_on_error(self, stub_ctx) -> None:
        from forze.application.execution.middleware import Failed

        seen: list[str] = []

        def on_fail(ctx: ExecutionContext):
            async def h(args: str, exc: Exception) -> None:
                seen.append("on_failure")

            return h

        def fin(ctx: ExecutionContext):
            async def h(args: str, outcome) -> None:
                assert isinstance(outcome, Failed)
                seen.append("finally")

            return h

        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .outer_finally("create", fin, priority=1)
            .outer_on_failure("create", on_fail, priority=1)
        )
        uc = plan.resolve("create", stub_ctx, lambda ctx: BoomUsecase(ctx=ctx))
        with pytest.raises(ValueError, match="boom"):
            await uc("x")
        assert seen == ["on_failure", "finally"]

    @pytest.mark.asyncio
    async def test_outer_finally_on_success_only(self, stub_ctx) -> None:
        from forze.application.execution.middleware import Successful

        seen: list[str] = []

        def fin(ctx: ExecutionContext):
            async def h(args: str, outcome) -> None:
                assert isinstance(outcome, Successful)
                seen.append("ok")

            return h

        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .outer_finally("create", fin, priority=0)
        )
        uc = plan.resolve("create", stub_ctx, lambda ctx: StubUsecase(ctx=ctx))
        await uc("x")
        assert seen == ["ok"]

    @pytest.mark.asyncio
    async def test_after_commit_skipped_when_main_raises(self, stub_ctx) -> None:
        seen: list[str] = []

        def ac(ctx: ExecutionContext):
            async def effect(args: str, res: str) -> str:
                seen.append("ac")
                return res

            return effect

        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .after_commit("create", ac, priority=1)
        )
        uc = plan.resolve("create", stub_ctx, lambda ctx: BoomUsecase(ctx=ctx))
        with pytest.raises(ValueError):
            await uc("x")
        assert seen == []

    @pytest.mark.asyncio
    async def test_in_tx_on_failure_before_outer_on_failure(self, stub_ctx) -> None:
        seen: list[str] = []

        def inner(ctx: ExecutionContext):
            async def h(args: str, exc: Exception) -> None:
                seen.append("in_tx")

            return h

        def outer(ctx: ExecutionContext):
            async def h(args: str, exc: Exception) -> None:
                seen.append("outer")

            return h

        plan = (
            UsecasePlan()
            .tx("create", route="mock")
            .outer_on_failure("create", outer, priority=1)
            .in_tx_on_failure("create", inner, priority=1)
        )
        uc = plan.resolve("create", stub_ctx, lambda ctx: BoomUsecase(ctx=ctx))
        with pytest.raises(ValueError):
            await uc("x")
        assert seen == ["in_tx", "outer"]

    @pytest.mark.asyncio
    async def test_outer_hooks_without_tx(self, stub_ctx) -> None:
        from forze.application.execution.middleware import Failed, Successful

        seen: list[str] = []

        def on_fail(ctx: ExecutionContext):
            async def h(args: str, exc: Exception) -> None:
                seen.append("fail")

            return h

        def fin(ctx: ExecutionContext):
            async def h(args: str, outcome) -> None:
                if isinstance(outcome, Successful):
                    seen.append("fin_ok")
                else:
                    assert isinstance(outcome, Failed)
                    seen.append("fin_err")

            return h

        plan = (
            UsecasePlan()
            .outer_finally("solo", fin, priority=0)
            .outer_on_failure("solo", on_fail, priority=0)
        )
        uc = plan.resolve("solo", stub_ctx, lambda ctx: BoomUsecase(ctx=ctx))
        with pytest.raises(ValueError):
            await uc("z")
        assert seen == ["fail", "fin_err"]

        seen.clear()
        uc2 = plan.resolve("solo", stub_ctx, lambda ctx: StubUsecase(ctx=ctx))
        await uc2("z")
        assert seen == ["fin_ok"]
