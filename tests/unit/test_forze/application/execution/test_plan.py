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


class TestOperationPlan:
    """Tests for OperationPlan."""

    def test_default_operation_plan_has_empty_buckets(self) -> None:
        op = OperationPlan()
        assert op.outer_before == ()
        assert op.outer_after == ()
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
