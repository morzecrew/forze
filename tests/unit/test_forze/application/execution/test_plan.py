"""Unit tests for forze.application.execution.plan."""

from forze.application.execution import ExecutionContext
from forze.application.execution.plan import OperationPlan, UsecasePlan

# ----------------------- #


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


class TestOperationPlan:
    """Tests for OperationPlan."""

    def test_default_operation_plan_has_empty_buckets(self) -> None:
        op = OperationPlan()
        assert op.outer_before == ()
        assert op.outer_after == ()
