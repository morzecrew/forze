"""Unit tests for forze.application.execution.plan."""

from forze.application.execution import ExecutionContext
from forze.application.execution.plan import OperationPlan, UsecasePlan

# ----------------------- #


class TestUsecasePlan:
    """Tests for UsecasePlan."""

    def test_override_sets_factory(self) -> None:
        def factory(ctx: ExecutionContext) -> None:
            return None

        plan = UsecasePlan().override("get", factory)
        assert plan.ops["get"].override is factory

    def test_before_adds_guard(self) -> None:
        def guard(ctx: ExecutionContext) -> None:
            return None

        plan = UsecasePlan().before("get", guard, priority=1)
        assert len(plan.ops["get"].guards) == 1
        assert plan.ops["get"].guards[0].priority == 1

    def test_after_adds_effect(self) -> None:
        def effect(ctx: ExecutionContext) -> None:
            return None

        plan = UsecasePlan().after("get", effect, priority=2)
        assert len(plan.ops["get"].effects) == 1
        assert plan.ops["get"].effects[0].priority == 2

    def test_merge_combines_plans(self) -> None:
        def factory_a(ctx: ExecutionContext) -> None:
            return None

        def factory_b(ctx: ExecutionContext) -> None:
            return None

        plan_a = UsecasePlan().override("get", factory_a)
        plan_b = UsecasePlan().override("create", factory_b)
        merged = UsecasePlan.merge(plan_a, plan_b)
        assert merged.ops["get"].override is factory_a
        assert merged.ops["create"].override is factory_b


class TestOperationPlan:
    """Tests for OperationPlan."""

    def test_default_operation_plan_has_no_override(self) -> None:
        op = OperationPlan()
        assert op.override is None
        assert op.guards == ()
        assert op.effects == ()
