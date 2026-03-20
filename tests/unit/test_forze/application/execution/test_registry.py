"""Unit tests for forze.application.execution.registry."""

import pytest

from forze.application.execution import Deps, ExecutionContext, Usecase, UsecaseRegistry

# ----------------------- #


def _stub_factory(ctx: ExecutionContext) -> Usecase[str, str]:
    return StubUsecase(ctx=ctx)


class StubUsecase(Usecase[str, str]):
    """Minimal usecase for registry tests."""

    async def main(self, args: str) -> str:
        return f"ok:{args}"


class TestUsecaseRegistry:
    """Tests for UsecaseRegistry."""

    def test_register_returns_new_instance(self) -> None:
        reg = UsecaseRegistry()
        new = reg.register("get", lambda ctx: StubUsecase(ctx=ctx))
        assert new is not reg
        assert new.exists("get")
        assert not reg.exists("get")

    def test_register_inplace_mutates(self) -> None:
        reg = UsecaseRegistry()
        reg.register("get", lambda ctx: StubUsecase(ctx=ctx), inplace=True)
        assert reg.exists("get")

    def test_register_duplicate_raises(self) -> None:
        from forze.base.errors import CoreError

        reg = UsecaseRegistry().register("get", _stub_factory)
        with pytest.raises(CoreError, match="already registered"):
            reg.register("get", _stub_factory)

    def test_register_inplace_returns_none(self) -> None:
        reg = UsecaseRegistry()
        result = reg.register("get", _stub_factory, inplace=True)
        assert result is None
        assert reg.exists("get")

    def test_override_replaces_factory(self) -> None:
        reg = UsecaseRegistry().register("get", lambda ctx: StubUsecase(ctx=ctx))
        new = reg.override("get", lambda ctx: StubUsecase(ctx=ctx))
        assert new is not reg
        assert new.exists("get")

    def test_override_unregistered_raises(self) -> None:
        from forze.base.errors import CoreError

        reg = UsecaseRegistry()
        with pytest.raises(CoreError, match="not registered"):
            reg.override("get", _stub_factory)

    def test_override_inplace_mutates(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory)
        result = reg.override("get", _stub_factory, inplace=True)
        assert result is None
        assert reg.exists("get")

    def test_register_many_adds_multiple(self) -> None:
        reg = UsecaseRegistry()
        new = reg.register_many({"get": _stub_factory, "create": _stub_factory})
        assert new.exists("get")
        assert new.exists("create")

    def test_register_many_duplicate_raises(self) -> None:
        from forze.base.errors import CoreError

        reg = UsecaseRegistry().register("get", _stub_factory)
        with pytest.raises(CoreError, match="already registered"):
            reg.register_many({"get": _stub_factory, "create": _stub_factory})

    def test_register_many_inplace_mutates(self) -> None:
        reg = UsecaseRegistry()
        result = reg.register_many(
            {"get": _stub_factory, "create": _stub_factory},
            inplace=True,
        )
        assert result is None
        assert reg.exists("get")
        assert reg.exists("create")

    def test_override_many_replaces_multiple(self) -> None:
        reg = (
            UsecaseRegistry()
            .register("get", _stub_factory)
            .register("create", _stub_factory)
        )
        new = reg.override_many({"get": _stub_factory, "create": _stub_factory})
        assert new.exists("get")
        assert new.exists("create")

    def test_override_many_unregistered_raises(self) -> None:
        from forze.base.errors import CoreError

        reg = UsecaseRegistry().register("get", _stub_factory)
        with pytest.raises(CoreError, match="not registered"):
            reg.override_many({"get": _stub_factory, "create": _stub_factory})

    def test_override_many_inplace_mutates(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory)
        result = reg.override_many({"get": _stub_factory}, inplace=True)
        assert result is None
        assert reg.exists("get")

    def test_exists_returns_true_for_registered(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory)
        assert reg.exists("get")

    def test_exists_returns_false_for_unregistered(self) -> None:
        reg = UsecaseRegistry()
        assert not reg.exists("get")

    def test_resolve_returns_usecase(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory)
        reg.finalize("test")
        ctx = ExecutionContext(deps=Deps())
        uc = reg.resolve("get", ctx)
        assert isinstance(uc, StubUsecase)

    def test_resolve_unregistered_raises(self) -> None:
        from forze.base.errors import CoreError

        reg = UsecaseRegistry()
        reg.finalize("test")
        ctx = ExecutionContext(deps=Deps())
        with pytest.raises(CoreError, match="not registered for operation"):
            reg.resolve("get", ctx)

    def test_extend_plan_returns_new_instance(self) -> None:
        from forze.application.execution.plan import UsecasePlan

        reg = UsecaseRegistry().register("get", _stub_factory)

        async def noop(args):
            pass

        def guard_factory(ctx):
            return noop

        plan = UsecasePlan().before("get", guard_factory, priority=1)
        new = reg.extend_plan(plan)
        assert new is not reg
        assert new.exists("get")

    def test_extend_plan_inplace_mutates(self) -> None:
        from forze.application.execution.plan import UsecasePlan

        reg = UsecaseRegistry().register("get", _stub_factory)

        async def noop(args):
            pass

        def guard_factory(ctx):
            return noop

        plan = UsecasePlan().before("get", guard_factory, priority=1)
        result = reg.extend_plan(plan, inplace=True)
        assert result is None
        assert reg.exists("get")


class TestUsecaseRegistryMerge:
    """Tests for UsecaseRegistry.merge."""

    def test_merge_empty_returns_empty_registry(self) -> None:
        merged = UsecaseRegistry.merge()
        assert merged.defaults == {}
        assert not merged.exists("get")

    def test_merge_single_registry_returns_copy(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory)
        merged = UsecaseRegistry.merge(reg)
        assert merged is not reg
        assert merged.exists("get")
        assert merged.defaults["get"] is reg.defaults["get"]

    def test_merge_multiple_no_conflicts(self) -> None:
        reg_a = UsecaseRegistry().register("get", _stub_factory)
        reg_b = UsecaseRegistry().register("create", _stub_factory)
        merged = UsecaseRegistry.merge(reg_a, reg_b)
        assert merged.exists("get")
        assert merged.exists("create")
        assert merged.defaults["get"] is reg_a.defaults["get"]
        assert merged.defaults["create"] is reg_b.defaults["create"]

    def test_merge_multiple_from_instance_no_conflicts(self) -> None:
        reg_a = UsecaseRegistry().register("get", _stub_factory)
        reg_b = UsecaseRegistry().register("create", _stub_factory)
        merged = reg_a.merge(reg_b)
        assert merged.exists("get")
        assert merged.exists("create")
        assert merged.defaults["get"] is reg_a.defaults["get"]
        assert merged.defaults["create"] is reg_b.defaults["create"]

    def test_merge_conflict_raises_when_error(self) -> None:
        from forze.base.errors import CoreError

        reg_a = UsecaseRegistry().register("get", _stub_factory)
        reg_b = UsecaseRegistry().register("get", lambda ctx: StubUsecase(ctx=ctx))
        with pytest.raises(CoreError, match="already registered for operation"):
            UsecaseRegistry.merge(reg_a, reg_b, on_conflict="error")

    def test_merge_conflict_overwrites_when_overwrite(self) -> None:
        def other_factory(ctx):
            return StubUsecase(ctx=ctx)

        reg_a = UsecaseRegistry().register("get", _stub_factory)
        reg_b = UsecaseRegistry().register("get", other_factory)
        merged = UsecaseRegistry.merge(reg_a, reg_b, on_conflict="overwrite")
        assert merged.exists("get")
        assert merged.defaults["get"] is other_factory

    def test_merge_plans_combined(self) -> None:
        from forze.application.execution.plan import UsecasePlan

        async def noop(args):
            pass

        def guard_a(ctx):
            return noop

        def guard_b(ctx):
            return noop

        plan_a = UsecasePlan().before("get", guard_a, priority=1)
        plan_b = UsecasePlan().before("create", guard_b, priority=2)

        reg_a = UsecaseRegistry().register("get", _stub_factory)
        reg_a.extend_plan(plan_a, inplace=True)
        reg_b = UsecaseRegistry().register("create", _stub_factory)
        reg_b.extend_plan(plan_b, inplace=True)

        merged = UsecaseRegistry.merge(reg_a, reg_b)
        merged.finalize("merged")
        assert merged.exists("get")
        assert merged.exists("create")
        ctx = ExecutionContext(deps=Deps())
        uc_get = merged.resolve("get", ctx)
        uc_create = merged.resolve("create", ctx)
        assert isinstance(uc_get, StubUsecase)
        assert isinstance(uc_create, StubUsecase)

    def test_merge_from_instance_equals_class_merge(self) -> None:
        """reg_a.merge(reg_b) equals UsecaseRegistry.merge(reg_a, reg_b)."""
        reg_a = UsecaseRegistry().register("get", _stub_factory)
        reg_b = UsecaseRegistry().register("create", _stub_factory)
        via_class = UsecaseRegistry.merge(reg_a, reg_b)
        via_instance = reg_a.merge(reg_b)
        assert via_class.exists("get") and via_instance.exists("get")
        assert via_class.exists("create") and via_instance.exists("create")

    def test_merge_from_instance_with_on_conflict_overwrite(self) -> None:
        """Instance merge supports on_conflict parameter."""

        def other_factory(ctx):
            return StubUsecase(ctx=ctx)

        reg_a = UsecaseRegistry().register("get", _stub_factory)
        reg_b = UsecaseRegistry().register("get", other_factory)
        merged = reg_a.merge(reg_b, on_conflict="overwrite")
        assert merged.exists("get")
        assert merged.defaults["get"] is other_factory

    def test_merge_from_instance_empty_returns_self(self) -> None:
        """reg.merge() with no args returns registry containing only self."""
        reg = UsecaseRegistry().register("get", _stub_factory)
        merged = reg.merge()
        assert merged is not reg
        assert merged.exists("get")
