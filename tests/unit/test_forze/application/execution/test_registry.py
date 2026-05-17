"""Unit tests for :mod:`forze.application.execution.registry`."""

from enum import StrEnum

import attrs
import pytest

from forze.application.execution import (
    Deps,
    ExecutionContext,
    OperationRef,
    Usecase,
    UsecaseRegistry,
)
from forze.application.execution.engine import Stage
from forze.base.errors import CoreError


def _stub_factory(ctx: ExecutionContext) -> Usecase[str, str]:
    return StubUsecase(ctx=ctx)


class StubUsecase(Usecase[str, str]):
    """Minimal usecase for registry tests."""

    async def main(self, args: str) -> str:
        return f"ok:{args}"


class TestUsecaseRegistry:
    def test_is_attrs_class_with_expected_init(self) -> None:
        assert attrs.has(UsecaseRegistry)
        names = {f.name for f in attrs.fields(UsecaseRegistry)}
        assert "_init_factories" in names
        assert "_namespace" in names
        assert "_factories" in names

    def test_construct_with_factories_kw_and_namespace(self) -> None:
        reg = UsecaseRegistry(
            factories={"get": _stub_factory},
            namespace="svc",
        )
        assert reg.exists("svc.get")
        assert "svc.get" in reg.factories

    def test_construct_with_positional_factories(self) -> None:
        reg = UsecaseRegistry({"get": _stub_factory})
        assert reg.exists("get")

    def test_register_mutates_and_returns_self(self) -> None:
        reg = UsecaseRegistry()
        returned = reg.register("get", _stub_factory)
        assert returned is reg
        assert reg.exists("get")

    def test_register_duplicate_raises(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory)
        with pytest.raises(CoreError, match="already registered"):
            reg.register("get", _stub_factory)

    def test_override_mutates_and_returns_self(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory)
        returned = reg.override("get", lambda ctx: StubUsecase(ctx=ctx))
        assert returned is reg
        assert reg.exists("get")

    def test_override_unregistered_raises(self) -> None:
        with pytest.raises(CoreError, match="not registered"):
            UsecaseRegistry().override("get", _stub_factory)

    def test_register_many_mutates_and_returns_self(self) -> None:
        reg = UsecaseRegistry()
        returned = reg.register_many({"get": _stub_factory, "create": _stub_factory})
        assert returned is reg
        assert reg.exists("get")
        assert reg.exists("create")

    def test_register_many_duplicate_raises(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory)
        with pytest.raises(CoreError, match="already registered"):
            reg.register_many({"get": _stub_factory, "create": _stub_factory})

    def test_override_many_mutates_and_returns_self(self) -> None:
        reg = (
            UsecaseRegistry()
            .register("get", _stub_factory)
            .register("create", _stub_factory)
        )
        returned = reg.override_many({"get": _stub_factory, "create": _stub_factory})
        assert returned is reg
        assert reg.exists("get")
        assert reg.exists("create")

    def test_override_many_unregistered_raises(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory)
        with pytest.raises(CoreError, match="not registered"):
            reg.override_many({"get": _stub_factory, "create": _stub_factory})

    def test_add_dispatch_edge_mutates_and_returns_self(self) -> None:
        reg = UsecaseRegistry().register("a", _stub_factory)
        returned = reg.add_dispatch_edge("a", "b")
        assert returned is reg
        assert ("a", "b") in reg._dispatch_graph.edges()

    def test_stage_authoring_mutates_and_returns_self(self) -> None:
        async def noop(_args: str) -> None:
            return None

        def guard_factory(_ctx: ExecutionContext):
            return noop

        reg = UsecaseRegistry().register("get", _stub_factory)
        returned = reg.before("get", guard_factory, priority=1)
        assert returned is reg
        assert len(reg._stages["get"].specs(Stage.before)) == 1

    def test_finalize_without_prefix_uses_logical_operation_ids(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory).finalize()
        assert reg.operation_id_for("get") == "get"

    def test_finalize_rejects_whitespace_only_prefix(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory)
        with pytest.raises(CoreError, match="operation_id_prefix must be non-empty"):
            reg.finalize("   ")

    def test_operation_id_for_before_finalize_raises(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory)
        with pytest.raises(CoreError, match="Registry is not finalized"):
            reg.operation_id_for("get")

    def test_finalize_mutates_and_returns_self(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory)
        returned = reg.finalize("test")
        assert returned is reg
        assert reg.operation_id_for("get") == "test.get"

    def test_finalize_freezes_dispatch_graph(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory).finalize("test")
        assert reg._dispatch_graph.is_frozen
        with pytest.raises(CoreError, match="Registry is finalized"):
            reg.add_dispatch_edge("get", "x")

    def test_resolve_returns_usecase(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory).finalize("test")
        ctx = ExecutionContext(deps=Deps())
        uc = reg.resolve("get", ctx)
        assert isinstance(uc, StubUsecase)

    def test_resolve_unregistered_raises(self) -> None:
        reg = UsecaseRegistry().finalize("test")
        ctx = ExecutionContext(deps=Deps())
        with pytest.raises(CoreError, match="not registered for operation"):
            reg.resolve("get", ctx)

    def test_finalize_and_operation_id_for_accept_str_enum_prefix(self) -> None:
        class RegistryId(StrEnum):
            NS = "my-doc"

        reg = UsecaseRegistry().register("get", _stub_factory).finalize(RegistryId.NS)
        assert reg.operation_id_for("get") == "my-doc.get"

    def test_namespace_qualifies_bare_suffixes(self) -> None:
        reg = UsecaseRegistry(namespace="test")
        reg.register("get", _stub_factory)

        assert reg.exists("get")
        assert reg.exists("test.get")
        assert "test.get" in reg.factories

    def test_namespace_preserves_full_keys(self) -> None:
        reg = UsecaseRegistry(namespace="test")
        reg.register("other.get", _stub_factory)

        assert reg.exists("other.get")
        assert not reg.exists("test.other.get")

    def test_ref_uses_registry_namespace_for_relative_keys(self) -> None:
        reg = UsecaseRegistry(namespace="test")
        assert reg.ref("get").op == "test.get"

    def test_operation_ref_full_key_resolves_without_namespace(self) -> None:
        ref = OperationRef.absolute("test.get")
        assert ref.op == "test.get"

    def test_ref_without_namespace_requires_full_key(self) -> None:
        reg = UsecaseRegistry()

        with pytest.raises(
            CoreError,
            match="Registry.ref requires a full operation key when registry.namespace is None",
        ):
            reg.ref("get")


class TestUsecaseRegistryMerge:
    def test_merge_empty_returns_empty_registry(self) -> None:
        merged = UsecaseRegistry.merge()
        assert merged.factories == {}
        assert not merged.exists("get")

    def test_merge_single_registry_returns_copy(self) -> None:
        reg = UsecaseRegistry().register("get", _stub_factory)
        merged = UsecaseRegistry.merge(reg)
        assert merged is not reg
        assert merged.exists("get")
        assert merged.factories["get"] is reg.factories["get"]

    def test_merge_multiple_no_conflicts(self) -> None:
        reg_a = UsecaseRegistry().register("get", _stub_factory)
        reg_b = UsecaseRegistry().register("create", _stub_factory)
        merged = UsecaseRegistry.merge(reg_a, reg_b)
        assert merged.exists("get")
        assert merged.exists("create")
        assert merged.factories["get"] is reg_a.factories["get"]
        assert merged.factories["create"] is reg_b.factories["create"]

    def test_merge_conflict_raises_when_error(self) -> None:
        reg_a = UsecaseRegistry().register("get", _stub_factory)
        reg_b = UsecaseRegistry().register("get", lambda ctx: StubUsecase(ctx=ctx))
        with pytest.raises(CoreError, match="already registered for operation"):
            UsecaseRegistry.merge(reg_a, reg_b, on_conflict="error")

    def test_merge_conflict_overwrites_when_overwrite(self) -> None:
        def other_factory(ctx: ExecutionContext) -> StubUsecase:
            return StubUsecase(ctx=ctx)

        reg_a = UsecaseRegistry().register("get", _stub_factory)
        reg_b = UsecaseRegistry().register("get", other_factory)
        merged = UsecaseRegistry.merge(reg_a, reg_b, on_conflict="overwrite")
        assert merged.exists("get")
        assert merged.factories["get"] is other_factory

    def test_merge_combines_stage_authoring(self) -> None:
        async def noop(_args: str) -> None:
            return None

        def guard_a(_ctx: ExecutionContext):
            return noop

        def guard_b(_ctx: ExecutionContext):
            return noop

        reg_a = (
            UsecaseRegistry()
            .register("get", _stub_factory)
            .before(
                "get",
                guard_a,
                priority=1,
            )
        )
        reg_b = (
            UsecaseRegistry()
            .register("create", _stub_factory)
            .before(
                "create",
                guard_b,
                priority=2,
            )
        )

        merged = UsecaseRegistry.merge(reg_a, reg_b).finalize("merged")
        assert merged.exists("get")
        assert merged.exists("create")

        ctx = ExecutionContext(deps=Deps())
        assert isinstance(merged.resolve("get", ctx), StubUsecase)
        assert isinstance(merged.resolve("create", ctx), StubUsecase)

    def test_merge_unions_dispatch_edges(self) -> None:
        reg_a = (
            UsecaseRegistry()
            .register("get", _stub_factory)
            .add_dispatch_edge("get", "create")
        )
        reg_b = (
            UsecaseRegistry()
            .register("create", _stub_factory)
            .add_dispatch_edge("create", "get")
        )
        merged = UsecaseRegistry.merge(reg_a, reg_b, on_conflict="overwrite")
        assert ("get", "create") in merged._dispatch_graph.edges()
        assert ("create", "get") in merged._dispatch_graph.edges()

    def test_merge_different_namespaces_drops_default_namespace(self) -> None:
        reg_a = UsecaseRegistry(namespace="a").register("get", _stub_factory)
        reg_b = UsecaseRegistry(namespace="b").register("get", _stub_factory)

        merged = UsecaseRegistry.merge(reg_a, reg_b, on_conflict="overwrite")

        assert merged.namespace is None
        assert merged.exists("a.get")
        assert merged.exists("b.get")

    def test_merge_without_default_namespace_requires_full_key_for_ref(self) -> None:
        reg_a = UsecaseRegistry(namespace="a").register("get", _stub_factory)
        reg_b = UsecaseRegistry(namespace="b").register("get", _stub_factory)
        merged = UsecaseRegistry.merge(reg_a, reg_b, on_conflict="overwrite")

        with pytest.raises(
            CoreError,
            match="Registry.ref requires a full operation key when registry.namespace is None",
        ):
            merged.ref("get")

        assert merged.ref("a.get").op == "a.get"


def _guard_mw_factory():
    from forze.application.execution.middleware import GuardMiddleware

    def factory(_ctx: ExecutionContext):
        async def guard(_args: str) -> None:
            return None

        return GuardMiddleware(guard=guard)

    return factory


class TestUsecaseRegistryCapabilityFinalize:
    def test_finalize_validates_capability_graph(self) -> None:
        reg = (
            UsecaseRegistry()
            .register("op", _stub_factory)
            .before(
                "op",
                _guard_mw_factory(),
                priority=1,
                requires=frozenset({"missing"}),
            )
        )
        with pytest.raises(CoreError, match="provides it"):
            reg.finalize("app")

    def test_finalize_duplicate_capability_provider_across_wildcard(self) -> None:
        reg = (
            UsecaseRegistry()
            .register("op", _stub_factory)
            .before(
                "*",
                _guard_mw_factory(),
                priority=1,
                provides=frozenset({"dup"}),
            )
            .before(
                "op",
                _guard_mw_factory(),
                priority=2,
                provides=frozenset({"dup"}),
            )
        )
        with pytest.raises(CoreError, match="more than one step"):
            reg.finalize("app")

    def test_finalize_with_dispatch_success_hook(self) -> None:
        reg = (
            UsecaseRegistry()
            .register("parent", _stub_factory)
            .register("child", _stub_factory)
        )
        reg.add_dispatch_edge("parent", "child")
        reg.after_success(
            "parent",
            reg.dispatch_success_hook(
                "child",
                map_in=lambda x, y: x,
            ),
        ).finalize("app")
