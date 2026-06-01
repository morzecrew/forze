"""Tests for :mod:`forze.application.execution.operations.registry.registries` mutations."""

import pytest

from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import exc
from forze.base.primitives import StrKeyNamespace


class TestOperationRegistryMutations:
    def test_set_handler_conflict_without_override(self) -> None:
        reg = OperationRegistry(handlers={"op": lambda _ctx: None})

        with pytest.raises(exc, match="already set"):
            reg.set_handler("op", lambda _ctx: None)

    def test_set_handler_with_namespace(self) -> None:
        ns = StrKeyNamespace(prefix="docs")
        reg = OperationRegistry().set_handler(
            "get",
            lambda _ctx: None,
            namespace=ns,
        )

        assert ns.key("get") in reg._handlers

    def test_set_handlers_batch_with_namespace(self) -> None:
        ns = StrKeyNamespace(prefix="docs")
        reg = OperationRegistry().set_handlers(
            {"a": lambda _ctx: None, "b": lambda _ctx: None},
            namespace=ns,
        )

        assert ns.key("a") in reg._handlers
        assert ns.key("b") in reg._handlers

    def test_bind_requires_operations(self) -> None:
        with pytest.raises(exc, match="No operations"):
            OperationRegistry().bind()

    def test_bind_with_namespace(self) -> None:
        ns = StrKeyNamespace(prefix="docs")
        binder = OperationRegistry(handlers={ns.key("op"): lambda _ctx: None}).bind(
            "op",
            namespace=ns,
        )
        assert binder is not None

    def test_commit_patch_merges_existing_selector(self) -> None:
        reg = OperationRegistry().commit_patch("op", OperationPlan())
        merged = reg.commit_patch("op", OperationPlan())

        assert len(merged._patches) == 1

    def test_extend_plan_with_namespace(self) -> None:
        ns = StrKeyNamespace(prefix="docs")
        plan = OperationPlan()
        reg = OperationRegistry().extend_plan("get", plan, namespace=ns)

        assert ns.key("get") in reg.get_plans()

    def test_extend_plans_with_namespace(self) -> None:
        ns = StrKeyNamespace(prefix="docs")
        reg = OperationRegistry().extend_plans(
            {"get": OperationPlan()},
            namespace=ns,
        )

        assert ns.key("get") in reg.get_plans()
